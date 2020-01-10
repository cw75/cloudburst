#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import random
import time

import zmq

from droplet.shared.proto.droplet_pb2 import GenericResponse
from droplet.shared.proto.shared_pb2 import StringSet
from droplet.server.scheduler.policy.base_policy import (
    BaseDropletSchedulerPolicy
)
from droplet.server.scheduler.utils import (
    get_cache_ip_key,
    get_pin_address,
    get_unpin_address
)

sys_random = random.SystemRandom()


class DefaultDropletSchedulerPolicy(BaseDropletSchedulerPolicy):

    def __init__(self, pin_accept_socket, pusher_cache, kvs_client, ip,
                 random_threshold=0.20):
        # This scheduler's IP address.
        self.ip = ip

        # A socket to listen for confirmations of pin operations' successes.
        self.pin_accept_socket = pin_accept_socket

        # A cache for zmq.PUSH sockets.
        self.pusher_cache = pusher_cache

        # This thread's Anna KVS client.
        self.kvs_client = kvs_client

        # A map to track how many requests have been routed to each executor in
        # the most recent timeslice.
        self.running_counts = {}

        # A map to track nodes which have recently reported high load. These
        # nodes will not be sent requests until after a cooling period.
        self.backoff = {}

        # A map to track which caches are currently caching which keys.
        self.key_locations = {}

        # Executors which currently have no functions pinned on them.
        self.unpinned_executors = set()

        # A map from function names to the executor(s) on which they are
        # pinned.
        self.function_locations = {}

        # A map to sequester function location information until all functions
        # in a DAG have accepted their pin operations.
        self.pending_dags = {}

        # The most recently reported statuses of each executor thread.
        self.thread_statuses = {}

        # This quantifies how many requests should be routed stochastically
        # rather than by policy.
        self.random_threshold = random_threshold

    def pick_executor(self, references, function_name=None):
        # Construct a map which maps from IP addresses to the number of
        # relevant arguments they have cached. For the time begin, we will
        # just pick the machine that has the most number of keys cached.
        arg_map = {}

        if function_name:
            executors = set(self.function_locations[function_name])
        else:
            executors = set(self.unpinned_executors)

        if len(executors) == 0:
            return None

        target_ip = sys_random.sample(executors, 1)[0]

        # Remove this IP/tid pair from the system's metadata until it notifies
        # us that it is available again, but only do this for non-DAG requests.
        if not function_name:
            self.unpinned_executors.discard(target_ip)

        return target_ip

    def pin_function(self, dag_name, function_name):
        # If there are no functions left to choose from, then we return None,
        # indicating that we ran out of resources to use.
        if len(self.unpinned_executors) == 0:
            return False

        if dag_name not in self.pending_dags:
            self.pending_dags[dag_name] = []

        # Make a copy of the set of executors, so that we don't modify the
        # system's metadata.
        candidates = set(self.unpinned_executors)

        while True:
            # Pick a random executor from the set of candidates and attempt to
            # pin this function there.
            node, tid = sys_random.sample(candidates, 1)[0]

            sckt = self.pusher_cache.get(get_pin_address(node, tid))
            msg = self.ip + ':' + function_name
            sckt.send_string(msg)

            response = GenericResponse()
            try:
                response.ParseFromString(self.pin_accept_socket.recv())
            except zmq.ZMQError:
                logging.error('Pin operation to %s:%d timed out. Retrying.' %
                              (node, tid))
                continue

            # Do not use this executor either way: If it rejected, it has
            # something else pinned, and if it accepted, it has pinned what we
            # just asked it to pin.
            self.unpinned_executors.discard((node, tid))
            candidates.discard((node, tid))

            if response.success:
                # The pin operation succeeded, so we return the node and thread
                # ID to the caller.
                self.pending_dags[dag_name].append((function_name, (node,
                                                                    tid)))
                return True
            else:
                # The pin operation was rejected, remove node and try again.
                logging.error('Node %s:%d rejected pin for %s. Retrying.'
                              % (node, tid, function_name))

                continue

    def commit_dag(self, dag_name):
        for function_name, location in self.pending_dags[dag_name]:
            if function_name not in self.function_locations:
                self.function_locations[function_name] = set()

            self.function_locations[function_name].add(location)

        del self.pending_dags[dag_name]

    def discard_dag(self, dag, pending=False):
        pinned_locations = []
        if pending:
            if dag.name in self.pending_dags:
                # If the DAG was pending, we can simply look at the sequestered
                # pending metadata.
                pinned_locations = list(self.pending_dags[dag.name])
                del self.pending_dags[dag.name]
        else:
            # If the DAG was not pinned, we construct a set of all the
            # locations where functions were pinned for this DAG.
            for function_name in dag.functions:
                for location in self.function_locations[function_name]:
                    pinned_locations.append((function_name, location))

        # For each location, we fire-and-forget an unpin message.
        for function_name, location in pinned_locations:
            ip, tid = location

            sckt = self.pusher_cache.get(get_unpin_address(ip, tid))
            sckt.send_string(function_name)

    def process_status(self, status):
        key = (status.ip, status.tid)
        logging.info('Received status update from executor %s:%d.' %
                     (key[0], int(key[1])))

        # This means that this node is currently departing, so we remove it
        # from all of our metadata tracking.
        if not status.running:
            if key in self.thread_statuses:
                for fname in self.thread_statuses[key].functions:
                    self.function_locations[fname].discard(key)

                del self.thread_statuses[key]

            self.unpinned_executors.discard(key)
            return

        if len(status.functions) == 0:
            self.unpinned_executors.add(key)

        # Remove all the old function locations, and all the new ones -- there
        # will probably be a large overlap, but this shouldn't be much
        # different than calculating two different set differences anyway.
        if key in self.thread_statuses and self.thread_statuses[key] != status:
            for function_name in self.thread_statuses[key].functions:
                if function_name in self.function_locations:
                    self.function_locations[function_name].discard(key)

        self.thread_statuses[key] = status
        for function_name in status.functions:
            if function_name not in self.function_locations:
                self.function_locations[function_name] = set()

            self.function_locations[function_name].add(key)

        # If the executor thread is overutilized, we add it to the backoff set
        # and ignore it for a period of time.
        if status.utilization > 0.70:
            self.backoff[key] = time.time()

    def update(self):
        # Periodically clean up the running counts map to drop any times older
        # than 5 seconds.
        for executor in self.running_counts:
            new_set = set()
            for ts in self.running_counts[executor]:
                if time.time() - ts < 5:
                    new_set.add(ts)

            self.running_counts[executor] = new_set

        # Clean up any backoff messages that were added more than 5 seconds ago
        # -- this should be enough to drain a queue.
        remove_set = set()
        for executor in self.backoff:
            if time.time() - self.backoff[executor] > 5:
                remove_set.add(executor)

        for executor in remove_set:
            del self.backoff[executor]

        executors = set(map(lambda status: status.ip,
                            self.thread_statuses.values()))

        # Update the sets of keys that are being cached at each IP address.
        self.key_locations.clear()
        for ip in executors:
            key = get_cache_ip_key(ip)

            # This is of type LWWPairLattice, which has a StringSet protobuf
            # packed into it; we want the keys in that StringSet protobuf.
            lattice = self.kvs_client.get(key)[key]
            if lattice is None:
                # We will only get None if this executor is still joining; if
                # so, we just ignore this for now and move on.
                continue

            st = StringSet()
            st.ParseFromString(lattice.reveal())

            for key in st.keys:
                if key not in self.key_locations:
                    self.key_locations[key] = []

                self.key_locations[key].append(ip)

    def update_function_locations(self, new_locations):
        for location in new_locations:
            function_name = location.name
            if function_name not in self.function_locations:
                self.function_locations[function_name] = set()

            key = (location.ip, location.tid)
            self.function_locations[function_name].add(key)
