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

import uuid
import zmq

from anna.lattices import (
    MapLattice,
    MultiKeyCausalLattice,
    SetLattice,
    VectorClock,
    MaxIntLattice,
)

import droplet.server.utils as sutils
from droplet.shared.serializer import Serializer

serializer = Serializer()


class AbstractDropletUserLibrary:
    # Stores a lattice value at ref.
    def put(self, ref, ltc):
        raise NotImplementedError

    # Retrives the lattice value at ref.
    def get(self, ref, ltype):
        raise NotImplementedError

    # Sends a bytestring message to the specified destination.
    # TODO: type and format for destination ID?
    def send(self, dest, bytestr):
        raise NotImplementedError

    # Receives messages sent by send() to this function.
    # Receives all outstanding messages as a list [(sender id,
    # bytestring message), ...]
    def recv(self):
        raise NotImplementedError


class DropletUserLibrary(AbstractDropletUserLibrary):

    # ip: Executor IP.
    # tid: Executor thread ID.
    # anna_client: The Anna client, used for interfacing with the kvs.
    def __init__(self, context, pusher_cache, ip, tid, anna_client):
        self.executor_ip = ip
        self.executor_tid = tid
        self.anna_client = anna_client

        self.pusher_cache = pusher_cache

        self.address = sutils.BIND_ADDR_TEMPLATE % (sutils.RECV_INBOX_PORT +
                                                    self.executor_tid)

        self.client_id = str(int(uuid.uuid4()))

        # Socket on which inbound messages, if any, will be received.
        self.recv_inbox_socket = context.socket(zmq.PULL)
        self.recv_inbox_socket.bind(self.address)

    def put(self, ref, value):
        # return self.anna_client.put(ref, serializer.dump_lattice(value))
        return self.causal_put(ref, value, {})

    def causal_put(self, ref, value, deps):
        data = SetLattice({serializer.dump(value)})
        mkc_value = MultiKeyCausalLattice(  
            VectorClock({self.client_id: MaxIntLattice(1)}),
            MapLattice(deps),
            data,
        )
        return self.anna_client.causal_put(
            ref, mkc_value, self.client_id)

    def causal_get(self, ref):
        versions, results = self.anna_client.causal_get(
            ref, client_id=self.client_id)

        if results[ref] is None:
            return None

        if type(results[ref]) == tuple:
            if results[ref][1] == None:
                return None

            return serializer.load(results[ref][1])

        return serializer.load(results[ref])  # (vc, value) or None  

    def get(self, ref):
        res = self.causal_get(ref)
        if res is None: return None
        return res
        # if type(ref) != list:
        #     refs = [ref]
        # else:
        #     refs = ref

        # kv_pairs = self.anna_client.get(refs)
        # result = {}

        # # Deserialize each of the lattice objects and return them to the
        # # client.
        # for key in kv_pairs:
        #     # If the key is not in the kvs, we can just return None.
        #     if kv_pairs[key] is None:
        #         result[key] = None
        #     else:
        #         result[key] = serializer.load_lattice(kv_pairs[key])

        # if type(ref) == list:
        #     return result
        # else:
        #     return result[ref]



    def getid(self):
        return (self.executor_ip, self.executor_tid)

    # dest is currently (IP string, thread id int) of destination executor.
    def send(self, dest, bytestr):
        ip, tid = dest
        dest_addr = sutils.get_user_msg_inbox_addr(ip, tid)
        sender = (self.executor_ip, self.executor_tid)

        socket = self.pusher_cache.get(dest_addr)
        socket.send_pyobj((sender, bytestr))

    # We see if any messages have been sent to this thread. We return an empty
    # list if there are none.
    def recv(self):
        res = []
        while True:
            try:
                # We pass in zmq.NOBLOCK here so that we only check for
                # messages that have already been received.
                msg = self.recv_inbox_socket.recv_pyobj(zmq.NOBLOCK)
                res.append(msg)
            except zmq.ZMQError as e:
                # ZMQ will throw an EAGAIN error with a timeout if there are no
                # pending messages. If that's the case, that means that there
                # are no more messages to be received.
                if e.errno == zmq.EAGAIN:
                    break
                else:
                    raise e

        return res

    def close(self):
        # Closes the context for this request by clearing any outstanding
        # messages.
        self.recv()
