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

import json
import logging
import sys
import time
import uuid
import zmq

import hmac

from anna.client import AnnaTcpClient
from anna.zmq_util import SocketCache
import requests

from cloudburst.server.scheduler.call import call_dag, call_function
from cloudburst.server.scheduler.create import (
    create_dag,
    create_function,
    delete_dag
)
from cloudburst.server.scheduler.policy.default_policy import (
    DefaultCloudburstSchedulerPolicy
)
import cloudburst.server.scheduler.utils as sched_utils
import cloudburst.server.utils as sutils
from cloudburst.shared.proto.cloudburst_pb2 import (
    Continuation,
    Dag,
    DagCall,
    GenericResponse,
    NO_SUCH_DAG,  # Cloudburst's error types
    Value,
    NORMAL
)
from cloudburst.shared.proto.internal_pb2 import (
    ExecutorStatistics,
    SchedulerStatus,
    ThreadStatus
)
from cloudburst.shared.proto.shared_pb2 import StringSet
from cloudburst.shared.serializer import Serializer
from cloudburst.shared.utils import (
    CONNECT_PORT,
    DAG_CALL_PORT,
    DAG_CREATE_PORT,
    DAG_DELETE_PORT,
    FUNC_CALL_PORT,
    FUNC_CREATE_PORT,
    LIST_PORT
)

from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import os

import cloudpickle as cp

METADATA_THRESHOLD = 5
REPORT_THRESHOLD = 5

logging.basicConfig(filename='log_scheduler.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

context = zmq.Context(1)

serializer = Serializer()

slack_credential = {}


def scheduler(ip, mgmt_ip, route_addr):

    # If the management IP is not set, we are running in local mode.
    local = (mgmt_ip is None)
    kvs = AnnaTcpClient(route_addr, ip, local=local)

    scheduler_id = str(uuid.uuid4())

    # A mapping from a DAG's name to its protobuf representation.
    dags = {}

    # Tracks how often a request for each function is received.
    call_frequency = {}

    # Tracks the time interval between successive requests for a particular
    # DAG.
    interarrivals = {}

    # Tracks the most recent arrival for each DAG -- used to calculate
    # interarrival times.
    last_arrivals = {}

    # Maintains a list of all other schedulers in the system, so we can
    # propagate metadata to them.
    schedulers = set()

    connect_socket = context.socket(zmq.REP)
    connect_socket.bind(sutils.BIND_ADDR_TEMPLATE % (CONNECT_PORT))

    func_create_socket = context.socket(zmq.REP)
    func_create_socket.bind(sutils.BIND_ADDR_TEMPLATE % (FUNC_CREATE_PORT))

    func_call_socket = context.socket(zmq.REP)
    func_call_socket.bind(sutils.BIND_ADDR_TEMPLATE % (FUNC_CALL_PORT))

    dag_create_socket = context.socket(zmq.REP)
    dag_create_socket.bind(sutils.BIND_ADDR_TEMPLATE % (DAG_CREATE_PORT))

    dag_call_socket = context.socket(zmq.REP)
    dag_call_socket.bind(sutils.BIND_ADDR_TEMPLATE % (DAG_CALL_PORT))

    dag_delete_socket = context.socket(zmq.REP)
    dag_delete_socket.bind(sutils.BIND_ADDR_TEMPLATE % (DAG_DELETE_PORT))

    list_socket = context.socket(zmq.REP)
    list_socket.bind(sutils.BIND_ADDR_TEMPLATE % (LIST_PORT))

    exec_status_socket = context.socket(zmq.PULL)
    exec_status_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.STATUS_PORT))

    sched_update_socket = context.socket(zmq.PULL)
    sched_update_socket.bind(sutils.BIND_ADDR_TEMPLATE %
                             (sutils.SCHED_UPDATE_PORT))

    pin_accept_socket = context.socket(zmq.PULL)
    pin_accept_socket.setsockopt(zmq.RCVTIMEO, 5000)
    pin_accept_socket.bind(sutils.BIND_ADDR_TEMPLATE %
                           (sutils.PIN_ACCEPT_PORT))

    continuation_socket = context.socket(zmq.PULL)
    continuation_socket.bind(sutils.BIND_ADDR_TEMPLATE %
                             (sutils.CONTINUATION_PORT))

    slack_socket = context.socket(zmq.PULL)
    slack_socket.bind('ipc:///slack/post')

    if not local:
        management_request_socket = context.socket(zmq.REQ)
        management_request_socket.setsockopt(zmq.RCVTIMEO, 500)
        # By setting this flag, zmq matches replies with requests.
        management_request_socket.setsockopt(zmq.REQ_CORRELATE, 1)
        # Relax strict alternation between request and reply.
        # For detailed explanation, see here: http://api.zeromq.org/4-1:zmq-setsockopt
        management_request_socket.setsockopt(zmq.REQ_RELAXED, 1)
        management_request_socket.connect(sched_utils.get_scheduler_list_address(mgmt_ip))

    pusher_cache = SocketCache(context, zmq.PUSH)

    poller = zmq.Poller()
    poller.register(connect_socket, zmq.POLLIN)
    poller.register(func_create_socket, zmq.POLLIN)
    poller.register(func_call_socket, zmq.POLLIN)
    poller.register(dag_create_socket, zmq.POLLIN)
    poller.register(dag_call_socket, zmq.POLLIN)
    poller.register(dag_delete_socket, zmq.POLLIN)
    poller.register(list_socket, zmq.POLLIN)
    poller.register(exec_status_socket, zmq.POLLIN)
    poller.register(sched_update_socket, zmq.POLLIN)
    poller.register(continuation_socket, zmq.POLLIN)
    poller.register(slack_socket, zmq.POLLIN)

    # Start the policy engine.
    policy = DefaultCloudburstSchedulerPolicy(pin_accept_socket, pusher_cache,
                                           kvs, ip, local=local)
    policy.update()

    start = time.time()

    while True:
        socks = dict(poller.poll(timeout=1000))

        if connect_socket in socks and socks[connect_socket] == zmq.POLLIN:
            logging.info('enter connect')
            msg = connect_socket.recv_string()
            connect_socket.send_string(route_addr)
            logging.info('exit connect')

        if (func_create_socket in socks and
                socks[func_create_socket] == zmq.POLLIN):
            logging.info('enter create function')
            create_function(func_create_socket, kvs)
            logging.info('exit create function')

        if func_call_socket in socks and socks[func_call_socket] == zmq.POLLIN:
            logging.info('enter call function')
            call_function(func_call_socket, pusher_cache, policy)
            logging.info('exit call function')

        if (dag_create_socket in socks and socks[dag_create_socket]
                == zmq.POLLIN):
            logging.info('enter create dag')
            create_dag(dag_create_socket, pusher_cache, kvs, dags, policy,
                       call_frequency, slack_credential)
            logging.info('exit create dag')

        if dag_call_socket in socks and socks[dag_call_socket] == zmq.POLLIN:
            logging.info('enter call dag')
            call = DagCall()
            call.ParseFromString(dag_call_socket.recv())

            name = call.name

            t = time.time()
            if name in last_arrivals:
                if name not in interarrivals:
                    interarrivals[name] = []

                interarrivals[name].append(t - last_arrivals[name])

            last_arrivals[name] = t

            if name not in dags:
                resp = GenericResponse()
                resp.success = False
                resp.error = NO_SUCH_DAG

                dag_call_socket.send(resp.SerializeToString())
                continue

            dag = dags[name]
            for fname in dag[0].functions:
                call_frequency[fname.name] += 1

            response = call_dag(call, pusher_cache, dags, policy)
            dag_call_socket.send(response.SerializeToString())
            logging.info('exit call dag')

        if (dag_delete_socket in socks and socks[dag_delete_socket] ==
                zmq.POLLIN):
            logging.info('enter delete dag')
            delete_dag(dag_delete_socket, dags, policy, call_frequency)
            logging.info('exit delete dag')

        if list_socket in socks and socks[list_socket] == zmq.POLLIN:
            logging.info('enter list')
            msg = list_socket.recv_string()
            prefix = msg if msg else ''

            resp = StringSet()
            resp.keys.extend(sched_utils.get_func_list(kvs, prefix))

            list_socket.send(resp.SerializeToString())
            logging.info('exit list')

        if exec_status_socket in socks and socks[exec_status_socket] == \
                zmq.POLLIN:
            logging.info('enter exec status')
            status = ThreadStatus()
            status.ParseFromString(exec_status_socket.recv())

            policy.process_status(status)
            logging.info('exit exec status')

        if sched_update_socket in socks and socks[sched_update_socket] == \
                zmq.POLLIN:
            logging.info('enter sched update')
            status = SchedulerStatus()
            status.ParseFromString(sched_update_socket.recv())

            # Retrieve any DAGs that some other scheduler knows about that we
            # do not yet know about.
            for dname in status.dags:
                if dname not in dags:
                    payload = kvs.get(dname)
                    while None in payload:
                        payload = kvs.get(dname)

                    dag = Dag()
                    dag.ParseFromString(payload[dname].reveal())
                    dags[dag.name] = (dag, sched_utils.find_dag_source(dag))

                    for fname in dag.functions:
                        if fname.name not in call_frequency:
                            call_frequency[fname.name] = 0

            policy.update_function_locations(status.function_locations)
            logging.info('exit sched update')

        if continuation_socket in socks and socks[continuation_socket] == \
                zmq.POLLIN:
            logging.info('enter continuation')
            continuation = Continuation()
            continuation.ParseFromString(continuation_socket.recv())

            call = continuation.call
            call.name = continuation.name

            result = Value()
            result.ParseFromString(continuation.result)

            dag, sources = dags[call.name]
            for source in sources:
                call.function_args[source].values.extend([result])

            call_dag(call, pusher_cache, dags, policy, continuation.id)
            logging.info('exit continuation')

        if slack_socket in socks and socks[slack_socket] == zmq.POLLIN:
            logging.info('enter slack')
            msg = slack_socket.recv()
            #logging.info('finish receiving')
            name, event = cp.loads(msg)
            #logging.info(name)
            #logging.info(event)
            #name, event = cp.loads(slack_socket.recv())
            #logging.info('finish parsing')

            if name not in dags:
                logging.error('Error: slack app not registered as DAG')
                continue

            dag = dags[name]
            for fname in dag[0].functions:
                call_frequency[fname.name] += 1

            #logging.info('forming DAG object')
            dc = DagCall()
            dc.name = name
            dc.consistency = NORMAL

            #logging.info('getting function')
            fname = dag[0].functions[0]
            logging.info(fname)
            args = [serializer.dump(event, serialize=False)]
            #logging.info('dumped args')
            al = dc.function_args[fname.name]
            al.values.extend(args)

            #logging.info('Calling slack app')
            call_dag(dc, pusher_cache, dags, policy)
            logging.info('exit slack')

        end = time.time()

        if end - start > METADATA_THRESHOLD:
            # Update the scheduler policy-related metadata.
            logging.info('enter metadata')
            policy.update()

            # If the management IP is None, that means we arre running in
            # local mode, so there is no need to deal with caches and other
            # schedulers.
            if not local:
                latest_schedulers = sched_utils.get_ip_set(management_request_socket, False)
                if latest_schedulers:
                    schedulers = latest_schedulers
            logging.info('exit metadata')

        if end - start > REPORT_THRESHOLD:
            logging.info('enter report')
            status = SchedulerStatus()
            for name in dags.keys():
                status.dags.append(name)

            for fname in policy.function_locations:
                for loc in policy.function_locations[fname]:
                    floc = status.function_locations.add()
                    floc.name = fname
                    floc.ip = loc[0]
                    floc.tid = loc[1]

            msg = status.SerializeToString()

            for sched_ip in schedulers:
                if sched_ip != ip:
                    sckt = pusher_cache.get(
                        sched_utils.get_scheduler_update_address
                        (sched_ip))
                    sckt.send(msg)

            stats = ExecutorStatistics()
            for fname in call_frequency:
                fstats = stats.functions.add()
                fstats.name = fname
                fstats.call_count = call_frequency[fname]
                logging.info('Reporting %d calls for function %s.' %
                             (call_frequency[fname], fname))

                call_frequency[fname] = 0

            for dname in interarrivals:
                dstats = stats.dags.add()
                dstats.name = dname
                dstats.call_count = len(interarrivals[dname]) + 1
                dstats.interarrival.extend(interarrivals[dname])

                interarrivals[dname].clear()

            # We only attempt to send the statistics if we are running in
            # cluster mode. If we are running in local mode, we write them to
            # the local log file.
            if mgmt_ip:
                sckt = pusher_cache.get(sutils.get_statistics_report_address
                                        (mgmt_ip))
                sckt.send(stats.SerializeToString())

            start = time.time()
            logging.info('exit report')

class MyHTTPServer(HTTPServer):
    def __init__(self, *args, **kwargs):
        HTTPServer.__init__(self, *args, **kwargs)
        self.pusher = context.socket(zmq.PUSH)
        self.pusher.connect('ipc:///slack/post')

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        logging.info('received GET')
        logging.info('path is ' + self.path)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'')

    def do_POST(self):
        logging.info('received POST')
        logging.info('path is ' + self.path)
        if '/slack/events' in self.path:
            raw_data = self.rfile.read(int(self.headers['Content-Length']))
            json_obj = json.loads(raw_data)
            logging.info(json_obj)

            if 'challenge' in json_obj:
                self.send_response(200)
                self.send_header('content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(json_obj['challenge'].encode())
            else:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'')
                app_id = json_obj['api_app_id']
                logging.info(app_id)
                if app_id not in slack_credential:
                    logging.error('app not registered!')
                else:
                    secret = slack_credential[app_id]
                    logging.info(secret)
                    timestamp = self.headers['X-Slack-Request-Timestamp']
                    sig_basestring = 'v0:' + timestamp + ':' + raw_data
                    my_signature = 'v0=' + hmac.compute_hash_sha256(secret, sig_basestring).hexdigest()
                    slack_signature = self.headers['X-Slack-Signature']
                    if hmac.compare(my_signature, slack_signature):
                        logging.info('authorized!')
                        event = json_obj['event']
                        logging.info('sending to main loop')
                        self.server.pusher.send(cp.dumps([app_id, event]))
                        logging.info('sent to main loop')
                    else:
                        logging.info('signature does not match!')
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'')

class WebThread(threading.Thread):
    def run(self):
        server_address = ('0.0.0.0', 80)
        httpd = MyHTTPServer(server_address, Handler)
        httpd.serve_forever()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        conf_file = sys.argv[1]
    else:
        conf_file = 'conf/cloudburst-config.yml'

    conf = sutils.load_conf(conf_file)
    sched_conf = conf['scheduler']

    WebThread().start()
    scheduler(conf['ip'], conf['mgmt_ip'], sched_conf['routing_address'])
