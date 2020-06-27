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
import sys
import time
import uuid
import random

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.proto.cloudburst_pb2 import CloudburstError, DAG_ALREADY_EXISTS
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.serializer import Serializer
from anna.lattices import VectorClock, MapLattice, SetLattice, MultiKeyCausalLattice

from cloudburst.shared.proto.cloudburst_pb2 import (
    MULTI,
    NORMAL,  # Cloudburst consistency modes
)

serializer = Serializer()

OSIZE = 1000000


def run(cloudburst_client, num_requests, create, sckt):
    dag_name = 'tweet_dag'
    kvs = cloudburst_client.kvs_client

    if create:
        print('create')
        ''' DEFINE AND REGISTER FUNCTIONS '''
        def tweet(_, a, dep=None):
            result = 'read ' + a
            if dep:
                for key in dep:
                    result += ' plus dep key: ' + key + ' dep val: ' + dep[key]
            else:
                result += ' no extra dependency'
            return result

        cloud_tweet = cloudburst_client.register(tweet, 'tweet')

        if cloud_tweet:
            logging.info('Successfully registered the dot function.')
        else:
            logging.info('Error registering function.')
            print('Error registering function.')
            sys.exit(1)

        ''' TEST REGISTERED FUNCTIONS '''
        result = cloud_tweet('000').get()[0]
        if result == 'read 000 no extra dependency':
            logging.info('Successfully tested function!')
        else:
            logging.info('Unexpected result %s.' % result)
            print('Unexpected result %s.' % result)
            sys.exit(1)
        ''' CREATE DAG '''
        functions = ['tweet']
        connections = []
        success, error = cloudburst_client.register_dag(dag_name, functions,
                                                     connections)
        if not success and error != DAG_ALREADY_EXISTS:
            print('Failed to register DAG: %s' % (CloudburstError.Name(error)))
            sys.exit(1)

        ''' CREATE DATA '''
        logging.info('Generating tweets')
        f = open('/graph', "rb")
        users, follow = cp.load(f)
        tweets = ['Today is a beautifle day', 'I love cat!', 'I watched an interesting movie', 'What is wrong with this?', 'I cannot believe it!']
        for i, uid in enumerate(users):
            if i % 100 == 0:
                logging.info(uid)
            others = users.copy()
            others.remove(uid)
            tids = set()
            user_tweets = np.random.choice(tweets, size=3, replace=False)
            parents = []
            for tweet in user_tweets:
                tid = str(uuid.uuid1())
                parents.append(tid)
                tids.add(serializer.dump(tid))
                vc = VectorClock({uid : 1}, True)
                dep = MapLattice({})
                values = SetLattice({serializer.dump(uid + ': ' + tweet),})
                mkl = MultiKeyCausalLattice(vc, dep, values)
                kvs.put(tid, mkl)
            reply_uids = np.random.choice(others, size=2, replace=False)
            for j, reply_uid in enumerate(reply_uids):
                tid = str(uuid.uuid1())
                tids.add(serializer.dump(tid))
                vc = VectorClock({reply_uid : 1}, True)
                dep_vc = VectorClock({uid : 1}, True)
                dep = MapLattice({parents[j] : dep_vc})
                values = SetLattice({serializer.dump(reply_uid + ' reply: this is great!'),})
                mkl = MultiKeyCausalLattice(vc, dep, values)
                kvs.put(tid, mkl)

            tids_lattice = SetLattice(tids)
            kvs.put(uid, tids_lattice)

        return [], [], [], 0

    else:
        ''' RUN DAG '''
        f = open('/graph', "rb")
        users, follow = cp.load(f)

        total_time = []
        scheduler_time = []
        kvs_time = []

        retries = 0

        log_start = time.time()

        log_epoch = 0
        epoch_total = []

        for i in range(num_requests):
            uid = np.random.choice(users, size=1, replace=False).tolist()[0]
            followers = follow[uid]
            target_uid = np.random.choice(followers, size=1, replace=False).tolist()[0]

            tids = serializer.load_lattice(kvs.get(target_uid)[target_uid])
            tid = np.random.choice(tids, size=1, replace=False).tolist()[0]

            refs = []
            refs.append(CloudburstReference(tid, True))
            arg_map = {'tweet': refs}
            start = time.time()
            if random.random() < 0.1:
                logging.info('post')
                cb_future = cloudburst_client.call_dag(dag_name, arg_map, False, MULTI, None, uid)
                result_id = cb_future.obj_id
                result = cb_future.get()
                logging.info(result)
                end = time.time()

                epoch_total.append(end - start)
                total_time.append(end - start)

                kvs.put(target_uid, SetLattice({serializer.dump(result_id),}))
            else:
                logging.info('read')
                result = cloudburst_client.call_dag(dag_name, arg_map, True, MULTI)
                logging.info(result)
                end = time.time()

                if result:
                    epoch_total.append(end - start)
                    total_time.append(end - start)

            log_end = time.time()
            if (log_end - log_start) > 5:
                throughput = len(epoch_total) / 5
                if sckt:
                    sckt.send(cp.dumps((throughput, epoch_total)))
                logging.info('EPOCH %d THROUGHPUT: %.2f' % (log_epoch, throughput))
                utils.print_latency_stats(epoch_total, 'EPOCH %d E2E' %
                                          (log_epoch), True)

                epoch_total.clear()
                log_epoch += 1
                log_start = time.time()

        return total_time, scheduler_time, kvs_time, retries
