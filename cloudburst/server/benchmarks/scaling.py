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
import numpy as np
import random

import cloudpickle as cp

from cloudburst.shared.proto.cloudburst_pb2 import (
    CloudburstError,
    NORMAL,
)
from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference


def get_base(N, skew):
    base = 0.0
    for k in range(1, N+1):
        base += np.power(k, -1*skew)
    return 1 / float(base)



def sample(n, base, sum_probs):
    zipf_value = None
    low = 1
    high = n

    z = random.random()
    while z == 0 or z == 1:
        z = random.random()

    while True:
        mid = int(np.floor((low + high) / 2))
        if sum_probs[mid] >= z and sum_probs[mid - 1] < z:
            zipf_value = mid
            break
        elif sum_probs[mid] >= z:
            high = mid - 1
        else:
            low = mid + 1
        if low > high:
            break
    return zipf_value

def run(cloudburst_client, num_requests, sckt, create):
    ''' DEFINE AND REGISTER FUNCTIONS '''
    dag_name = 'scaling'
    num_keys = 1000000

    if create:
        def slp(cloudburst, x, y):
            import time
            time.sleep(.050)
            return x

        cloud_sleep = cloudburst_client.register(slp, 'sleep')

        if cloud_sleep:
            logging.info('Successfully registered sleep function.')
            print('Successfully registered sleep function.')
        else:
            sys.exit(1)

        ''' TEST REGISTERED FUNCTIONS '''
        sleep_test = cloud_sleep(2, 2).get()
        if sleep_test != 2:
            logging.info('Unexpected result from sleep(2, 2): %s' % (str(sleep_test)))
            print('Unexpected result from sleep(2, 2): %s' % (str(sleep_test)))
            sys.exit(1)
        logging.info('Successfully tested functions!')
        print('Successfully tested functions!')

        ''' CREATE DAG '''
        functions = ['sleep']
        success, error = cloudburst_client.register_dag(dag_name, functions, [])

        if not success:
            logging.info('Failed to register DAG: %s' % (CloudburstError.Name(error)))
            print('Failed to register DAG: %s' % (CloudburstError.Name(error)))
            sys.exit(1)

        ''' WARMUP KEYS '''
        logging.info('Begin warmup')
        val = '0'.zfill(8)
        for i in range(num_keys):
            if i % 1000 == 0:
                logging.info('Warmed up key %s', i)
            k = str(i).zfill(7)
            cloudburst_client.put_object(k, val)
        logging.info('Finish warmup')



        return [], [], [], 0
    else:
        ''' RUN DAG '''
        #arg_map = {'sleep': [1, 1]}

        zipf = 1.0
        base = get_base(num_keys, zipf)
        sum_probs = {}
        sum_probs[0] = 0.0
        for i in range(1, num_keys+1):
            sum_probs[i] = sum_probs[i - 1] + (base / np.power(float(i), zipf))

        total_time = []
        epoch_req_count = 0
        epoch_latencies = []

        epoch_start = time.time()
        epoch = 0
        for _ in range(num_requests):
            refs = []
            for _ in range(2):
                k = sample(num_keys, base, sum_probs)
                k = str(k).zfill(7)
                refs.append(CloudburstReference(k, True))
            arg_map = {'sleep': refs}
            out_key = sample(num_keys, base, sum_probs)
            out_key = str(out_key).zfill(7)
            start = time.time()
            res = cloudburst_client.call_dag(dag_name, arg_map, True, NORMAL, out_key)
            #res = cloudburst_client.call_dag(dag_name, arg_map, True)
            end = time.time()

            if res is not None:
                epoch_req_count += 1
                total_time += [end - start]
                epoch_latencies += [end - start]

            epoch_end = time.time()
            if epoch_end - epoch_start > 10:
                if sckt:
                    sckt.send(cp.dumps((epoch_req_count, epoch_latencies)))

                logging.info('EPOCH %d THROUGHPUT: %.2f' %
                             (epoch, (epoch_req_count / 10)))
                if len(epoch_latencies) > 0:
                    utils.print_latency_stats(epoch_latencies,
                                              'EPOCH %d E2E' % epoch, True)
                else:
                    logging.info('No data to print for this epoch')
                epoch += 1

                epoch_req_count = 0
                epoch_latencies.clear()
                epoch_start = time.time()

        return total_time, [], [], 0
