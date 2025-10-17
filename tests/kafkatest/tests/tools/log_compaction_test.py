# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.tests.test import Test
from ducktape.mark.resource import cluster

from kafkatest.services.kafka import config_property
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.log_compaction_tester import LogCompactionTester

class LogCompactionTest(Test):

    # Configure smaller segment size to create more segments for compaction
    LOG_SEGMENT_BYTES = "1048576"

    def __init__(self, test_context):
        super(LogCompactionTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1

        self.kafka = None
        self.compaction_verifier = None

    def start_kafka(self, security_protocol, interbroker_security_protocol):
        self.kafka = KafkaService(
            self.test_context,
            num_nodes = self.num_brokers,
            zk = None,
            security_protocol=security_protocol,
            interbroker_security_protocol=interbroker_security_protocol,
            server_prop_overrides=[
                [config_property.LOG_SEGMENT_BYTES, LogCompactionTest.LOG_SEGMENT_BYTES],
            ],
            controller_num_nodes_override=self.num_zk)
        self.kafka.start()

    def start_test_log_compaction_tool(self, security_protocol, compression_config={}):
        self.compaction_verifier = LogCompactionTester(self.test_context, self.kafka, security_protocol=security_protocol, compression_config=compression_config)
        self.compaction_verifier.start()

    @cluster(num_nodes=4)
    @matrix(metadata_quorum=quorum.all_non_upgrade, compression_config=[
        {},
        {'type': 'gzip', 'level': 1},
        {'type': 'gzip', 'level': 9},
        {'type': 'snappy'},
        {'type': 'lz4', 'level': 1},
        {'type': 'lz4', 'level': 10},
        {'type': 'zstd', 'level': 1},
        {'type': 'zstd', 'level': 10}
    ])
    def test_log_compaction(self, security_protocol='PLAINTEXT', metadata_quorum=quorum.zk, compression_config={}):

        self.start_kafka(security_protocol, security_protocol)
        self.start_test_log_compaction_tool(security_protocol, compression_config)

        # Verify that compacted data verification completed in LogCompactionTester
        wait_until(lambda: self.compaction_verifier.is_done, timeout_sec=240, err_msg="Timed out waiting to complete compaction")
