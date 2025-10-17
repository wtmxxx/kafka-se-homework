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

from ducktape.utils.util import wait_until
from ducktape.mark import matrix, parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.performance import ProducerPerformanceService, ConsumerPerformanceService, EndToEndLatencyService, ShareConsumerPerformanceService
from kafkatest.services.performance import latency, compute_aggregate_throughput
from kafkatest.version import DEV_BRANCH, LATEST_2_1, V_4_1_0, KafkaVersion


class PerformanceServiceTest(Test):
    def __init__(self, test_context):
        super(PerformanceServiceTest, self).__init__(test_context)
        self.record_size = 100
        self.num_records = 10000
        self.topic = "topic"

    @cluster(num_nodes=6)
    @matrix(version=[str(LATEST_2_1)], metadata_quorum=quorum.all_kraft)
    @matrix(version=[str(DEV_BRANCH)], metadata_quorum=quorum.all_kraft, use_share_groups=[True])
    def test_version(self, version=str(LATEST_2_1), metadata_quorum=quorum.zk, use_share_groups=False):
        """
        Sanity check out producer performance service - verify that we can run the service with a small
        number of messages. The actual stats here are pretty meaningless since the number of messages is quite small.
        """
        version = KafkaVersion(version)
        self.kafka = KafkaService(
            self.test_context, 1,
            None, topics={self.topic: {'partitions': 1, 'replication-factor': 1}}, version=DEV_BRANCH)
        self.kafka.start()

        # check basic run of producer performance
        self.producer_perf = ProducerPerformanceService(
            self.test_context, 1, self.kafka, topic=self.topic,
            num_records=self.num_records, record_size=self.record_size,
            throughput=1000000000,  # Set impossibly for no throttling for equivalent behavior between 0.8.X and 0.9.X
            version=version,
            settings={
                'acks': 1,
                'batch.size': 8*1024,
                'buffer.memory': 64*1024*1024})
        self.producer_perf.run()
        producer_perf_data = compute_aggregate_throughput(self.producer_perf)
        assert producer_perf_data['records_per_sec'] > 0

        # check basic run of end to end latency
        self.end_to_end = EndToEndLatencyService(
            self.test_context, 1, self.kafka,
            topic=self.topic, num_records=self.num_records, version=version)
        self.end_to_end.run()
        end_to_end_data = latency(self.end_to_end.results[0]['latency_50th_ms'],  self.end_to_end.results[0]['latency_99th_ms'], self.end_to_end.results[0]['latency_999th_ms'])

        # check basic run of consumer performance service
        self.consumer_perf = ConsumerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=self.topic, version=version, messages=self.num_records)
        self.consumer_perf.group = "test-consumer-group"
        self.consumer_perf.run()
        consumer_perf_data = compute_aggregate_throughput(self.consumer_perf)
        assert consumer_perf_data['records_per_sec'] > 0

        results = {
            "producer_performance": producer_perf_data,
            "end_to_end_latency": end_to_end_data,
            "consumer_performance": consumer_perf_data,
        }

        if version >= V_4_1_0:
            # check basic run of share consumer performance service
            self.share_consumer_perf = ShareConsumerPerformanceService(
                self.test_context, 1, self.kafka,
                topic=self.topic, version=version, messages=self.num_records)
            share_group = "test-share-consumer-group"
            self.share_consumer_perf.group = share_group
            wait_until(lambda: self.kafka.set_share_group_offset_reset_strategy(group=share_group, strategy="earliest"),
                   timeout_sec=20, backoff_sec=2, err_msg="share.auto.offset.reset not set to earliest")
            self.share_consumer_perf.run()
            share_consumer_perf_data = compute_aggregate_throughput(self.share_consumer_perf)
            assert share_consumer_perf_data['records_per_sec'] > 0
            results["share_consumer_performance"] = share_consumer_perf_data

        return results
