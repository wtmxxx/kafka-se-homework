/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group.metrics;

import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.ShareGroupState;
import org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CONSUMER_GROUP_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.METRICS_GROUP;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.OFFSET_COMMITS_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.OFFSET_EXPIRED_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.SHARE_GROUP_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.STREAMS_GROUP_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.MetricsTestUtils.assertGaugeValue;
import static org.apache.kafka.coordinator.group.metrics.MetricsTestUtils.assertMetricsForTypeEqual;
import static org.apache.kafka.coordinator.group.metrics.MetricsTestUtils.metricName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GroupCoordinatorMetricsTest {

    @Test
    public void testMetricNames() {
        MetricsRegistry registry = new MetricsRegistry();
        Metrics metrics = new Metrics();

        Set<org.apache.kafka.common.MetricName> expectedMetrics = Set.of(
            metrics.metricName("offset-commit-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("offset-commit-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("offset-expiration-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("offset-expiration-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("offset-deletion-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("offset-deletion-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("group-completed-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("group-completed-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("consumer-group-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("consumer-group-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName(
                "group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("protocol", "classic")),
            metrics.metricName(
                "group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("protocol", "consumer")),
            metrics.metricName(
                "consumer-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("state", ConsumerGroupState.EMPTY.toString())),
            metrics.metricName(
                "consumer-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("state", ConsumerGroupState.ASSIGNING.toString())),
            metrics.metricName(
                "consumer-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("state", ConsumerGroupState.RECONCILING.toString())),
            metrics.metricName(
                "consumer-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("state", ConsumerGroupState.STABLE.toString())),
            metrics.metricName(
                "consumer-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("state", ConsumerGroupState.DEAD.toString())),
            metrics.metricName(
                "group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("protocol", Group.GroupType.SHARE.toString())),
            metrics.metricName("share-group-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("share-group-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName(
                "share-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                "The number of share groups in empty state.",
                "state", GroupState.EMPTY.toString()),
            metrics.metricName(
                "share-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                "The number of share groups in stable state.",
                "state", GroupState.STABLE.toString()),
            metrics.metricName(
                "share-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                "The number of share groups in dead state.",
                "state", GroupState.DEAD.toString()),
            metrics.metricName(
                "group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("protocol", Group.GroupType.STREAMS.toString())),
            metrics.metricName("streams-group-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("streams-group-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName(
                "streams-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("state", StreamsGroupState.EMPTY.toString())),
            metrics.metricName(
                "streams-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("state", StreamsGroupState.ASSIGNING.toString())),
            metrics.metricName(
                "streams-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("state", StreamsGroupState.RECONCILING.toString())),
            metrics.metricName(
                "streams-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("state", StreamsGroupState.STABLE.toString())),
            metrics.metricName(
                "streams-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("state", StreamsGroupState.DEAD.toString())),
            metrics.metricName(
                "streams-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Map.of("state", StreamsGroupState.NOT_READY.toString()))
        );

        try {
            try (GroupCoordinatorMetrics ignored = new GroupCoordinatorMetrics(registry, metrics)) {
                Set<String> expectedRegistry = Set.of(
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumOffsets",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroups",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsPreparingRebalance",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsCompletingRebalance",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsStable",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsDead",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsEmpty"
                );

                assertMetricsForTypeEqual(registry, "kafka.coordinator.group", expectedRegistry);
                expectedMetrics.forEach(metricName -> assertTrue(metrics.metrics().containsKey(metricName), metricName + " is missing"));
            }
            assertMetricsForTypeEqual(registry, "kafka.coordinator.group", Set.of());
            expectedMetrics.forEach(metricName -> assertFalse(metrics.metrics().containsKey(metricName)));
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void aggregateShards() {
        MetricsRegistry registry = new MetricsRegistry();
        Metrics metrics = new Metrics();
        GroupCoordinatorMetrics coordinatorMetrics = new GroupCoordinatorMetrics(registry, metrics);
        SnapshotRegistry snapshotRegistry0 = new SnapshotRegistry(new LogContext());
        SnapshotRegistry snapshotRegistry1 = new SnapshotRegistry(new LogContext());
        TopicPartition tp0 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0);
        TopicPartition tp1 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1);
        GroupCoordinatorMetricsShard shard0 = coordinatorMetrics.newMetricsShard(snapshotRegistry0, tp0);
        GroupCoordinatorMetricsShard shard1 = coordinatorMetrics.newMetricsShard(snapshotRegistry1, tp1);
        coordinatorMetrics.activateMetricsShard(shard0);
        coordinatorMetrics.activateMetricsShard(shard1);

        shard0.setClassicGroupGauges(Utils.mkMap(
            Utils.mkEntry(ClassicGroupState.PREPARING_REBALANCE, 1L),
            Utils.mkEntry(ClassicGroupState.COMPLETING_REBALANCE, 1L),
            Utils.mkEntry(ClassicGroupState.STABLE, 1L),
            Utils.mkEntry(ClassicGroupState.EMPTY, 1L)
        ));
        shard1.setClassicGroupGauges(Utils.mkMap(
            Utils.mkEntry(ClassicGroupState.PREPARING_REBALANCE, 1L),
            Utils.mkEntry(ClassicGroupState.COMPLETING_REBALANCE, 1L),
            Utils.mkEntry(ClassicGroupState.STABLE, 1L),
            Utils.mkEntry(ClassicGroupState.EMPTY, 1L),
            Utils.mkEntry(ClassicGroupState.DEAD, 1L)
        ));

        shard0.setConsumerGroupGauges(Map.of(ConsumerGroupState.ASSIGNING, 5L));
        shard1.setConsumerGroupGauges(Map.of(
            ConsumerGroupState.RECONCILING, 1L,
            ConsumerGroupState.DEAD, 1L
        ));

        shard0.setStreamsGroupGauges(Map.of(StreamsGroupState.ASSIGNING, 2L));
        shard1.setStreamsGroupGauges(Map.of(
            StreamsGroupState.RECONCILING, 1L,
            StreamsGroupState.DEAD, 1L,
            StreamsGroupState.NOT_READY, 1L
        ));

        shard0.setShareGroupGauges(Map.of(ShareGroupState.STABLE, 2L));
        shard1.setShareGroupGauges(Map.of(
            ShareGroupState.EMPTY, 2L,
            ShareGroupState.STABLE, 3L,
            ShareGroupState.DEAD, 1L
        ));

        IntStream.range(0, 6).forEach(__ -> shard0.incrementNumOffsets());
        IntStream.range(0, 2).forEach(__ -> shard1.incrementNumOffsets());
        IntStream.range(0, 1).forEach(__ -> shard1.decrementNumOffsets());

        assertEquals(4, shard0.numClassicGroups());
        assertEquals(5, shard1.numClassicGroups());
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumGroups"), 9);
        assertGaugeValue(
            metrics,
            metrics.metricName("group-count", METRICS_GROUP, Map.of("protocol", "classic")),
            9
        );

        snapshotRegistry0.idempotentCreateSnapshot(1000);
        snapshotRegistry1.idempotentCreateSnapshot(1500);
        shard0.commitUpTo(1000);
        shard1.commitUpTo(1500);

        assertEquals(5, shard0.numConsumerGroups());
        assertEquals(2, shard1.numConsumerGroups());

        assertEquals(6, shard0.numOffsets());
        assertEquals(1, shard1.numOffsets());
        assertGaugeValue(
            metrics,
            metrics.metricName("group-count", METRICS_GROUP, Map.of("protocol", "consumer")),
            7
        );
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumOffsets"), 7);

        assertEquals(2, shard0.numShareGroups());
        assertEquals(6, shard1.numShareGroups());
        assertGaugeValue(
            metrics,
            metrics.metricName("group-count", METRICS_GROUP, Map.of("protocol", "share")),
            8
        );
        
        assertEquals(2, shard0.numStreamsGroups());
        assertEquals(3, shard1.numStreamsGroups());
        assertGaugeValue(
            metrics,
            metrics.metricName("group-count", METRICS_GROUP, Map.of("protocol", "streams")),
            5
        );
    }

    @Test
    public void testGlobalSensors() {
        MetricsRegistry registry = new MetricsRegistry();
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);
        GroupCoordinatorMetrics coordinatorMetrics = new GroupCoordinatorMetrics(registry, metrics);
        GroupCoordinatorMetricsShard shard = coordinatorMetrics.newMetricsShard(
            new SnapshotRegistry(new LogContext()), new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
        );

        shard.record(CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME, 10);
        assertMetricValue(metrics, metrics.metricName("group-completed-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP), 1.0 / 3.0);
        assertMetricValue(metrics, metrics.metricName("group-completed-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP), 10);

        shard.record(OFFSET_COMMITS_SENSOR_NAME, 20);
        assertMetricValue(metrics, metrics.metricName("offset-commit-rate", GroupCoordinatorMetrics.METRICS_GROUP), 2.0 / 3.0);
        assertMetricValue(metrics, metrics.metricName("offset-commit-count", GroupCoordinatorMetrics.METRICS_GROUP), 20);

        shard.record(OFFSET_EXPIRED_SENSOR_NAME, 30);
        assertMetricValue(metrics, metrics.metricName("offset-expiration-rate", GroupCoordinatorMetrics.METRICS_GROUP), 1.0);
        assertMetricValue(metrics, metrics.metricName("offset-expiration-count", GroupCoordinatorMetrics.METRICS_GROUP), 30);

        shard.record(CONSUMER_GROUP_REBALANCES_SENSOR_NAME, 50);
        assertMetricValue(metrics, metrics.metricName("consumer-group-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP), 5.0 / 3.0);
        assertMetricValue(metrics, metrics.metricName("consumer-group-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP), 50);

        shard.record(SHARE_GROUP_REBALANCES_SENSOR_NAME, 50);
        assertMetricValue(metrics, metrics.metricName(
            "share-group-rebalance-rate",
            GroupCoordinatorMetrics.METRICS_GROUP,
            "The rate of share group rebalances"
        ), 5.0 / 3.0);
        assertMetricValue(metrics, metrics.metricName(
            "share-group-rebalance-count",
            GroupCoordinatorMetrics.METRICS_GROUP,
            "The total number of share group rebalances"
        ), 50);

        shard.record(STREAMS_GROUP_REBALANCES_SENSOR_NAME, 50);
        assertMetricValue(metrics, metrics.metricName(
            "streams-group-rebalance-rate",
            GroupCoordinatorMetrics.METRICS_GROUP,
            "The rate of streams group rebalances"
        ), 5.0 / 3.0);
        assertMetricValue(metrics, metrics.metricName(
            "streams-group-rebalance-count",
            GroupCoordinatorMetrics.METRICS_GROUP,
            "The total number of streams group rebalances"
        ), 50);
    }

    private void assertMetricValue(Metrics metrics, MetricName metricName, double val) {
        assertEquals(val, metrics.metric(metricName).metricValue());
    }
}
