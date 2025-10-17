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
package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerGroupServiceTest {
    public static final String GROUP = "testGroup";

    public static final int NUM_PARTITIONS = 10;

    private static final List<String> TOPICS = IntStream.range(0, 5).mapToObj(i -> "testTopic" + i).toList();

    private static final List<TopicPartition> TOPIC_PARTITIONS = TOPICS.stream()
            .flatMap(topic -> IntStream.range(0, NUM_PARTITIONS).mapToObj(i -> new TopicPartition(topic, i)))
            .toList();

    private final Admin admin = mock(Admin.class);

    @Test
    public void testAdminRequestsForDescribeOffsets() throws Exception {
        String[] args = new String[]{"--bootstrap-server", "localhost:9092", "--group", GROUP, "--describe", "--offsets"};
        ConsumerGroupCommand.ConsumerGroupService groupService = consumerGroupService(args);

        when(admin.describeConsumerGroups(ArgumentMatchers.eq(List.of(GROUP)), any()))
                .thenReturn(describeGroupsResult(GroupState.STABLE));
        when(admin.listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec()), any()))
                .thenReturn(listGroupOffsetsResult(GROUP));
        when(admin.listOffsets(offsetsArgMatcher(), any()))
                .thenReturn(listOffsetsResult());
        when(admin.describeTopics(ArgumentMatchers.anySet()))
                .thenReturn(describeTopicsResult());

        Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>> statesAndAssignments = groupService.collectGroupOffsets(GROUP);
        assertEquals(Optional.of(GroupState.STABLE), statesAndAssignments.getKey());
        assertTrue(statesAndAssignments.getValue().isPresent());
        assertEquals(TOPIC_PARTITIONS.size(), statesAndAssignments.getValue().get().size());

        verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(List.of(GROUP)), any());
        verify(admin, times(1)).listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec()), any());
        verify(admin, times(1)).listOffsets(offsetsArgMatcher(), any());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testAdminRequestsForDescribeNegativeOffsets() throws Exception {
        String[] args = new String[]{"--bootstrap-server", "localhost:9092", "--group", GROUP, "--describe", "--offsets"};
        ConsumerGroupCommand.ConsumerGroupService groupService = consumerGroupService(args);

        TopicPartition testTopicPartition0 = new TopicPartition("testTopic1", 0);
        TopicPartition testTopicPartition1 = new TopicPartition("testTopic1", 1);
        TopicPartition testTopicPartition2 = new TopicPartition("testTopic1", 2);
        TopicPartition testTopicPartition3 = new TopicPartition("testTopic2", 0);
        TopicPartition testTopicPartition4 = new TopicPartition("testTopic2", 1);
        TopicPartition testTopicPartition5 = new TopicPartition("testTopic2", 2);

        // Some topic's partitions gets valid OffsetAndMetadata values, other gets nulls values (negative integers) and others aren't defined
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();

        committedOffsets.put(testTopicPartition1, new OffsetAndMetadata(100, Optional.of(1), ""));
        committedOffsets.put(testTopicPartition2, null);
        committedOffsets.put(testTopicPartition3, new OffsetAndMetadata(100, Optional.of(1), ""));
        committedOffsets.put(testTopicPartition4, new OffsetAndMetadata(100, Optional.of(1), ""));
        committedOffsets.put(testTopicPartition5, null);

        ListOffsetsResultInfo resultInfo = new ListOffsetsResultInfo(100, System.currentTimeMillis(), Optional.of(1));
        Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> endOffsets = new HashMap<>();

        endOffsets.put(testTopicPartition0, KafkaFuture.completedFuture(resultInfo));
        endOffsets.put(testTopicPartition1, KafkaFuture.completedFuture(resultInfo));
        endOffsets.put(testTopicPartition2, KafkaFuture.completedFuture(resultInfo));
        endOffsets.put(testTopicPartition3, KafkaFuture.completedFuture(resultInfo));
        endOffsets.put(testTopicPartition4, KafkaFuture.completedFuture(resultInfo));
        endOffsets.put(testTopicPartition5, KafkaFuture.completedFuture(resultInfo));

        Set<TopicPartition> assignedTopicPartitions = Set.of(testTopicPartition0, testTopicPartition1, testTopicPartition2);
        Set<TopicPartition> unassignedTopicPartitions = Set.of(testTopicPartition3, testTopicPartition4, testTopicPartition5);

        ConsumerGroupDescription consumerGroupDescription = new ConsumerGroupDescription(GROUP,
                true,
                Set.of(
                    new MemberDescription(
                        "member1", Optional.of("instance1"), "client1", "host1", new MemberAssignment(assignedTopicPartitions),
                        Optional.empty(), Optional.empty(), Optional.empty()
                    )
                ),
                RangeAssignor.class.getName(),
                GroupType.CLASSIC,
                GroupState.STABLE,
                new Node(1, "localhost", 9092),
                Set.of(),
                Optional.empty(),
                Optional.empty());

        Function<Collection<TopicPartition>, ArgumentMatcher<Map<TopicPartition, OffsetSpec>>> offsetsArgMatcher = expectedPartitions ->
                topicPartitionOffsets -> topicPartitionOffsets != null && topicPartitionOffsets.keySet().equals(expectedPartitions);

        KafkaFutureImpl<ConsumerGroupDescription> future = new KafkaFutureImpl<>();
        future.complete(consumerGroupDescription);
        when(admin.describeConsumerGroups(ArgumentMatchers.eq(List.of(GROUP)), any()))
                .thenReturn(new DescribeConsumerGroupsResult(Map.of(GROUP, future)));
        when(admin.listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec()), any()))
                .thenReturn(
                        AdminClientTestUtils.listConsumerGroupOffsetsResult(
                                Map.of(GROUP, committedOffsets)));
        when(admin.listOffsets(
                ArgumentMatchers.argThat(offsetsArgMatcher.apply(assignedTopicPartitions)),
                any()
        )).thenReturn(new ListOffsetsResult(endOffsets.entrySet().stream().filter(e -> assignedTopicPartitions.contains(e.getKey()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue))));
        when(admin.listOffsets(
                ArgumentMatchers.argThat(offsetsArgMatcher.apply(unassignedTopicPartitions)),
                any()
        )).thenReturn(new ListOffsetsResult(endOffsets.entrySet().stream().filter(e -> unassignedTopicPartitions.contains(e.getKey()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue))));
        when(admin.describeTopics(ArgumentMatchers.anySet())).thenReturn(describeTopicsResult());

        Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>> statesAndAssignments = groupService.collectGroupOffsets(GROUP);
        Optional<GroupState> state = statesAndAssignments.getKey();
        Optional<Collection<PartitionAssignmentState>> assignments = statesAndAssignments.getValue();

        Map<TopicPartition, Optional<Long>> returnedOffsets = assignments.map(results ->
            results.stream().collect(Collectors.toMap(
                assignment -> new TopicPartition(assignment.topic().get(), assignment.partition().get()),
                assignment -> assignment.offset()))
        ).orElse(Map.of());
        Map<TopicPartition, Optional<Integer>> returnedLeaderEpoch = assignments.map(results ->
            results.stream().collect(Collectors.toMap(
                assignment -> new TopicPartition(assignment.topic().get(), assignment.partition().get()),
                assignment -> assignment.leaderEpoch()))
        ).orElse(Map.of());

        Map<TopicPartition, Optional<Long>> expectedOffsets = Map.of(
            testTopicPartition0, Optional.empty(),
            testTopicPartition1, Optional.of(100L),
            testTopicPartition2, Optional.empty(),
            testTopicPartition3, Optional.of(100L),
            testTopicPartition4, Optional.of(100L),
            testTopicPartition5, Optional.empty()
        );
        Map<TopicPartition, Optional<Integer>> expectedLeaderEpoch = Map.of(
            testTopicPartition0, Optional.empty(),
            testTopicPartition1, Optional.of(1),
            testTopicPartition2, Optional.empty(),
            testTopicPartition3, Optional.of(1),
            testTopicPartition4, Optional.of(1),
            testTopicPartition5, Optional.empty()
        );

        assertEquals(Optional.of(GroupState.STABLE), state);
        assertEquals(expectedOffsets, returnedOffsets);
        assertEquals(expectedLeaderEpoch, returnedLeaderEpoch);

        verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(List.of(GROUP)), any());
        verify(admin, times(1)).listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec()), any());
        verify(admin, times(1)).listOffsets(ArgumentMatchers.argThat(offsetsArgMatcher.apply(assignedTopicPartitions)), any());
        verify(admin, times(1)).listOffsets(ArgumentMatchers.argThat(offsetsArgMatcher.apply(unassignedTopicPartitions)), any());
    }

    @Test
    public void testAdminRequestsForResetOffsets() {
        List<String> args = new ArrayList<>(List.of("--bootstrap-server", "localhost:9092", "--group", GROUP, "--reset-offsets", "--to-latest"));
        List<String> topicsWithoutPartitionsSpecified = TOPICS.subList(1, TOPICS.size());
        List<String> topicArgs = new ArrayList<>(List.of("--topic", TOPICS.get(0) + ":" + (IntStream.range(0, NUM_PARTITIONS).mapToObj(Integer::toString).collect(Collectors.joining(",")))));
        topicsWithoutPartitionsSpecified.forEach(topic -> topicArgs.addAll(List.of("--topic", topic)));

        args.addAll(topicArgs);
        ConsumerGroupCommand.ConsumerGroupService groupService = consumerGroupService(args.toArray(new String[0]));

        when(admin.describeConsumerGroups(ArgumentMatchers.eq(List.of(GROUP)), any()))
                .thenReturn(describeGroupsResult(GroupState.DEAD));
        when(admin.describeTopics(ArgumentMatchers.eq(topicsWithoutPartitionsSpecified), any()))
                .thenReturn(describeTopicsResult(topicsWithoutPartitionsSpecified));
        when(admin.describeTopics(anySet()))
                .thenReturn(describeTopicsResult(TOPICS));
        when(admin.listOffsets(offsetsArgMatcher(), any()))
                .thenReturn(listOffsetsResult());

        Map<String, Map<TopicPartition, OffsetAndMetadata>> resetResult = groupService.resetOffsets();
        assertEquals(Set.of(GROUP), resetResult.keySet());
        assertEquals(new HashSet<>(TOPIC_PARTITIONS), resetResult.get(GROUP).keySet());

        verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(List.of(GROUP)), any());
        verify(admin, times(1)).describeTopics(ArgumentMatchers.eq(topicsWithoutPartitionsSpecified), any());
        verify(admin, times(1)).listOffsets(offsetsArgMatcher(), any());
    }

    private ConsumerGroupCommand.ConsumerGroupService consumerGroupService(String[] args) {
        return new ConsumerGroupCommand.ConsumerGroupService(ConsumerGroupCommandOptions.fromArgs(args), Map.of()) {
            @Override
            protected Admin createAdminClient(Map<String, String> configOverrides) {
                return admin;
            }
        };
    }

    @SuppressWarnings("deprecation")
    private DescribeConsumerGroupsResult describeGroupsResult(GroupState groupState) {
        MemberDescription member1 = new MemberDescription(
            "member1", Optional.of("instance1"), "client1", "host1", null,
            Optional.empty(), Optional.empty(), Optional.empty());
        ConsumerGroupDescription description = new ConsumerGroupDescription(GROUP,
                true,
                Set.of(member1),
                RangeAssignor.class.getName(),
                GroupType.CLASSIC,
                groupState,
                new Node(1, "localhost", 9092),
                Set.of(),
                Optional.empty(),
                Optional.empty());
        KafkaFutureImpl<ConsumerGroupDescription> future = new KafkaFutureImpl<>();
        future.complete(description);
        return new DescribeConsumerGroupsResult(Map.of(GROUP, future));
    }

    private ListConsumerGroupOffsetsResult listGroupOffsetsResult(String groupId) {
        Map<TopicPartition, OffsetAndMetadata> offsets = TOPIC_PARTITIONS.stream().collect(Collectors.toMap(
                Function.identity(),
                __ -> new OffsetAndMetadata(100)));
        return AdminClientTestUtils.listConsumerGroupOffsetsResult(Map.of(groupId, offsets));
    }

    private Map<TopicPartition, OffsetSpec> offsetsArgMatcher() {
        Map<TopicPartition, OffsetSpec> expectedOffsets = TOPIC_PARTITIONS.stream().collect(Collectors.toMap(
                Function.identity(),
                __ -> OffsetSpec.latest()
        ));
        return ArgumentMatchers.argThat(map ->
                Objects.equals(map.keySet(), expectedOffsets.keySet()) && map.values().stream().allMatch(v -> v instanceof OffsetSpec.LatestSpec)
        );
    }

    private DescribeTopicsResult describeTopicsResult() {
        Map<String, TopicDescription> topicDescriptionMap = TOPICS.stream().collect(Collectors.toMap(
                Function.identity(),
                topic -> new TopicDescription(
                        topic,
                        false,
                        IntStream.range(0, NUM_PARTITIONS)
                                .mapToObj(i -> new TopicPartitionInfo(i, Node.noNode(), List.of(), List.of()))
                                .toList())));
        return AdminClientTestUtils.describeTopicsResult(topicDescriptionMap);
    }

    private ListOffsetsResult listOffsetsResult() {
        ListOffsetsResultInfo resultInfo = new ListOffsetsResultInfo(100, System.currentTimeMillis(), Optional.of(1));
        Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = TOPIC_PARTITIONS.stream().collect(Collectors.toMap(
                Function.identity(),
                __ -> KafkaFuture.completedFuture(resultInfo)));
        return new ListOffsetsResult(futures);
    }

    private DescribeTopicsResult describeTopicsResult(Collection<String> topics) {
        var topicDescriptions  = topics.stream().collect(Collectors.toMap(Function.identity(),
            topic -> new TopicDescription(topic, false, IntStream.range(0, NUM_PARTITIONS)
                .mapToObj(i -> new TopicPartitionInfo(i, Node.noNode(), List.of(), List.of()))
                .toList())));
        return AdminClientTestUtils.describeTopicsResult(topicDescriptions);
    }

    private Map<String, ListConsumerGroupOffsetsSpec> listConsumerGroupOffsetsSpec() {
        return Map.of(GROUP, new ListConsumerGroupOffsetsSpec());
    }
}
