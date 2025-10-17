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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataValue;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.InitMapValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkOrderedAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkOrderedTopicAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupEpochTombstoneRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GroupCoordinatorRecordHelpersTest {

    @Test
    public void testNewConsumerGroupMemberSubscriptionRecord() {
        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = new ArrayList<>();
        protocols.add(new ConsumerGroupMemberMetadataValue.ClassicProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(Arrays.asList("foo", "zar", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignorName("range")
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(protocols))
            .build();

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new ConsumerGroupMemberMetadataKey()
                .setGroupId("group-id")
                .setMemberId("member-id"),
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataValue()
                    .setInstanceId("instance-id")
                    .setRackId("rack-id")
                    .setRebalanceTimeoutMs(5000)
                    .setClientId("client-id")
                    .setClientHost("client-host")
                    .setSubscribedTopicNames(Arrays.asList("bar", "foo", "zar"))
                    .setSubscribedTopicRegex("regex")
                    .setServerAssignor("range")
                    .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                        .setSupportedProtocols(protocols)),
                (short) 0
            )
        );

        assertEquals(expectedRecord, newConsumerGroupMemberSubscriptionRecord(
            "group-id",
            member
        ));
    }

    @Test
    public void testNewConsumerGroupMemberSubscriptionTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new ConsumerGroupMemberMetadataKey()
                .setGroupId("group-id")
                .setMemberId("member-id")
        );

        assertEquals(expectedRecord, newConsumerGroupMemberSubscriptionTombstoneRecord(
            "group-id",
            "member-id"
        ));
    }

    @Test
    public void testNewConsumerGroupSubscriptionMetadataTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new ConsumerGroupPartitionMetadataKey()
                .setGroupId("group-id")
        );

        assertEquals(expectedRecord, newConsumerGroupSubscriptionMetadataTombstoneRecord(
            "group-id"
        ));
    }

    @Test
    public void testNewConsumerGroupEpochRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new ConsumerGroupMetadataKey()
                .setGroupId("group-id"),
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataValue()
                    .setEpoch(10)
                    .setMetadataHash(10),
                (short) 0
            )
        );

        assertEquals(expectedRecord, newConsumerGroupEpochRecord(
            "group-id",
            10,
            10
        ));
    }

    @Test
    public void testNewConsumerGroupEpochTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new ConsumerGroupMetadataKey()
                .setGroupId("group-id")
        );

        assertEquals(expectedRecord, newConsumerGroupEpochTombstoneRecord(
            "group-id"
        ));
    }

    @Test
    public void testNewShareGroupEpochTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new ShareGroupMetadataKey()
                .setGroupId("group-id")
        );

        assertEquals(expectedRecord, newShareGroupEpochTombstoneRecord(
            "group-id"
        ));
    }

    @Test
    public void testNewShareGroupPartitionMetadataRecord() {
        String groupId = "group-id";
        String topicName1 = "t1";
        Uuid topicId1 = Uuid.randomUuid();
        String topicName2 = "t2";
        Uuid topicId2 = Uuid.randomUuid();
        Set<Integer> partitions = new LinkedHashSet<>();
        partitions.add(0);
        partitions.add(1);

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new ShareGroupStatePartitionMetadataKey()
                .setGroupId(groupId),
            new ApiMessageAndVersion(
                new ShareGroupStatePartitionMetadataValue()
                    .setInitializedTopics(
                        List.of(
                            new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                                .setTopicId(topicId1)
                                .setTopicName(topicName1)
                                .setPartitions(List.of(0, 1))
                        )
                    )
                    .setDeletingTopics(
                        List.of(
                            new ShareGroupStatePartitionMetadataValue.TopicInfo()
                                .setTopicId(topicId2)
                                .setTopicName(topicName2)
                        )
                    ),
                (short) 0
            )
        );

        CoordinatorRecord record = GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
            groupId,
            Map.of(),
            Map.of(
                topicId1,
                new InitMapValue(topicName1, partitions, 1)
            ),
            Map.of(
                topicId2,
                topicName2
            )
        );

        assertEquals(expectedRecord, record);
    }

    @Test
    public void testNewConsumerGroupTargetAssignmentRecord() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        Map<Uuid, Set<Integer>> partitions = mkOrderedAssignment(
            mkTopicAssignment(topicId1, 11, 12, 13),
            mkTopicAssignment(topicId2, 21, 22, 23)
        );

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new ConsumerGroupTargetAssignmentMemberKey()
                .setGroupId("group-id")
                .setMemberId("member-id"),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberValue()
                    .setTopicPartitions(Arrays.asList(
                        new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                            .setTopicId(topicId1)
                            .setPartitions(Arrays.asList(11, 12, 13)),
                        new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                            .setTopicId(topicId2)
                            .setPartitions(Arrays.asList(21, 22, 23)))),
                (short) 0
            )
        );

        assertEquals(expectedRecord, newConsumerGroupTargetAssignmentRecord(
            "group-id",
            "member-id",
            partitions
        ));
    }

    @Test
    public void testNewConsumerGroupTargetAssignmentTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new ConsumerGroupTargetAssignmentMemberKey()
                .setGroupId("group-id")
                .setMemberId("member-id")
        );

        assertEquals(expectedRecord, newConsumerGroupTargetAssignmentTombstoneRecord(
            "group-id",
            "member-id"
        ));
    }

    @Test
    public void testNewConsumerGroupTargetAssignmentEpochRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new ConsumerGroupTargetAssignmentMetadataKey()
                .setGroupId("group-id"),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataValue()
                    .setAssignmentEpoch(10),
                (short) 0
            )
        );

        assertEquals(expectedRecord, newConsumerGroupTargetAssignmentEpochRecord(
            "group-id",
            10
        ));
    }

    @Test
    public void testNewConsumerGroupTargetAssignmentEpochTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new ConsumerGroupTargetAssignmentMetadataKey()
                .setGroupId("group-id")
        );

        assertEquals(expectedRecord, newConsumerGroupTargetAssignmentEpochTombstoneRecord(
            "group-id"
        ));
    }

    @Test
    public void testNewConsumerGroupCurrentAssignmentRecord() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        Map<Uuid, Set<Integer>> assigned = mkOrderedAssignment(
            mkOrderedTopicAssignment(topicId1, 11, 12, 13),
            mkOrderedTopicAssignment(topicId2, 21, 22, 23)
        );

        Map<Uuid, Set<Integer>> revoking = mkOrderedAssignment(
            mkOrderedTopicAssignment(topicId1, 14, 15, 16),
            mkOrderedTopicAssignment(topicId2, 24, 25, 26)
        );

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new ConsumerGroupCurrentMemberAssignmentKey()
                .setGroupId("group-id")
                .setMemberId("member-id"),
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentValue()
                    .setState(MemberState.UNREVOKED_PARTITIONS.value())
                    .setMemberEpoch(22)
                    .setPreviousMemberEpoch(21)
                    .setAssignedPartitions(Arrays.asList(
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId1)
                            .setPartitions(Arrays.asList(11, 12, 13)),
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId2)
                            .setPartitions(Arrays.asList(21, 22, 23))))
                    .setPartitionsPendingRevocation(Arrays.asList(
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId1)
                            .setPartitions(Arrays.asList(14, 15, 16)),
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId2)
                            .setPartitions(Arrays.asList(24, 25, 26)))),
                (short) 0
            )
        );

        assertEquals(expectedRecord, newConsumerGroupCurrentAssignmentRecord(
            "group-id",
            new ConsumerGroupMember.Builder("member-id")
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(22)
                .setPreviousMemberEpoch(21)
                .setAssignedPartitions(assigned)
                .setPartitionsPendingRevocation(revoking)
                .build()
        ));
    }

    @Test
    public void testNewConsumerGroupCurrentAssignmentTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new ConsumerGroupCurrentMemberAssignmentKey()
                .setGroupId("group-id")
                .setMemberId("member-id")
        );

        assertEquals(expectedRecord, newConsumerGroupCurrentAssignmentTombstoneRecord(
            "group-id",
            "member-id"
        ));
    }

    @Test
    public void testNewGroupMetadataRecord() {
        Time time = new MockTime();

        List<GroupMetadataValue.MemberMetadata> expectedMembers = new ArrayList<>();
        expectedMembers.add(
            new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-1")
                .setClientId("client-1")
                .setClientHost("host-1")
                .setRebalanceTimeout(1000)
                .setSessionTimeout(1500)
                .setGroupInstanceId("group-instance-1")
                .setSubscription(new byte[]{0, 1})
                .setAssignment(new byte[]{1, 2})
        );

        expectedMembers.add(
            new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-2")
                .setClientId("client-2")
                .setClientHost("host-2")
                .setRebalanceTimeout(1000)
                .setSessionTimeout(1500)
                .setGroupInstanceId("group-instance-2")
                .setSubscription(new byte[]{1, 2})
                .setAssignment(new byte[]{2, 3})
        );

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new GroupMetadataKey()
                .setGroup("group-id"),
            new ApiMessageAndVersion(
                new GroupMetadataValue()
                    .setProtocol("range")
                    .setProtocolType("consumer")
                    .setLeader("member-1")
                    .setGeneration(1)
                    .setCurrentStateTimestamp(time.milliseconds())
                    .setMembers(expectedMembers),
                (short) 3
            )
        );

        ClassicGroup group = new ClassicGroup(
            new LogContext(),
            "group-id",
            ClassicGroupState.PREPARING_REBALANCE,
            time
        );

        Map<String, byte[]> assignment = new HashMap<>();

        expectedMembers.forEach(member -> {
            JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
            protocols.add(new JoinGroupRequestProtocol()
                .setName("range")
                .setMetadata(member.subscription()));

            group.add(new ClassicGroupMember(
                member.memberId(),
                Optional.of(member.groupInstanceId()),
                member.clientId(),
                member.clientHost(),
                member.rebalanceTimeout(),
                member.sessionTimeout(),
                "consumer",
                protocols,
                ClassicGroupMember.EMPTY_ASSIGNMENT
            ));

            assignment.put(member.memberId(), member.assignment());
        });

        group.initNextGeneration();
        CoordinatorRecord groupMetadataRecord = GroupCoordinatorRecordHelpers.newGroupMetadataRecord(
            group,
            assignment
        );

        assertEquals(expectedRecord, groupMetadataRecord);
    }

    @Test
    public void testNewGroupMetadataTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new GroupMetadataKey()
                .setGroup("group-id")
        );

        CoordinatorRecord groupMetadataRecord = GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord("group-id");
        assertEquals(expectedRecord, groupMetadataRecord);
    }

    @Test
    public void testNewGroupMetadataRecordThrowsWhenNullSubscription() {
        Time time = new MockTime();

        List<GroupMetadataValue.MemberMetadata> expectedMembers = new ArrayList<>();
        expectedMembers.add(
            new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-1")
                .setClientId("client-1")
                .setClientHost("host-1")
                .setRebalanceTimeout(1000)
                .setSessionTimeout(1500)
                .setGroupInstanceId("group-instance-1")
                .setSubscription(new byte[]{0, 1})
                .setAssignment(new byte[]{1, 2})
        );

        ClassicGroup group = new ClassicGroup(
            new LogContext(),
            "group-id",
            ClassicGroupState.PREPARING_REBALANCE,
            time
        );

        expectedMembers.forEach(member -> {
            JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
            protocols.add(new JoinGroupRequestProtocol()
                .setName("range")
                .setMetadata(null));

            group.add(new ClassicGroupMember(
                member.memberId(),
                Optional.of(member.groupInstanceId()),
                member.clientId(),
                member.clientHost(),
                member.rebalanceTimeout(),
                member.sessionTimeout(),
                "consumer",
                protocols,
                member.assignment()
            ));
        });

        assertThrows(IllegalStateException.class, () ->
            GroupCoordinatorRecordHelpers.newGroupMetadataRecord(
                group,
                Map.of()
            ));
    }

    @Test
    public void testNewGroupMetadataRecordThrowsWhenEmptyAssignment() {
        Time time = new MockTime();

        List<GroupMetadataValue.MemberMetadata> expectedMembers = new ArrayList<>();
        expectedMembers.add(
            new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-1")
                .setClientId("client-1")
                .setClientHost("host-1")
                .setRebalanceTimeout(1000)
                .setSessionTimeout(1500)
                .setGroupInstanceId("group-instance-1")
                .setSubscription(new byte[]{0, 1})
                .setAssignment(null)
        );

        ClassicGroup group = new ClassicGroup(
            new LogContext(),
            "group-id",
            ClassicGroupState.PREPARING_REBALANCE,
            time
        );

        expectedMembers.forEach(member -> {
            JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
            protocols.add(new JoinGroupRequestProtocol()
                .setName("range")
                .setMetadata(member.subscription()));

            group.add(new ClassicGroupMember(
                member.memberId(),
                Optional.of(member.groupInstanceId()),
                member.clientId(),
                member.clientHost(),
                member.rebalanceTimeout(),
                member.sessionTimeout(),
                "consumer",
                protocols,
                member.assignment()
            ));
        });

        assertThrows(IllegalStateException.class, () ->
            GroupCoordinatorRecordHelpers.newGroupMetadataRecord(
                group,
                Map.of()
            ));
    }
      
    @Test
    public void testEmptyGroupMetadataRecord() {
        Time time = new MockTime();

        List<GroupMetadataValue.MemberMetadata> expectedMembers = List.of();

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new GroupMetadataKey()
                .setGroup("group-id"),
            new ApiMessageAndVersion(
                new GroupMetadataValue()
                    .setProtocol(null)
                    .setProtocolType("")
                    .setLeader(null)
                    .setGeneration(0)
                    .setCurrentStateTimestamp(time.milliseconds())
                    .setMembers(expectedMembers),
                (short) 3
            )
        );

        ClassicGroup group = new ClassicGroup(
            new LogContext(),
            "group-id",
            ClassicGroupState.PREPARING_REBALANCE,
            time
        );

        group.initNextGeneration();
        CoordinatorRecord groupMetadataRecord = GroupCoordinatorRecordHelpers.newEmptyGroupMetadataRecord(
            group
        );

        assertEquals(expectedRecord, groupMetadataRecord);
    }

    @Test
    public void testOffsetCommitValueVersion() {
        assertEquals((short) 1, GroupCoordinatorRecordHelpers.offsetCommitValueVersion(true));
        assertEquals((short) 4, GroupCoordinatorRecordHelpers.offsetCommitValueVersion(false));
    }

    private static Stream<Uuid> uuids() {
        return Stream.of(
            Uuid.ZERO_UUID,
            Uuid.randomUuid()
        );
    }

    @ParameterizedTest
    @MethodSource("uuids")
    public void testNewOffsetCommitRecord(Uuid topicId) {
        OffsetCommitKey key = new OffsetCommitKey()
            .setGroup("group-id")
            .setTopic("foo")
            .setPartition(1);
        OffsetCommitValue value = new OffsetCommitValue()
            .setOffset(100L)
            .setLeaderEpoch(10)
            .setMetadata("metadata")
            .setCommitTimestamp(1234L)
            .setExpireTimestamp(-1L)
            .setTopicId(topicId);

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(
                value,
                GroupCoordinatorRecordHelpers.offsetCommitValueVersion(false)
            )
        );

        assertEquals(expectedRecord, GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
            "group-id",
            "foo",
            1,
            new OffsetAndMetadata(
                -1L,
                100L,
                OptionalInt.of(10),
                "metadata",
                1234L,
                OptionalLong.empty(),
                topicId
            )
        ));

        value.setLeaderEpoch(-1);

        assertEquals(expectedRecord, GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
            "group-id",
            "foo",
            1,
            new OffsetAndMetadata(
                100L,
                OptionalInt.empty(),
                "metadata",
                1234L,
                OptionalLong.empty(),
                topicId
            )
        ));
    }

    @Test
    public void testNewOffsetCommitRecordWithExpireTimestamp() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new OffsetCommitKey()
                .setGroup("group-id")
                .setTopic("foo")
                .setPartition(1),
            new ApiMessageAndVersion(
                new OffsetCommitValue()
                    .setOffset(100L)
                    .setLeaderEpoch(10)
                    .setMetadata("metadata")
                    .setCommitTimestamp(1234L)
                    .setExpireTimestamp(5678L),
                (short) 1 // When expire timestamp is set, it is always version 1.
            )
        );

        assertEquals(expectedRecord, GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
            "group-id",
            "foo",
            1,
            new OffsetAndMetadata(
                100L,
                OptionalInt.of(10),
                "metadata",
                1234L,
                OptionalLong.of(5678L),
                Uuid.ZERO_UUID
            )
        ));
    }

    @Test
    public void testNewOffsetCommitTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new OffsetCommitKey()
                .setGroup("group-id")
                .setTopic("foo")
                .setPartition(1)
        );

        CoordinatorRecord record = GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group-id", "foo", 1);
        assertEquals(expectedRecord, record);
    }

    @Test
    public void testNewConsumerGroupRegularExpressionRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new ConsumerGroupRegularExpressionKey()
                .setGroupId("group-id")
                .setRegularExpression("ab*"),
            new ApiMessageAndVersion(
                new ConsumerGroupRegularExpressionValue()
                    .setTopics(Arrays.asList("abc", "abcd"))
                    .setVersion(10L)
                    .setTimestamp(12345L),
                (short) 0
            )
        );

        CoordinatorRecord record = GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
            "group-id",
            "ab*",
            new ResolvedRegularExpression(
                Set.of("abc", "abcd"),
                10L,
                12345L
            )
        );

        assertEquals(expectedRecord, record);
    }

    @Test
    public void testNewConsumerGroupRegularExpressionTombstone() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new ConsumerGroupRegularExpressionKey()
                .setGroupId("group-id")
                .setRegularExpression("ab*")
        );

        CoordinatorRecord record = GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(
            "group-id",
            "ab*"
        );

        assertEquals(expectedRecord, record);
    }

    @Test
    public void testNewShareGroupEpochRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new ShareGroupMetadataKey()
                .setGroupId("group-id"),
            new ApiMessageAndVersion(
                new ShareGroupMetadataValue()
                    .setEpoch(10)
                    .setMetadataHash(10),
                (short) 0
            )
        );

        assertEquals(expectedRecord, newShareGroupEpochRecord(
            "group-id",
            10,
            10
        ));
    }
}
