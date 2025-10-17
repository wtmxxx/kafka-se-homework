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
package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.share.persister.PersisterStateBatch;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShareCoordinatorRecordHelpersTest {
    @Test
    public void testNewShareSnapshotRecord() {
        String groupId = "test-group";
        Uuid topicId = Uuid.randomUuid();
        long timestamp = System.currentTimeMillis();
        int partitionId = 1;
        PersisterStateBatch batch = new PersisterStateBatch(1L, 10L, (byte) 0, (short) 1);
        CoordinatorRecord record = ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            groupId,
            topicId,
            partitionId,
            new ShareGroupOffset.Builder()
                .setSnapshotEpoch(0)
                .setStateEpoch(1)
                .setLeaderEpoch(5)
                .setStartOffset(0)
                .setCreateTimestamp(timestamp)
                .setWriteTimestamp(timestamp)
                .setStateBatches(List.of(batch))
                .build()
        );

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(groupId)
                .setTopicId(topicId)
                .setPartition(partitionId),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(0)
                    .setStateEpoch(1)
                    .setLeaderEpoch(5)
                    .setStartOffset(0)
                    .setCreateTimestamp(timestamp)
                    .setWriteTimestamp(timestamp)
                    .setStateBatches(List.of(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(1L)
                            .setLastOffset(10L)
                            .setDeliveryState((byte) 0)
                            .setDeliveryCount((short) 1))),
                (short) 0));

        assertEquals(expectedRecord, record);
    }

    @Test
    public void testNewShareUpdateRecord() {
        String groupId = "test-group";
        Uuid topicId = Uuid.randomUuid();
        int partitionId = 1;
        PersisterStateBatch batch = new PersisterStateBatch(1L, 10L, (byte) 0, (short) 1);
        CoordinatorRecord record = ShareCoordinatorRecordHelpers.newShareUpdateRecord(
            groupId,
            topicId,
            partitionId,
            new ShareGroupOffset.Builder()
                .setSnapshotEpoch(0)
                .setStateEpoch(-1)  // ignored for share update
                .setLeaderEpoch(5)
                .setStartOffset(0)
                .setStateBatches(List.of(batch))
                .build()
        );

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new ShareUpdateKey()
                .setGroupId(groupId)
                .setTopicId(topicId)
                .setPartition(partitionId),
            new ApiMessageAndVersion(
                new ShareUpdateValue()
                    .setSnapshotEpoch(0)
                    .setLeaderEpoch(5)
                    .setStartOffset(0)
                    .setStateBatches(List.of(
                        new ShareUpdateValue.StateBatch()
                            .setFirstOffset(1L)
                            .setLastOffset(10L)
                            .setDeliveryState((byte) 0)
                            .setDeliveryCount((short) 1))),
                (short) 0));

        assertEquals(expectedRecord, record);
    }

    @Test
    public void testNewShareStateTombstoneRecord() {
        String groupId = "test-group";
        Uuid topicId = Uuid.randomUuid();
        int partitionId = 1;
        CoordinatorRecord record = ShareCoordinatorRecordHelpers.newShareStateTombstoneRecord(
            groupId,
            topicId,
            partitionId
        );

        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new ShareSnapshotKey()
                .setGroupId(groupId)
                .setTopicId(topicId)
                .setPartition(partitionId)
        );

        assertEquals(expectedRecord, record);
    }
}
