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
package org.apache.kafka.server.share.fetch;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class ShareFetchTest {

    private static final String GROUP_ID = "groupId";
    private static final String MEMBER_ID = "memberId";
    private static final int BATCH_SIZE = 500;

    private BrokerTopicStats brokerTopicStats;

    @BeforeEach
    public void setUp() {
        brokerTopicStats = new BrokerTopicStats();
    }

    @AfterEach
    public void tearDown() throws Exception {
        brokerTopicStats.close();
    }

    @Test
    public void testErrorInAllPartitions() {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        ShareFetch shareFetch = new ShareFetch(mock(FetchParams.class), GROUP_ID, MEMBER_ID, new CompletableFuture<>(),
            List.of(topicIdPartition), BATCH_SIZE, 100, brokerTopicStats);
        assertFalse(shareFetch.errorInAllPartitions());

        shareFetch.addErroneous(topicIdPartition, new RuntimeException());
        assertTrue(shareFetch.errorInAllPartitions());
    }

    @Test
    public void testDontCacheAnyData() {
        final TopicIdPartition tidp = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");
        MemoryRecords records = buildRecords(1L, 3, 1);

        ShareFetchResponse shareFetch = shareFetchResponse(tidp, records, Errors.NONE, "", (short) 0,
                "", List.of(), 0);
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData = shareFetch.responseData(Map.of(tidp.topicId(), tidp.topic()));
        assertEquals(1, responseData.size());
        responseData.forEach((topicIdPartition, partitionData) -> assertEquals(records, partitionData.records()));

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> nonResponseData = shareFetch.responseData(Map.of());
        assertEquals(0, nonResponseData.size());
    }

    @Test
    public void testErrorInAllPartitionsWithMultipleTopicIdPartitions() {
        TopicIdPartition topicIdPartition0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        ShareFetch shareFetch = new ShareFetch(mock(FetchParams.class), GROUP_ID, MEMBER_ID, new CompletableFuture<>(),
            List.of(topicIdPartition0, topicIdPartition1), BATCH_SIZE, 100, brokerTopicStats);
        assertFalse(shareFetch.errorInAllPartitions());

        shareFetch.addErroneous(topicIdPartition0, new RuntimeException());
        assertFalse(shareFetch.errorInAllPartitions());

        shareFetch.addErroneous(topicIdPartition1, new RuntimeException());
        assertTrue(shareFetch.errorInAllPartitions());
    }

    @Test
    public void testFilterErroneousTopicPartitions() {
        TopicIdPartition topicIdPartition0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        ShareFetch shareFetch = new ShareFetch(mock(FetchParams.class), GROUP_ID, MEMBER_ID, new CompletableFuture<>(),
            List.of(topicIdPartition0, topicIdPartition1), BATCH_SIZE, 100, brokerTopicStats);
        Set<TopicIdPartition> result = shareFetch.filterErroneousTopicPartitions(Set.of(topicIdPartition0, topicIdPartition1));
        // No erroneous partitions, hence all partitions should be returned.
        assertEquals(2, result.size());
        assertTrue(result.contains(topicIdPartition0));
        assertTrue(result.contains(topicIdPartition1));

        // Add an erroneous partition and verify that it is filtered out.
        shareFetch.addErroneous(topicIdPartition0, new RuntimeException());
        result = shareFetch.filterErroneousTopicPartitions(Set.of(topicIdPartition0, topicIdPartition1));
        assertEquals(1, result.size());
        assertTrue(result.contains(topicIdPartition1));

        // Add another erroneous partition and verify that it is filtered out.
        shareFetch.addErroneous(topicIdPartition1, new RuntimeException());
        result = shareFetch.filterErroneousTopicPartitions(Set.of(topicIdPartition0, topicIdPartition1));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testMaybeCompleteWithErroneousTopicPartitions() {
        TopicIdPartition topicIdPartition0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));

        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(mock(FetchParams.class), GROUP_ID, MEMBER_ID, future,
            List.of(topicIdPartition0, topicIdPartition1), BATCH_SIZE, 100, brokerTopicStats);

        // Add both erroneous partition and complete request.
        shareFetch.addErroneous(topicIdPartition0, new RuntimeException());
        shareFetch.addErroneous(topicIdPartition1, new RuntimeException());
        shareFetch.maybeComplete(Map.of());
        assertEquals(2, future.join().size());
        assertTrue(future.join().containsKey(topicIdPartition0));
        assertTrue(future.join().containsKey(topicIdPartition1));
        // Validate failed share fetch request metrics, though 2 partitions failed but only 1 topic failed.
        assertEquals(1, brokerTopicStats.allTopicsStats().failedShareFetchRequestRate().count());
        assertEquals(1, brokerTopicStats.topicStats("foo").failedShareFetchRequestRate().count());
    }

    @Test
    public void testMaybeCompleteWithPartialErroneousTopicPartitions() {
        TopicIdPartition topicIdPartition0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));

        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(mock(FetchParams.class), GROUP_ID, MEMBER_ID, future,
            List.of(topicIdPartition0, topicIdPartition1), BATCH_SIZE, 100, brokerTopicStats);

        // Add an erroneous partition and complete request.
        shareFetch.addErroneous(topicIdPartition0, new RuntimeException());
        shareFetch.maybeComplete(Map.of());
        assertTrue(future.isDone());
        assertEquals(1, future.join().size());
        assertTrue(future.join().containsKey(topicIdPartition0));
        // Validate failed share fetch request metrics, 1 topic partition failed and 1 succeeded.
        assertEquals(1, brokerTopicStats.allTopicsStats().failedShareFetchRequestRate().count());
        assertEquals(1, brokerTopicStats.topicStats("foo").failedShareFetchRequestRate().count());
    }

    @Test
    public void testMaybeCompleteWithException() {
        TopicIdPartition topicIdPartition0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));

        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(mock(FetchParams.class), GROUP_ID, MEMBER_ID, future,
            List.of(topicIdPartition0, topicIdPartition1), BATCH_SIZE, 100, brokerTopicStats);

        shareFetch.maybeCompleteWithException(List.of(topicIdPartition0, topicIdPartition1), new RuntimeException());
        assertEquals(2, future.join().size());
        assertTrue(future.join().containsKey(topicIdPartition0));
        assertTrue(future.join().containsKey(topicIdPartition1));
        // Validate failed share fetch request metrics, though 2 partitions failed but only 1 topic failed.
        assertEquals(1, brokerTopicStats.allTopicsStats().failedShareFetchRequestRate().count());
        assertEquals(1, brokerTopicStats.topicStats("foo").failedShareFetchRequestRate().count());
    }

    @Test
    public void testMaybeCompleteWithExceptionPartialFailure() {
        TopicIdPartition topicIdPartition0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        TopicIdPartition topicIdPartition2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));

        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(mock(FetchParams.class), GROUP_ID, MEMBER_ID, future,
            List.of(topicIdPartition0, topicIdPartition1, topicIdPartition2), BATCH_SIZE, 100, brokerTopicStats);

        shareFetch.maybeCompleteWithException(List.of(topicIdPartition0, topicIdPartition2), new RuntimeException());
        assertEquals(2, future.join().size());
        assertTrue(future.join().containsKey(topicIdPartition0));
        // Validate failed share fetch request metrics, 1 topic partition failed and 1 succeeded.
        assertEquals(2, brokerTopicStats.allTopicsStats().failedShareFetchRequestRate().count());
        assertEquals(1, brokerTopicStats.topicStats("foo").failedShareFetchRequestRate().count());
        assertEquals(1, brokerTopicStats.topicStats("foo1").failedShareFetchRequestRate().count());
    }

    @Test
    public void testMaybeCompleteWithExceptionWithExistingErroneousTopicPartition() {
        TopicIdPartition topicIdPartition0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));

        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(mock(FetchParams.class), GROUP_ID, MEMBER_ID, future,
            List.of(topicIdPartition0, topicIdPartition1), BATCH_SIZE, 100, brokerTopicStats);

        shareFetch.addErroneous(topicIdPartition0, new RuntimeException());
        shareFetch.maybeCompleteWithException(List.of(topicIdPartition1), new RuntimeException());
        assertEquals(2, future.join().size());
        assertTrue(future.join().containsKey(topicIdPartition0));
        assertTrue(future.join().containsKey(topicIdPartition1));
        // Validate failed share fetch request metrics, though 2 partitions failed but only 1 topic failed.
        assertEquals(1, brokerTopicStats.allTopicsStats().failedShareFetchRequestRate().count());
        assertEquals(1, brokerTopicStats.topicStats("foo").failedShareFetchRequestRate().count());
    }

    private MemoryRecords buildRecords(long baseOffset, int count, long firstMessageId) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, baseOffset);
        for (int i = 0; i < count; i++)
            builder.append(0L, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
        return builder.build();
    }

    private ShareFetchResponse shareFetchResponse(TopicIdPartition tp, MemoryRecords records, Errors error,
                                                  String errorMessage, short acknowledgeErrorCode, String acknowledgeErrorMessage,
                                                  List<ShareFetchResponseData.AcquiredRecords> acquiredRecords, int throttleTime) {
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitions = Map.of(tp,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tp.topicPartition().partition())
                        .setErrorCode(error.code())
                        .setErrorMessage(errorMessage)
                        .setAcknowledgeErrorCode(acknowledgeErrorCode)
                        .setAcknowledgeErrorMessage(acknowledgeErrorMessage)
                        .setRecords(records)
                        .setAcquiredRecords(acquiredRecords));
        return ShareFetchResponse.of(Errors.NONE, throttleTime, new LinkedHashMap<>(partitions), List.of(), Integer.MAX_VALUE);
    }
}
