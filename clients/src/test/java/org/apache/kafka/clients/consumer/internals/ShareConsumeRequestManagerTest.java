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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementCommitCallbackEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.ShareSessionNotFoundException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class ShareConsumeRequestManagerTest {
    private final String topicName = "test";
    private final String topicName2 = "test-2";
    private final String groupId = "test-group";
    private final Uuid topicId = Uuid.randomUuid();
    private final Uuid topicId2 = Uuid.randomUuid();
    private final Map<String, Uuid> topicIds = new HashMap<>() {
        {
            put(topicName, topicId);
            put(topicName2, topicId2);
        }
    };
    private final Map<String, Integer> topicPartitionCounts = new HashMap<>() {
        {
            put(topicName, 2);
            put(topicName2, 1);
        }
    };
    private final TopicPartition tp0 = new TopicPartition(topicName, 0);
    private final TopicIdPartition tip0 = new TopicIdPartition(topicId, tp0);
    private final TopicPartition tp1 = new TopicPartition(topicName, 1);
    private final TopicIdPartition tip1 = new TopicIdPartition(topicId, tp1);
    private final TopicPartition t2p0 = new TopicPartition(topicName2, 0);
    private final TopicIdPartition t2ip0 = new TopicIdPartition(topicId2, t2p0);
    private final int validLeaderEpoch = 0;
    private final MetadataResponse initialUpdateResponse =
            RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName, 2), topicIds);

    private final long retryBackoffMs = 100;
    private final long requestTimeoutMs = 30000;
    private final long defaultApiTimeoutMs = 60000;
    private MockTime time = new MockTime(1);
    private SubscriptionState subscriptions;
    private ShareConsumerMetadata metadata;
    private ShareFetchMetricsManager metricsManager;
    private MockClient client;
    private Metrics metrics;
    private TestableShareConsumeRequestManager<?, ?> shareConsumeRequestManager;
    private TestableNetworkClientDelegate networkClientDelegate;
    private MemoryRecords records;
    private List<ShareFetchResponseData.AcquiredRecords> acquiredRecords;
    private List<ShareFetchResponseData.AcquiredRecords> emptyAcquiredRecords;
    private ShareFetchMetricsRegistry shareFetchMetricsRegistry;
    private List<Map<TopicIdPartition, Acknowledgements>> completedAcknowledgements;

    @BeforeEach
    public void setup() {
        records = buildRecords(1L, 3, 1);
        acquiredRecords = ShareCompletedFetchTest.acquiredRecords(1L, 3);
        emptyAcquiredRecords = new ArrayList<>();
        completedAcknowledgements = new LinkedList<>();
    }

    private void assignFromSubscribed(Set<TopicPartition> partitions) {
        subscriptions.subscribeToShareGroup(partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet()));
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(initialUpdateResponse);

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("kafka-cluster", 1,
                Collections.emptyMap(), topicPartitionCounts,
                tp -> validLeaderEpoch, topicIds), false, 0L);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (metrics != null)
            metrics.close();
        if (shareConsumeRequestManager != null)
            shareConsumeRequestManager.close();
    }

    private int sendFetches() {
        return shareConsumeRequestManager.sendFetches();
    }

    @Test
    public void testFetchNormal() {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp0);
        assertEquals(3, records.size());
    }

    @Test
    public void testFetchWithAcquiredRecords() {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        // As only 1 record was acquired, we must fetch only 1 record.
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());
    }

    @Test
    public void testMultipleFetches() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);
        assignFromSubscribed(Collections.singleton(tp0));

        sendFetchAndVerifyResponse(records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        // As only 1 record was acquired, we must fetch only 1 record.
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());

        sendFetchAndVerifyResponse(records, ShareCompletedFetchTest.acquiredRecords(2L, 1), Errors.NONE);
        assertEquals(1.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
        completedAcknowledgements.clear();

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(2L, AcknowledgeType.REJECT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)), Collections.emptyMap());

        // Preparing a response with an acknowledgement error.
        sendFetchAndVerifyResponse(records, Collections.emptyList(), Errors.NONE, Errors.INVALID_RECORD_STATE);

        assertEquals(2.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());
        assertEquals(1.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementErrorTotal)).metricValue());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.isEmpty());
        assertEquals(Map.of(tip0, acknowledgements2), completedAcknowledgements.get(0));
    }

    @Test
    public void testCommitSync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(2000)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
    }

    @Test
    public void testCommitAsync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
    }

    @Test
    public void testServerDisconnectedOnShareAcknowledge() throws InterruptedException {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        fetchRecords();

        Acknowledgements acknowledgements = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(3L, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        client.prepareResponse(null, true);
        networkClientDelegate.poll(time.timer(0));

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
        assertInstanceOf(UnknownServerException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
        completedAcknowledgements.clear();

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));

        TestUtils.retryOnExceptionWithTimeout(() -> {
            assertEquals(0, shareConsumeRequestManager.sendAcknowledgements());
            // We expect the remaining acknowledgements to be cleared due to share session epoch being set to 0.
            assertNull(shareConsumeRequestManager.requestStates(0));
            // The callback for these unsent acknowledgements will be invoked with an error code.
            assertEquals(Map.of(tip0, acknowledgements2), completedAcknowledgements.get(0));
            assertInstanceOf(ShareSessionNotFoundException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
        });

        // Attempt a normal fetch to check if nodesWithPendingRequests is empty.
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
    }

    @Test
    public void testAcknowledgeOnClose() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);

        // Piggyback acknowledgements
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());

        // Remaining acknowledgements sent with close().
        Acknowledgements acknowledgements2 = getAcknowledgements(2, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.acknowledgeOnClose(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertEquals(1, completedAcknowledgements.size());

        Acknowledgements mergedAcks = acknowledgements.merge(acknowledgements2);
        mergedAcks.complete(null);
        // Verifying that all 3 offsets were acknowledged as part of the final ShareAcknowledge on close.
        assertEquals(mergedAcks.getAcknowledgementsTypeMap(), completedAcknowledgements.get(0).get(tip0).getAcknowledgementsTypeMap());
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
    }

    @Test
    public void testAcknowledgeOnCloseWithPendingCommitAsync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));
        shareConsumeRequestManager.acknowledgeOnClose(Collections.emptyMap(),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        client.prepareResponse(emptyAcknowledgeResponse());
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
    }

    @Test
    public void testAcknowledgeOnCloseWithPendingCommitSync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(100)));
        shareConsumeRequestManager.acknowledgeOnClose(Collections.emptyMap(),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        client.prepareResponse(emptyAcknowledgeResponse());
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
    }

    @Test
    public void testResultHandlerOnCommitAsync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        ShareConsumeRequestManager.ResultHandler resultHandler = shareConsumeRequestManager.buildResultHandler(null, Optional.empty());

        // Passing null acknowledgements should mean we do not send the background event at all.
        resultHandler.complete(tip0, null, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_ASYNC);
        assertEquals(0, completedAcknowledgements.size());

        // Setting the request type to COMMIT_SYNC should still not send any background event
        // as we have initialized remainingResults to null.
        resultHandler.complete(tip0, acknowledgements, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_SYNC);
        assertEquals(0, completedAcknowledgements.size());

        // Sending non-null acknowledgements means we do send the background event
        resultHandler.complete(tip0, acknowledgements, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_ASYNC);
        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
    }

    @Test
    public void testResultHandlerOnCommitSync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        final CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future = new CompletableFuture<>();

        // Initializing resultCount to 3.
        AtomicInteger resultCount = new AtomicInteger(3);

        ShareConsumeRequestManager.ResultHandler resultHandler = shareConsumeRequestManager.buildResultHandler(resultCount, Optional.of(future));

        // We only send the background event after all results have been completed.
        resultHandler.complete(tip0, acknowledgements, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_SYNC);
        assertEquals(0, completedAcknowledgements.size());
        assertFalse(future.isDone());

        resultHandler.complete(t2ip0, null, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_SYNC);
        assertEquals(0, completedAcknowledgements.size());
        assertFalse(future.isDone());

        // After third response is received, we send the background event.
        resultHandler.complete(tip1, acknowledgements, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_SYNC);
        assertEquals(1, completedAcknowledgements.size());
        assertEquals(2, completedAcknowledgements.get(0).size());
        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(3, completedAcknowledgements.get(0).get(tip1).size());
        assertTrue(future.isDone());
    }

    @Test
    public void testResultHandlerCompleteIfEmpty() {
        buildRequestManager();

        final CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future = new CompletableFuture<>();

        // Initializing resultCount to 1.
        AtomicInteger resultCount = new AtomicInteger(1);

        ShareConsumeRequestManager.ResultHandler resultHandler = shareConsumeRequestManager.buildResultHandler(resultCount, Optional.of(future));

        resultHandler.completeIfEmpty();
        assertFalse(future.isDone());

        resultCount.decrementAndGet();

        resultHandler.completeIfEmpty();
        assertTrue(future.isDone());
    }

    @Test
    public void testBatchingAcknowledgeRequestStates() {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        Acknowledgements acknowledgements2 = getAcknowledgements(4, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(6, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));
    }

    @Test
    public void testPendingCommitAsyncBeforeCommitSync() {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        Acknowledgements acknowledgements2 = getAcknowledgements(4, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)),
                calculateDeadlineMs(time.timer(60000L)));

        assertEquals(3, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));
        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(3, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        assertEquals(3, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));
        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(3, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(3, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));
    }

    @Test
    public void testRetryAcknowledgements() throws InterruptedException {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT,
                AcknowledgeType.ACCEPT, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), 60000L);
        assertNull(shareConsumeRequestManager.requestStates(0).getAsyncRequest());

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.REQUEST_TIMED_OUT));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getIncompleteAcknowledgementsCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        TestUtils.retryOnExceptionWithTimeout(() -> assertEquals(1, shareConsumeRequestManager.sendAcknowledgements()));

        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getIncompleteAcknowledgementsCount(tip0));
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER", "UNKNOWN_TOPIC_OR_PARTITION"})
    public void testFatalErrorsAcknowledgementResponse(Errors error) {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        client.prepareResponse(fullAcknowledgeResponse(tip0, error));
        networkClientDelegate.poll(time.timer(0));

        // Assert these errors are not retried even if they are retriable. They are treated as fatal and a metadata update is triggered.
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getIncompleteAcknowledgementsCount(tip0));
        assertEquals(1, completedAcknowledgements.size());
        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
    }

    @Test
    public void testRetryAcknowledgementsMultipleCommitAsync() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        // commitAsync() acknowledges the first 2 records.
        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), calculateDeadlineMs(time, 1000L));

        assertEquals(2, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(2, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));

        // Response contains a retriable exception, so we retry.
        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.REQUEST_TIMED_OUT));
        networkClientDelegate.poll(time.timer(0));

        Acknowledgements acknowledgements1 = getAcknowledgements(3, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        // 2nd commitAsync() acknowledges the next 2 records.
        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements1)), calculateDeadlineMs(time, 1000L));
        assertEquals(2, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getIncompleteAcknowledgementsCount(tip0));

        Acknowledgements acknowledgements2 = getAcknowledgements(5, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        // 3rd commitAsync() acknowledges the next 2 records.
        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)), calculateDeadlineMs(time, 1000L));

        time.sleep(2000L);

        // As the timer for the initial commitAsync() was 1000ms, the request times out, and we fill the callback with a timeout exception.
        assertEquals(0, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(1, completedAcknowledgements.size());
        assertEquals(2, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(Errors.REQUEST_TIMED_OUT.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
        completedAcknowledgements.clear();

        // Further requests which came before the timeout are processed as expected.
        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(4, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));
        assertEquals(1, completedAcknowledgements.size());
        assertEquals(4, completedAcknowledgements.get(0).get(tip0).size());
        assertNull(completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
    }

    @Test
    public void testRetryAcknowledgementsMultipleCommitSync() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        // commitSync() for the first 2 acknowledgements.
        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), calculateDeadlineMs(time, 1000L));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        // Response contains a retriable exception.
        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.REQUEST_TIMED_OUT));
        networkClientDelegate.poll(time.timer(0));
        assertEquals(2, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getIncompleteAcknowledgementsCount(tip0));

        // We expire the commitSync request as it had a timer of 1000ms.
        time.sleep(2000L);

        Acknowledgements acknowledgements1 = getAcknowledgements(3, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        // commitSync() for the next 4 acknowledgements.
        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements1)), calculateDeadlineMs(time, 1000L));

        // We send the 2nd commitSync request, and fail the first one as timer has expired.
        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(2, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(Errors.REQUEST_TIMED_OUT.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
        completedAcknowledgements.clear();

        // We get a successful response for the 2nd commitSync request.
        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(1, completedAcknowledgements.size());
        assertEquals(4, completedAcknowledgements.get(0).get(tip0).size());
        assertNull(completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
    }

    @Test
    public void testPiggybackAcknowledgementsInFlight() {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        // Reading records from the share fetch buffer.
        fetchRecords();

        // Piggyback acknowledgements
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(2.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(3L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)), Collections.emptyMap());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        fetchRecords();

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        assertEquals(3.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());
    }

    @Test
    public void testCommitAsyncWithSubscriptionChange() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName2));
        subscriptions.assignFromSubscribed(Collections.singleton(t2p0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName2, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertNull(completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        // We should send a fetch to the newly subscribed partition.
        assertEquals(1, sendFetches());

        client.prepareResponse(fullFetchResponse(t2ip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
    }

    @Test
    public void testCommitSyncWithSubscriptionChange() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName2));
        subscriptions.assignFromSubscribed(Collections.singleton(t2p0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName2, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertNull(completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        // We should send a fetch to the newly subscribed partition.
        assertEquals(1, sendFetches());

        client.prepareResponse(fullFetchResponse(t2ip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
    }

    @Test
    public void testCloseWithSubscriptionChange() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName2));
        subscriptions.assignFromSubscribed(Collections.singleton(t2p0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName2, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        shareConsumeRequestManager.acknowledgeOnClose(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertNull(completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        // As we are closing, we would not send any more fetches.
        assertEquals(0, sendFetches());
    }

    @Test
    public void testShareFetchWithSubscriptionChange() {
        buildRequestManager();

        assignFromSubscribed(singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        // Send acknowledgements via ShareFetch
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());
        fetchRecords();
        // Subscription changes.
        subscriptions.subscribeToShareGroup(Collections.singleton(topicName2));
        subscriptions.assignFromSubscribed(Collections.singleton(t2p0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName2, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        assertEquals(3.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());
    }

    @Test
    public void testShareFetchWithSubscriptionChangeMultipleNodes() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        subscriptions.assignFromSubscribed(Collections.singletonList(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        Node tp0Leader = metadata.fetch().leaderFor(tp0);
        Node tp1Leader = metadata.fetch().leaderFor(tp1);

        assertEquals(nodeId0, tp0Leader);
        assertEquals(nodeId1, tp1Leader);

        sendFetchAndVerifyResponse(records, emptyAcquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(0, AcknowledgeType.ACCEPT, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        // Send acknowledgements via ShareFetch
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());
        fetchRecords();
        // Subscription changes.
        subscriptions.assignFromSubscribed(Collections.singletonList(tp1));

        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(2, pollResult.unsentRequests.size());

        ShareFetchRequest.Builder builder1, builder2;
        if (pollResult.unsentRequests.get(0).node().get() == nodeId0) {
            builder1 = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
            builder2 = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(1).requestBuilder();
            assertEquals(nodeId1, pollResult.unsentRequests.get(1).node().get());
        } else {
            builder1 = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(1).requestBuilder();
            builder2 = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
            assertEquals(nodeId0, pollResult.unsentRequests.get(1).node().get());
            assertEquals(nodeId1, pollResult.unsentRequests.get(0).node().get());
        }

        // Verify the builder data for node0.
        assertEquals(1, builder1.data().topics().size());
        ShareFetchRequestData.FetchTopic fetchTopic = builder1.data().topics().stream().findFirst().get();
        assertEquals(tip0.topicId(), fetchTopic.topicId());
        assertEquals(1, fetchTopic.partitions().size());
        ShareFetchRequestData.FetchPartition fetchPartition = fetchTopic.partitions().stream().findFirst().get();
        assertEquals(0, fetchPartition.partitionIndex());
        assertEquals(1, fetchPartition.acknowledgementBatches().size());
        assertEquals(0L, fetchPartition.acknowledgementBatches().get(0).firstOffset());
        assertEquals(2L, fetchPartition.acknowledgementBatches().get(0).lastOffset());

        assertEquals(1, builder1.data().forgottenTopicsData().size());
        assertEquals(tip0.topicId(), builder1.data().forgottenTopicsData().get(0).topicId());
        assertEquals(1, builder1.data().forgottenTopicsData().get(0).partitions().size());
        assertEquals(0, builder1.data().forgottenTopicsData().get(0).partitions().get(0));

        // Verify the builder data for node1.
        assertEquals(1, builder2.data().topics().size());
        fetchTopic = builder2.data().topics().stream().findFirst().get();
        assertEquals(tip1.topicId(), fetchTopic.topicId());
        assertEquals(1, fetchTopic.partitions().size());
        assertEquals(1, fetchTopic.partitions().stream().findFirst().get().partitionIndex());
    }

    @Test
    public void testShareFetchWithSubscriptionChangeMultipleNodesEmptyAcknowledgements() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        subscriptions.assignFromSubscribed(Collections.singletonList(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        Node tp0Leader = metadata.fetch().leaderFor(tp0);
        Node tp1Leader = metadata.fetch().leaderFor(tp1);

        assertEquals(nodeId0, tp0Leader);
        assertEquals(nodeId1, tp1Leader);

        // Send the first ShareFetch with an empty response
        sendFetchAndVerifyResponse(records, emptyAcquiredRecords, Errors.NONE);

        fetchRecords();

        // Change the subscription.
        subscriptions.assignFromSubscribed(Collections.singletonList(tp1));


        // Now we will be sending the request to node1 only as leader for tip1 is node1.
        // We do not build the request for tip0 as there are no acknowledgements to send.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        assertEquals(nodeId1, pollResult.unsentRequests.get(0).node().get());

        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();

        assertEquals(1, builder.data().topics().size());
        ShareFetchRequestData.FetchTopic fetchTopic = builder.data().topics().stream().findFirst().get();
        assertEquals(tip1.topicId(), fetchTopic.topicId());
        assertEquals(1, fetchTopic.partitions().size());
        assertEquals(1, fetchTopic.partitions().stream().findFirst().get().partitionIndex());
        assertEquals(0, builder.data().forgottenTopicsData().size());
    }

    @Test
    public void testShareFetchAndCloseMultipleNodes() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0, tp1));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        client.prepareResponse(fullFetchResponse(tip1, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);
        Acknowledgements acknowledgements1 = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        Map<TopicIdPartition, NodeAcknowledgements> acknowledgementsMap = new HashMap<>();
        acknowledgementsMap.put(tip0, new NodeAcknowledgements(0, acknowledgements));
        acknowledgementsMap.put(tip1, new NodeAcknowledgements(1, acknowledgements1));
        shareConsumeRequestManager.acknowledgeOnClose(acknowledgementsMap, calculateDeadlineMs(time, 1000L));

        assertEquals(2, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        client.prepareResponse(fullAcknowledgeResponse(tip1, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(3, completedAcknowledgements.get(0).get(tip1).size());

        assertEquals(0, shareConsumeRequestManager.sendAcknowledgements());
        assertNull(shareConsumeRequestManager.requestStates(0));
        assertNull(shareConsumeRequestManager.requestStates(1));
    }

    @Test
    public void testRetryAcknowledgementsWithLeaderChange() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 1),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        LinkedList<Node> nodeList = new LinkedList<>(Arrays.asList(nodeId0, nodeId1));

        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT,
                AcknowledgeType.ACCEPT, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
            calculateDeadlineMs(time.timer(60000L)));
        assertNull(shareConsumeRequestManager.requestStates(0).getAsyncRequest());

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        // Fail the acknowledgement and provide the new current leader information - this should stop the retry
        client.prepareResponse(fullAcknowledgeResponse(tip0,
            Errors.NOT_LEADER_OR_FOLLOWER,
            new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(nodeId1.id()).setLeaderEpoch(validLeaderEpoch + 1),
            nodeList));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getIncompleteAcknowledgementsCount(tip0));
    }

    @Test
    public void testCallbackHandlerConfig() throws InterruptedException {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));

        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));

        completedAcknowledgements.clear();

        // Setting the boolean to false, indicating there is no callback handler registered.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(false);

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(3L, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        TestUtils.retryOnExceptionWithTimeout(() -> assertEquals(1, shareConsumeRequestManager.sendAcknowledgements()));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // We expect no acknowledgements to be added as the callback handler is not configured.
        assertEquals(0, completedAcknowledgements.size());
    }

    @Test
    public void testAcknowledgementCommitCallbackMultiplePartitionCommitAsync() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(t2p0);

        assignFromSubscribed(partitions);

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
        partitionDataMap.put(tip0, partitionDataForFetch(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE));
        partitionDataMap.put(t2ip0, partitionDataForFetch(t2ip0, records, acquiredRecords, Errors.NONE, Errors.NONE));
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, partitionDataMap, Collections.emptyList(), 0));

        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        Acknowledgements acknowledgements2 = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        Map<TopicIdPartition, NodeAcknowledgements> acks = new HashMap<>();
        acks.put(tip0, new NodeAcknowledgements(0, acknowledgements));
        acks.put(t2ip0, new NodeAcknowledgements(0, acknowledgements2));

        shareConsumeRequestManager.commitAsync(acks, calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        Map<TopicIdPartition, Errors> errorsMap = new HashMap<>();
        errorsMap.put(tip0, Errors.NONE);
        errorsMap.put(t2ip0, Errors.NONE);
        client.prepareResponse(fullAcknowledgeResponse(errorsMap));

        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // Verifying that the acknowledgement commit callback is invoked for both the partitions.
        assertEquals(2, completedAcknowledgements.size());
        assertEquals(1, completedAcknowledgements.get(0).size());
        assertEquals(1, completedAcknowledgements.get(1).size());
    }

    @Test
    public void testMultipleTopicsFetch() {
        buildRequestManager();
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(t2p0);

        assignFromSubscribed(partitions);

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
        partitionDataMap.put(tip0, partitionDataForFetch(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE));
        partitionDataMap.put(t2ip0, partitionDataForFetch(t2ip0, records, emptyAcquiredRecords, Errors.TOPIC_AUTHORIZATION_FAILED, Errors.NONE));
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, partitionDataMap, Collections.emptyList(), 0));

        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        ShareFetch<Object, Object> shareFetch = collectFetch();
        assertEquals(1, shareFetch.records().size());
        // The first topic-partition is fetched successfully and returns all the records.
        assertEquals(3, shareFetch.records().get(tp0).size());
        // As the second topic failed authorization, we do not get the records in the ShareFetch.
        assertThrows(NullPointerException.class, (Executable) shareFetch.records().get(t2p0));
        assertThrows(TopicAuthorizationException.class, this::collectFetch);
    }

    @Test
    public void testMultipleTopicsFetchError() {
        buildRequestManager();
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(t2p0);

        assignFromSubscribed(partitions);

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
        partitionDataMap.put(t2ip0, partitionDataForFetch(t2ip0, records, emptyAcquiredRecords, Errors.TOPIC_AUTHORIZATION_FAILED, Errors.NONE));
        partitionDataMap.put(tip0, partitionDataForFetch(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE));
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, partitionDataMap, Collections.emptyList(), 0));

        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // The first call throws TopicAuthorizationException because there are no records ready to return when the
        // exception is noticed.
        assertThrows(TopicAuthorizationException.class, this::collectFetch);
        // And then a second iteration returns the records.
        ShareFetch<Object, Object> shareFetch = collectFetch();
        assertEquals(1, shareFetch.records().size());
        // The first topic-partition is fetched successfully and returns all the records.
        assertEquals(3, shareFetch.records().get(tp0).size());
        // As the second topic failed authorization, we do not get the records in the ShareFetch.
        assertThrows(NullPointerException.class, (Executable) shareFetch.records().get(t2p0));
    }

    @Test
    public void testShareFetchInvalidResponse() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        subscriptions.assignFromSubscribed(Collections.singleton(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(t2ip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
    }

    @Test
    public void testShareAcknowledgeInvalidResponse() throws InterruptedException {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        subscriptions.assignFromSubscribed(Collections.singleton(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        fetchRecords();

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        // If a top-level error is received, we still retry the acknowledgements independent of the topic-partitions received in the response.
        client.prepareResponse(acknowledgeResponseWithTopLevelError(t2ip0, Errors.LEADER_NOT_AVAILABLE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getIncompleteAcknowledgementsCount(tip0));

        TestUtils.retryOnExceptionWithTimeout(() -> assertEquals(1, shareConsumeRequestManager.sendAcknowledgements()));

        client.prepareResponse(fullAcknowledgeResponse(t2ip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        // If we do not get the expected partitions in the response, we fail these acknowledgements with InvalidRecordStateException.
        assertEquals(InvalidRecordStateException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException().getClass());
        completedAcknowledgements.clear();

        // Send remaining acknowledgements through piggybacking on the next fetch.
        Acknowledgements acknowledgements1 = getAcknowledgements(2,
                AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements1)), Collections.emptyMap());

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(t2ip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        // If we do not get the expected partitions in the response, we fail these acknowledgements with InvalidRecordStateException.
        assertEquals(InvalidRecordStateException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException().getClass());
    }

    @Test
    public void testCloseShouldBeIdempotent() {
        buildRequestManager();

        shareConsumeRequestManager.close();
        shareConsumeRequestManager.close();
        shareConsumeRequestManager.close();

        verify(shareConsumeRequestManager, times(1)).closeInternal();
    }

    @Test
    public void testFetchError() {
        buildRequestManager();

        assignFromSubscribed(singleton(tp0));
        sendFetchAndVerifyResponse(records, emptyAcquiredRecords, Errors.NOT_LEADER_OR_FOLLOWER);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertFalse(partitionRecords.containsKey(tp0));
    }

    @Test
    public void testPiggybackAcknowledgementsOnInitialShareSessionError() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(singleton(tp0));

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());

        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
        assertEquals(1, builder.data().topics().size());
        // We should not add the acknowledgements as part of the request.
        assertEquals(0, builder.data().topics().find(tip0.topicId()).partitions().find(0).acknowledgementBatches().size());

        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
    }

    @Test
    public void testPiggybackAcknowledgementsOnInitialShareSessionErrorSubscriptionChange() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        fetchRecords();

        // Simulate a broker restart, but no leader change, this resets share session epoch to 0.
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        client.prepareResponse(fetchResponseWithTopLevelError(tip0, Errors.SHARE_SESSION_NOT_FOUND));
        networkClientDelegate.poll(time.timer(0));

        // Simulate a metadata update with no topics in the response.
        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Collections.emptyMap(),
                        tp -> validLeaderEpoch, null, false));

        // The acknowledgements for the initial fetch from tip0 are processed now and sent to the background thread.
        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());

        assertEquals(0, completedAcknowledgements.size());

        // Next fetch would not include any acknowledgements.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(0, pollResult.unsentRequests.size());

        // We should fail any waiting acknowledgements for tip-0 as it would have a share session epoch equal to 0.
        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
    }

    @Test
    public void testPiggybackAcknowledgementsOnInitialShareSession_ShareSessionNotFound() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(singleton(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        fetchRecords();

        // The acknowledgements for the initial fetch from tip0 are processed now and sent to the background thread.
        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());

        // We attempt to send the acknowledgements piggybacking on the fetch.
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        // Simulate a broker restart, but no leader change, this resets share session epoch to 0.
        client.prepareResponse(fetchResponseWithTopLevelError(tip0, Errors.SHARE_SESSION_NOT_FOUND));
        networkClientDelegate.poll(time.timer(0));

        // We would complete these acknowledgements with the error code from the response.
        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(Errors.SHARE_SESSION_NOT_FOUND.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        // Next fetch would proceed as expected and would not include any acknowledgements.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
        assertEquals(0, builder.data().topics().find(topicId).partitions().find(0).acknowledgementBatches().size());
    }

    @Test
    public void testInvalidDefaultRecordBatch() {
        buildRequestManager();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteBufferOutputStream out = new ByteBufferOutputStream(buffer);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(out,
                DefaultRecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L, 10L, 0L, (short) 0, 0, false, false, 0, 1024);
        builder.append(10L, "key".getBytes(), "value".getBytes());
        builder.close();
        buffer.flip();

        // Garble the CRC
        buffer.position(17);
        buffer.put("beef".getBytes());
        buffer.position(0);

        assignFromSubscribed(singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0,
                MemoryRecords.readableRecords(buffer),
                ShareCompletedFetchTest.acquiredRecords(0L, 1),
                Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        // The first call to collectFetch, throws an exception
        assertThrows(KafkaException.class, this::collectFetch);

        // The exception is cleared once thrown
        ShareFetch<String, String> fetch = this.collectFetch();
        assertTrue(fetch.isEmpty());
    }

    @Test
    public void testParseInvalidRecordBatch() {
        buildRequestManager();
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                Compression.NONE, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        ByteBuffer buffer = records.buffer();

        // flip some bits to fail the crc
        buffer.putInt(32, buffer.get(32) ^ 87238423);

        assignFromSubscribed(singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0,
                MemoryRecords.readableRecords(buffer),
                ShareCompletedFetchTest.acquiredRecords(0L, 3),
                Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertThrows(KafkaException.class, this::collectFetch);
    }

    @Test
    public void testHeaders() {
        buildRequestManager();

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, 1L);
        builder.append(0L, "key".getBytes(), "value-1".getBytes());

        Header[] headersArray = new Header[1];
        headersArray[0] = new RecordHeader("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        builder.append(0L, "key".getBytes(), "value-2".getBytes(), headersArray);

        Header[] headersArray2 = new Header[2];
        headersArray2[0] = new RecordHeader("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        headersArray2[1] = new RecordHeader("headerKey", "headerValue2".getBytes(StandardCharsets.UTF_8));
        builder.append(0L, "key".getBytes(), "value-3".getBytes(), headersArray2);

        MemoryRecords memoryRecords = builder.build();

        List<ConsumerRecord<byte[], byte[]>> records;
        assignFromSubscribed(singleton(tp0));

        client.prepareResponse(fullFetchResponse(tip0,
                memoryRecords,
                ShareCompletedFetchTest.acquiredRecords(1L, 3),
                Errors.NONE));

        assertEquals(1, sendFetches());
        networkClientDelegate.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchRecords();
        records = recordsByPartition.get(tp0);

        assertEquals(3, records.size());

        Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = records.iterator();

        ConsumerRecord<byte[], byte[]> record = recordIterator.next();
        assertNull(record.headers().lastHeader("headerKey"));

        record = recordIterator.next();
        assertEquals("headerValue", new String(record.headers().lastHeader("headerKey").value(), StandardCharsets.UTF_8));
        assertEquals("headerKey", record.headers().lastHeader("headerKey").key());

        record = recordIterator.next();
        assertEquals("headerValue2", new String(record.headers().lastHeader("headerKey").value(), StandardCharsets.UTF_8));
        assertEquals("headerKey", record.headers().lastHeader("headerKey").key());
    }

    @Test
    public void testUnauthorizedTopic() {
        buildRequestManager();

        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0, records, emptyAcquiredRecords, Errors.TOPIC_AUTHORIZATION_FAILED));
        networkClientDelegate.poll(time.timer(0));
        try {
            collectFetch();
            fail("collectFetch should have thrown a TopicAuthorizationException");
        } catch (TopicAuthorizationException e) {
            assertEquals(singleton(topicName), e.unauthorizedTopics());
        }
    }

    @Test
    public void testUnknownTopicIdError() {
        buildRequestManager();
        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponse(fetchResponseWithTopLevelError(tip0, Errors.UNKNOWN_TOPIC_ID));
        networkClientDelegate.poll(time.timer(0));
        assertEmptyFetch("Should not return records on fetch error");
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @ParameterizedTest
    @MethodSource("handleFetchResponseErrorSupplier")
    public void testHandleFetchResponseError(Errors error,
                                             boolean hasTopLevelError,
                                             boolean shouldRequestMetadataUpdate) {
        buildRequestManager();
        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());

        final ShareFetchResponse fetchResponse;

        if (hasTopLevelError)
            fetchResponse = fetchResponseWithTopLevelError(tip0, error);
        else
            fetchResponse = fullFetchResponse(tip0, records, emptyAcquiredRecords, error);

        client.prepareResponse(fetchResponse);
        networkClientDelegate.poll(time.timer(0));

        assertEmptyFetch("Should not return records on fetch error");

        long timeToNextUpdate = metadata.timeToNextUpdate(time.milliseconds());

        if (shouldRequestMetadataUpdate)
            assertEquals(0L, timeToNextUpdate, "Should have requested metadata update");
        else
            assertNotEquals(0L, timeToNextUpdate, "Should not have requested metadata update");
    }

    /**
     * Supplies parameters to {@link #testHandleFetchResponseError(Errors, boolean, boolean)}.
     */
    private static Stream<Arguments> handleFetchResponseErrorSupplier() {
        return Stream.of(
                Arguments.of(Errors.NOT_LEADER_OR_FOLLOWER, false, true),
                Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, false, true),
                Arguments.of(Errors.UNKNOWN_TOPIC_ID, true, true),
                Arguments.of(Errors.INCONSISTENT_TOPIC_ID, false, true),
                Arguments.of(Errors.FENCED_LEADER_EPOCH, false, true),
                Arguments.of(Errors.UNKNOWN_LEADER_EPOCH, false, false)
        );
    }

    @Test
    public void testFetchDisconnected() {
        buildRequestManager();

        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE), true);
        networkClientDelegate.poll(time.timer(0));
        assertEmptyFetch("Should not return records on disconnect");
    }

    @Test
    public void testFetchWithLastRecordMissingFromBatch() {
        buildRequestManager();

        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord("0".getBytes(), "v".getBytes()),
                new SimpleRecord("1".getBytes(), "v".getBytes()),
                new SimpleRecord("2".getBytes(), "v".getBytes()),
                new SimpleRecord(null, "value".getBytes()));

        // Remove the last record to simulate compaction
        MemoryRecords.FilterResult result = records.filterTo(new MemoryRecords.RecordFilter(0, 0) {
            @Override
            protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                return new BatchRetentionResult(BatchRetention.DELETE_EMPTY, false);
            }

            @Override
            protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                return record.key() != null;
            }
        }, ByteBuffer.allocate(1024), BufferSupplier.NO_CACHING);
        result.outputBuffer().flip();
        MemoryRecords compactedRecords = MemoryRecords.readableRecords(result.outputBuffer());

        assignFromSubscribed(singleton(tp0));
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0,
                compactedRecords,
                ShareCompletedFetchTest.acquiredRecords(0L, 3),
                Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> allFetchedRecords = fetchRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(3, fetchedRecords.size());

        for (int i = 0; i < 3; i++) {
            assertEquals(Integer.toString(i), new String(fetchedRecords.get(i).key()));
        }
    }

    private MemoryRecords buildRecords(long baseOffset, int count, long firstMessageId) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, baseOffset);
        for (int i = 0; i < count; i++)
            builder.append(0L, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
        return builder.build();
    }

    @Test
    public void testCorruptMessageError() {
        buildRequestManager();
        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        // Prepare a response with the CORRUPT_MESSAGE error.
        client.prepareResponse(fullFetchResponse(
                tip0,
                buildRecords(1L, 1, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 1),
                Errors.CORRUPT_MESSAGE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // Trigger the exception.
        assertThrows(KafkaException.class, this::fetchRecords);
    }

    /**
     * Test the scenario that ShareFetchResponse returns with an error indicating leadership change for the partition,
     * but it does not contain new leader info (defined in KIP-951).
     */
    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER"})
    public void testWhenShareFetchResponseReturnsALeadershipChangeErrorButNoNewLeaderInformation(Errors error) {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        Node tp0Leader = metadata.fetch().leaderFor(tp0);
        Node tp1Leader = metadata.fetch().leaderFor(tp1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId0);
        partitionData.clear();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(error.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());

        assertEquals(startingClusterMetadata, metadata.fetch());

        // Validate metadata update is requested due to the leadership error
        assertTrue(metadata.updateRequested());

        // Move the leadership of tp1 onto node 1
        HashMap<TopicPartition, Metadata.LeaderIdAndEpoch> partitionLeaders = new HashMap<>();
        partitionLeaders.put(tp1, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId0.id()), Optional.of(validLeaderEpoch + 1)));
        LinkedList<Node> leaderNodes = new LinkedList<>(Arrays.asList(tp0Leader, tp1Leader));
        metadata.updatePartitionLeadership(partitionLeaders, leaderNodes);

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // And now the partitions are on the same leader so only one fetch is sent
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(2L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());
        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(1, fetchedRecords.size());
    }

    /**
     * Test the scenario that ShareFetchResponse returns with an error indicating leadership change for the partition,
     * along with new leader info (defined in KIP-951).
     */
    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER"})
    public void testWhenFetchResponseReturnsWithALeadershipChangeErrorAndNewLeaderInformation(Errors error) {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        Node tp0Leader = metadata.fetch().leaderFor(tp0);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId0);
        partitionData.clear();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(error.code())
                .setCurrentLeader(new ShareFetchResponseData.LeaderIdAndEpoch()
                    .setLeaderId(tp0Leader.id())
                    .setLeaderEpoch(validLeaderEpoch + 1)));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, singletonList(tp0Leader), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());

        // The metadata snapshot will have been updated with the new leader information
        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // Validate metadata update is still requested even though the current leader was returned
        assertTrue(metadata.updateRequested());

        // And now the partitions are on the same leader so only one fetch is sent
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(2L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());
        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(1, fetchedRecords.size());
    }

    /**
     * Test the scenario that the metadata indicated a change in leadership between ShareFetch requests such
     * as could occur when metadata is periodically updated.
     */
    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER"})
    public void testWhenLeadershipChangeBetweenShareFetchRequests(Errors error) {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId0);
        partitionData.clear();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());

        assertEquals(startingClusterMetadata, metadata.fetch());

        // Move the leadership of tp0 onto node 1
        HashMap<TopicPartition, Metadata.LeaderIdAndEpoch> partitionLeaders = new HashMap<>();
        partitionLeaders.put(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1)));
        metadata.updatePartitionLeadership(partitionLeaders, List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // Even though the partitions are on the same leader, records were fetched on the previous leader.
        // We do not send those acknowledgements to the previous leader, we fail them with NOT_LEADER_OR_FOLLOWER exception.
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        assertEquals(acknowledgements, completedAcknowledgements.get(0).get(tip0));
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setAcknowledgeErrorCode(error.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId0);
        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());
        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(1, fetchedRecords.size());
    }

    @Test
    void testLeadershipChangeAfterFetchBeforeCommitAsync() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tip0.topicPartition().partition())
                        .setErrorCode(Errors.NONE.code())
                        .setRecords(records)
                        .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                        .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId0);
        partitionData.clear();
        partitionData.put(tip1,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tip1.topicPartition().partition())
                        .setErrorCode(Errors.NONE.code())
                        .setRecords(records)
                        .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 2))
                        .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(2, fetchedRecords.size());

        Acknowledgements acknowledgementsTp0 = Acknowledgements.empty();
        acknowledgementsTp0.add(1L, AcknowledgeType.ACCEPT);

        Acknowledgements acknowledgementsTp1 = getAcknowledgements(1,
                        AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        Map<TopicIdPartition, NodeAcknowledgements> commitAcks = new HashMap<>();
        commitAcks.put(tip0, new NodeAcknowledgements(0, acknowledgementsTp0));
        commitAcks.put(tip1, new NodeAcknowledgements(1, acknowledgementsTp1));

        // Move the leadership of tp0 onto node 1
        HashMap<TopicPartition, Metadata.LeaderIdAndEpoch> partitionLeaders = new HashMap<>();
        partitionLeaders.put(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1)));
        metadata.updatePartitionLeadership(partitionLeaders, List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // We fail the acknowledgements for records which were received from node0 with NOT_LEADER_OR_FOLLOWER exception.
        shareConsumeRequestManager.commitAsync(commitAcks, calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));
        assertEquals(1, completedAcknowledgements.get(0).size());
        assertEquals(acknowledgementsTp0, completedAcknowledgements.get(0).get(tip0));
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        // We only send acknowledgements for tip1 to node1.
        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip1, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(1, completedAcknowledgements.get(1).size());
        assertEquals(acknowledgementsTp1, completedAcknowledgements.get(1).get(tip1));
        assertNull(completedAcknowledgements.get(1).get(tip1).getAcknowledgeException());
    }

    @Test
    void testLeadershipChangeAfterFetchBeforeCommitSync() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0, tp1));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tip0.topicPartition().partition())
                        .setErrorCode(Errors.NONE.code())
                        .setRecords(records)
                        .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                        .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId0);
        partitionData.clear();
        partitionData.put(tip1,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tip1.topicPartition().partition())
                        .setErrorCode(Errors.NONE.code())
                        .setRecords(records)
                        .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 2))
                        .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(2, fetchedRecords.size());

        Acknowledgements acknowledgementsTp0 = Acknowledgements.empty();
        acknowledgementsTp0.add(1L, AcknowledgeType.ACCEPT);

        Acknowledgements acknowledgementsTp1 = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        Map<TopicIdPartition, NodeAcknowledgements> commitAcks = new HashMap<>();
        commitAcks.put(tip0, new NodeAcknowledgements(0, acknowledgementsTp0));
        commitAcks.put(tip1, new NodeAcknowledgements(1, acknowledgementsTp1));

        // Move the leadership of tp0 onto node 1
        HashMap<TopicPartition, Metadata.LeaderIdAndEpoch> partitionLeaders = new HashMap<>();
        partitionLeaders.put(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1)));
        metadata.updatePartitionLeadership(partitionLeaders, List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // We fail the acknowledgements for records which were received from node0 with NOT_LEADER_OR_FOLLOWER exception.
        shareConsumeRequestManager.commitSync(commitAcks, calculateDeadlineMs(time.timer(100)));

        // Verify if the callback was invoked with the failed acknowledgements.
        assertEquals(1, completedAcknowledgements.get(0).size());
        assertEquals(acknowledgementsTp0, completedAcknowledgements.get(0).get(tip0));
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        // We only send acknowledgements for tip1 to node1.
        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip1, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(1, completedAcknowledgements.get(1).size());
        assertEquals(acknowledgementsTp1, completedAcknowledgements.get(1).get(tip1));
        assertNull(completedAcknowledgements.get(1).get(tip1).getAcknowledgeException());
    }

    @Test
    void testLeadershipChangeAfterFetchBeforeClose() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tip0.topicPartition().partition())
                        .setErrorCode(Errors.NONE.code())
                        .setRecords(records)
                        .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                        .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId0);
        partitionData.clear();
        partitionData.put(tip1,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tip1.topicPartition().partition())
                        .setErrorCode(Errors.NONE.code())
                        .setRecords(records)
                        .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 2))
                        .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(2, fetchedRecords.size());

        Acknowledgements acknowledgementsTp0 = Acknowledgements.empty();
        acknowledgementsTp0.add(1L, AcknowledgeType.ACCEPT);

        Acknowledgements acknowledgementsTp1 = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.fetch(Map.of(tip1, new NodeAcknowledgements(1, acknowledgementsTp1)), Collections.emptyMap());

        // Move the leadership of tp0 onto node 1
        HashMap<TopicPartition, Metadata.LeaderIdAndEpoch> partitionLeaders = new HashMap<>();
        partitionLeaders.put(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1)));
        metadata.updatePartitionLeadership(partitionLeaders, List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // We fail the acknowledgements for records which were received from node0 with NOT_LEADER_OR_FOLLOWER exception.
        shareConsumeRequestManager.acknowledgeOnClose(Map.of(tip0, new NodeAcknowledgements(0, acknowledgementsTp0)),
                calculateDeadlineMs(time.timer(100)));

        // Verify if the callback was invoked with the failed acknowledgements.
        assertEquals(1, completedAcknowledgements.get(0).size());
        assertEquals(acknowledgementsTp0.getAcknowledgementsTypeMap(), completedAcknowledgements.get(0).get(tip0).getAcknowledgementsTypeMap());
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
        completedAcknowledgements.clear();

        // As we are closing, we still send the request to both the nodes, but with empty acknowledgements to node0, as it is no longer the leader.
        assertEquals(2, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponseFrom(fullAcknowledgeResponse(tip1, Errors.NONE), nodeId1);
        networkClientDelegate.poll(time.timer(0));

        client.prepareResponseFrom(emptyAcknowledgeResponse(), nodeId0);
        networkClientDelegate.poll(time.timer(0));

        assertEquals(1, completedAcknowledgements.get(0).size());
        assertEquals(acknowledgementsTp1, completedAcknowledgements.get(0).get(tip1));
        assertNull(completedAcknowledgements.get(0).get(tip1).getAcknowledgeException());
    }

    @Test
    void testWhenLeadershipChangedAfterDisconnected() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId0);
        partitionData.clear();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());

        assertEquals(startingClusterMetadata, metadata.fetch());

        acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), Collections.emptyMap());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId0, true);
        partitionData.clear();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // The node was disconnected, so the acknowledgement failed
        assertInstanceOf(DisconnectException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
        completedAcknowledgements.clear();

        partitionRecords = fetchRecords();
        assertFalse(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(1, fetchedRecords.size());

        // Move the leadership of tp0 onto node 1
        HashMap<TopicPartition, Metadata.LeaderIdAndEpoch> partitionLeaders = new HashMap<>();
        partitionLeaders.put(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1)));
        metadata.updatePartitionLeadership(partitionLeaders, List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        shareConsumeRequestManager.fetch(Map.of(tip1, new NodeAcknowledgements(1, acknowledgements)), Collections.emptyMap());

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertNull(completedAcknowledgements.get(0).get(tip1).getAcknowledgeException());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());
    }

    @Test
    public void testCloseInternalClosesShareFetchMetricsManager() throws Exception {
        buildRequestManager();

        // Define all sensor names that should be created and removed
        String[] sensorNames = {
            "fetch-throttle-time",
            "bytes-fetched",
            "records-fetched",
            "fetch-latency",
            "sent-acknowledgements",
            "failed-acknowledgements"
        };

        // Verify that sensors exist before closing
        for (String sensorName : sensorNames) {
            assertNotNull(metrics.getSensor(sensorName),
                "Sensor " + sensorName + " should exist before closing");
        }

        // Close the request manager
        shareConsumeRequestManager.close();

        // Verify that all sensors are removed after closing
        for (String sensorName : sensorNames) {
            assertNull(metrics.getSensor(sensorName),
                "Sensor " + sensorName + " should be removed after closing");
        }
    }

    private ShareFetchResponse fetchResponseWithTopLevelError(TopicIdPartition tp, Errors error) {
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitions = Map.of(tp,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tp.topicPartition().partition())
                        .setErrorCode(error.code()));
        return ShareFetchResponse.of(error, 0, new LinkedHashMap<>(partitions), Collections.emptyList(), 0);
    }

    private ShareFetchResponse fullFetchResponse(TopicIdPartition tp,
                                                 MemoryRecords records,
                                                 List<ShareFetchResponseData.AcquiredRecords> acquiredRecords,
                                                 Errors error) {
        return fullFetchResponse(tp, records, acquiredRecords, error, Errors.NONE);
    }

    private ShareFetchResponse fullFetchResponse(TopicIdPartition tp,
                                                 MemoryRecords records,
                                                 List<ShareFetchResponseData.AcquiredRecords> acquiredRecords,
                                                 Errors error,
                                                 Errors acknowledgeError) {
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitions = Map.of(tp,
                partitionDataForFetch(tp, records, acquiredRecords, error, acknowledgeError));
        return ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), Collections.emptyList(), 0);
    }

    private ShareAcknowledgeResponse emptyAcknowledgeResponse() {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = Collections.emptyMap();
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), Collections.emptyList());
    }

    private ShareAcknowledgeResponse acknowledgeResponseWithTopLevelError(TopicIdPartition tp, Errors error) {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = Map.of(tp,
                partitionDataForAcknowledge(tp, Errors.NONE));
        return ShareAcknowledgeResponse.of(error, 0, new LinkedHashMap<>(partitions), Collections.emptyList());
    }

    private ShareAcknowledgeResponse fullAcknowledgeResponse(TopicIdPartition tp, Errors error) {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = Map.of(tp,
                partitionDataForAcknowledge(tp, error));
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), Collections.emptyList());
    }

    private ShareAcknowledgeResponse fullAcknowledgeResponse(Map<TopicIdPartition, Errors> partitionErrorsMap) {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = new HashMap<>();
        partitionErrorsMap.forEach((tip, error) -> partitions.put(tip, partitionDataForAcknowledge(tip, error)));
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), Collections.emptyList());
    }

    private ShareAcknowledgeResponse fullAcknowledgeResponse(TopicIdPartition tp,
                                                             Errors error,
                                                             ShareAcknowledgeResponseData.LeaderIdAndEpoch currentLeader,
                                                             List<Node> nodeEndpoints) {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = Map.of(tp,
            partitionDataForAcknowledge(tp, error, currentLeader));
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), nodeEndpoints);
    }

    private ShareFetchResponseData.PartitionData partitionDataForFetch(TopicIdPartition tp,
                                                                       MemoryRecords records,
                                                                       List<ShareFetchResponseData.AcquiredRecords> acquiredRecords,
                                                                       Errors error,
                                                                       Errors acknowledgeError) {
        return new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tp.topicPartition().partition())
                .setErrorCode(error.code())
                .setAcknowledgeErrorCode(acknowledgeError.code())
                .setRecords(records)
                .setAcquiredRecords(acquiredRecords);
    }

    private ShareAcknowledgeResponseData.PartitionData partitionDataForAcknowledge(TopicIdPartition tp, Errors error) {
        return new ShareAcknowledgeResponseData.PartitionData()
                .setPartitionIndex(tp.topicPartition().partition())
                .setErrorCode(error.code());
    }

    private ShareAcknowledgeResponseData.PartitionData partitionDataForAcknowledge(TopicIdPartition tp,
                                                                                   Errors error,
                                                                                   ShareAcknowledgeResponseData.LeaderIdAndEpoch currentLeader) {
        return new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(tp.topicPartition().partition())
            .setErrorCode(error.code())
            .setCurrentLeader(currentLeader);
    }

    /**
     * Assert that the {@link ShareFetchCollector#collect(ShareFetchBuffer) latest fetch} does not contain any
     * {@link ShareFetch#records() user-visible records}, and is {@link ShareFetch#isEmpty() empty}.
     *
     * @param reason the reason to include for assertion methods such as {@link org.junit.jupiter.api.Assertions#assertTrue(boolean, String)}
     */
    private void assertEmptyFetch(String reason) {
        ShareFetch<?, ?> fetch = collectFetch();
        assertEquals(Collections.emptyMap(), fetch.records(), reason);
        assertTrue(fetch.isEmpty(), reason);
    }

    private Acknowledgements getAcknowledgements(int startIndex, AcknowledgeType... acknowledgeTypes) {
        Acknowledgements acknowledgements = Acknowledgements.empty();
        int index = startIndex;
        for (AcknowledgeType type : acknowledgeTypes) {
            acknowledgements.add(index++, type);
        }
        return acknowledgements;
    }

    private <K, V> Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchRecords() {
        ShareFetch<K, V> fetch = collectFetch();
        if (fetch.isEmpty()) {
            return Collections.emptyMap();
        }
        return fetch.records();
    }

    @SuppressWarnings("unchecked")
    private <K, V> ShareFetch<K, V> collectFetch() {
        return (ShareFetch<K, V>) shareConsumeRequestManager.collectFetch();
    }

    private void buildRequestManager() {
        buildRequestManager(new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private <K, V> void buildRequestManager(Deserializer<K> keyDeserializer,
                                            Deserializer<V> valueDeserializer) {
        buildRequestManager(new MetricConfig(), keyDeserializer, valueDeserializer);
    }

    private <K, V> void buildRequestManager(MetricConfig metricConfig,
                                            Deserializer<K> keyDeserializer,
                                            Deserializer<V> valueDeserializer) {
        LogContext logContext = new LogContext();
        SubscriptionState subscriptionState = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        buildRequestManager(metricConfig, keyDeserializer, valueDeserializer,
                subscriptionState, logContext);
    }

    private <K, V> void buildRequestManager(MetricConfig metricConfig,
                                            Deserializer<K> keyDeserializer,
                                            Deserializer<V> valueDeserializer,
                                            SubscriptionState subscriptionState,
                                            LogContext logContext) {
        buildDependencies(metricConfig, subscriptionState, logContext);
        Deserializers<K, V> deserializers = new Deserializers<>(keyDeserializer, valueDeserializer, metrics);
        int maxWaitMs = 0;
        int maxBytes = Integer.MAX_VALUE;
        int fetchSize = 1000;
        int minBytes = 1;
        FetchConfig fetchConfig = new FetchConfig(
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                Integer.MAX_VALUE,
                true, // check crc
                CommonClientConfigs.DEFAULT_CLIENT_RACK,
                IsolationLevel.READ_UNCOMMITTED);
        ShareFetchCollector<K, V> shareFetchCollector = new ShareFetchCollector<>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                deserializers);
        BackgroundEventHandler backgroundEventHandler = new TestableBackgroundEventHandler(time, completedAcknowledgements);
        shareConsumeRequestManager = spy(new TestableShareConsumeRequestManager<>(
                logContext,
                groupId,
                metadata,
                subscriptionState,
                fetchConfig,
                new ShareFetchBuffer(logContext),
                backgroundEventHandler,
                metricsManager,
                shareFetchCollector));
    }

    private void buildDependencies(MetricConfig metricConfig,
                                   SubscriptionState subscriptionState,
                                   LogContext logContext) {
        time = new MockTime(1, 0, 0);
        subscriptions = subscriptionState;
        metadata = new ShareConsumerMetadata(0, 0, Long.MAX_VALUE, false,
                subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        metrics = new Metrics(metricConfig, time);
        shareFetchMetricsRegistry = new ShareFetchMetricsRegistry(metricConfig.tags().keySet(), "consumer-share" + groupId);
        metricsManager = new ShareFetchMetricsManager(metrics, shareFetchMetricsRegistry);

        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
        properties.setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs));
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        ConsumerConfig config = new ConsumerConfig(properties);
        networkClientDelegate = spy(new TestableNetworkClientDelegate(
            time, config, logContext, client, metadata,
            new BackgroundEventHandler(new LinkedBlockingQueue<>(), time, mock(AsyncConsumerMetrics.class)), false));
    }

    private class TestableShareConsumeRequestManager<K, V> extends ShareConsumeRequestManager {

        private final ShareFetchCollector<K, V> shareFetchCollector;

        public TestableShareConsumeRequestManager(LogContext logContext,
                                                  String groupId,
                                                  ShareConsumerMetadata metadata,
                                                  SubscriptionState subscriptions,
                                                  FetchConfig fetchConfig,
                                                  ShareFetchBuffer shareFetchBuffer,
                                                  BackgroundEventHandler backgroundEventHandler,
                                                  ShareFetchMetricsManager metricsManager,
                                                  ShareFetchCollector<K, V> fetchCollector) {
            super(time, logContext, groupId, metadata, subscriptions, fetchConfig, shareFetchBuffer,
                    backgroundEventHandler, metricsManager, retryBackoffMs, 1000);
            this.shareFetchCollector = fetchCollector;
            onMemberEpochUpdated(Optional.empty(), Uuid.randomUuid().toString());
        }

        private ShareFetch<K, V> collectFetch() {
            return shareFetchCollector.collect(shareFetchBuffer);
        }

        private int sendFetches() {
            fetch(new HashMap<>(), new HashMap<>());
            NetworkClientDelegate.PollResult pollResult = poll(time.milliseconds());
            networkClientDelegate.addAll(pollResult.unsentRequests);
            return pollResult.unsentRequests.size();
        }

        private NetworkClientDelegate.PollResult sendFetchesReturnPollResult() {
            fetch(new HashMap<>(), new HashMap<>());
            NetworkClientDelegate.PollResult pollResult = poll(time.milliseconds());
            networkClientDelegate.addAll(pollResult.unsentRequests);
            return pollResult;
        }

        private int sendAcknowledgements() {
            NetworkClientDelegate.PollResult pollResult = poll(time.milliseconds());
            networkClientDelegate.addAll(pollResult.unsentRequests);
            return pollResult.unsentRequests.size();
        }

        public ResultHandler buildResultHandler(final AtomicInteger remainingResults,
                                                final Optional<CompletableFuture<Map<TopicIdPartition, Acknowledgements>>> future) {
            return new ResultHandler(remainingResults, future);
        }

        public Tuple<AcknowledgeRequestState> requestStates(int nodeId) {
            return super.requestStates(nodeId);
        }
    }

    private class TestableNetworkClientDelegate extends NetworkClientDelegate {
        private final ConcurrentLinkedQueue<Node> pendingDisconnects = new ConcurrentLinkedQueue<>();

        public TestableNetworkClientDelegate(Time time,
                                             ConsumerConfig config,
                                             LogContext logContext,
                                             KafkaClient client,
                                             Metadata metadata,
                                             BackgroundEventHandler backgroundEventHandler,
                                             boolean notifyMetadataErrorsViaErrorQueue) {
            super(time, config, logContext, client, metadata, backgroundEventHandler, notifyMetadataErrorsViaErrorQueue, mock(AsyncConsumerMetrics.class));
        }

        @Override
        public void poll(final long timeoutMs, final long currentTimeMs) {
            handlePendingDisconnects();
            super.poll(timeoutMs, currentTimeMs);
        }

        public void poll(final Timer timer) {
            long pollTimeout = Math.min(timer.remainingMs(), requestTimeoutMs);
            if (client.inFlightRequestCount() == 0)
                pollTimeout = Math.min(pollTimeout, retryBackoffMs);
            poll(pollTimeout, timer.currentTimeMs());
        }

        private Set<Node> unsentRequestNodes() {
            Set<Node> set = new HashSet<>();

            for (UnsentRequest u : unsentRequests())
                u.node().ifPresent(set::add);

            return set;
        }

        private List<UnsentRequest> removeUnsentRequestByNode(Node node) {
            List<UnsentRequest> list = new ArrayList<>();

            Iterator<UnsentRequest> it = unsentRequests().iterator();

            while (it.hasNext()) {
                UnsentRequest u = it.next();

                if (node.equals(u.node().orElse(null))) {
                    it.remove();
                    list.add(u);
                }
            }

            return list;
        }

        @Override
        protected void checkDisconnects(final long currentTimeMs) {
            // any disconnects affecting requests that have already been transmitted will be handled
            // by NetworkClient, so we just need to check whether connections for any of the unsent
            // requests have been disconnected; if they have, then we complete the corresponding future
            // and set the disconnect flag in the ClientResponse
            for (Node node : unsentRequestNodes()) {
                if (client.connectionFailed(node)) {
                    // Remove entry before invoking request callback to avoid callbacks handling
                    // coordinator failures traversing the unsent list again.
                    for (UnsentRequest unsentRequest : removeUnsentRequestByNode(node)) {
                        FutureCompletionHandler handler = unsentRequest.handler();
                        AuthenticationException authenticationException = client.authenticationException(node);
                        long startMs = unsentRequest.timer().currentTimeMs() - unsentRequest.timer().elapsedMs();
                        handler.onComplete(new ClientResponse(makeHeader(unsentRequest.requestBuilder().latestAllowedVersion()),
                                unsentRequest.handler(), unsentRequest.node().toString(), startMs, currentTimeMs, true,
                                null, authenticationException, null));
                    }
                }
            }
        }

        private RequestHeader makeHeader(short version) {
            return new RequestHeader(
                    new RequestHeaderData()
                            .setRequestApiKey(ApiKeys.SHARE_FETCH.id)
                            .setRequestApiVersion(version),
                    ApiKeys.SHARE_FETCH.requestHeaderVersion(version));
        }

        private void handlePendingDisconnects() {
            while (true) {
                Node node = pendingDisconnects.poll();
                if (node == null)
                    break;

                failUnsentRequests(node);
                client.disconnect(node.idString());
            }
        }

        private void failUnsentRequests(Node node) {
            // clear unsent requests to node and fail their corresponding futures
            for (UnsentRequest unsentRequest : removeUnsentRequestByNode(node)) {
                FutureCompletionHandler handler = unsentRequest.handler();
                handler.onFailure(time.milliseconds(), DisconnectException.INSTANCE);
            }
        }
    }

    private static class TestableBackgroundEventHandler extends BackgroundEventHandler {
        List<Map<TopicIdPartition, Acknowledgements>> completedAcknowledgements;

        public TestableBackgroundEventHandler(Time time, List<Map<TopicIdPartition, Acknowledgements>> completedAcknowledgements) {
            super(new LinkedBlockingQueue<>(), time, mock(AsyncConsumerMetrics.class));
            this.completedAcknowledgements = completedAcknowledgements;
        }

        public void add(BackgroundEvent event) {
            if (event.type() == BackgroundEvent.Type.SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK) {
                ShareAcknowledgementCommitCallbackEvent shareAcknowledgementCommitCallbackEvent = (ShareAcknowledgementCommitCallbackEvent) event;
                completedAcknowledgements.add(shareAcknowledgementCommitCallbackEvent.acknowledgementsMap());
            }
        }
    }

    @Test
    void testFetchWithControlRecords() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        Map<TopicIdPartition, NodeAcknowledgements> nodeAcknowledgementsMap = new HashMap<>();

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        nodeAcknowledgementsMap.put(tip0, new NodeAcknowledgements(0, acknowledgements));

        Map<TopicIdPartition, NodeAcknowledgements> nodeAcknowledgementsControlRecordMap = new HashMap<>();

        Acknowledgements controlAcknowledgements = Acknowledgements.empty();
        controlAcknowledgements.addGap(2L);
        nodeAcknowledgementsControlRecordMap.put(tip0, new NodeAcknowledgements(0, controlAcknowledgements));

        shareConsumeRequestManager.fetch(nodeAcknowledgementsMap, nodeAcknowledgementsControlRecordMap);

        Map<TopicIdPartition, Acknowledgements> fetchAcksToSend = shareConsumeRequestManager.getFetchAcknowledgementsToSend(0);
        assertEquals(1, fetchAcksToSend.size());
        assertEquals(AcknowledgeType.ACCEPT, fetchAcksToSend.get(tip0).get(1L));
        assertEquals(2, fetchAcksToSend.get(tip0).size());
        assertNull(fetchAcksToSend.get(tip0).get(3L));
    }

    private void sendFetchAndVerifyResponse(MemoryRecords records,
                                    List<ShareFetchResponseData.AcquiredRecords> acquiredRecords,
                                    Errors... error) {
        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        if (error.length > 1) {
            client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, error[0], error[1]));
        } else {
            client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, error[0]));
        }
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
    }

}
