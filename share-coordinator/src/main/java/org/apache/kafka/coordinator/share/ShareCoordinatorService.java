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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
import org.apache.kafka.common.message.DeleteShareGroupStateResponseData;
import org.apache.kafka.common.message.InitializeShareGroupStateRequestData;
import org.apache.kafka.common.message.InitializeShareGroupStateResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteShareGroupStateResponse;
import org.apache.kafka.common.requests.InitializeShareGroupStateResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateSummaryResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorEventProcessor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilderSupplier;
import org.apache.kafka.coordinator.common.runtime.MultiThreadedEventProcessor;
import org.apache.kafka.coordinator.common.runtime.PartitionWriter;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.server.common.ShareVersion;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static org.apache.kafka.coordinator.common.runtime.CoordinatorOperationExceptionHelper.handleOperationException;

@SuppressWarnings("ClassDataAbstractionCoupling")
public class ShareCoordinatorService implements ShareCoordinator {
    private final ShareCoordinatorConfig config;
    private final Logger log;
    private final AtomicBoolean isActive = new AtomicBoolean(false);  // for controlling start and stop
    private final CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime;
    private final ShareCoordinatorMetrics shareCoordinatorMetrics;
    private volatile int numPartitions = -1; // Number of partitions for __share_group_state. Provided when component is started.
    private final Time time;
    private final Timer timer;
    private final PartitionWriter writer;
    private final Map<TopicPartition, Long> lastPrunedOffsets;
    private final Supplier<Boolean> shareGroupConfigEnabledSupplier;
    private volatile boolean shouldRunPeriodicJob;

    public static class Builder {
        private final int nodeId;
        private final ShareCoordinatorConfig config;
        private PartitionWriter writer;
        private CoordinatorLoader<CoordinatorRecord> loader;
        private Time time;
        private Timer timer;
        private Supplier<Boolean> shareGroupConfigEnabledSupplier;
        private ShareCoordinatorMetrics coordinatorMetrics;
        private CoordinatorRuntimeMetrics coordinatorRuntimeMetrics;

        public Builder(int nodeId, ShareCoordinatorConfig config) {
            this.nodeId = nodeId;
            this.config = config;
        }

        public Builder withWriter(PartitionWriter writer) {
            this.writer = writer;
            return this;
        }

        public Builder withLoader(CoordinatorLoader<CoordinatorRecord> loader) {
            this.loader = loader;
            return this;
        }

        public Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        public Builder withCoordinatorMetrics(ShareCoordinatorMetrics coordinatorMetrics) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        public Builder withCoordinatorRuntimeMetrics(CoordinatorRuntimeMetrics coordinatorRuntimeMetrics) {
            this.coordinatorRuntimeMetrics = coordinatorRuntimeMetrics;
            return this;
        }

        public Builder withShareGroupEnabledConfigSupplier(Supplier<Boolean> shareGroupConfigEnabledSupplier) {
            this.shareGroupConfigEnabledSupplier = shareGroupConfigEnabledSupplier;
            return this;
        }

        public ShareCoordinatorService build() {
            if (config == null) {
                throw new IllegalArgumentException("Config must be set.");
            }
            if (writer == null) {
                throw new IllegalArgumentException("Writer must be set.");
            }
            if (loader == null) {
                throw new IllegalArgumentException("Loader must be set.");
            }
            if (time == null) {
                throw new IllegalArgumentException("Time must be set.");
            }
            if (timer == null) {
                throw new IllegalArgumentException("Timer must be set.");
            }
            if (coordinatorMetrics == null) {
                throw new IllegalArgumentException("Share Coordinator metrics must be set.");
            }
            if (coordinatorRuntimeMetrics == null) {
                throw new IllegalArgumentException("Coordinator runtime metrics must be set.");
            }
            if (shareGroupConfigEnabledSupplier == null) {
                throw new IllegalArgumentException("Share group enabled config enabled supplier must be set.");
            }

            String logPrefix = String.format("ShareCoordinator id=%d", nodeId);
            LogContext logContext = new LogContext(String.format("[%s] ", logPrefix));

            CoordinatorShardBuilderSupplier<ShareCoordinatorShard, CoordinatorRecord> supplier = () ->
                new ShareCoordinatorShard.Builder(config);

            CoordinatorEventProcessor processor = new MultiThreadedEventProcessor(
                logContext,
                "share-coordinator-event-processor-",
                config.shareCoordinatorNumThreads(),
                time,
                coordinatorRuntimeMetrics
            );

            CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime =
                new CoordinatorRuntime.Builder<ShareCoordinatorShard, CoordinatorRecord>()
                    .withTime(time)
                    .withTimer(timer)
                    .withLogPrefix(logPrefix)
                    .withLogContext(logContext)
                    .withEventProcessor(processor)
                    .withPartitionWriter(writer)
                    .withLoader(loader)
                    .withCoordinatorShardBuilderSupplier(supplier)
                    .withTime(time)
                    .withDefaultWriteTimeOut(Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()))
                    .withCoordinatorRuntimeMetrics(coordinatorRuntimeMetrics)
                    .withCoordinatorMetrics(coordinatorMetrics)
                    .withSerializer(new ShareCoordinatorRecordSerde())
                    .withCompression(Compression.of(config.shareCoordinatorStateTopicCompressionType()).build())
                    .withAppendLingerMs(config.shareCoordinatorAppendLingerMs())
                    .withExecutorService(Executors.newSingleThreadExecutor())
                    .build();

            return new ShareCoordinatorService(
                logContext,
                config,
                runtime,
                coordinatorMetrics,
                time,
                timer,
                writer,
                shareGroupConfigEnabledSupplier
            );
        }
    }

    public ShareCoordinatorService(
        LogContext logContext,
        ShareCoordinatorConfig config,
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime,
        ShareCoordinatorMetrics shareCoordinatorMetrics,
        Time time,
        Timer timer,
        PartitionWriter writer,
        Supplier<Boolean> shareGroupConfigEnabledSupplier
    ) {
        this.log = logContext.logger(ShareCoordinatorService.class);
        this.config = config;
        this.runtime = runtime;
        this.shareCoordinatorMetrics = shareCoordinatorMetrics;
        this.time = time;
        this.timer = timer;
        this.writer = writer;
        this.lastPrunedOffsets = new ConcurrentHashMap<>();
        this.shareGroupConfigEnabledSupplier = shareGroupConfigEnabledSupplier;
    }

    @Override
    public int partitionFor(SharePartitionKey key) {
        throwIfNotActive();
        // Call to asCoordinatorKey is necessary as we depend only on topicId (Uuid) and
        // not topic name. We do not want this calculation to distinguish between 2
        // SharePartitionKeys where everything except topic name is the same.
        return Utils.abs(key.asCoordinatorKey().hashCode()) % numPartitions;
    }

    @Override
    public Properties shareGroupStateTopicConfigs() {
        Properties properties = new Properties();
        // As defined in KIP-932.
        properties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        properties.put(TopicConfig.COMPRESSION_TYPE_CONFIG, BrokerCompressionType.PRODUCER.name);
        properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, config.shareCoordinatorStateTopicSegmentBytes());
        properties.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, config.shareCoordinatorStateTopicMinIsr());
        properties.put(TopicConfig.RETENTION_MS_CONFIG, -1);
        return properties;
    }

    /**
     * The share coordinator startup method will get invoked from BrokerMetadataPublisher.
     * At the time of writing, the publisher uses metadata cache to fetch the number of partitions
     * of the share state topic. In case this information is not available, the user provided
     * config will be used to fetch the value.
     * This is consistent with the group coordinator startup functionality.
     *
     * @param shareGroupTopicPartitionCount - supplier returning the number of partitions for __share_group_state topic
     */
    @Override
    public void startup(
        IntSupplier shareGroupTopicPartitionCount
    ) {
        if (!isActive.compareAndSet(false, true)) {
            log.warn("Share coordinator is already running.");
            return;
        }

        log.info("Starting up.");
        numPartitions = shareGroupTopicPartitionCount.getAsInt();
        log.info("Startup complete.");
    }

    private void setupPeriodicJobs() {
        setupRecordPruning();
        setupSnapshotColdPartitions();
    }

    // Visibility for tests
    void setupRecordPruning() {
        log.debug("Scheduling share-group state topic prune job.");
        timer.add(new TimerTask(config.shareCoordinatorTopicPruneIntervalMs()) {
            @Override
            public void run() {
                if (!shouldRunPeriodicJob) {
                    return;
                }
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                runtime.activeTopicPartitions().forEach(tp -> futures.add(performRecordPruning(tp)));

                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[]{}))
                    .whenComplete((res, exp) -> {
                        if (exp != null) {
                            log.error("Received error in share-group state topic prune.", exp);
                        }
                        // Perpetual recursion, failure or not.
                        setupRecordPruning();
                    });
            }
        });
    }

    private CompletableFuture<Void> performRecordPruning(TopicPartition tp) {
        CompletableFuture<Void> fut = new CompletableFuture<>();

        runtime.scheduleWriteOperation(
            "write-state-record-prune",
            tp,
            Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()),
            ShareCoordinatorShard::lastRedundantOffset
        ).whenComplete((result, exception) -> {
            if (exception != null) {
                log.debug("Last redundant offset for tp {} lookup threw an error.", tp, exception);
                Errors error = Errors.forException(exception);
                // These errors might result from partition metadata not loaded
                // or shard re-election. Will cause unnecessary noise, hence not logging
                if (!(error.equals(Errors.COORDINATOR_LOAD_IN_PROGRESS) || error.equals(Errors.NOT_COORDINATOR))) {
                    log.error("Last redundant offset lookup for tp {} threw an error.", tp, exception);
                    fut.completeExceptionally(exception);
                    return;
                }
                fut.complete(null);
                return;
            }
            if (result.isPresent()) {
                Long off = result.get();
                Long lastPrunedOffset = lastPrunedOffsets.get(tp);
                if (lastPrunedOffset != null && lastPrunedOffset.longValue() == off) {
                    log.debug("{} already pruned till offset {}", tp, off);
                    fut.complete(null);
                    return;
                }

                log.debug("Pruning records in {} till offset {}.", tp, off);
                writer.deleteRecords(tp, off)
                    .whenComplete((res, exp) -> {
                        if (exp != null) {
                            log.error("Exception while deleting records in {} till offset {}.", tp, off, exp);
                            fut.completeExceptionally(exp);
                            return;
                        }
                        shareCoordinatorMetrics.recordPrune(
                            off,
                            tp
                        );
                        fut.complete(null);
                        // Best effort prevention of issuing duplicate delete calls.
                        lastPrunedOffsets.put(tp, off);
                    });
            } else {
                log.debug("No offset value for tp {} found.", tp);
                fut.complete(null);
            }
        });
        return fut;
    }

    // Visibility for tests
    void setupSnapshotColdPartitions() {
        log.debug("Scheduling cold share-partition snapshotting.");
        timer.add(new TimerTask(config.shareCoordinatorColdPartitionSnapshotIntervalMs()) {
            @Override
            public void run() {
                if (!shouldRunPeriodicJob) {
                    return;
                }
                List<CompletableFuture<Void>> futures = runtime.scheduleWriteAllOperation(
                    "snapshot-cold-partitions",
                    Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()),
                    ShareCoordinatorShard::snapshotColdPartitions
                );

                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[]{}))
                    .whenComplete((__, exp) -> {
                        if (exp != null) {
                            log.error("Received error while snapshotting cold partitions.", exp);
                        }
                        setupSnapshotColdPartitions();
                    });
            }
        });
    }

    @Override
    public void shutdown() {
        if (!isActive.compareAndSet(true, false)) {
            log.warn("Share coordinator is already shutting down.");
            return;
        }

        log.info("Shutting down.");
        Utils.closeQuietly(runtime, "coordinator runtime");
        Utils.closeQuietly(shareCoordinatorMetrics, "share coordinator metrics");
        log.info("Shutdown complete.");
    }

    @Override
    public CompletableFuture<WriteShareGroupStateResponseData> writeState(RequestContext context, WriteShareGroupStateRequestData request) {
        // Send an empty response if topic data is empty
        if (isEmpty(request.topics())) {
            log.error("Topic Data is empty: {}", request);
            return CompletableFuture.completedFuture(
                new WriteShareGroupStateResponseData()
            );
        }

        // Send an empty response if partition data is empty for any topic
        for (WriteShareGroupStateRequestData.WriteStateData topicData : request.topics()) {
            if (isEmpty(topicData.partitions())) {
                log.error("Partition Data for topic {} is empty: {}", topicData.topicId(), request);
                return CompletableFuture.completedFuture(
                    new WriteShareGroupStateResponseData()
                );
            }
        }

        String groupId = request.groupId();
        // Send an empty response if groupId is invalid
        if (isGroupIdEmpty(groupId)) {
            log.error("Group id must be specified and non-empty: {}", request);
            return CompletableFuture.completedFuture(
                new WriteShareGroupStateResponseData()
            );
        }

        // Send an empty response if the coordinator is not active
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(
                generateErrorWriteStateResponse(
                    request,
                    Errors.COORDINATOR_NOT_AVAILABLE,
                    "Share coordinator is not available."
                )
            );
        }

        // The request received here could have multiple keys of structure group:topic:partition. However,
        // the writeState method in ShareCoordinatorShard expects a single key in the request. Hence, we will
        // be looping over the keys below and constructing new WriteShareGroupStateRequestData objects to pass
        // onto the shard method.
        Map<Uuid, Map<Integer, CompletableFuture<WriteShareGroupStateResponseData>>> futureMap = new HashMap<>();
        long startTimeMs = time.hiResClockMs();

        request.topics().forEach(topicData -> {
            Map<Integer, CompletableFuture<WriteShareGroupStateResponseData>> partitionFut =
                futureMap.computeIfAbsent(topicData.topicId(), k -> new HashMap<>());
            topicData.partitions().forEach(
                partitionData -> {
                    CompletableFuture<WriteShareGroupStateResponseData> future = runtime.scheduleWriteOperation(
                            "write-share-group-state",
                            topicPartitionFor(SharePartitionKey.getInstance(groupId, topicData.topicId(), partitionData.partition())),
                            Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()),
                            coordinator -> coordinator.writeState(new WriteShareGroupStateRequestData()
                                .setGroupId(groupId)
                                .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                                    .setTopicId(topicData.topicId())
                                    .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                                        .setPartition(partitionData.partition())
                                        .setStartOffset(partitionData.startOffset())
                                        .setLeaderEpoch(partitionData.leaderEpoch())
                                        .setStateEpoch(partitionData.stateEpoch())
                                        .setStateBatches(partitionData.stateBatches())))))))
                        .exceptionally(exception -> handleOperationException(
                            "write-share-group-state",
                            request,
                            exception,
                            (error, message) -> WriteShareGroupStateResponse.toErrorResponseData(
                                topicData.topicId(),
                                partitionData.partition(),
                                error,
                                "Unable to write share group state: " + exception.getMessage()
                            ),
                            log
                        ));
                    partitionFut.put(partitionData.partition(), future);
                });
        });

        // Combine all futures into a single CompletableFuture<Void>.
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futureMap.values().stream()
            .flatMap(partMap -> partMap.values().stream()).toArray(CompletableFuture[]::new));

        // topicId -> {partitionId -> responseFuture}
        return combinedFuture.thenApply(v -> {
            List<WriteShareGroupStateResponseData.WriteStateResult> writeStateResults = new ArrayList<>(futureMap.size());
            futureMap.forEach(
                (topicId, topicEntry) -> {
                    List<WriteShareGroupStateResponseData.PartitionResult> partitionResults = new ArrayList<>(topicEntry.size());
                    topicEntry.forEach(
                        // Map of partition id -> responses from api.
                        (partitionId, responseFut) -> {
                            // This is the future returned by runtime.scheduleWriteOperation which returns when the
                            // operation has completed including error information. When this line executes, the future
                            // should be complete as we used CompletableFuture::allOf to get a combined future from
                            // all futures in the map.
                            WriteShareGroupStateResponseData partitionData = responseFut.getNow(null);
                            partitionResults.addAll(partitionData.results().get(0).partitions());
                        }
                    );
                    writeStateResults.add(WriteShareGroupStateResponse.toResponseWriteStateResult(topicId, partitionResults));
                }
            );

            // Time taken for write.
            // At this point all futures are completed written above.
            shareCoordinatorMetrics.record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_LATENCY_SENSOR_NAME,
                time.hiResClockMs() - startTimeMs);

            return new WriteShareGroupStateResponseData()
                .setResults(writeStateResults);
        });
    }

    @Override
    public CompletableFuture<ReadShareGroupStateResponseData> readState(RequestContext context, ReadShareGroupStateRequestData request) {
        String groupId = request.groupId();
        // A map to store the futures for each topicId and partition.
        Map<Uuid, Map<Integer, CompletableFuture<ReadShareGroupStateResponseData>>> futureMap = new HashMap<>();

        // Send an empty response if topic data is empty.
        if (isEmpty(request.topics())) {
            log.error("Topic Data is empty: {}", request);
            return CompletableFuture.completedFuture(
                new ReadShareGroupStateResponseData()
            );
        }

        // Send an empty response if partition data is empty for any topic.
        for (ReadShareGroupStateRequestData.ReadStateData topicData : request.topics()) {
            if (isEmpty(topicData.partitions())) {
                log.error("Partition Data for topic {} is empty: {}", topicData.topicId(), request);
                return CompletableFuture.completedFuture(
                    new ReadShareGroupStateResponseData()
                );
            }
        }

        // Send an empty response if groupId is invalid.
        if (isGroupIdEmpty(groupId)) {
            log.error("Group id must be specified and non-empty: {}", request);
            return CompletableFuture.completedFuture(
                new ReadShareGroupStateResponseData()
            );
        }

        // Send an empty response if the coordinator is not active.
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(
                generateErrorReadStateResponse(
                    request,
                    Errors.COORDINATOR_NOT_AVAILABLE,
                    "Share coordinator is not available."
                )
            );
        }

        // The request received here could have multiple keys of structure group:topic:partition. However,
        // the readState method in ShareCoordinatorShard expects a single key in the request. Hence, we will
        // be looping over the keys below and constructing new ReadShareGroupStateRequestData objects to pass
        // onto the shard method.

        // It is possible that a read state request contains a leaderEpoch which is the higher than seen so
        // far, for a specific share partition. Hence, for each read request - we must check for this
        // and update the state appropriately.

        for (ReadShareGroupStateRequestData.ReadStateData topicData : request.topics()) {
            Uuid topicId = topicData.topicId();
            for (ReadShareGroupStateRequestData.PartitionData partitionData : topicData.partitions()) {
                SharePartitionKey coordinatorKey = SharePartitionKey.getInstance(request.groupId(), topicId, partitionData.partition());

                ReadShareGroupStateRequestData requestForCurrentPartition = new ReadShareGroupStateRequestData()
                    .setGroupId(groupId)
                    .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                        .setTopicId(topicId)
                        .setPartitions(List.of(partitionData))));

                // We are issuing a scheduleWriteOperation even though the request is of read type since
                // we might want to update the leader epoch, if it is the highest seen so far for the specific
                // share partition. In that case, we require the strong consistency offered by scheduleWriteOperation.
                // At the time of writing, read after write consistency for the readState and writeState requests
                // is not guaranteed.
                CompletableFuture<ReadShareGroupStateResponseData> readFuture = runtime.scheduleWriteOperation(
                    "read-update-leader-epoch-state",
                    topicPartitionFor(coordinatorKey),
                    Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()),
                    coordinator -> coordinator.readStateAndMaybeUpdateLeaderEpoch(requestForCurrentPartition)
                ).exceptionally(readException ->
                    handleOperationException(
                        "read-update-leader-epoch-state",
                        request,
                        readException,
                        (error, message) -> ReadShareGroupStateResponse.toErrorResponseData(
                            topicData.topicId(),
                            partitionData.partition(),
                            error,
                            "Unable to read share group state: " + readException.getMessage()
                        ),
                        log
                    ));

                futureMap.computeIfAbsent(topicId, k -> new HashMap<>())
                    .put(partitionData.partition(), readFuture);
            }
        }

        // Combine all futures into a single CompletableFuture<Void>.
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futureMap.values().stream()
            .flatMap(map -> map.values().stream()).toArray(CompletableFuture[]::new));

        // Transform the combined CompletableFuture<Void> into CompletableFuture<ReadShareGroupStateResponseData>.
        return combinedFuture.thenApply(v -> {
            List<ReadShareGroupStateResponseData.ReadStateResult> readStateResult = new ArrayList<>(futureMap.size());
            futureMap.forEach(
                (topicId, topicEntry) -> {
                    List<ReadShareGroupStateResponseData.PartitionResult> partitionResults = new ArrayList<>(topicEntry.size());
                    topicEntry.forEach(
                        (partitionId, responseFut) -> {
                            // ResponseFut would already be completed by now since we have used
                            // CompletableFuture::allOf to create a combined future from the future map.
                            partitionResults.add(
                                responseFut.getNow(null).results().get(0).partitions().get(0)
                            );
                        }
                    );
                    readStateResult.add(ReadShareGroupStateResponse.toResponseReadStateResult(topicId, partitionResults));
                }
            );
            return new ReadShareGroupStateResponseData()
                .setResults(readStateResult);
        });
    }

    @Override
    public CompletableFuture<ReadShareGroupStateSummaryResponseData> readStateSummary(RequestContext context, ReadShareGroupStateSummaryRequestData request) {
        // Send an empty response if the coordinator is not active.
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(
                generateErrorReadStateSummaryResponse(
                    request,
                    Errors.COORDINATOR_NOT_AVAILABLE,
                    "Share coordinator is not available."
                )
            );
        }

        String groupId = request.groupId();
        // Send an empty response if groupId is invalid.
        if (isGroupIdEmpty(groupId)) {
            log.error("Group id must be specified and non-empty: {}", request);
            return CompletableFuture.completedFuture(
                new ReadShareGroupStateSummaryResponseData()
            );
        }

        // Send an empty response if topic data is empty.
        if (isEmpty(request.topics())) {
            log.error("Topic Data is empty: {}", request);
            return CompletableFuture.completedFuture(
                new ReadShareGroupStateSummaryResponseData()
            );
        }

        // Send an empty response if partition data is empty for any topic.
        for (ReadShareGroupStateSummaryRequestData.ReadStateSummaryData topicData : request.topics()) {
            if (isEmpty(topicData.partitions())) {
                log.error("Partition Data for topic {} is empty: {}", topicData.topicId(), request);
                return CompletableFuture.completedFuture(
                    new ReadShareGroupStateSummaryResponseData()
                );
            }
        }

        // A map to store the futures for each topicId and partition.
        Map<Uuid, Map<Integer, CompletableFuture<ReadShareGroupStateSummaryResponseData>>> futureMap = new HashMap<>();

        // The request received here could have multiple keys of structure group:topic:partition. However,
        // the readStateSummary method in ShareCoordinatorShard expects a single key in the request. Hence, we will
        // be looping over the keys below and constructing new ReadShareGroupStateSummaryRequestData objects to pass
        // onto the shard method.

        for (ReadShareGroupStateSummaryRequestData.ReadStateSummaryData topicData : request.topics()) {
            Uuid topicId = topicData.topicId();
            for (ReadShareGroupStateSummaryRequestData.PartitionData partitionData : topicData.partitions()) {
                SharePartitionKey coordinatorKey = SharePartitionKey.getInstance(request.groupId(), topicId, partitionData.partition());

                ReadShareGroupStateSummaryRequestData requestForCurrentPartition = new ReadShareGroupStateSummaryRequestData()
                    .setGroupId(groupId)
                    .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                        .setTopicId(topicId)
                        .setPartitions(List.of(partitionData))));

                CompletableFuture<ReadShareGroupStateSummaryResponseData> readFuture = runtime.scheduleWriteOperation(
                    "read-share-group-state-summary",
                    topicPartitionFor(coordinatorKey),
                    Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()),
                    coordinator -> coordinator.readStateSummary(requestForCurrentPartition)
                ).exceptionally(readException ->
                    handleOperationException(
                        "read-share-group-state-summary",
                        request,
                        readException,
                        (error, message) -> ReadShareGroupStateSummaryResponse.toErrorResponseData(
                            topicData.topicId(),
                            partitionData.partition(),
                            error,
                            "Unable to read share group state summary: " + readException.getMessage()
                        ),
                        log
                    ));

                futureMap.computeIfAbsent(topicId, k -> new HashMap<>())
                    .put(partitionData.partition(), readFuture);
            }
        }

        // Combine all futures into a single CompletableFuture<Void>.
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futureMap.values().stream()
            .flatMap(map -> map.values().stream()).toArray(CompletableFuture[]::new));

        // Transform the combined CompletableFuture<Void> into CompletableFuture<ReadShareGroupStateSummaryResponseData>.
        return combinedFuture.thenApply(v -> {
            List<ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult> readStateSummaryResult = new ArrayList<>(futureMap.size());
            futureMap.forEach(
                (topicId, topicEntry) -> {
                    List<ReadShareGroupStateSummaryResponseData.PartitionResult> partitionResults = new ArrayList<>(topicEntry.size());
                    topicEntry.forEach(
                        (partitionId, responseFut) -> {
                            // ResponseFut would already be completed by now since we have used
                            // CompletableFuture::allOf to create a combined future from the future map.
                            partitionResults.add(
                                responseFut.getNow(null).results().get(0).partitions().get(0)
                            );
                        }
                    );
                    readStateSummaryResult.add(ReadShareGroupStateSummaryResponse.toResponseReadStateSummaryResult(topicId, partitionResults));
                }
            );
            return new ReadShareGroupStateSummaryResponseData()
                .setResults(readStateSummaryResult);
        });
    }

    @Override
    public CompletableFuture<DeleteShareGroupStateResponseData> deleteState(RequestContext context, DeleteShareGroupStateRequestData request) {
        // Send an empty response if the coordinator is not active.
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(
                generateErrorDeleteStateResponse(
                    request,
                    Errors.COORDINATOR_NOT_AVAILABLE,
                    "Share coordinator is not available."
                )
            );
        }

        String groupId = request.groupId();
        // Send an empty response if groupId is invalid.
        if (isGroupIdEmpty(groupId)) {
            log.error("Group id must be specified and non-empty: {}", request);
            return CompletableFuture.completedFuture(
                new DeleteShareGroupStateResponseData()
            );
        }

        // Send an empty response if topic data is empty.
        if (isEmpty(request.topics())) {
            log.error("Topic Data is empty: {}", request);
            return CompletableFuture.completedFuture(
                new DeleteShareGroupStateResponseData()
            );
        }

        // Send an empty response if partition data is empty for any topic.
        for (DeleteShareGroupStateRequestData.DeleteStateData topicData : request.topics()) {
            if (isEmpty(topicData.partitions())) {
                log.error("Partition Data for topic {} is empty: {}", topicData.topicId(), request);
                return CompletableFuture.completedFuture(
                    new DeleteShareGroupStateResponseData()
                );
            }
        }

        // A map to store the futures for each topicId and partition.
        Map<Uuid, Map<Integer, CompletableFuture<DeleteShareGroupStateResponseData>>> futureMap = new HashMap<>();

        // The request received here could have multiple keys of structure group:topic:partition. However,
        // the deleteState method in ShareCoordinatorShard expects a single key in the request. Hence, we will
        // be looping over the keys below and constructing new DeleteShareGroupStateRequestData objects to pass
        // onto the shard method.

        for (DeleteShareGroupStateRequestData.DeleteStateData topicData : request.topics()) {
            Uuid topicId = topicData.topicId();
            for (DeleteShareGroupStateRequestData.PartitionData partitionData : topicData.partitions()) {
                SharePartitionKey coordinatorKey = SharePartitionKey.getInstance(request.groupId(), topicId, partitionData.partition());

                DeleteShareGroupStateRequestData requestForCurrentPartition = new DeleteShareGroupStateRequestData()
                    .setGroupId(groupId)
                    .setTopics(List.of(new DeleteShareGroupStateRequestData.DeleteStateData()
                        .setTopicId(topicId)
                        .setPartitions(List.of(partitionData))));

                CompletableFuture<DeleteShareGroupStateResponseData> deleteFuture = runtime.scheduleWriteOperation(
                    "delete-share-group-state",
                    topicPartitionFor(coordinatorKey),
                    Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()),
                    coordinator -> coordinator.deleteState(requestForCurrentPartition)
                ).exceptionally(deleteException ->
                    handleOperationException(
                        "delete-share-group-state",
                        request,
                        deleteException,
                        (error, message) -> DeleteShareGroupStateResponse.toErrorResponseData(
                            topicData.topicId(),
                            partitionData.partition(),
                            error,
                            "Unable to delete share group state: " + deleteException.getMessage()
                        ),
                        log
                    ));

                futureMap.computeIfAbsent(topicId, k -> new HashMap<>())
                    .put(partitionData.partition(), deleteFuture);
            }
        }

        // Combine all futures into a single CompletableFuture<Void>.
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futureMap.values().stream()
            .flatMap(map -> map.values().stream()).toArray(CompletableFuture[]::new));

        // Transform the combined CompletableFuture<Void> into CompletableFuture<DeleteShareGroupStateResponseData>.
        return combinedFuture.thenApply(v -> {
            List<DeleteShareGroupStateResponseData.DeleteStateResult> deleteStateResult = new ArrayList<>(futureMap.size());
            futureMap.forEach(
                (topicId, topicEntry) -> {
                    List<DeleteShareGroupStateResponseData.PartitionResult> partitionResults = new ArrayList<>(topicEntry.size());
                    topicEntry.forEach(
                        (partitionId, responseFut) -> {
                            // ResponseFut would already be completed by now since we have used
                            // CompletableFuture::allOf to create a combined future from the future map.
                            partitionResults.add(
                                responseFut.getNow(null).results().get(0).partitions().get(0)
                            );
                        }
                    );
                    deleteStateResult.add(DeleteShareGroupStateResponse.toResponseDeleteStateResult(topicId, partitionResults));
                }
            );
            return new DeleteShareGroupStateResponseData()
                .setResults(deleteStateResult);
        });
    }

    @Override
    public CompletableFuture<InitializeShareGroupStateResponseData> initializeState(RequestContext context, InitializeShareGroupStateRequestData request) {
        // Send an empty response if the coordinator is not active.
        if (!isActive.get()) {
            return CompletableFuture.completedFuture(
                generateErrorInitStateResponse(
                    request,
                    Errors.COORDINATOR_NOT_AVAILABLE,
                    "Share coordinator is not available."
                )
            );
        }

        String groupId = request.groupId();
        // Send an empty response if groupId is invalid.
        if (isGroupIdEmpty(groupId)) {
            log.error("Group id must be specified and non-empty: {}", request);
            return CompletableFuture.completedFuture(
                new InitializeShareGroupStateResponseData()
            );
        }

        // Send an empty response if topic data is empty.
        if (isEmpty(request.topics())) {
            log.error("Topic Data is empty: {}", request);
            return CompletableFuture.completedFuture(
                new InitializeShareGroupStateResponseData()
            );
        }

        // A map to store the futures for each topicId and partition.
        Map<Uuid, Map<Integer, CompletableFuture<InitializeShareGroupStateResponseData>>> futureMap = new HashMap<>();

        // The request received here could have multiple keys of structure group:topic:partition. However,
        // the initializeState method in ShareCoordinatorShard expects a single key in the request. Hence, we will
        // be looping over the keys below and constructing new InitializeShareGroupStateRequestData objects to pass
        // onto the shard method.

        for (InitializeShareGroupStateRequestData.InitializeStateData topicData : request.topics()) {
            Uuid topicId = topicData.topicId();
            for (InitializeShareGroupStateRequestData.PartitionData partitionData : topicData.partitions()) {
                SharePartitionKey coordinatorKey = SharePartitionKey.getInstance(request.groupId(), topicId, partitionData.partition());

                InitializeShareGroupStateRequestData requestForCurrentPartition = new InitializeShareGroupStateRequestData()
                    .setGroupId(groupId)
                    .setTopics(List.of(new InitializeShareGroupStateRequestData.InitializeStateData()
                        .setTopicId(topicId)
                        .setPartitions(List.of(partitionData))));

                CompletableFuture<InitializeShareGroupStateResponseData> initializeFuture = runtime.scheduleWriteOperation(
                    "initialize-share-group-state",
                    topicPartitionFor(coordinatorKey),
                    Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()),
                    coordinator -> coordinator.initializeState(requestForCurrentPartition)
                ).exceptionally(initializeException ->
                    handleOperationException(
                        "initialize-share-group-state",
                        request,
                        initializeException,
                        (error, message) -> InitializeShareGroupStateResponse.toErrorResponseData(
                            topicData.topicId(),
                            partitionData.partition(),
                            error,
                            "Unable to initialize share group state: " + initializeException.getMessage()
                        ),
                        log
                    ));

                futureMap.computeIfAbsent(topicId, k -> new HashMap<>())
                    .put(partitionData.partition(), initializeFuture);
            }
        }

        // Combine all futures into a single CompletableFuture<Void>.
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futureMap.values().stream()
            .flatMap(map -> map.values().stream()).toArray(CompletableFuture[]::new));

        // Transform the combined CompletableFuture<Void> into CompletableFuture<InitializeShareGroupStateResponseData>.
        return combinedFuture.thenApply(v -> {
            List<InitializeShareGroupStateResponseData.InitializeStateResult> initializeStateResult = new ArrayList<>(futureMap.size());
            futureMap.forEach(
                (topicId, topicEntry) -> {
                    List<InitializeShareGroupStateResponseData.PartitionResult> partitionResults = new ArrayList<>(topicEntry.size());
                    topicEntry.forEach(
                        (partitionId, responseFut) -> {
                            // ResponseFut would already be completed by now since we have used
                            // CompletableFuture::allOf to create a combined future from the future map.
                            partitionResults.add(
                                responseFut.getNow(null).results().get(0).partitions().get(0)
                            );
                        }
                    );
                    initializeStateResult.add(InitializeShareGroupStateResponse.toResponseInitializeStateResult(topicId, partitionResults));
                }
            );
            return new InitializeShareGroupStateResponseData()
                .setResults(initializeStateResult);
        });
    }

    private ReadShareGroupStateResponseData generateErrorReadStateResponse(
        ReadShareGroupStateRequestData request,
        Errors error,
        String errorMessage
    ) {
        return new ReadShareGroupStateResponseData().setResults(request.topics().stream()
            .map(topicData -> {
                ReadShareGroupStateResponseData.ReadStateResult resultData = new ReadShareGroupStateResponseData.ReadStateResult();
                resultData.setTopicId(topicData.topicId());
                resultData.setPartitions(topicData.partitions().stream()
                    .map(partitionData -> ReadShareGroupStateResponse.toErrorResponsePartitionResult(
                        partitionData.partition(), error, errorMessage
                    )).toList());
                return resultData;
            }).toList());
    }

    private ReadShareGroupStateSummaryResponseData generateErrorReadStateSummaryResponse(
        ReadShareGroupStateSummaryRequestData request,
        Errors error,
        String errorMessage
    ) {
        return new ReadShareGroupStateSummaryResponseData().setResults(request.topics().stream()
            .map(topicData -> {
                ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult resultData = new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult();
                resultData.setTopicId(topicData.topicId());
                resultData.setPartitions(topicData.partitions().stream()
                    .map(partitionData -> ReadShareGroupStateSummaryResponse.toErrorResponsePartitionResult(
                        partitionData.partition(), error, errorMessage
                    )).toList());
                return resultData;
            }).toList());
    }

    private WriteShareGroupStateResponseData generateErrorWriteStateResponse(
        WriteShareGroupStateRequestData request,
        Errors error,
        String errorMessage
    ) {
        return new WriteShareGroupStateResponseData()
            .setResults(request.topics().stream()
                .map(topicData -> {
                    WriteShareGroupStateResponseData.WriteStateResult resultData = new WriteShareGroupStateResponseData.WriteStateResult();
                    resultData.setTopicId(topicData.topicId());
                    resultData.setPartitions(topicData.partitions().stream()
                        .map(partitionData -> WriteShareGroupStateResponse.toErrorResponsePartitionResult(
                            partitionData.partition(), error, errorMessage
                        )).toList());
                    return resultData;
                }).toList());
    }

    private DeleteShareGroupStateResponseData generateErrorDeleteStateResponse(
        DeleteShareGroupStateRequestData request,
        Errors error,
        String errorMessage
    ) {
        return new DeleteShareGroupStateResponseData().setResults(request.topics().stream()
            .map(topicData -> {
                DeleteShareGroupStateResponseData.DeleteStateResult resultData = new DeleteShareGroupStateResponseData.DeleteStateResult();
                resultData.setTopicId(topicData.topicId());
                resultData.setPartitions(topicData.partitions().stream()
                    .map(partitionData -> DeleteShareGroupStateResponse.toErrorResponsePartitionResult(
                        partitionData.partition(), error, errorMessage
                    )).toList());
                return resultData;
            }).toList());
    }

    private InitializeShareGroupStateResponseData generateErrorInitStateResponse(
        InitializeShareGroupStateRequestData request,
        Errors error,
        String errorMessage
    ) {
        return new InitializeShareGroupStateResponseData().setResults(request.topics().stream()
            .map(topicData -> {
                InitializeShareGroupStateResponseData.InitializeStateResult resultData = new InitializeShareGroupStateResponseData.InitializeStateResult();
                resultData.setTopicId(topicData.topicId());
                resultData.setPartitions(topicData.partitions().stream()
                    .map(partitionData -> InitializeShareGroupStateResponse.toErrorResponsePartitionResult(
                        partitionData.partition(), error, errorMessage
                    )).toList());
                return resultData;
            }).toList());
    }

    private static boolean isGroupIdEmpty(String groupId) {
        return groupId == null || groupId.isEmpty();
    }

    @Override
    public void onElection(int partitionIndex, int partitionLeaderEpoch) {
        throwIfNotActive();
        runtime.scheduleLoadOperation(
            new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, partitionIndex),
            partitionLeaderEpoch
        );
    }

    @Override
    public void onResignation(int partitionIndex, OptionalInt partitionLeaderEpoch) {
        throwIfNotActive();
        TopicPartition tp = new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, partitionIndex);
        lastPrunedOffsets.remove(tp);
        runtime.scheduleUnloadOperation(
            tp,
            partitionLeaderEpoch
        );
    }

    @Override
    public void onTopicsDeleted(Set<Uuid> deletedTopicIds, BufferSupplier bufferSupplier) throws ExecutionException, InterruptedException {
        throwIfNotActive();
        if (deletedTopicIds.isEmpty()) {
            return;
        }
        CompletableFuture.allOf(
            FutureUtils.mapExceptionally(
                runtime.scheduleWriteAllOperation(
                    "on-topics-deleted",
                    Duration.ofMillis(config.shareCoordinatorWriteTimeoutMs()),
                    coordinator -> coordinator.maybeCleanupShareState(deletedTopicIds)
                ),
                exception -> {
                    log.error("Received error while trying to cleanup deleted topics.", exception);
                    return null;
                }
            ).toArray(new CompletableFuture<?>[0])
        ).get();
    }

    @Override
    public void onNewMetadataImage(CoordinatorMetadataImage newImage, FeaturesImage newFeaturesImage, CoordinatorMetadataDelta delta) {
        throwIfNotActive();
        this.runtime.onNewMetadataImage(newImage, delta);
        boolean enabled = isShareGroupsEnabled(newFeaturesImage);
        // enabled    shouldRunJob         result (XOR)
        // 0            0               no op on flag, do not call jobs
        // 0            1               disable flag, do not call jobs                      => action
        // 1            0               enable flag, call jobs as they are not recursing    => action
        // 1            1               no op on flag, do not call jobs
        if (enabled ^ shouldRunPeriodicJob) {
            shouldRunPeriodicJob = enabled;
            if (enabled) {
                setupPeriodicJobs();
            }
        }
    }

    TopicPartition topicPartitionFor(SharePartitionKey key) {
        return new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, partitionFor(key));
    }

    private static <P> boolean isEmpty(List<P> list) {
        return list == null || list.isEmpty();
    }

    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }

    private boolean isShareGroupsEnabled(FeaturesImage image) {
        return shareGroupConfigEnabledSupplier.get() || ShareVersion.fromFeatureLevel(
            image.finalizedVersions().getOrDefault(ShareVersion.FEATURE_NAME, (short) 0)
        ).supportsShareGroups();
    }

    // Visibility for tests
    boolean shouldRunPeriodicJob() {
        return shouldRunPeriodicJob;
    }
}
