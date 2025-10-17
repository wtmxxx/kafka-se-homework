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
import org.apache.kafka.common.errors.UnsupportedVersionException;
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
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteShareGroupStateResponse;
import org.apache.kafka.common.requests.InitializeShareGroupStateResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateSummaryResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilder;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.share.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetricsShard;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PersisterStateBatch;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ShareCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {
    private final Logger log;
    private final ShareCoordinatorConfig config;
    private final CoordinatorMetrics coordinatorMetrics;
    private final CoordinatorMetricsShard metricsShard;
    private final TimelineHashMap<SharePartitionKey, ShareGroupOffset> shareStateMap;  // coord key -> ShareGroupOffset
    // leaderEpochMap can be updated by writeState call
    // or if a newer leader makes a readState call.
    private final TimelineHashMap<SharePartitionKey, Integer> leaderEpochMap;
    private final TimelineHashMap<SharePartitionKey, Integer> snapshotUpdateCount;
    private final TimelineHashMap<SharePartitionKey, Integer> stateEpochMap;
    private CoordinatorMetadataImage metadataImage;
    private final ShareCoordinatorOffsetsManager offsetsManager;
    private final Time time;

    public static final Exception NULL_TOPIC_ID = new Exception("The topic id cannot be null.");
    public static final Exception NEGATIVE_PARTITION_ID = new Exception("The partition id cannot be a negative number.");
    public static final Exception WRITE_UNINITIALIZED_SHARE_PARTITION = new Exception("Write operation on uninitialized share partition not allowed.");
    public static final Exception READ_UNINITIALIZED_SHARE_PARTITION = new Exception("Read operation on uninitialized share partition not allowed.");

    public static class Builder implements CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> {
        private final ShareCoordinatorConfig config;
        private LogContext logContext;
        private SnapshotRegistry snapshotRegistry;
        private CoordinatorMetrics coordinatorMetrics;
        private TopicPartition topicPartition;
        private Time time;

        public Builder(ShareCoordinatorConfig config) {
            this.config = config;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withTime(Time time) {
            this.time = time;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withTimer(CoordinatorTimer<Void, CoordinatorRecord> timer) {
            // method is required due to interface
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withExecutor(CoordinatorExecutor<CoordinatorRecord> executor) {
            // method is required due to interface
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withCoordinatorMetrics(CoordinatorMetrics coordinatorMetrics) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withTopicPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
            return this;
        }

        @Override
        @SuppressWarnings("NPathComplexity")
        public ShareCoordinatorShard build() {
            if (logContext == null) logContext = new LogContext();
            if (config == null)
                throw new IllegalArgumentException("Config must be set.");
            if (snapshotRegistry == null)
                throw new IllegalArgumentException("SnapshotRegistry must be set.");
            if (coordinatorMetrics == null || !(coordinatorMetrics instanceof ShareCoordinatorMetrics))
                throw new IllegalArgumentException("CoordinatorMetrics must be set and be of type ShareCoordinatorMetrics.");
            if (topicPartition == null)
                throw new IllegalArgumentException("TopicPartition must be set.");

            ShareCoordinatorMetricsShard metricsShard = ((ShareCoordinatorMetrics) coordinatorMetrics)
                .newMetricsShard(snapshotRegistry, topicPartition);

            return new ShareCoordinatorShard(
                logContext,
                config,
                coordinatorMetrics,
                metricsShard,
                snapshotRegistry,
                time
            );
        }
    }

    ShareCoordinatorShard(
        LogContext logContext,
        ShareCoordinatorConfig config,
        CoordinatorMetrics coordinatorMetrics,
        CoordinatorMetricsShard metricsShard,
        SnapshotRegistry snapshotRegistry,
        Time time
    ) {
        this(logContext, config, coordinatorMetrics, metricsShard, snapshotRegistry, new ShareCoordinatorOffsetsManager(snapshotRegistry), time);
    }

    ShareCoordinatorShard(
        LogContext logContext,
        ShareCoordinatorConfig config,
        CoordinatorMetrics coordinatorMetrics,
        CoordinatorMetricsShard metricsShard,
        SnapshotRegistry snapshotRegistry,
        ShareCoordinatorOffsetsManager offsetsManager,
        Time time
    ) {
        this.log = logContext.logger(ShareCoordinatorShard.class);
        this.config = config;
        this.coordinatorMetrics = coordinatorMetrics;
        this.metricsShard = metricsShard;
        this.shareStateMap = new TimelineHashMap<>(snapshotRegistry, 0);
        this.leaderEpochMap = new TimelineHashMap<>(snapshotRegistry, 0);
        this.snapshotUpdateCount = new TimelineHashMap<>(snapshotRegistry, 0);
        this.stateEpochMap = new TimelineHashMap<>(snapshotRegistry, 0);
        this.offsetsManager = offsetsManager;
        this.time = time;
    }

    @Override
    public void onLoaded(CoordinatorMetadataImage newImage) {
        this.metadataImage = newImage;
        coordinatorMetrics.activateMetricsShard(metricsShard);
    }

    @Override
    public void onNewMetadataImage(CoordinatorMetadataImage newImage, CoordinatorMetadataDelta delta) {
        this.metadataImage = newImage;
    }

    @Override
    public void onUnloaded() {
        coordinatorMetrics.deactivateMetricsShard(metricsShard);
    }

    @Override
    public void replay(long offset, long producerId, short producerEpoch, CoordinatorRecord record) throws RuntimeException {
        ApiMessage key = record.key();
        ApiMessageAndVersion value = record.value();

        try {
            switch (CoordinatorRecordType.fromId(key.apiKey())) {
                case SHARE_SNAPSHOT:
                    handleShareSnapshot((ShareSnapshotKey) key, (ShareSnapshotValue) messageOrNull(value), offset);
                    break;
                case SHARE_UPDATE:
                    handleShareUpdate((ShareUpdateKey) key, (ShareUpdateValue) messageOrNull(value));
                    break;
                default:
                    // Noop
            }
        } catch (UnsupportedVersionException ex) {
            // Ignore
        }
    }

    private void handleShareSnapshot(ShareSnapshotKey key, ShareSnapshotValue value, long offset) {
        SharePartitionKey mapKey = SharePartitionKey.getInstance(key.groupId(), key.topicId(), key.partition());
        if (value == null) {
            log.debug("Tombstone records received for share partition key: {}", mapKey);
            // Consider this a tombstone.
            shareStateMap.remove(mapKey);
            leaderEpochMap.remove(mapKey);
            stateEpochMap.remove(mapKey);
            snapshotUpdateCount.remove(mapKey);
        } else {
            maybeUpdateLeaderEpochMap(mapKey, value.leaderEpoch());
            maybeUpdateStateEpochMap(mapKey, value.stateEpoch());

            ShareGroupOffset offsetRecord = ShareGroupOffset.fromRecord(value);
            // This record is the complete snapshot.
            shareStateMap.put(mapKey, offsetRecord);
            // If number of share updates is exceeded, then reset it.
            if (snapshotUpdateCount.containsKey(mapKey)) {
                if (snapshotUpdateCount.get(mapKey) >= config.shareCoordinatorSnapshotUpdateRecordsPerSnapshot()) {
                    snapshotUpdateCount.put(mapKey, 0);
                }
            }
        }

        offsetsManager.updateState(mapKey, offset, value == null);
    }

    private void handleShareUpdate(ShareUpdateKey key, ShareUpdateValue value) {
        SharePartitionKey mapKey = SharePartitionKey.getInstance(key.groupId(), key.topicId(), key.partition());
        maybeUpdateLeaderEpochMap(mapKey, value.leaderEpoch());

        // Share update does not hold state epoch information.

        ShareGroupOffset offsetRecord = ShareGroupOffset.fromRecord(value);
        // This is an incremental snapshot,
        // so we need to apply it to our current soft state.
        shareStateMap.compute(mapKey, (k, v) -> v == null ? offsetRecord : merge(v, value));
        snapshotUpdateCount.compute(mapKey, (k, v) -> v == null ? 0 : v + 1);
    }

    private void maybeUpdateLeaderEpochMap(SharePartitionKey mapKey, int leaderEpoch) {
        leaderEpochMap.putIfAbsent(mapKey, leaderEpoch);
        if (leaderEpochMap.get(mapKey) < leaderEpoch) {
            leaderEpochMap.put(mapKey, leaderEpoch);
        }
    }

    private void maybeUpdateStateEpochMap(SharePartitionKey mapKey, int stateEpoch) {
        stateEpochMap.putIfAbsent(mapKey, stateEpoch);
        if (stateEpochMap.get(mapKey) < stateEpoch) {
            stateEpochMap.put(mapKey, stateEpoch);
        }
    }

    @Override
    public void replayEndTransactionMarker(long producerId, short producerEpoch, TransactionResult result) throws RuntimeException {
        CoordinatorShard.super.replayEndTransactionMarker(producerId, producerEpoch, result);
    }

    /**
     * This method generates the ShareSnapshotValue record corresponding to the requested topic partition information.
     * The generated record is then written to the __share_group_state topic and replayed to the in-memory state
     * of the coordinator shard, shareStateMap, by CoordinatorRuntime.
     * <p>
     * This method as called by the ShareCoordinatorService will be provided with
     * the request data which covers only a single key i.e. group1:topic1:partition1. The implementation
     * below was done keeping this in mind.
     *
     * @param request - WriteShareGroupStateRequestData for a single key
     * @return CoordinatorResult(records, response)
     */
    public CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> writeState(
        WriteShareGroupStateRequestData request
    ) {
        // Records to write (with both key and value of snapshot type), response to caller
        // only one key will be there in the request by design.
        metricsShard.record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
        Optional<CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord>> error = maybeGetWriteStateError(request);
        if (error.isPresent()) {
            return error.get();
        }

        WriteShareGroupStateRequestData.WriteStateData topicData = request.topics().get(0);
        WriteShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);
        SharePartitionKey key = SharePartitionKey.getInstance(request.groupId(), topicData.topicId(), partitionData.partition());

        CoordinatorRecord record = generateShareStateRecord(partitionData, key, false);
        // build successful response if record is correctly created
        WriteShareGroupStateResponseData responseData = new WriteShareGroupStateResponseData().setResults(
            List.of(WriteShareGroupStateResponse.toResponseWriteStateResult(key.topicId(),
                List.of(WriteShareGroupStateResponse.toResponsePartitionResult(
                    key.partition()))
            ))
        );

        return new CoordinatorResult<>(List.of(record), responseData);
    }

    /**
     * Method reads data from the soft state and if needed updates the leader epoch.
     * It can happen that a read state call for a share partition has a higher leaderEpoch
     * value than seen so far.
     * In case an update is not required, empty record list will be generated along with a success response.
     *
     * @param request - represents ReadShareGroupStateRequestData
     * @return CoordinatorResult object
     */
    public CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> readStateAndMaybeUpdateLeaderEpoch(
        ReadShareGroupStateRequestData request
    ) {
        // Only one key will be there in the request by design.
        Optional<ReadShareGroupStateResponseData> error = maybeGetReadStateError(request);
        if (error.isPresent()) {
            return new CoordinatorResult<>(List.of(), error.get());
        }

        ReadShareGroupStateRequestData.ReadStateData topicData = request.topics().get(0);
        ReadShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();
        int leaderEpoch = partitionData.leaderEpoch();
        SharePartitionKey key = SharePartitionKey.getInstance(request.groupId(), topicId, partitionId);

        ShareGroupOffset offsetValue = shareStateMap.get(key);
        List<ReadShareGroupStateResponseData.StateBatch> stateBatches = (offsetValue.stateBatches() != null && !offsetValue.stateBatches().isEmpty()) ?
            offsetValue.stateBatches().stream()
                .map(
                    stateBatch -> new ReadShareGroupStateResponseData.StateBatch()
                        .setFirstOffset(stateBatch.firstOffset())
                        .setLastOffset(stateBatch.lastOffset())
                        .setDeliveryState(stateBatch.deliveryState())
                        .setDeliveryCount(stateBatch.deliveryCount())
                ).toList() : List.of();

        ReadShareGroupStateResponseData responseData = ReadShareGroupStateResponse.toResponseData(
            topicId,
            partitionId,
            offsetValue.startOffset(),
            offsetValue.stateEpoch(),
            stateBatches
        );

        // Optimization in case leaderEpoch update is not required.
        if (leaderEpoch == -1 ||
            (leaderEpochMap.get(key) != null && leaderEpochMap.get(key) == leaderEpoch)) {
            return new CoordinatorResult<>(List.of(), responseData);
        }

        // It is OK to info log this since this reaching this codepoint should be quite infrequent.
        log.info("Read with leader epoch update call for key {} having new leader epoch {}.", key, leaderEpoch);

        // Recording the sensor here as above if condition will not produce any record.
        metricsShard.record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);

        // Generate record with leaderEpoch info.
        WriteShareGroupStateRequestData.PartitionData writePartitionData = new WriteShareGroupStateRequestData.PartitionData()
            .setPartition(partitionId)
            .setLeaderEpoch(leaderEpoch)
            .setStateBatches(List.of())
            .setStartOffset(responseData.results().get(0).partitions().get(0).startOffset())
            .setStateEpoch(responseData.results().get(0).partitions().get(0).stateEpoch());

        CoordinatorRecord record = generateShareStateRecord(writePartitionData, key, true);
        return new CoordinatorResult<>(List.of(record), responseData);
    }

    /**
     * This method finds the ShareSnapshotValue record corresponding to the requested topic partition from the
     * in-memory state of coordinator shard, the shareStateMap.
     * <p>
     * This method as called by the ShareCoordinatorService will be provided with
     * the request data which covers only key i.e. group1:topic1:partition1. The implementation
     * below was done keeping this in mind.
     *
     * @param request - ReadShareGroupStateSummaryRequestData for a single key
     * @return CoordinatorResult(records, response)
     */

    public CoordinatorResult<ReadShareGroupStateSummaryResponseData, CoordinatorRecord> readStateSummary(
        ReadShareGroupStateSummaryRequestData request
    ) {
        // Only one key will be there in the request by design.
        Optional<ReadShareGroupStateSummaryResponseData> error = maybeGetReadStateSummaryError(request);
        if (error.isPresent()) {
            return new CoordinatorResult<>(List.of(), error.get());
        }

        ReadShareGroupStateSummaryRequestData.ReadStateSummaryData topicData = request.topics().get(0);
        ReadShareGroupStateSummaryRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();
        SharePartitionKey key = SharePartitionKey.getInstance(request.groupId(), topicId, partitionId);

        ReadShareGroupStateSummaryResponseData responseData = null;

        if (!shareStateMap.containsKey(key)) {
            responseData = ReadShareGroupStateSummaryResponse.toResponseData(
                topicId,
                partitionId,
                PartitionFactory.UNINITIALIZED_START_OFFSET,
                PartitionFactory.DEFAULT_LEADER_EPOCH,
                PartitionFactory.DEFAULT_STATE_EPOCH
            );
        } else {
            ShareGroupOffset offsetValue = shareStateMap.get(key);
            if (offsetValue == null) {
                log.error("Data not found for topic {}, partition {} for group {}, in the in-memory state of share coordinator", topicId, partitionId, request.groupId());
                responseData = ReadShareGroupStateSummaryResponse.toErrorResponseData(
                    topicId,
                    partitionId,
                    Errors.UNKNOWN_SERVER_ERROR,
                    "Data not found for the topics " + topicId + ", partition " + partitionId + " for group " + request.groupId() + ", in the in-memory state of share coordinator"
                );
            } else {
                responseData = ReadShareGroupStateSummaryResponse.toResponseData(
                    topicId,
                    partitionId,
                    offsetValue.startOffset(),
                    offsetValue.leaderEpoch(),
                    offsetValue.stateEpoch()
                );
            }
        }

        return new CoordinatorResult<>(List.of(), responseData);
    }

    /**
     * Method which returns the last known redundant offset from the partition
     * led by this shard.
     *
     * @return CoordinatorResult containing empty record list and an Optional<Long> representing the offset.
     */
    public CoordinatorResult<Optional<Long>, CoordinatorRecord> lastRedundantOffset() {
        return new CoordinatorResult<>(
            List.of(),
            this.offsetsManager.lastRedundantOffset()
        );
    }

    /**
     * This method writes tombstone records corresponding to the requested topic partitions.
     * <p>
     * This method as called by the ShareCoordinatorService will be provided with
     * the request data which covers only key i.e. group1:topic1:partition1. The implementation
     * below was done keeping this in mind.
     *
     * @param request - DeleteShareGroupStateRequestData for a single key
     * @return CoordinatorResult(records, response)
     */

    public CoordinatorResult<DeleteShareGroupStateResponseData, CoordinatorRecord> deleteState(
        DeleteShareGroupStateRequestData request
    ) {
        // Records to write (with both key and value of snapshot type), response to caller
        // only one key will be there in the request by design.
        Optional<CoordinatorResult<DeleteShareGroupStateResponseData, CoordinatorRecord>> error = maybeGetDeleteStateError(request);
        if (error.isPresent()) {
            return error.get();
        }

        DeleteShareGroupStateRequestData.DeleteStateData topicData = request.topics().get(0);
        DeleteShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);
        SharePartitionKey key = SharePartitionKey.getInstance(request.groupId(), topicData.topicId(), partitionData.partition());

        if (!shareStateMap.containsKey(key)) {
            log.warn("Attempted to delete non-existent share partition {}.", key);
            return new CoordinatorResult<>(List.of(), new DeleteShareGroupStateResponseData().setResults(
                List.of(DeleteShareGroupStateResponse.toResponseDeleteStateResult(key.topicId(),
                    List.of(DeleteShareGroupStateResponse.toResponsePartitionResult(
                        key.partition()))
                ))
            ));
        }

        CoordinatorRecord record = generateTombstoneRecord(key);
        // build successful response if record is correctly created
        DeleteShareGroupStateResponseData responseData = new DeleteShareGroupStateResponseData().setResults(
            List.of(DeleteShareGroupStateResponse.toResponseDeleteStateResult(key.topicId(),
                List.of(DeleteShareGroupStateResponse.toResponsePartitionResult(
                    key.partition()))
            ))
        );

        return new CoordinatorResult<>(List.of(record), responseData);
    }

    /**
     * This method writes a share snapshot records corresponding to the requested topic partitions.
     * <p>
     * This method as called by the ShareCoordinatorService will be provided with
     * the request data which covers only key i.e. group1:topic1:partition1. The implementation
     * below was done keeping this in mind.
     *
     * @param request - InitializeShareGroupStateRequestData for a single key
     * @return CoordinatorResult(records, response)
     */

    public CoordinatorResult<InitializeShareGroupStateResponseData, CoordinatorRecord> initializeState(
        InitializeShareGroupStateRequestData request
    ) {
        // Records to write (with both key and value of snapshot type), response to caller
        // only one key will be there in the request by design.
        Optional<CoordinatorResult<InitializeShareGroupStateResponseData, CoordinatorRecord>> error = maybeGetInitializeStateError(request);
        if (error.isPresent()) {
            return error.get();
        }

        InitializeShareGroupStateRequestData.InitializeStateData topicData = request.topics().get(0);
        InitializeShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);
        SharePartitionKey key = SharePartitionKey.getInstance(request.groupId(), topicData.topicId(), partitionData.partition());

        CoordinatorRecord record = generateInitializeStateRecord(partitionData, key);
        // build successful response if record is correctly created
        InitializeShareGroupStateResponseData responseData = new InitializeShareGroupStateResponseData().setResults(
            List.of(InitializeShareGroupStateResponse.toResponseInitializeStateResult(key.topicId(),
                List.of(InitializeShareGroupStateResponse.toResponsePartitionResult(
                    key.partition()))
            ))
        );

        return new CoordinatorResult<>(List.of(record), responseData);
    }

    /**
     * Iterates over the soft state to determine the share partitions whose last snapshot is
     * older than the allowed time interval. The candidate share partitions are force snapshotted.
     *
     * @return A result containing snapshot records, if any, and a void response.
     */
    public CoordinatorResult<Void, CoordinatorRecord> snapshotColdPartitions() {
        long coldSnapshottedPartitionsCount = shareStateMap.values().stream()
            .filter(shareGroupOffset -> shareGroupOffset.createTimestamp() - shareGroupOffset.writeTimestamp() != 0)
            .count();

        // If all share partitions are snapshotted, it means that
        // system is quiet and cold snapshotting will not help much.
        if (coldSnapshottedPartitionsCount == shareStateMap.size()) {
            log.debug("All share snapshot records already cold snapshotted, skipping.");
            return new CoordinatorResult<>(List.of(), null);
        }

        // Some active partitions are there.
        List<CoordinatorRecord> records = new ArrayList<>();

        shareStateMap.forEach((sharePartitionKey, shareGroupOffset) -> {
            long timeSinceLastSnapshot = time.milliseconds() - shareGroupOffset.writeTimestamp();
            if (timeSinceLastSnapshot >= config.shareCoordinatorColdPartitionSnapshotIntervalMs()) {
                // We need to force create a snapshot here
                log.debug("Last snapshot for {} is older than allowed interval (last snapshot delta {}).", sharePartitionKey, timeSinceLastSnapshot);
                records.add(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
                    sharePartitionKey.groupId(),
                    sharePartitionKey.topicId(),
                    sharePartitionKey.partition(),
                    shareGroupOffset.builderSupplier()
                        .setSnapshotEpoch(shareGroupOffset.snapshotEpoch() + 1) // We need to increment by one as this is a new snapshot.
                        .setWriteTimestamp(time.milliseconds())
                        .build()
                ));
            }
        });
        return new CoordinatorResult<>(records, null);
    }

    /**
     * Remove share partitions corresponding to the input topic ids, if present.
     * @param deletedTopicIds   The topic ids which have been deleted
     * @return A result containing relevant coordinator records and void response
     */
    public CoordinatorResult<Void, CoordinatorRecord> maybeCleanupShareState(Set<Uuid> deletedTopicIds) {
        if (deletedTopicIds.isEmpty()) {
            return new CoordinatorResult<>(List.of());
        }
        Set<SharePartitionKey> eligibleKeys = new HashSet<>();
        shareStateMap.forEach((key, __) -> {
            if (deletedTopicIds.contains(key.topicId())) {
                eligibleKeys.add(key);
            }
        });

        return new CoordinatorResult<>(eligibleKeys.stream()
            .map(key -> ShareCoordinatorRecordHelpers.newShareStateTombstoneRecord(key.groupId(), key.topicId(), key.partition()))
            .toList()
        );
    }

    /**
     * Util method to generate a ShareSnapshot or ShareUpdate type record for a key, based on various conditions.
     * <p>
     * If number of ShareUpdate records for key >= max allowed per snapshot per key or stateEpoch is highest
     * seen so far => create a new ShareSnapshot record else create a new ShareUpdate record. This method assumes
     * that share partition key is present in shareStateMap since it should be called on initialized share partitions.
     *
     * @param partitionData     - Represents the data which should be written into the share state record.
     * @param key               - The {@link SharePartitionKey} object.
     * @param updateLeaderEpoch - Should the leader epoch be updated, if higher.
     * @return {@link CoordinatorRecord} representing ShareSnapshot or ShareUpdate
     */
    private CoordinatorRecord generateShareStateRecord(
        WriteShareGroupStateRequestData.PartitionData partitionData,
        SharePartitionKey key,
        boolean updateLeaderEpoch
    ) {
        long timestamp = time.milliseconds();
        int updatesPerSnapshotLimit = config.shareCoordinatorSnapshotUpdateRecordsPerSnapshot();
        ShareGroupOffset currentState = shareStateMap.get(key); // This method assumes containsKey is true.

        int newLeaderEpoch = currentState.leaderEpoch();
        if (updateLeaderEpoch) {
            newLeaderEpoch = partitionData.leaderEpoch() != -1 ? partitionData.leaderEpoch() : newLeaderEpoch;
        }

        if (snapshotUpdateCount.getOrDefault(key, 0) >= updatesPerSnapshotLimit) {
            // shareStateMap will have the entry as containsKey is true
            long newStartOffset = partitionData.startOffset() == -1 ? currentState.startOffset() : partitionData.startOffset();

            // Since the number of update records for this share part key exceeds snapshotUpdateRecordsPerSnapshot
            // or state epoch has incremented, we should be creating a share snapshot record.
            // The incoming partition data could have overlapping state batches, we must merge them.
            return ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
                key.groupId(), key.topicId(), partitionData.partition(),
                new ShareGroupOffset.Builder()
                    .setSnapshotEpoch(currentState.snapshotEpoch() + 1)   // We must increment snapshot epoch as this is new snapshot.
                    .setStartOffset(newStartOffset)
                    .setLeaderEpoch(newLeaderEpoch)
                    .setStateEpoch(currentState.stateEpoch())
                    .setStateBatches(mergeBatches(currentState.stateBatches(), partitionData, newStartOffset))
                    .setCreateTimestamp(timestamp)
                    .setWriteTimestamp(timestamp)
                    .build());
        } else {
            // Share snapshot is present and number of share snapshot update records < snapshotUpdateRecordsPerSnapshot
            // so create a share update record.
            // The incoming partition data could have overlapping state batches, we must merge them.
            return ShareCoordinatorRecordHelpers.newShareUpdateRecord(
                key.groupId(), key.topicId(), partitionData.partition(),
                new ShareGroupOffset.Builder()
                    .setSnapshotEpoch(currentState.snapshotEpoch()) // Use same snapshotEpoch as last share snapshot.
                    .setStartOffset(partitionData.startOffset())
                    .setLeaderEpoch(newLeaderEpoch)
                    .setStateBatches(mergeBatches(List.of(), partitionData))
                    .build());
        }
    }

    private CoordinatorRecord generateTombstoneRecord(SharePartitionKey key) {
        return ShareCoordinatorRecordHelpers.newShareStateTombstoneRecord(
            key.groupId(),
            key.topicId(),
            key.partition()
        );
    }

    private CoordinatorRecord generateInitializeStateRecord(
        InitializeShareGroupStateRequestData.PartitionData partitionData,
        SharePartitionKey key
    ) {
        // We need to create a new share snapshot here, with
        // appropriate state information. We will not be merging
        // state here with previous snapshots as init state implies
        // fresh start.

        int snapshotEpoch = shareStateMap.containsKey(key) ? shareStateMap.get(key).snapshotEpoch() + 1 : 0;
        return ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            key.groupId(),
            key.topicId(),
            key.partition(),
            ShareGroupOffset.fromRequest(partitionData, snapshotEpoch, time.milliseconds())
        );
    }

    private List<PersisterStateBatch> mergeBatches(
        List<PersisterStateBatch> soFar,
        WriteShareGroupStateRequestData.PartitionData partitionData) {
        return mergeBatches(soFar, partitionData, partitionData.startOffset());
    }

    private List<PersisterStateBatch> mergeBatches(
        List<PersisterStateBatch> soFar,
        WriteShareGroupStateRequestData.PartitionData partitionData,
        long startOffset
    ) {
        return new PersisterStateBatchCombiner(soFar, partitionData.stateBatches().stream()
            .map(PersisterStateBatch::from)
            .toList(),
            startOffset
        ).combineStateBatches();
    }

    private Optional<CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord>> maybeGetWriteStateError(
        WriteShareGroupStateRequestData request
    ) {
        String groupId = request.groupId();
        WriteShareGroupStateRequestData.WriteStateData topicData = request.topics().get(0);
        WriteShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();

        if (topicId == null) {
            return Optional.of(getWriteErrorCoordinatorResult(Errors.INVALID_REQUEST, NULL_TOPIC_ID, null, partitionId));
        }

        if (partitionId < 0) {
            return Optional.of(getWriteErrorCoordinatorResult(Errors.INVALID_REQUEST, NEGATIVE_PARTITION_ID, topicId, partitionId));
        }

        SharePartitionKey mapKey = SharePartitionKey.getInstance(groupId, topicId, partitionId);

        if (!shareStateMap.containsKey(mapKey)) {
            return Optional.of(getWriteErrorCoordinatorResult(Errors.INVALID_REQUEST, WRITE_UNINITIALIZED_SHARE_PARTITION, topicId, partitionId));
        }

        if (partitionData.leaderEpoch() != -1 && leaderEpochMap.containsKey(mapKey) && leaderEpochMap.get(mapKey) > partitionData.leaderEpoch()) {
            log.error("Write request leader epoch is smaller than last recorded current: {}, requested: {}.", leaderEpochMap.get(mapKey), partitionData.leaderEpoch());
            return Optional.of(getWriteErrorCoordinatorResult(Errors.FENCED_LEADER_EPOCH, null, topicId, partitionId));
        }
        if (partitionData.stateEpoch() != -1 && stateEpochMap.containsKey(mapKey) && stateEpochMap.get(mapKey) > partitionData.stateEpoch()) {
            log.info("Write request state epoch is smaller than last recorded current: {}, requested: {}.", stateEpochMap.get(mapKey), partitionData.stateEpoch());
            return Optional.of(getWriteErrorCoordinatorResult(Errors.FENCED_STATE_EPOCH, null, topicId, partitionId));
        }
        if (metadataImage == null) {
            log.error("Metadata image is null");
            return Optional.of(getWriteErrorCoordinatorResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, topicId, partitionId));
        }
        Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadataOp = metadataImage.topicMetadata(topicId);
        if (topicMetadataOp.isEmpty() ||
            topicMetadataOp.get().partitionCount() <= partitionId) {
            log.error("Topic/TopicPartition not found in metadata image.");
            return Optional.of(getWriteErrorCoordinatorResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, topicId, partitionId));
        }

        return Optional.empty();
    }

    private Optional<ReadShareGroupStateResponseData> maybeGetReadStateError(ReadShareGroupStateRequestData request) {
        String groupId = request.groupId();
        ReadShareGroupStateRequestData.ReadStateData topicData = request.topics().get(0);
        ReadShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();

        if (topicId == null) {
            log.error("Request topic id is null.");
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(
                null, partitionId, Errors.INVALID_REQUEST, NULL_TOPIC_ID.getMessage()));
        }

        if (partitionId < 0) {
            log.error("Request partition id is negative.");
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(
                topicId, partitionId, Errors.INVALID_REQUEST, NEGATIVE_PARTITION_ID.getMessage()));
        }

        SharePartitionKey mapKey = SharePartitionKey.getInstance(groupId, topicId, partitionId);

        if (!shareStateMap.containsKey(mapKey)) {
            log.error("Read on uninitialized share partition {}", mapKey);
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(
                topicId, partitionId, Errors.INVALID_REQUEST, READ_UNINITIALIZED_SHARE_PARTITION.getMessage()));
        }

        if (leaderEpochMap.containsKey(mapKey) && leaderEpochMap.get(mapKey) > partitionData.leaderEpoch()) {
            log.error("Read request leader epoch is smaller than last recorded current: {}, requested: {}.", leaderEpochMap.get(mapKey), partitionData.leaderEpoch());
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.FENCED_LEADER_EPOCH, Errors.FENCED_LEADER_EPOCH.message()));
        }

        if (metadataImage == null) {
            log.error("Metadata image is null");
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
        }

        Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadataOp = metadataImage.topicMetadata(topicId);
        if (topicMetadataOp.isEmpty() ||
            topicMetadataOp.get().partitionCount() <= partitionId) {
            log.error("Topic/TopicPartition not found in metadata image.");
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
        }

        return Optional.empty();
    }

    private Optional<ReadShareGroupStateSummaryResponseData> maybeGetReadStateSummaryError(ReadShareGroupStateSummaryRequestData request) {
        ReadShareGroupStateSummaryRequestData.ReadStateSummaryData topicData = request.topics().get(0);
        ReadShareGroupStateSummaryRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();

        if (topicId == null) {
            log.error("Request topic id is null.");
            return Optional.of(ReadShareGroupStateSummaryResponse.toErrorResponseData(
                null, partitionId, Errors.INVALID_REQUEST, NULL_TOPIC_ID.getMessage()));
        }

        if (partitionId < 0) {
            log.error("Request partition id is negative.");
            return Optional.of(ReadShareGroupStateSummaryResponse.toErrorResponseData(
                topicId, partitionId, Errors.INVALID_REQUEST, NEGATIVE_PARTITION_ID.getMessage()));
        }

        if (metadataImage == null) {
            log.error("Metadata image is null");
            return Optional.of(ReadShareGroupStateSummaryResponse.toErrorResponseData(topicId, partitionId, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
        }

        Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadataOp = metadataImage.topicMetadata(topicId);
        if (topicMetadataOp.isEmpty() ||
            topicMetadataOp.get().partitionCount() <= partitionId) {
            log.error("Topic/TopicPartition not found in metadata image.");
            return Optional.of(ReadShareGroupStateSummaryResponse.toErrorResponseData(topicId, partitionId, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
        }

        return Optional.empty();
    }

    private Optional<CoordinatorResult<DeleteShareGroupStateResponseData, CoordinatorRecord>> maybeGetDeleteStateError(
        DeleteShareGroupStateRequestData request
    ) {
        DeleteShareGroupStateRequestData.DeleteStateData topicData = request.topics().get(0);
        DeleteShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();

        if (topicId == null) {
            return Optional.of(getDeleteErrorCoordinatorResult(Errors.INVALID_REQUEST, NULL_TOPIC_ID, null, partitionId));
        }

        if (partitionId < 0) {
            return Optional.of(getDeleteErrorCoordinatorResult(Errors.INVALID_REQUEST, NEGATIVE_PARTITION_ID, topicId, partitionId));
        }

        if (metadataImage == null) {
            log.error("Metadata image is null");
            return Optional.of(getDeleteErrorCoordinatorResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, topicId, partitionId));
        }

        Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadataOp = metadataImage.topicMetadata(topicId);
        if (topicMetadataOp.isEmpty() ||
            topicMetadataOp.get().partitionCount() <= partitionId) {
            log.error("Topic/TopicPartition not found in metadata image.");
            return Optional.of(getDeleteErrorCoordinatorResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, topicId, partitionId));
        }

        return Optional.empty();
    }

    private Optional<CoordinatorResult<InitializeShareGroupStateResponseData, CoordinatorRecord>> maybeGetInitializeStateError(
        InitializeShareGroupStateRequestData request
    ) {
        InitializeShareGroupStateRequestData.InitializeStateData topicData = request.topics().get(0);
        InitializeShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();

        if (topicId == null) {
            return Optional.of(getInitializeErrorCoordinatorResult(Errors.INVALID_REQUEST, NULL_TOPIC_ID, null, partitionId));
        }

        if (partitionId < 0) {
            return Optional.of(getInitializeErrorCoordinatorResult(Errors.INVALID_REQUEST, NEGATIVE_PARTITION_ID, topicId, partitionId));
        }

        SharePartitionKey key = SharePartitionKey.getInstance(request.groupId(), topicId, partitionId);
        if (partitionData.stateEpoch() != -1 && stateEpochMap.containsKey(key) && stateEpochMap.get(key) > partitionData.stateEpoch()) {
            log.info("Initialize request state epoch is smaller than last recorded current: {}, requested: {}.", stateEpochMap.get(key), partitionData.stateEpoch());
            return Optional.of(getInitializeErrorCoordinatorResult(Errors.FENCED_STATE_EPOCH, Errors.FENCED_STATE_EPOCH.exception(), topicId, partitionId));
        }

        if (metadataImage == null) {
            log.error("Metadata image is null");
            return Optional.of(getInitializeErrorCoordinatorResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, topicId, partitionId));
        }

        Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadataOp = metadataImage.topicMetadata(topicId);
        if (topicMetadataOp.isEmpty() ||
            topicMetadataOp.get().partitionCount() <= partitionId) {
            log.error("Topic/TopicPartition not found in metadata image.");
            return Optional.of(getInitializeErrorCoordinatorResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, topicId, partitionId));
        }

        return Optional.empty();
    }

    private CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> getWriteErrorCoordinatorResult(
        Errors error,
        Exception exception,
        Uuid topicId,
        int partitionId
    ) {
        String message = exception == null ? error.message() : exception.getMessage();
        WriteShareGroupStateResponseData responseData = WriteShareGroupStateResponse.toErrorResponseData(topicId, partitionId, error, message);
        return new CoordinatorResult<>(List.of(), responseData);
    }

    private CoordinatorResult<DeleteShareGroupStateResponseData, CoordinatorRecord> getDeleteErrorCoordinatorResult(
        Errors error,
        Exception exception,
        Uuid topicId,
        int partitionId
    ) {
        String message = exception == null ? error.message() : exception.getMessage();
        DeleteShareGroupStateResponseData responseData = DeleteShareGroupStateResponse.toErrorResponseData(topicId, partitionId, error, message);
        return new CoordinatorResult<>(List.of(), responseData);
    }

    private CoordinatorResult<InitializeShareGroupStateResponseData, CoordinatorRecord> getInitializeErrorCoordinatorResult(
        Errors error,
        Exception exception,
        Uuid topicId,
        int partitionId
    ) {
        String message = exception == null ? error.message() : exception.getMessage();
        InitializeShareGroupStateResponseData responseData = InitializeShareGroupStateResponse.toErrorResponseData(topicId, partitionId, error, message);
        return new CoordinatorResult<>(List.of(), responseData);
    }

    // Visible for testing
    Integer getLeaderMapValue(SharePartitionKey key) {
        return this.leaderEpochMap.get(key);
    }

    // Visible for testing
    Integer getStateEpochMapValue(SharePartitionKey key) {
        return this.stateEpochMap.get(key);
    }

    // Visible for testing
    ShareGroupOffset getShareStateMapValue(SharePartitionKey key) {
        return this.shareStateMap.get(key);
    }

    // Visible for testing
    CoordinatorMetricsShard getMetricsShard() {
        return metricsShard;
    }

    private static ShareGroupOffset merge(ShareGroupOffset soFar, ShareUpdateValue newData) {
        // Snapshot epoch should be same as last share snapshot.
        // state epoch is not present
        List<PersisterStateBatch> currentBatches = soFar.stateBatches();
        long newStartOffset = newData.startOffset() == -1 ? soFar.startOffset() : newData.startOffset();
        int newLeaderEpoch = newData.leaderEpoch() == -1 ? soFar.leaderEpoch() : newData.leaderEpoch();

        return new ShareGroupOffset.Builder()
            .setSnapshotEpoch(soFar.snapshotEpoch())
            .setStateEpoch(soFar.stateEpoch())
            .setStartOffset(newStartOffset)
            .setLeaderEpoch(newLeaderEpoch)
            .setStateBatches(new PersisterStateBatchCombiner(currentBatches, newData.stateBatches().stream()
                .map(ShareCoordinatorShard::toPersisterStateBatch)
                .toList(), newStartOffset)
                .combineStateBatches())
            .setCreateTimestamp(soFar.createTimestamp())
            .setWriteTimestamp(soFar.writeTimestamp())
            .build();
    }

    private static ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
        if (apiMessageAndVersion == null) {
            return null;
        } else {
            return apiMessageAndVersion.message();
        }
    }

    /**
     * Util function to convert a state batch of type {@link ShareUpdateValue.StateBatch }
     * to {@link PersisterStateBatch}.
     *
     * @param batch - The object representing {@link ShareUpdateValue.StateBatch}
     * @return {@link PersisterStateBatch}
     */
    private static PersisterStateBatch toPersisterStateBatch(ShareUpdateValue.StateBatch batch) {
        return new PersisterStateBatch(
            batch.firstOffset(),
            batch.lastOffset(),
            batch.deliveryState(),
            batch.deliveryCount()
        );
    }
}
