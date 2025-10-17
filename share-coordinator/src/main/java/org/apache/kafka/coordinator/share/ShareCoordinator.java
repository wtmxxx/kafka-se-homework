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
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.server.share.SharePartitionKey;

import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.IntSupplier;

public interface ShareCoordinator {
    /**
     * Return the partition index for the given key.
     *
     * @param key - reference to {@link SharePartitionKey}.
     * @return The partition index.
     */
    int partitionFor(SharePartitionKey key);

    /**
     * Return the configuration properties of the share-group state topic.
     *
     * @return Properties of the share-group state topic.
     */
    Properties shareGroupStateTopicConfigs();

    /**
     * Start the share coordinator
     *
     * @param shareGroupTopicPartitionCount - supplier returning the number of partitions for __share_group_state topic
     */
    void startup(IntSupplier shareGroupTopicPartitionCount);

    /**
     * Stop the share coordinator
     */
    void shutdown();

    /**
     * Handle write share state call
     * @param context - represents the incoming write request context
     * @param request - actual RPC request object
     * @return completable future comprising write RPC response data
     */
    CompletableFuture<WriteShareGroupStateResponseData> writeState(RequestContext context, WriteShareGroupStateRequestData request);


    /**
     * Handle read share state call
     * @param context - represents the incoming read request context
     * @param request - actual RPC request object
     * @return completable future comprising read RPC response data
     */
    CompletableFuture<ReadShareGroupStateResponseData> readState(RequestContext context, ReadShareGroupStateRequestData request);

    /**
     * Handle read share state summary call
     * @param context - represents the incoming read summary request context
     * @param request - actual RPC request object
     * @return completable future comprising ReadShareGroupStateSummaryRequestData
     */
    CompletableFuture<ReadShareGroupStateSummaryResponseData> readStateSummary(RequestContext context, ReadShareGroupStateSummaryRequestData request);

    /**
     * Handle delete share group state call
     * @param context - represents the incoming delete share group request context
     * @param request - actual RPC request object
     * @return completable future representing delete share group RPC response data
     */
    CompletableFuture<DeleteShareGroupStateResponseData> deleteState(RequestContext context, DeleteShareGroupStateRequestData request);

    /**
     * Handle initialize share group state call
     * @param context - represents the incoming initialize share group request context
     * @param request - actual RPC request object
     * @return completable future representing initialize share group RPC response data
     */
    CompletableFuture<InitializeShareGroupStateResponseData> initializeState(RequestContext context, InitializeShareGroupStateRequestData request);

    /**
     * Called when new coordinator is elected
     * @param partitionIndex - The partition index (internal topic)
     * @param partitionLeaderEpoch - Leader epoch of the partition (internal topic)
     */
    void onElection(int partitionIndex, int partitionLeaderEpoch);

    /**
     * Called when coordinator goes down
     * @param partitionIndex - The partition index (internal topic)
     * @param partitionLeaderEpoch - Leader epoch of the partition (internal topic). Empty optional means deleted.
     */
    void onResignation(int partitionIndex, OptionalInt partitionLeaderEpoch);

    /**
     * Remove share group state related to deleted topic ids.
     *
     * @param topicPartitions   The deleted topic ids.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     */
    void onTopicsDeleted(Set<Uuid> topicPartitions, BufferSupplier bufferSupplier) throws ExecutionException, InterruptedException;

    /**
     * A new metadata image is available.
     *
     * @param newImage         The new metadata image.
     * @param newFeaturesImage The features image.
     * @param delta            The metadata delta.
     */
    void onNewMetadataImage(
        CoordinatorMetadataImage newImage,
        FeaturesImage newFeaturesImage, CoordinatorMetadataDelta delta
    );
}
