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
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * The ShareFetch class is used to store the fetch parameters for a share fetch request.
 */
public class ShareFetch {

    /**
     * The future that will be completed when the fetch is done.
     */
    private final CompletableFuture<Map<TopicIdPartition, PartitionData>> future;

    /**
     * The fetch parameters for the fetch request.
     */
    private final FetchParams fetchParams;
    /**
     * The group id of the share group that is fetching the records.
     */
    private final String groupId;
    /**
     * The member id of the share group that is fetching the records.
     */
    private final String memberId;
    /**
     * The topic partitions to be fetched.
     */
    private final List<TopicIdPartition> topicIdPartitions;
    /**
     * The batch size of the fetch request.
     */
    private final int batchSize;
    /**
     * The maximum number of records that can be fetched for the request.
     */
    private final int maxFetchRecords;
    /**
     * The handler to update the failed share fetch metrics.
     */
    private final BrokerTopicStats brokerTopicStats;

    /**
     * The partitions that had an error during the fetch.
     */
    private Map<TopicIdPartition, Throwable> erroneous;

    public ShareFetch(
        FetchParams fetchParams,
        String groupId,
        String memberId,
        CompletableFuture<Map<TopicIdPartition, PartitionData>> future,
        List<TopicIdPartition> topicIdPartitions,
        int batchSize,
        int maxFetchRecords,
        BrokerTopicStats brokerTopicStats
    ) {
        this.fetchParams = fetchParams;
        this.groupId = groupId;
        this.memberId = memberId;
        this.future = future;
        this.topicIdPartitions = topicIdPartitions;
        this.batchSize = batchSize;
        this.maxFetchRecords = maxFetchRecords;
        this.brokerTopicStats = brokerTopicStats;
    }

    public String groupId() {
        return groupId;
    }

    public String memberId() {
        return memberId;
    }

    public List<TopicIdPartition> topicIdPartitions() {
        return topicIdPartitions;
    }

    public FetchParams fetchParams() {
        return fetchParams;
    }

    public int batchSize() {
        return batchSize;
    }

    public int maxFetchRecords() {
        return maxFetchRecords;
    }

    /**
     * Add an erroneous partition to the share fetch request. If the erroneous map is null, it will
     * be created.
     * <p>
     * The method is synchronized to avoid concurrent modification of the erroneous map, as for
     * some partitions the pending initialization can be on some threads and for other partitions
     * share fetch request can be processed in purgatory.
     *
     * @param topicIdPartition The partition that had an error.
     * @param throwable The error that occurred.
     */
    public synchronized void addErroneous(TopicIdPartition topicIdPartition, Throwable throwable) {
        if (erroneous == null) {
            erroneous = new HashMap<>();
        }
        erroneous.put(topicIdPartition, throwable);
    }

    /**
     * Check if the share fetch request is completed.
     * @return true if the request is completed, false otherwise.
     */
    public boolean isCompleted() {
        return future.isDone();
    }

    /**
     * Check if all the partitions in the request have errored.
     * @return true if all the partitions in the request have errored, false otherwise.
     */
    public synchronized boolean errorInAllPartitions() {
        return erroneous != null && erroneous.size() == topicIdPartitions().size();
    }

    /**
     * May be complete the share fetch request with the given partition data. If the request is already completed,
     * this method does nothing. If there are any erroneous partitions, they will be added to the response.
     *
     * @param partitionData The partition data to complete the fetch with.
     */
    public void maybeComplete(Map<TopicIdPartition, PartitionData> partitionData) {
        if (isCompleted()) {
            return;
        }

        Map<TopicIdPartition, PartitionData> response = new HashMap<>(partitionData);
        // Add any erroneous partitions to the response.
        addErroneousToResponse(response);
        future.complete(response);
    }

    /**
     * Maybe complete the share fetch request with the given exception for the topicIdPartitions.
     * If the request is already completed, this method does nothing. If there are any erroneous partitions,
     * they will be added to the response.
     *
     * @param topicIdPartitions The topic id partitions which errored out.
     * @param throwable The exception to complete the fetch with.
     */
    public void maybeCompleteWithException(Collection<TopicIdPartition> topicIdPartitions, Throwable throwable) {
        if (isCompleted()) {
            return;
        }
        Map<TopicIdPartition, PartitionData> response = new HashMap<>();
        // Add the exception to erroneous partitions to track the error.
        topicIdPartitions.forEach(tp -> addErroneous(tp, throwable));
        // Add any erroneous partitions to the response.
        addErroneousToResponse(response);
        future.complete(response);
    }

    /**
     * Filter out the erroneous partitions from the given set of topicIdPartitions. The order of
     * partitions is important hence the method expects an ordered set as input and returns the ordered
     * set as well.
     *
     * @param topicIdPartitions The topic id partitions to filter.
     * @return The topic id partitions without the erroneous partitions.
     */
    public synchronized Set<TopicIdPartition> filterErroneousTopicPartitions(Set<TopicIdPartition> topicIdPartitions) {
        if (erroneous != null) {
            Set<TopicIdPartition> retain = new LinkedHashSet<>(topicIdPartitions);
            retain.removeAll(erroneous.keySet());
            return retain;
        }
        return topicIdPartitions;
    }

    private synchronized void addErroneousToResponse(Map<TopicIdPartition, PartitionData> response) {
        if (erroneous != null) {
            // Track the failed topics for metrics.
            Set<String> erroneousTopics = new HashSet<>();
            erroneous.forEach((topicIdPartition, throwable) -> {
                erroneousTopics.add(topicIdPartition.topic());
                response.put(topicIdPartition, new PartitionData()
                    .setPartitionIndex(topicIdPartition.partition())
                    .setErrorCode(Errors.forException(throwable).code())
                    .setErrorMessage(throwable.getMessage())
                    .setRecords(MemoryRecords.EMPTY));
            });
            erroneousTopics.forEach(topic -> {
                brokerTopicStats.allTopicsStats().failedShareFetchRequestRate().mark();
                brokerTopicStats.topicStats(topic).failedShareFetchRequestRate().mark();
            });
        }
    }
}
