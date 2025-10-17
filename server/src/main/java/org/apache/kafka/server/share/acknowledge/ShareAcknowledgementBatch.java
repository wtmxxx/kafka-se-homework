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

package org.apache.kafka.server.share.acknowledge;

import java.util.List;

/**
 * The ShareAcknowledgementBatch containing the fields required to acknowledge the fetched records.
 * The class abstracts the acknowledgement request for <code>SharePartition</code> class constructed
 * from {@link org.apache.kafka.common.message.ShareFetchRequestData.AcknowledgementBatch} and
 * {@link org.apache.kafka.common.message.ShareAcknowledgeRequestData.AcknowledgementBatch} classes.
 * <p>
 * Acknowledge types are represented as a list of bytes, where each byte corresponds to an acknowledge
 * type defined in {@link org.apache.kafka.clients.consumer.AcknowledgeType}.
 */
public record ShareAcknowledgementBatch(
    long firstOffset,
    long lastOffset,
    List<Byte> acknowledgeTypes
) {
    @Override
    public String toString() {
        return "ShareAcknowledgementBatch(" +
            "firstOffset=" + firstOffset +
            ", lastOffset=" + lastOffset +
            ", acknowledgeTypes=" + ((acknowledgeTypes == null) ? "" : acknowledgeTypes) +
            ")";
    }
}
