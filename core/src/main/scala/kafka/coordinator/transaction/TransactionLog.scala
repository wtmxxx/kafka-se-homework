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
package kafka.coordinator.transaction

import java.nio.ByteBuffer
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.protocol.{ByteBufferAccessor, MessageUtil}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.coordinator.transaction.{TransactionMetadata, TransactionState, TxnTransitMetadata}
import org.apache.kafka.coordinator.transaction.generated.{CoordinatorRecordType, TransactionLogKey, TransactionLogValue}
import org.apache.kafka.server.common.TransactionVersion

import java.util

import scala.jdk.CollectionConverters._

/**
 * Messages stored for the transaction topic represent the producer id and transactional status of the corresponding
 * transactional id, which have versions for both the key and value fields. Key and value
 * versions are used to evolve the message formats:
 *
 * key version 0:               [transactionalId]
 *    -> value version 0:       [producer_id, producer_epoch, expire_timestamp, status, [topic, [partition] ], timestamp]
 */
object TransactionLog {

  // enforce always using
  //  1. cleanup policy = compact
  //  2. compression = none
  //  3. unclean leader election = disabled
  //  4. required acks = -1 when writing
  val EnforcedCompression: Compression = Compression.NONE
  val EnforcedRequiredAcks: Short = (-1).toShort

  /**
    * Generates the bytes for transaction log message key
    *
    * @return key bytes
    */
  def keyToBytes(transactionalId: String): Array[Byte] = {
    MessageUtil.toCoordinatorTypePrefixedBytes(new TransactionLogKey().setTransactionalId(transactionalId))
  }

  /**
    * Generates the payload bytes for transaction log message value
    *
    * @return value payload bytes
    */
  def valueToBytes(txnMetadata: TxnTransitMetadata,
                                        transactionVersionLevel: TransactionVersion): Array[Byte] = {
    if (txnMetadata.txnState == TransactionState.EMPTY && !txnMetadata.topicPartitions.isEmpty)
        throw new IllegalStateException(s"Transaction is not expected to have any partitions since its state is ${txnMetadata.txnState}: $txnMetadata")

      val transactionPartitions = if (txnMetadata.txnState == TransactionState.EMPTY) null
      else txnMetadata.topicPartitions.asScala
        .groupBy(_.topic)
        .map { case (topic, partitions) =>
          new TransactionLogValue.PartitionsSchema()
            .setTopic(topic)
            .setPartitionIds(partitions.map(tp => Integer.valueOf(tp.partition)).toList.asJava)
        }.toList.asJava

    // Serialize with version 0 (highest non-flexible version) until transaction.version 1 is enabled
    // which enables flexible fields in records.
    MessageUtil.toVersionPrefixedBytes(transactionVersionLevel.transactionLogValueVersion(),
      new TransactionLogValue()
        .setProducerId(txnMetadata.producerId)
        .setProducerEpoch(txnMetadata.producerEpoch)
        .setTransactionTimeoutMs(txnMetadata.txnTimeoutMs)
        .setTransactionStatus(txnMetadata.txnState.id)
        .setTransactionLastUpdateTimestampMs(txnMetadata.txnLastUpdateTimestamp)
        .setTransactionStartTimestampMs(txnMetadata.txnStartTimestamp)
        .setTransactionPartitions(transactionPartitions)
        .setClientTransactionVersion(txnMetadata.clientTransactionVersion.featureLevel()))
  }

  /**
    * Decodes the transaction log messages' key
    *
    * @return left with the version if the key is not a transaction log key, right with the transactional id otherwise
    */
  def readTxnRecordKey(buffer: ByteBuffer): Either[Short, String] = {
    val version = buffer.getShort
    Either.cond(
      version == CoordinatorRecordType.TRANSACTION_LOG.id,
      new TransactionLogKey(new ByteBufferAccessor(buffer), 0.toShort).transactionalId,
      version
    )
  }

  /**
    * Decodes the transaction log messages' payload and retrieves the transaction metadata from it
    *
    * @return a transaction metadata object from the message
    */
  def readTxnRecordValue(transactionalId: String, buffer: ByteBuffer): Option[TransactionMetadata] = {
    // tombstone
    if (buffer == null) None
    else {
      val version = buffer.getShort
      if (version >= TransactionLogValue.LOWEST_SUPPORTED_VERSION && version <= TransactionLogValue.HIGHEST_SUPPORTED_VERSION) {
        val value = new TransactionLogValue(new ByteBufferAccessor(buffer), version)
        val state = TransactionState.fromId(value.transactionStatus)
        val tps: util.Set[TopicPartition] = new util.HashSet[TopicPartition]()
        if (!state.equals(TransactionState.EMPTY))
          value.transactionPartitions.forEach(partitionsSchema => {
            partitionsSchema.partitionIds.forEach(partitionId => tps.add(new TopicPartition(partitionsSchema.topic, partitionId.intValue())))
          })
        Some(new TransactionMetadata(
          transactionalId,
          value.producerId,
          value.previousProducerId,
          value.nextProducerId,
          value.producerEpoch,
          RecordBatch.NO_PRODUCER_EPOCH,
          value.transactionTimeoutMs,
          state,
          tps,
          value.transactionStartTimestampMs,
          value.transactionLastUpdateTimestampMs,
          TransactionVersion.fromFeatureLevel(value.clientTransactionVersion)))
      } else throw new IllegalStateException(s"Unknown version $version from the transaction log message value")
    }
  }
}
