/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse}
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.apache.kafka.storage.internals.log.{LogStartOffsetIncrementReason, OffsetResultHolder, UnifiedLog}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}

import java.io.File
import java.util.{Optional, Properties, Random}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

@Timeout(300)
class LogOffsetTest extends BaseRequestTest {

  override def brokerCount = 1

  protected override def brokerPropertyOverrides(props: Properties): Unit = {
    props.put("log.flush.interval.messages", "1")
    props.put("num.partitions", "20")
    props.put("log.retention.hours", "10")
    props.put("log.retention.check.interval.ms", (5 * 1000 * 60).toString)
  }

  @Test
  def testGetOffsetsForUnknownTopic(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val request = ListOffsetsRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.LATEST_TIMESTAMP).asJava).build(1)
    val response = sendListOffsetsRequest(request)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, findPartition(response.topics.asScala, topicPartition).errorCode)
  }

  @deprecated("ListOffsetsRequest V0", since = "")
  @Test
  def testGetOffsetsAfterDeleteRecords(): Unit = {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)
    val log = createTopicAndGetLog(topic, topicPartition)

    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), 0)
    log.flush(false)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(3, LogStartOffsetIncrementReason.ClientRecordDeletion)
    log.deleteOldSegments()

    val offset = log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty).timestampAndOffsetOpt.map(_.offset)
    assertEquals(Optional.of(20L), offset)

    TestUtils.waitUntilTrue(() => isLeaderLocalOnBroker(topic, topicPartition.partition, broker),
      "Leader should be elected")
    val request = ListOffsetsRequest.Builder.forReplica(1, 1)
      .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.LATEST_TIMESTAMP).asJava).build()
    val consumerOffset = findPartition(sendListOffsetsRequest(request).topics.asScala, topicPartition).offset
    assertEquals(20L, consumerOffset)
  }

  @Test
  def testFetchOffsetByTimestampForMaxTimestampAfterTruncate(): Unit = {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)
    val log = createTopicAndGetLog(topic, topicPartition)

    for (timestamp <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes(), timestamp = timestamp.toLong), 0)
    log.flush(false)

    log.updateHighWatermark(log.logEndOffset)

    val firstOffset = log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP, Optional.empty).timestampAndOffsetOpt
    assertEquals(19L, firstOffset.get.offset)
    assertEquals(19L, firstOffset.get.timestamp)

    log.truncateTo(0)

    assertEquals(Optional.empty, log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP, Optional.empty).timestampAndOffsetOpt)
  }

  @Test
  def testFetchOffsetByTimestampForMaxTimestampWithUnorderedTimestamps(): Unit = {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)
    val log = createTopicAndGetLog(topic, topicPartition)

    for (timestamp <- List(0L, 1L, 2L, 3L, 4L, 6L, 5L))
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes(), timestamp = timestamp), 0)
    log.flush(false)

    log.updateHighWatermark(log.logEndOffset)

    val maxTimestampOffset = log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP, Optional.empty).timestampAndOffsetOpt
    assertEquals(7L, log.logEndOffset)
    assertEquals(5L, maxTimestampOffset.get.offset)
    assertEquals(6L, maxTimestampOffset.get.timestamp)
  }

  @Test
  def testGetOffsetsBeforeLatestTime(): Unit = {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)
    val log = createTopicAndGetLog(topic, topicPartition)

    val topicIds = getTopicIds(Seq("kafka-")).asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    val topicId = topicIds.get(topic)

    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), 0)
    log.flush(false)

    val offset = log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty).timestampAndOffsetOpt.map(_.offset)
    assertEquals(Optional.of(20L), offset)

    TestUtils.waitUntilTrue(() => isLeaderLocalOnBroker(topic, 0, broker),
      "Leader should be elected")
    val request = ListOffsetsRequest.Builder.forReplica(1, 1)
      .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.LATEST_TIMESTAMP).asJava).build()
    val consumerOffset = findPartition(sendListOffsetsRequest(request).topics.asScala, topicPartition).offset
    assertEquals(20L, consumerOffset)

    // try to fetch using latest offset
    val fetchRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion, 0, 1,
      Map(topicPartition -> new FetchRequest.PartitionData(topicId, consumerOffset, FetchRequest.INVALID_LOG_START_OFFSET,
        300 * 1024, Optional.empty())).asJava).build()
    val fetchResponse = sendFetchRequest(fetchRequest)
    assertFalse(FetchResponse.recordsOrFail(fetchResponse.responseData(topicNames, ApiKeys.FETCH.latestVersion).get(topicPartition)).batches.iterator.hasNext)
  }

  @Test
  def testEmptyLogsGetOffsets(): Unit = {
    val random = new Random
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, random.nextInt(10))
    val topicPartitionPath = s"${TestUtils.tempDir().getAbsolutePath}/$topic-${topicPartition.partition}"
    val topicLogDir = new File(topicPartitionPath)
    topicLogDir.mkdir()

    createTopic(topic)

    var offsetChanged = false
    for (_ <- 1 to 14) {
      val topicPartition = new TopicPartition(topic, 0)
      val request = ListOffsetsRequest.Builder.forReplica(1, 1)
        .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.EARLIEST_TIMESTAMP).asJava).build()
      val consumerOffset = findPartition(sendListOffsetsRequest(request).topics.asScala, topicPartition).offset
      if (consumerOffset == 1)
        offsetChanged = true
    }
    assertFalse(offsetChanged)
  }

  @Test
  def testFetchOffsetByTimestampForMaxTimestampWithEmptyLog(): Unit = {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)
    val log = createTopicAndGetLog(topic, topicPartition)

    log.updateHighWatermark(log.logEndOffset)

    assertEquals(0L, log.logEndOffset)
    assertEquals(new OffsetResultHolder(Optional.empty[FileRecords.TimestampAndOffset]()), log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP, Optional.empty))
  }

  @Test
  def testGetOffsetsBeforeEarliestTime(): Unit = {
    val random = new Random
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, random.nextInt(3))

    createTopic(topic, 3)

    val logManager = broker.logManager
    val log = logManager.getOrCreateLog(topicPartition, topicId = Optional.empty)
    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), 0)
    log.flush(false)

    val offset = log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Optional.empty).timestampAndOffsetOpt.map(_.offset)
    assertEquals(Optional.of(0L), offset)

    TestUtils.waitUntilTrue(() => isLeaderLocalOnBroker(topic, topicPartition.partition, broker),
      "Leader should be elected")
    val request = ListOffsetsRequest.Builder.forReplica(1, 1)
      .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.EARLIEST_TIMESTAMP).asJava).build()
    val offsetFromResponse = findPartition(sendListOffsetsRequest(request).topics.asScala, topicPartition).offset
    assertEquals(0L, offsetFromResponse)
  }

  private def broker: KafkaBroker = brokers.head

  private def sendListOffsetsRequest(request: ListOffsetsRequest): ListOffsetsResponse = {
    connectAndReceive[ListOffsetsResponse](request)
  }

  private def sendFetchRequest(request: FetchRequest): FetchResponse = {
    connectAndReceive[FetchResponse](request)
  }

  private def buildTargetTimes(tp: TopicPartition, timestamp: Long): List[ListOffsetsTopic] = {
    List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(timestamp)).asJava)
    )
  }

  private def findPartition(topics: mutable.Buffer[ListOffsetsTopicResponse], tp: TopicPartition): ListOffsetsPartitionResponse = {
    topics.find(_.name == tp.topic).get
      .partitions.asScala.find(_.partitionIndex == tp.partition).get
  }

  private def createTopicAndGetLog(topic: String, topicPartition: TopicPartition): UnifiedLog = {
    createTopic(topic)

    val logManager = broker.logManager
    TestUtils.waitUntilTrue(() => logManager.getLog(topicPartition).isDefined,
      "Log for partition [topic,0] should be created")
    logManager.getLog(topicPartition).get
  }

  private def isLeaderLocalOnBroker(topic: String, partitionId: Int, broker: KafkaBroker): Boolean = {
    broker.replicaManager.onlinePartition(new TopicPartition(topic, partitionId)).exists(_.leaderLogIfLocal.isDefined)
  }
}
