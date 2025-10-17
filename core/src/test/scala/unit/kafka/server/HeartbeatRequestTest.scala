/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.message.SyncGroupRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.group.classic.ClassicGroupState

import java.util.Collections
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
  )
)
class HeartbeatRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest
  def testHeartbeatWithOldConsumerGroupProtocol(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    for (version <- ApiKeys.HEARTBEAT.oldestVersion() to ApiKeys.HEARTBEAT.latestVersion(isUnstableApiEnabled)) {
      val metadata = ConsumerProtocol.serializeSubscription(
        new ConsumerPartitionAssignor.Subscription(Collections.singletonList("foo"))
      ).array

      val (leaderMemberId, leaderEpoch) = joinDynamicConsumerGroupWithOldProtocol(
        groupId = "grp",
        metadata = metadata,
        completeRebalance = false
      )

      // Heartbeat with unknown group id and unknown member id.
      heartbeat(
        groupId = "grp-unknown",
        memberId = "member-id-unknown",
        generationId = -1,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )

      // Heartbeat with unknown group id.
      heartbeat(
        groupId = "grp-unknown",
        memberId = leaderMemberId,
        generationId = -1,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )

      // Heartbeat with unknown member id.
      heartbeat(
        groupId = "grp",
        memberId = "member-id-unknown",
        generationId = -1,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )

      // Heartbeat with unmatched generation id.
      heartbeat(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = -1,
        expectedError = Errors.ILLEGAL_GENERATION,
        version = version.toShort
      )

      // Heartbeat COMPLETING_REBALANCE group.
      heartbeat(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = leaderEpoch,
        version = version.toShort
      )

      verifySyncGroupWithOldProtocol(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = leaderEpoch,
        assignments = List(new SyncGroupRequestData.SyncGroupRequestAssignment()
          .setMemberId(leaderMemberId)
          .setAssignment(Array[Byte](1))
        ),
        expectedAssignment = Array[Byte](1)
      )

      // Heartbeat STABLE group.
      heartbeat(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = leaderEpoch,
        version = version.toShort
      )

      // Join the second member.
      val joinFollowerResponseData = sendJoinRequest(
        groupId = "grp",
        metadata = metadata
      )

      Future {
        sendJoinRequest(
          groupId = "grp",
          memberId = joinFollowerResponseData.memberId,
          metadata = metadata
        )
      }

      TestUtils.waitUntilTrue(() => {
        val described = describeGroups(groupIds = List("grp"))
        ClassicGroupState.PREPARING_REBALANCE.toString == described.head.groupState
      }, msg = s"The group is not in PREPARING_REBALANCE state.")

      // Heartbeat PREPARING_REBALANCE group.
      heartbeat(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = leaderEpoch,
        expectedError = Errors.REBALANCE_IN_PROGRESS,
        version = version.toShort
      )

      sendJoinRequest(
        groupId = "grp",
        memberId = leaderMemberId,
        metadata = metadata
      )

      leaveGroup(
        groupId = "grp",
        memberId = leaderMemberId,
        useNewProtocol = false,
        version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
      )
      leaveGroup(
        groupId = "grp",
        memberId = joinFollowerResponseData.memberId,
        useNewProtocol = false,
        version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
      )

      // Heartbeat empty group.
      heartbeat(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = -1,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )

      // Heartbeat with empty group id.
      heartbeat(
        groupId = "",
        memberId = leaderMemberId,
        generationId = -1,
        expectedError = Errors.INVALID_GROUP_ID,
        version = version.toShort
      )
    }
  }
}
