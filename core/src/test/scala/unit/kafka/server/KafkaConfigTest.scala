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

import java.net.InetSocketAddress
import java.util
import java.util.{Arrays, Collections, Properties}
import kafka.utils.TestUtils.assertBadConfigContainingMessage
import kafka.utils.{CoreUtils, TestUtils}
import org.apache.kafka.common.{Endpoint, Node}
import org.apache.kafka.common.config.{AbstractConfig, ConfigException, SaslConfigs, SecurityConfig, SslConfigs, TopicConfig}
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.{CompressionType, Records}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.utils.LogCaptureAppender
import org.apache.kafka.coordinator.group.ConsumerGroupMigrationPolicy
import org.apache.kafka.coordinator.group.Group.GroupType
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig
import org.apache.kafka.coordinator.transaction.{TransactionLogConfig, TransactionStateManagerConfig}
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.{MetadataLogConfig, QuorumConfig}
import org.apache.kafka.server.config.{DelegationTokenManagerConfigs, KRaftConfigs, QuotaConfig, ReplicationConfigs, ServerConfigs, ServerLogConfigs, ServerTopicConfigSynonyms}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.MetricConfigs
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.apache.logging.log4j.Level
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable

import scala.jdk.CollectionConverters._
import scala.util.Using

class KafkaConfigTest {

  def createDefaultConfig(): Properties = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker,controller")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:0,CONTROLLER://localhost:5000")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:5000")
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "PLAINTEXT:PLAINTEXT,CONTROLLER:SASL_SSL")
    props
  }

  @Test
  def testLogRetentionTimeHoursProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(60L * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeMinutesProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, "30")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeMsProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "1800000")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeNoConfigProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeBothMinutesAndHoursProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, "30")
    props.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeBothMinutesAndMsProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "1800000")
    props.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, "10")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionUnlimited(): Unit = {
    val props1 = TestUtils.createBrokerConfig(0, port = 8181)
    val props2 = TestUtils.createBrokerConfig(0, port = 8181)
    val props3 = TestUtils.createBrokerConfig(0, port = 8181)
    val props4 = TestUtils.createBrokerConfig(0, port = 8181)
    val props5 = TestUtils.createBrokerConfig(0, port = 8181)

    props1.setProperty("log.retention.ms", "-1")
    props2.setProperty("log.retention.minutes", "-1")
    props3.setProperty("log.retention.hours", "-1")

    val cfg1 = KafkaConfig.fromProps(props1)
    val cfg2 = KafkaConfig.fromProps(props2)
    val cfg3 = KafkaConfig.fromProps(props3)
    assertEquals(-1, cfg1.logRetentionTimeMillis, "Should be -1")
    assertEquals(-1, cfg2.logRetentionTimeMillis, "Should be -1")
    assertEquals(-1, cfg3.logRetentionTimeMillis, "Should be -1")

    props4.setProperty("log.retention.ms", "-1")
    props4.setProperty("log.retention.minutes", "30")

    val cfg4 = KafkaConfig.fromProps(props4)
    assertEquals(-1, cfg4.logRetentionTimeMillis, "Should be -1")

    props5.setProperty("log.retention.ms", "0")

    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props5))
  }

  @Test
  def testLogRetentionValid(): Unit = {
    val props1 = TestUtils.createBrokerConfig(0, port = 8181)
    val props2 = TestUtils.createBrokerConfig(0, port = 8181)
    val props3 = TestUtils.createBrokerConfig(0, port = 8181)

    props1.setProperty("log.retention.ms", "0")
    props2.setProperty("log.retention.minutes", "0")
    props3.setProperty("log.retention.hours", "0")

    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props1))
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props2))
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props3))
  }

  @Test
  def testAdvertiseDefaults(): Unit = {
    val brokerProt = 9999
    val controllerPort = 10000
    val hostName = "fake-host"
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, s"$hostName:$controllerPort")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, s"PLAINTEXT://$hostName:$brokerProt")
    val serverConfig = KafkaConfig.fromProps(props)

    val endpoints = serverConfig.effectiveAdvertisedBrokerListeners
    assertEquals(1, endpoints.size)
    val endpoint = endpoints.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get
    assertEquals(endpoint.host, hostName)
    assertEquals(endpoint.port, brokerProt)
  }

  @Test
  def testAdvertiseConfigured(): Unit = {
    val advertisedHostName = "routable-host"
    val advertisedPort = 1234

    val props = createDefaultConfig()
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, s"PLAINTEXT://$advertisedHostName:$advertisedPort")

    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.effectiveAdvertisedBrokerListeners
    val endpoint = endpoints.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get

    assertEquals(endpoint.host, advertisedHostName)
    assertEquals(endpoint.port, advertisedPort)
  }

  @Test
  def testDuplicateListeners(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "localhost:9095")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")

    // listeners with duplicate port
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9091,SSL://localhost:9091")
    assertBadConfigContainingMessage(props, "Each listener must have a different port")

    // listeners with duplicate name
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9091,PLAINTEXT://localhost:9092")
    assertBadConfigContainingMessage(props, "Each listener must have a different name")

    // advertised listeners can have duplicate ports
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "HOST:SASL_SSL,LB:SASL_SSL,CONTROLLER:SASL_SSL")
    props.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "HOST")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "HOST://localhost:9091,LB://localhost:9092")
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "HOST://localhost:9091,LB://localhost:9091")
    KafkaConfig.fromProps(props)

    // but not duplicate names
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "HOST://localhost:9091,HOST://localhost:9091")
    assertBadConfigContainingMessage(props, "Configuration 'advertised.listeners' values must not be duplicated.")
  }

  @Test
  def testIPv4AndIPv6SamePortListeners(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.put(ServerConfigs.BROKER_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "CONTROLLER://localhost:9091")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT + ",CONTROLLER:PLAINTEXT")

    props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://[::1]:9092,SSL://[::1]:9092")
    var caught = assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
    assertTrue(caught.getMessage.contains("If you have two listeners on the same port then one needs to be IPv4 and the other IPv6"))

    props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9092")
    caught = assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
    assertTrue(caught.getMessage.contains("If you have two listeners on the same port then one needs to be IPv4 and the other IPv6"))

    props.put(SocketServerConfigs.LISTENERS_CONFIG, "SSL://[::1]:9096,PLAINTEXT://127.0.0.1:9096,SASL_SSL://:9096")
    caught = assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
    assertTrue(caught.getMessage.contains("If you have two listeners on the same port then one needs to be IPv4 and the other IPv6"))

    props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092,PLAINTEXT://127.0.0.1:9092")
    val exception = assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
    assertTrue(exception.getMessage.contains("values must not be duplicated."))

    props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9092,SASL_SSL://127.0.0.1:9092")
    caught = assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
    assertTrue(caught.getMessage.contains("Each listener must have a different port"))

    props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://apache.org:9092,SSL://[::1]:9092")
    caught = assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
    assertTrue(caught.getMessage.contains("Each listener must have a different port"))

    props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://apache.org:9092,SSL://127.0.0.1:9092")
    caught = assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
    assertTrue(caught.getMessage.contains("Each listener must have a different port"))

    // Happy case
    props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092,SSL://[::1]:9092")
    assertTrue(isValidKafkaConfig(props))
    props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://[::1]:9093,SSL://127.0.0.1:9093")
    assertTrue(isValidKafkaConfig(props))
    props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9094,SSL://[::1]:9094,SASL_SSL://127.0.0.1:9095,SASL_PLAINTEXT://[::1]:9095")
    assertTrue(isValidKafkaConfig(props))
    props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://[::1]:9096,SSL://127.0.0.1:9096,SASL_SSL://[::1]:9097,SASL_PLAINTEXT://127.0.0.1:9097")
    assertTrue(isValidKafkaConfig(props))
  }

  @Test
  def testControllerListenerNames(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker,controller")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:0,CONTROLLER://localhost:5000")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:5000")
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "PLAINTEXT:PLAINTEXT,CONTROLLER:SASL_SSL")

    val serverConfig = KafkaConfig.fromProps(props)
    val controllerEndpoints = serverConfig.controllerListeners
    assertEquals(1, controllerEndpoints.size)
    val controllerEndpoint = controllerEndpoints.iterator.next()
    assertEquals("localhost", controllerEndpoint.host)
    assertEquals(5000, controllerEndpoint.port)
    assertEquals(SecurityProtocol.SASL_SSL, controllerEndpoint.securityProtocol)
  }

  @Test
  def testControllerListenerDefinedForKRaftController(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "SSL://localhost:9093")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")

    assertBadConfigContainingMessage(props, 
      "Missing required configuration \"controller.listener.names\" which has no default value.")

    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    KafkaConfig.fromProps(props)

    // confirm that redirecting via listener.security.protocol.map is acceptable
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:SSL")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://localhost:9093")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testControllerListenerDefinedForKRaftBroker(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")

    assertFalse(isValidKafkaConfig(props))
    assertBadConfigContainingMessage(props, 
      "Missing required configuration \"controller.listener.names\" which has no default value.")

    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    KafkaConfig.fromProps(props)

    // confirm that redirecting via listener.security.protocol.map is acceptable
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testEffectAdvertiseControllerListenerForControllerWithAdvertisement(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://localhost:9093")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "CONTROLLER://lb1.example.com:9000")

    val config = KafkaConfig.fromProps(props)
    assertEquals(
      Seq(new Endpoint("CONTROLLER", SecurityProtocol.PLAINTEXT, "lb1.example.com", 9000)),
      config.effectiveAdvertisedControllerListeners
    )
  }

  @Test
  def testEffectAdvertiseControllerListenerForControllerWithoutAdvertisement(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://localhost:9093")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")

    val config = KafkaConfig.fromProps(props)
    assertEquals(
      Seq(new Endpoint("CONTROLLER", SecurityProtocol.PLAINTEXT, "localhost", 9093)),
      config.effectiveAdvertisedControllerListeners
    )
  }

  @Test
  def testEffectAdvertiseControllerListenerForControllerWithMixedAdvertisement(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://localhost:9093,CONTROLLER_NEW://localhost:9094")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER,CONTROLLER_NEW")
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "CONTROLLER://lb1.example.com:9000")

    val config = KafkaConfig.fromProps(props)
    assertEquals(
      Seq(
        new Endpoint("CONTROLLER", SecurityProtocol.PLAINTEXT, "lb1.example.com", 9000),
        new Endpoint("CONTROLLER_NEW", SecurityProtocol.PLAINTEXT, "localhost", 9094)
      ),
      config.effectiveAdvertisedControllerListeners
    )
  }

  @Test
  def testPortInQuorumVotersNotRequiredToMatchFirstControllerListenerPortForThisKRaftController(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller,broker")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:9094")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093,3@anotherhost:9094")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL,SASL_SSL")
    KafkaConfig.fromProps(props)

    // change each of the 4 ports to port 5555 -- should pass in all circumstances since we can't validate the
    // controller.quorum.voters ports (which are the ports that clients use and are semantically "advertised" ports
    // even though the controller configuration doesn't list them in advertised.listeners) against the
    // listener ports (which are semantically different then the ports that clients use).
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092,SSL://localhost:5555,SASL_SSL://localhost:9094")
    KafkaConfig.fromProps(props)
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:5555")
    KafkaConfig.fromProps(props)
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:9094") // reset to original value
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:5555,3@anotherhost:9094")
    KafkaConfig.fromProps(props)
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093,3@anotherhost:5555")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testSeparateControllerListenerDefinedForKRaftBrokerController(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker,controller")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "SSL://localhost:9093")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")

    assertFalse(isValidKafkaConfig(props))
    assertBadConfigContainingMessage(
      props,
      "There must be at least one broker advertised listener. Perhaps all listeners appear in controller.listener.names?"
    )

    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
    KafkaConfig.fromProps(props)

    // confirm that redirecting via listener.security.protocol.map is acceptable
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093")
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testControllerListenerNameMapsToPlaintextByDefaultForKRaft(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")
    val controllerListenerName = new ListenerName("CONTROLLER")
    assertEquals(SecurityProtocol.PLAINTEXT,
      KafkaConfig.fromProps(props).effectiveListenerSecurityProtocolMap.get(controllerListenerName))
    // ensure we don't map it to PLAINTEXT when there is a SSL or SASL controller listener
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER,SSL")
    val controllerNotFoundInMapMessage = "Controller listener with name CONTROLLER defined in controller.listener.names not found in listener.security.protocol.map"
    assertBadConfigContainingMessage(props, controllerNotFoundInMapMessage)
    // ensure we don't map it to PLAINTEXT when there is a SSL or SASL listener
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "SSL://localhost:9092")
    assertBadConfigContainingMessage(props, controllerNotFoundInMapMessage)
    props.remove(SocketServerConfigs.LISTENERS_CONFIG)
    // ensure we don't map it to PLAINTEXT when it is explicitly mapped otherwise
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL")
    assertEquals(SecurityProtocol.SSL,
      KafkaConfig.fromProps(props).effectiveListenerSecurityProtocolMap.get(controllerListenerName))
    // ensure we don't map it to PLAINTEXT when anything is explicitly given
    // (i.e. it is only part of the default value, even with KRaft)
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "PLAINTEXT:PLAINTEXT")
    assertBadConfigContainingMessage(props, controllerNotFoundInMapMessage)
    // ensure we can map it to a non-PLAINTEXT security protocol by default (i.e. when nothing is given)
    props.remove(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG)
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    assertEquals(SecurityProtocol.SSL,
      KafkaConfig.fromProps(props).effectiveListenerSecurityProtocolMap.get(new ListenerName("SSL")))
  }

  @Test
  def testMultipleControllerListenerNamesMapToPlaintextByDefaultForKRaft(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER1://localhost:9092,CONTROLLER2://localhost:9093")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER1,CONTROLLER2")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9092")
    assertEquals(SecurityProtocol.PLAINTEXT,
      KafkaConfig.fromProps(props).effectiveListenerSecurityProtocolMap.get(new ListenerName("CONTROLLER1")))
    assertEquals(SecurityProtocol.PLAINTEXT,
      KafkaConfig.fromProps(props).effectiveListenerSecurityProtocolMap.get(new ListenerName("CONTROLLER2")))
  }

  @Test
  def testBadListenerProtocol(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "CONTROLLER://localhost:9092")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "BAD://localhost:9091")

    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testListenerNamesWithAdvertisedListenerUnset(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "CONTROLLER://localhost:9092")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")

    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "CLIENT://localhost:9091,REPLICATION://localhost:9092,INTERNAL://localhost:9093")
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CLIENT:SSL,REPLICATION:SSL,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
    props.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "REPLICATION")
    val config = KafkaConfig.fromProps(props)
    val expectedListeners = Seq(
      new Endpoint("CLIENT", SecurityProtocol.SSL, "localhost", 9091),
      new Endpoint("REPLICATION", SecurityProtocol.SSL, "localhost", 9092),
      new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9093))
    assertEquals(expectedListeners, config.listeners)
    assertEquals(expectedListeners, config.effectiveAdvertisedBrokerListeners)
    val expectedSecurityProtocolMap = util.Map.of(
      new ListenerName("CLIENT"), SecurityProtocol.SSL,
      new ListenerName("REPLICATION"), SecurityProtocol.SSL,
      new ListenerName("INTERNAL"), SecurityProtocol.PLAINTEXT,
      new ListenerName("CONTROLLER"), SecurityProtocol.PLAINTEXT
    )
    assertEquals(expectedSecurityProtocolMap, config.effectiveListenerSecurityProtocolMap)
  }

  @Test
  def testListenerAndAdvertisedListenerNames(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "CONTROLLER://localhost:9092")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")

    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "EXTERNAL://localhost:9091,INTERNAL://localhost:9093")
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "EXTERNAL://lb1.example.com:9000,INTERNAL://host1:9093")
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "EXTERNAL:SSL,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
    props.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "INTERNAL")
    val config = KafkaConfig.fromProps(props)

    val expectedListeners = Seq(
      new Endpoint("EXTERNAL", SecurityProtocol.SSL, "localhost", 9091),
      new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9093)
    )
    assertEquals(expectedListeners, config.listeners)

    val expectedAdvertisedListeners = Seq(
      new Endpoint("EXTERNAL", SecurityProtocol.SSL, "lb1.example.com", 9000),
      new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "host1", 9093)
    )
    assertEquals(expectedAdvertisedListeners, config.effectiveAdvertisedBrokerListeners)

    val expectedSecurityProtocolMap = util.Map.of(
      new ListenerName("EXTERNAL"), SecurityProtocol.SSL,
      new ListenerName("INTERNAL"), SecurityProtocol.PLAINTEXT,
      new ListenerName("CONTROLLER"), SecurityProtocol.PLAINTEXT
    )
    assertEquals(expectedSecurityProtocolMap, config.effectiveListenerSecurityProtocolMap)
  }

  @Test
  def testListenerNameMissingFromListenerSecurityProtocolMap(): Unit = {
    val props = createDefaultConfig()

    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "SSL://localhost:9091,REPLICATION://localhost:9092")
    props.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "SSL")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testInterBrokerListenerNameMissingFromListenerSecurityProtocolMap(): Unit = {
    val props = createDefaultConfig()

    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "SSL://localhost:9091")
    props.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "REPLICATION")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testInterBrokerListenerNameAndSecurityProtocolSet(): Unit = {
    val props = createDefaultConfig()

    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "SSL://localhost:9091")
    props.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "SSL")
    props.setProperty(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG, "SSL")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testCaseInsensitiveListenerProtocol(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "CONTROLLER://localhost:9093")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "plaintext://localhost:9091,SsL://localhost:9092")
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "PLAINTEXT:PLAINTEXT,SSL:SSL,CONTROLLER:PLAINTEXT")
    val config = KafkaConfig.fromProps(props)
    assertEndpointsEqual(new Endpoint("SSL", SecurityProtocol.SSL, "localhost", 9092),
      config.listeners.find(_.listener == "SSL").getOrElse(fail("SSL endpoint not found")))
    assertEndpointsEqual( new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9091),
      config.listeners.find(_.listener == "PLAINTEXT").getOrElse(fail("PLAINTEXT endpoint not found")))
  }

  private def assertEndpointsEqual(expected: Endpoint, actual: Endpoint): Unit = {
    assertEquals(expected.host(), actual.host(), "Host mismatch")
    assertEquals(expected.port(), actual.port(), "Port mismatch")
    assertEquals(expected.listener(), actual.listener(), "Listener mismatch")
    assertEquals(expected.securityProtocol(), actual.securityProtocol(), "Security protocol mismatch")
  }

  private def listenerListToEndPoints(listenerList: java.util.List[String],
                              securityProtocolMap: util.Map[ListenerName, SecurityProtocol] = SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO) =
    CoreUtils.listenerListToEndPoints(listenerList, securityProtocolMap)

  @Test
  def testListenerDefaults(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "localhost:9093")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")

    // configuration with no listeners
    val conf = KafkaConfig.fromProps(props)
    assertEquals(listenerListToEndPoints(util.List.of("PLAINTEXT://:9092")), conf.listeners)
    assertNull(conf.listeners.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get.host)
    assertEquals(conf.effectiveAdvertisedBrokerListeners, listenerListToEndPoints(util.List.of("PLAINTEXT://:9092")))
  }

  private def isValidKafkaConfig(props: Properties): Boolean = {
    try {
      KafkaConfig.fromProps(props)
      true
    } catch {
      case _: IllegalArgumentException | _: ConfigException => false
    }
  }

  @Test
  def testUncleanLeaderElectionDefault(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, false)
  }

  @Test
  def testUncleanElectionDisabled(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, String.valueOf(false))
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, false)
  }

  @Test
  def testUncleanElectionEnabled(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, String.valueOf(true))
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, true)
  }

  @Test
  def testUncleanElectionInvalid(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "invalid")

    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testLogRollTimeMsProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, "1800000")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRollTimeMillis)
  }

  @Test
  def testLogRollTimeBothMsAndHoursProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, "1800000")
    props.setProperty(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRollTimeMillis)
  }

  @Test
  def testLogRollTimeNoConfigProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRollTimeMillis																									)
  }

  @Test
  def testDefaultCompressionType(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    val serverConfig = KafkaConfig.fromProps(props)
    assertEquals(serverConfig.compressionType, "producer")
  }

  @Test
  def testValidCompressionType(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty("compression.type", "gzip")
    val serverConfig = KafkaConfig.fromProps(props)
    assertEquals(serverConfig.compressionType, "gzip")
  }

  @Test
  def testInvalidCompressionType(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ServerConfigs.COMPRESSION_TYPE_CONFIG, "abc")
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testInvalidGzipCompressionLevel(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ServerConfigs.COMPRESSION_TYPE_CONFIG, "gzip")
    props.setProperty(ServerConfigs.COMPRESSION_GZIP_LEVEL_CONFIG, (CompressionType.GZIP.maxLevel() + 1).toString)
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testInvalidLz4CompressionLevel(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ServerConfigs.COMPRESSION_TYPE_CONFIG, "lz4")
    props.setProperty(ServerConfigs.COMPRESSION_LZ4_LEVEL_CONFIG, (CompressionType.LZ4.maxLevel() + 1).toString)
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testInvalidZstdCompressionLevel(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ServerConfigs.COMPRESSION_TYPE_CONFIG, "zstd")
    props.setProperty(ServerConfigs.COMPRESSION_ZSTD_LEVEL_CONFIG, (CompressionType.ZSTD.maxLevel() + 1).toString)
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testInvalidInterBrokerSecurityProtocol(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "SSL://localhost:0")
    props.setProperty(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.toString)
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testEqualAdvertisedListenersProtocol(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testInvalidAdvertisedListenersProtocol(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "TRACE://localhost:9091,SSL://localhost:9093")
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "PLAINTEXT://localhost:9092")
    assertBadConfigContainingMessage(props, "No security protocol defined for listener TRACE")

    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,TRACE:PLAINTEXT,SSL:SSL")
    assertBadConfigContainingMessage(props, "advertised.listeners listener names must be equal to or a subset of the ones defined in listeners")
  }

  @Test
  def testFromPropsInvalid(): Unit = {
    def baseProperties: Properties = {
      val validRequiredProperties = new Properties()
      validRequiredProperties.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
      validRequiredProperties.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
      validRequiredProperties.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      validRequiredProperties.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      validRequiredProperties
    }
    // to ensure a basis is valid - bootstraps all needed validation
    KafkaConfig.fromProps(baseProperties)

    KafkaConfig.configNames.foreach { name =>
      name match {
        case AbstractConfig.CONFIG_PROVIDERS_CONFIG => // ignore string
        case ServerConfigs.BROKER_ID_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ServerConfigs.NUM_IO_THREADS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerConfigs.BACKGROUND_THREADS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerConfigs.NUM_REPLICA_ALTER_LOG_DIRS_THREADS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")

        // KRaft mode configs
        case KRaftConfigs.PROCESS_ROLES_CONFIG => // ignore
        case KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KRaftConfigs.NODE_ID_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case MetadataLogConfig.METADATA_LOG_DIR_CONFIG => // ignore string
        case MetadataLogConfig.METADATA_LOG_SEGMENT_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case MetadataLogConfig.METADATA_LOG_SEGMENT_MILLIS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case MetadataLogConfig.METADATA_MAX_RETENTION_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case MetadataLogConfig.METADATA_MAX_RETENTION_MILLIS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case MetadataLogConfig.INTERNAL_METADATA_LOG_SEGMENT_BYTES_CONFIG => // no op
        case MetadataLogConfig.INTERNAL_METADATA_MAX_BATCH_SIZE_IN_BYTES_CONFIG => // no op
        case MetadataLogConfig.INTERNAL_METADATA_MAX_FETCH_SIZE_IN_BYTES_CONFIG => // no op
        case MetadataLogConfig.INTERNAL_METADATA_DELETE_DELAY_MILLIS_CONFIG => // no op
        case KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG => // ignore string
        case MetadataLogConfig.METADATA_MAX_IDLE_INTERVAL_MS_CONFIG  => assertPropertyInvalid(baseProperties, name, "not_a_number")

        case ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG => //ignore string
        case ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG => //ignore string

        case SocketServerConfigs.SOCKET_SEND_BUFFER_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case SocketServerConfigs.SOCKET_RECEIVE_BUFFER_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case SocketServerConfigs.SOCKET_LISTEN_BACKLOG_SIZE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG =>
          assertPropertyInvalid(baseProperties, name, "127.0.0.1:not_a_number")
        case SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1")
        case SocketServerConfigs.QUEUED_MAX_REQUESTS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case SocketServerConfigs.QUEUED_MAX_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")

        case ServerLogConfigs.NUM_PARTITIONS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerLogConfigs.LOG_DIRS_CONFIG => assertPropertyInvalid(baseProperties, name, "")
        case ServerLogConfigs.LOG_DIR_CONFIG => assertPropertyInvalid(baseProperties, name, "")
        case ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", Records.LOG_OVERHEAD - 1)

        case ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")

        case ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")

        case ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG => assertPropertyInvalid(baseProperties, name, "unknown_policy", "0")
        case CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", "1024")
        case CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case CleanerConfig.LOG_CLEANER_ENABLE_PROP => assertPropertyInvalid(baseProperties, name, "not_a_boolean")
        case CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "3")
        case ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_boolean", "0")
        case ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "-2")
        case ReplicationConfigs.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.REPLICA_FETCH_WAIT_MAX_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.REPLICA_FETCH_MIN_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.REPLICA_FETCH_RESPONSE_MAX_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG => // Ignore string
        case ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-1")
        case ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_boolean", "0")
        case ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_boolean", "0")
        case ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_boolean", "0")
        case GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-1")
        case GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1")
        case GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case TransactionStateManagerConfig.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case TransactionStateManagerConfig.TRANSACTIONS_MAX_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case TransactionLogConfig.TRANSACTIONS_LOAD_BUFFER_SIZE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case TransactionLogConfig.TRANSACTIONS_TOPIC_SEGMENT_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_boolean", "0")

        case MetricConfigs.METRIC_NUM_SAMPLES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1", "0")
        case MetricConfigs.METRIC_SAMPLE_WINDOW_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1", "0")
        case MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG => // ignore string
        case MetricConfigs.METRIC_RECORDING_LEVEL_CONFIG => // ignore string
        case ServerConfigs.BROKER_RACK_CONFIG => // ignore string

        case ServerConfigs.COMPRESSION_GZIP_LEVEL_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerConfigs.COMPRESSION_LZ4_LEVEL_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case ServerConfigs.COMPRESSION_ZSTD_LEVEL_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", CompressionType.ZSTD.maxLevel() + 1)

        //SSL Configs
        case BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG =>
        case BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG =>
        case SslConfigs.SSL_PROTOCOL_CONFIG => // ignore string
        case SslConfigs.SSL_PROVIDER_CONFIG => // ignore string
        case SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG =>
        case SslConfigs.SSL_KEYSTORE_TYPE_CONFIG => // ignore string
        case SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG => // ignore string
        case SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG => // ignore string
        case SslConfigs.SSL_KEY_PASSWORD_CONFIG => // ignore string
        case SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG => // ignore string
        case SslConfigs.SSL_KEYSTORE_KEY_CONFIG => // ignore string
        case SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG => // ignore string
        case SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG => // ignore string
        case SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG => // ignore string
        case SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG => // ignore string
        case SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG =>
        case SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG =>
        case BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG => // ignore string
        case SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG => // ignore string
        case SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG => // ignore string
        case SslConfigs.SSL_CIPHER_SUITES_CONFIG => // ignore string
        case BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG => // ignore string

        //Sasl Configs
        case KRaftConfigs.SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG => // ignore
        case BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG => // ignore
        case BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG =>
        case SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS =>
        case BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG =>
        case SaslConfigs.SASL_LOGIN_CLASS =>
        case SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS =>
        case SaslConfigs.SASL_KERBEROS_SERVICE_NAME => // ignore string
        case SaslConfigs.SASL_KERBEROS_KINIT_CMD =>
        case SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR =>
        case SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER =>
        case SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN =>
        case BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG => // ignore string
        case SaslConfigs.SASL_JAAS_CONFIG =>
        case SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR =>
        case SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER =>
        case SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS =>
        case SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS =>
        case SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS =>
        case SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS =>
        case SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS =>
        case SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS =>
        case SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM =>
        case SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD =>
        case SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS =>
        case SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS =>
        case SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE =>
        case SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS =>
        case SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB =>
        case SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE =>
        case SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE =>
        case SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE =>
        case SaslConfigs.SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE =>
        case SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID =>
        case SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET =>
        case SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS =>
        case SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE =>
        case SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER =>
        case SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE =>
        case SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS =>
        case SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS =>
        case SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS =>
        case SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL =>
        case SaslConfigs.SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS =>
        case SaslConfigs.SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS =>
        case SaslConfigs.SASL_OAUTHBEARER_SCOPE =>
        case SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME =>
        case SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME =>
        case SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL =>

        // Security config
        case SecurityConfig.SECURITY_PROVIDERS_CONFIG =>

        //delegation token configs
        case DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG => // ignore
        case DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFETIME_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")

        //Kafka Yammer metrics reporter configs
        case MetricConfigs.KAFKA_METRICS_REPORTER_CLASSES_CONFIG => // ignore
        case MetricConfigs.KAFKA_METRICS_POLLING_INTERVAL_SECONDS_CONFIG => //ignore

        case BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")

        // Raft Quorum Configs
        case QuorumConfig.QUORUM_VOTERS_CONFIG => // ignore string
        case QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG => // ignore string
        case QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case QuorumConfig.QUORUM_LINGER_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")

        // Remote Log Manager Configs
        case RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP => assertPropertyInvalid(baseProperties, name, "not_a_boolean")
        case RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1, -2)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1, -2)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", -1, 0.51)
        case RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", -3)
        case RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", -3)

        /** New group coordinator configs */
        case GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)

        /** Consumer groups configs */
        case GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG => // ignore string

        /** Share groups configs */
        case GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.SHARE_GROUP_MAX_SIZE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case ShareGroupConfig.SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case ShareGroupConfig.SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case ShareGroupConfig.SHARE_GROUP_PERSISTER_CLASS_NAME_CONFIG =>  //ignore string

        /** Streams groups configs */
        case GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", -1)
        case GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", -1)

        case _ => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1")
      }
    }
  }

  @Test
  def testDynamicLogConfigs(): Unit = {
    def baseProperties: Properties = {
      val validRequiredProperties = new Properties()
      validRequiredProperties.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
      validRequiredProperties.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
      validRequiredProperties.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "localhost:9093")
      validRequiredProperties.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      validRequiredProperties
    }

    val props = baseProperties
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    val config = KafkaConfig.fromProps(props)

    def assertDynamic(property: String, value: Any, accessor: () => Any): Unit = {
      val initial = accessor()
      props.setProperty(property, value.toString)
      config.updateCurrentConfig(new KafkaConfig(props))
      assertNotEquals(initial, accessor())
    }

    // Test dynamic log config values can be correctly passed through via KafkaConfig to LogConfig
    // Every log config prop must be explicitly accounted for here.
    // A value other than the default value for this config should be set to ensure that we can check whether
    // the value is dynamically updatable.
    ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.forEach { case (logConfig, kafkaConfigProp) =>
      logConfig match {
        case TopicConfig.CLEANUP_POLICY_CONFIG =>
          assertDynamic(kafkaConfigProp, TopicConfig.CLEANUP_POLICY_COMPACT, () => config.logCleanupPolicy)
        case TopicConfig.COMPRESSION_TYPE_CONFIG =>
          assertDynamic(kafkaConfigProp, "lz4", () => config.compressionType)
        case TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG =>
          assertDynamic(kafkaConfigProp, "5", () => config.gzipCompressionLevel)
        case TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG =>
          assertDynamic(kafkaConfigProp, "5", () => config.lz4CompressionLevel)
        case TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG =>
          assertDynamic(kafkaConfigProp, "5", () => config.zstdCompressionLevel)
        case TopicConfig.SEGMENT_BYTES_CONFIG =>
          assertDynamic(kafkaConfigProp, 1048576, () => config.logSegmentBytes)
        case TopicConfig.SEGMENT_MS_CONFIG =>
          assertDynamic(kafkaConfigProp, 10001L, () => config.logRollTimeMillis)
        case TopicConfig.DELETE_RETENTION_MS_CONFIG =>
          assertDynamic(kafkaConfigProp, 10002L, () => config.logCleanerDeleteRetentionMs)
        case TopicConfig.FILE_DELETE_DELAY_MS_CONFIG =>
          assertDynamic(kafkaConfigProp, 10003L, () => config.logDeleteDelayMs)
        case TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG =>
          assertDynamic(kafkaConfigProp, 10004L, () => config.logFlushIntervalMessages)
        case TopicConfig.FLUSH_MS_CONFIG =>
          assertDynamic(kafkaConfigProp, 10005L, () => config.logFlushIntervalMs)
        case TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG =>
          assertDynamic(kafkaConfigProp, 10006L, () => config.logCleanerMaxCompactionLagMs)
        case TopicConfig.INDEX_INTERVAL_BYTES_CONFIG =>
          assertDynamic(kafkaConfigProp, 10007, () => config.logIndexIntervalBytes)
        case TopicConfig.MAX_MESSAGE_BYTES_CONFIG =>
          assertDynamic(kafkaConfigProp, 10008, () => config.messageMaxBytes)
        case TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG =>
          assertDynamic(kafkaConfigProp, 10015L, () => config.logMessageTimestampBeforeMaxMs)
        case TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG =>
          assertDynamic(kafkaConfigProp, 10016L, () => config.logMessageTimestampAfterMaxMs)
        case TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG =>
          assertDynamic(kafkaConfigProp, "LogAppendTime", () => config.logMessageTimestampType.name)
        case TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG =>
          assertDynamic(kafkaConfigProp, 0.01, () => config.logCleanerMinCleanRatio)
        case TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG =>
          assertDynamic(kafkaConfigProp, 10010L, () => config.logCleanerMinCompactionLagMs)
        case TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG =>
          assertDynamic(kafkaConfigProp, 4, () => config.minInSyncReplicas)
        case TopicConfig.PREALLOCATE_CONFIG =>
          assertDynamic(kafkaConfigProp, true, () => config.logPreAllocateEnable)
        case TopicConfig.RETENTION_BYTES_CONFIG =>
          assertDynamic(kafkaConfigProp, 10011L, () => config.logRetentionBytes)
        case TopicConfig.RETENTION_MS_CONFIG =>
          assertDynamic(kafkaConfigProp, 10012L, () => config.logRetentionTimeMillis)
        case TopicConfig.SEGMENT_INDEX_BYTES_CONFIG =>
          assertDynamic(kafkaConfigProp, 10013, () => config.logIndexSizeMaxBytes)
        case TopicConfig.SEGMENT_JITTER_MS_CONFIG =>
          assertDynamic(kafkaConfigProp, 10014L, () => config.logRollTimeJitterMillis)
        case TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG =>
          assertDynamic(kafkaConfigProp, true, () => config.uncleanLeaderElectionEnable)
        case TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG =>
          assertDynamic(kafkaConfigProp, 10015L, () => config.remoteLogManagerConfig.logLocalRetentionMs)
        case TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG =>
          assertDynamic(kafkaConfigProp, 10016L, () => config.remoteLogManagerConfig.logLocalRetentionBytes)
        // not dynamically updatable
        case QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG =>
        // topic only config
        case QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG =>
        // topic only config
        case "internal.segment.bytes" =>
        // topic internal config
        case prop =>
          fail(prop + " must be explicitly checked for dynamic updatability. Note that LogConfig(s) require that KafkaConfig value lookups are dynamic and not static values.")
      }
    }
  }

  @Test
  def testSpecificProperties(): Unit = {
    val defaults = new Properties()
    defaults.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    defaults.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "CONTROLLER://localhost:9092")
    defaults.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    defaults.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
    defaults.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:1122")
    defaults.setProperty(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG, "127.0.0.1:2, 127.0.0.2:3")
    defaults.setProperty(ServerLogConfigs.LOG_DIR_CONFIG, "/tmp1,/tmp2")
    defaults.setProperty(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, "12")
    defaults.setProperty(ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG, "11")
    defaults.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, "10")
    //For LOG_FLUSH_INTERVAL_MS_CONFIG
    defaults.setProperty(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG, "123")
    defaults.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG, CompressionType.SNAPPY.id.toString)
    // For MetricRecordingLevelProp
    defaults.setProperty(MetricConfigs.METRIC_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString)

    val config = KafkaConfig.fromProps(defaults)
    assertEquals(1, config.brokerId)
    assertEndpointsEqual(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "127.0.0.1", 1122),
      config.effectiveAdvertisedBrokerListeners.head)
    assertEquals(Map("127.0.0.1" -> 2, "127.0.0.2" -> 3), config.maxConnectionsPerIpOverrides)
    assertEquals(util.List.of("/tmp1", "/tmp2"), config.logDirs)
    assertEquals(12 * 60L * 1000L * 60, config.logRollTimeMillis)
    assertEquals(11 * 60L * 1000L * 60, config.logRollTimeJitterMillis)
    assertEquals(10 * 60L * 1000L * 60, config.logRetentionTimeMillis)
    assertEquals(123L, config.logFlushIntervalMs)
    assertEquals(CompressionType.SNAPPY, config.groupCoordinatorConfig.offsetTopicCompressionType)
    assertEquals(Sensor.RecordingLevel.DEBUG.toString, config.metricRecordingLevel)
  }

  @Test
  def testNonroutableAdvertisedListeners(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "CONTROLLER://localhost:9092")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://0.0.0.0:9092")
    assertBadConfigContainingMessage(props, "advertised.listeners cannot use the nonroutable meta-address 0.0.0.0. Use a routable IP address.")
  }

  @Test
  def testMaxConnectionsPerIpProp(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG, "0")
    assertFalse(isValidKafkaConfig(props))
    props.setProperty(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG, "127.0.0.1:100")
    KafkaConfig.fromProps(props)
    props.setProperty(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG, "127.0.0.0#:100")
    assertFalse(isValidKafkaConfig(props))
  }

  private def assertPropertyInvalid(validRequiredProps: => Properties, name: String, values: Any*): Unit = {
    values.foreach { value =>
      val props = validRequiredProps
      props.setProperty(name, value.toString)

      val buildConfig: Executable = () => KafkaConfig.fromProps(props)
      assertThrows(classOf[Exception], buildConfig,
      s"Expected exception for property `$name` with invalid value `$value` was not thrown")
    }
  }

  @Test
  def testDistinctControllerAndAdvertisedListenersAllowedForKRaftBroker(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094")
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "PLAINTEXT://A:9092,SSL://B:9093") // explicitly setting it in KRaft
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SASL_SSL")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "3@localhost:9094")

    // invalid due to extra listener also appearing in controller listeners
    assertBadConfigContainingMessage(props,
      "controller.listener.names must not contain a value appearing in the 'listeners' configuration when running KRaft with just the broker role")

    // Valid now
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://A:9092,SSL://B:9093")
    KafkaConfig.fromProps(props)

    // Also valid if we let advertised listeners be derived from listeners/controller.listener.names
    // since listeners and advertised.listeners are explicitly identical at this point
    props.remove(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testImplicitAllBindingListenersCanBeAdvertisedForBroker(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    val listeners = "PLAINTEXT://:9092"
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, listeners)
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, listeners) // explicitly setting it in broker
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9093")

    // Valid
    KafkaConfig.fromProps(props)

    // Also valid if we allow advertised listeners to derive from listeners
    props.remove(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testExplicitAllBindingListenersCannotBeUsedForBroker(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    val listeners = "PLAINTEXT://0.0.0.0:9092"
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, listeners)
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, listeners) // explicitly setting it in KRaft
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9093")

    val expectedExceptionContainsText = "advertised.listeners cannot use the nonroutable meta-address 0.0.0.0. Use a routable IP address."
    assertBadConfigContainingMessage(props, expectedExceptionContainsText)

    // invalid if we allow advertised listeners to derive from listeners
    props.remove(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)
    assertBadConfigContainingMessage(props, expectedExceptionContainsText)
  }

  @Test
  def testImplicitAllBindingControllerListenersCanBeAdvertisedForKRaftController(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    val listeners = "CONTROLLER://:9093"
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, listeners)
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, listeners) // explicitly setting it in KRaft
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")

    // Valid
    KafkaConfig.fromProps(props)

    // Also valid if we allow advertised listeners to derive from listeners/controller.listener.names
    props.remove(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testExplicitAllBindingControllerListenersCanBeAdvertisedForKRaftController(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    val listeners = "CONTROLLER://0.0.0.0:9093"
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, listeners)
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, listeners) // explicitly setting it in KRaft
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")

    val expectedExceptionContainsText = "advertised.listeners cannot use the nonroutable meta-address 0.0.0.0. Use a routable IP address."
    assertBadConfigContainingMessage(props, expectedExceptionContainsText)

    // Valid if we allow advertised listeners to derive from listeners/controller.listener.names
    props.remove(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testControllerListenersCanBeAdvertisedForKRaftCombined(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker,controller")
    val listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094"
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, listeners)
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, listeners) // explicitly setting it in KRaft
    props.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "SASL_SSL")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "PLAINTEXT,SSL")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9092")

    // Valid
    KafkaConfig.fromProps(props)

    // Also valid if we allow advertised listeners to derive from listeners/controller.listener.names
    props.remove(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testAdvertisedListenersAllowedForKRaftControllerOnlyRole(): Unit = {
    // Test that listeners must enumerate every controller listener
    // Test that controller listener must enumerate every listener
    val correctListeners = "PLAINTEXT://A:9092,SSL://B:9093"
    val incorrectListeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094"

    val correctControllerListenerNames = "PLAINTEXT,SSL"

    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, correctListeners)
    props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, correctListeners)
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, correctControllerListenerNames)
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9092")

    // Valid
    KafkaConfig.fromProps(props)

    // Invalid if listeners contains names not in controller.listener.names
    props.remove(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, incorrectListeners)
    var expectedExceptionContainsText = """The listeners config must only contain KRaft controller listeners from
    |controller.listener.names when process.roles=controller""".stripMargin.replaceAll("\n", " ")
    assertBadConfigContainingMessage(props, expectedExceptionContainsText)

    // Invalid if listeners doesn't contain every name in controller.listener.names
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, correctListeners)
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, correctControllerListenerNames + ",SASL_SSL")
    expectedExceptionContainsText = """controller.listener.names must only contain values appearing in the 'listeners'
    |configuration when running the KRaft controller role""".stripMargin.replaceAll("\n", " ")
    assertBadConfigContainingMessage(props, expectedExceptionContainsText)

    // Valid now
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, correctControllerListenerNames)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testControllerListenerNamesValidForKRaftControllerOnly(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9092")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "SASL_SSL://:9092,CONTROLLER://:9093")
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "SASL_SSL:SASL_SSL,CONTROLLER:SASL_SSL")
    props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "SASL_SSL")
    props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER,SASL_SSL")

    val expectedExceptionContainsText =
      """controller.listener.names must not contain an explicitly set inter.broker.listener.name configuration value
        |when process.roles=controller""".stripMargin.replaceAll("\n", " ")
    assertBadConfigContainingMessage(props, expectedExceptionContainsText)
  }

  @Test
  def testControllerQuorumVoterStringsToNodes(): Unit = {
    assertThrows(classOf[ConfigException], () => QuorumConfig.quorumVoterStringsToNodes(Collections.singletonList("")))
    assertEquals(Seq(new Node(3000, "example.com", 9093)),
      QuorumConfig.quorumVoterStringsToNodes(util.Arrays.asList("3000@example.com:9093")).asScala.toSeq)
    assertEquals(Seq(new Node(3000, "example.com", 9093),
      new Node(3001, "example.com", 9094)),
      QuorumConfig.quorumVoterStringsToNodes(util.Arrays.asList("3000@example.com:9093","3001@example.com:9094")).asScala.toSeq)
  }

  @Test
  def testInvalidQuorumVoterConfig(): Unit = {
    assertInvalidQuorumVoters("")
    assertInvalidQuorumVoters("1")
    assertInvalidQuorumVoters("1@")
    assertInvalidQuorumVoters("1:")
    assertInvalidQuorumVoters("blah@")
    assertInvalidQuorumVoters("1@kafka1")
    assertInvalidQuorumVoters("1@kafka1:9092,")
    assertInvalidQuorumVoters("1@kafka1:9092,")
    assertInvalidQuorumVoters("1@kafka1:9092,2")
    assertInvalidQuorumVoters("1@kafka1:9092,2@")
    assertInvalidQuorumVoters("1@kafka1:9092,2@blah")
    assertInvalidQuorumVoters("1@kafka1:9092,2@blah,")
    assertInvalidQuorumVoters("1@kafka1:9092:1@kafka2:9092")
  }

  private def assertInvalidQuorumVoters(value: String): Unit = {
    val props = TestUtils.createBrokerConfig(0)
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, value)
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testValidQuorumVotersParsingWithIpAddress(): Unit = {
    val expected = new util.HashMap[Integer, InetSocketAddress]()
    expected.put(1, new InetSocketAddress("127.0.0.1", 9092))
    assertValidQuorumVoters(expected, "1@127.0.0.1:9092")
  }

  @Test
  def testValidQuorumVotersParsingWithMultipleHost(): Unit = {
    val expected = new util.HashMap[Integer, InetSocketAddress]()
    expected.put(1, new InetSocketAddress("kafka1", 9092))
    expected.put(2, new InetSocketAddress("kafka2", 9092))
    expected.put(3, new InetSocketAddress("kafka3", 9092))
    assertValidQuorumVoters(expected, "1@kafka1:9092,2@kafka2:9092,3@kafka3:9092")
  }

  private def assertValidQuorumVoters(expectedVoters: util.Map[Integer, InetSocketAddress], value: String): Unit = {
    val props = TestUtils.createBrokerConfig(0)
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, value)
    val addresses = QuorumConfig.parseVoterConnections(KafkaConfig.fromProps(props).quorumConfig.voters)
    assertEquals(expectedVoters, addresses)
  }

  @Test
  def testParseQuorumBootstrapServers(): Unit = {
    val expected = Arrays.asList(
      InetSocketAddress.createUnresolved("kafka1", 9092),
      InetSocketAddress.createUnresolved("kafka2", 9092)
    )

    val props = TestUtils.createBrokerConfig(0)
    props.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092")

    val addresses = QuorumConfig.parseBootstrapServers(
      KafkaConfig.fromProps(props).quorumConfig.bootstrapServers
    )

    assertEquals(expected, addresses)
  }

  @Test
  def testInvalidQuorumAutoJoinForKRaftBroker(): Unit = {
    val props = TestUtils.createBrokerConfig(0)
    props.setProperty(QuorumConfig.QUORUM_AUTO_JOIN_ENABLE_CONFIG, String.valueOf(true))
    assertEquals(
      "requirement failed: controller.quorum.auto.join.enable is only " +
        "supported when process.roles contains the 'controller' role.",
      assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props)).getMessage
    )

  }

  @Test
  def testAcceptsLargeId(): Unit = {
    val largeBrokerId = 2000
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, largeBrokerId.toString)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testRejectsNegativeNodeId(): Unit = {
    val props = createDefaultConfig()
    props.remove(ServerConfigs.BROKER_ID_CONFIG)
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "-1")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testRejectsNegativeBrokerId(): Unit = {
    val props = createDefaultConfig()
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "-1")
    props.remove(KRaftConfigs.NODE_ID_CONFIG)
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testCustomMetadataLogDir(): Unit = {
    val metadataDir = "/path/to/metadata/dir"
    val dataDir = "/path/to/data/dir"

    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    props.setProperty(MetadataLogConfig.METADATA_LOG_DIR_CONFIG, metadataDir)
    props.setProperty(ServerLogConfigs.LOG_DIR_CONFIG, dataDir)
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")
    KafkaConfig.fromProps(props)

    val config = KafkaConfig.fromProps(props)
    assertEquals(metadataDir, config.metadataLogDir)
    assertEquals(util.List.of(dataDir), config.logDirs)
  }

  @Test
  def testDefaultMetadataLogDir(): Unit = {
    val dataDir1 = "/path/to/data/dir/1"
    val dataDir2 = "/path/to/data/dir/2"

    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    props.setProperty(ServerLogConfigs.LOG_DIR_CONFIG, s"$dataDir1,$dataDir2")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")
    KafkaConfig.fromProps(props)

    val config = KafkaConfig.fromProps(props)
    assertEquals(dataDir1, config.metadataLogDir)
    assertEquals(util.List.of(dataDir1, dataDir2), config.logDirs)
  }

  @Test
  def testPopulateSynonymsOnEmptyMap(): Unit = {
    assertEquals(Collections.emptyMap(), KafkaConfig.populateSynonyms(Collections.emptyMap()))
  }

  @Test
  def testPopulateSynonymsOnMapWithoutNodeId(): Unit = {
    val input =  new util.HashMap[String, String]()
    input.put(ServerConfigs.BROKER_ID_CONFIG, "4")
    val expectedOutput = new util.HashMap[String, String]()
    expectedOutput.put(ServerConfigs.BROKER_ID_CONFIG, "4")
    expectedOutput.put(KRaftConfigs.NODE_ID_CONFIG, "4")
    assertEquals(expectedOutput, KafkaConfig.populateSynonyms(input))
  }

  @Test
  def testPopulateSynonymsOnMapWithoutBrokerId(): Unit = {
    val input =  new util.HashMap[String, String]()
    input.put(KRaftConfigs.NODE_ID_CONFIG, "4")
    val expectedOutput = new util.HashMap[String, String]()
    expectedOutput.put(ServerConfigs.BROKER_ID_CONFIG, "4")
    expectedOutput.put(KRaftConfigs.NODE_ID_CONFIG, "4")
    assertEquals(expectedOutput, KafkaConfig.populateSynonyms(input))
  }

  @Test
  def testNodeIdMustNotBeDifferentThanBrokerId(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    assertEquals("You must set `node.id` to the same value as `broker.id`.",
      assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props)).getMessage())
  }

  @Test
  def testNodeIdOrBrokerIdMustBeSetWithKraft(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")
    assertEquals("Missing required configuration \"node.id\" which has no default value.",
      assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props)).getMessage())
  }

  @Test
  def testNodeIdIsInferredByBrokerIdWithKraft(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "3")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9093")
    val config = KafkaConfig.fromProps(props)
    assertEquals(3, config.brokerId)
    assertEquals(3, config.nodeId)
    val originals = config.originals()
    assertEquals("3", originals.get(ServerConfigs.BROKER_ID_CONFIG))
    assertEquals("3", originals.get(KRaftConfigs.NODE_ID_CONFIG))
  }

  def kraftProps(): Properties = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "3")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9093")
    props
  }

  @Test
  def testBrokerIdIsInferredByNodeIdWithKraft(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())
    val config = KafkaConfig.fromProps(props)
    assertEquals(3, config.brokerId)
    assertEquals(3, config.nodeId)
    val originals = config.originals()
    assertEquals("3", originals.get(ServerConfigs.BROKER_ID_CONFIG))
    assertEquals("3", originals.get(KRaftConfigs.NODE_ID_CONFIG))
  }

  @Test
  def testSaslJwksEndpointRetryDefaults(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    props.setProperty(ServerConfigs.BROKER_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "CONTROLLER://localhost:9092")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    val config = KafkaConfig.fromProps(props)
    assertNotNull(config.getLong(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS))
    assertNotNull(config.getLong(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS))
  }

  @Test
  def testInvalidAuthorizerClassName(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    val configs = new util.HashMap[Object, Object](props)
    configs.put(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, null)
    val ce = assertThrows(classOf[ConfigException], () => KafkaConfig.apply(configs))
    assertTrue(ce.getMessage.contains(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG))
  }

  @Test
  def testInvalidSecurityInterBrokerProtocol(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG, "abc")
    val ce = assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
    assertTrue(ce.getMessage.contains(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG))
  }

  @Test
  def testEarlyStartListenersDefault(): Unit = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://:8092")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9093")
    val config = new KafkaConfig(props)
    assertEquals(Set("CONTROLLER"), config.earlyStartListeners.map(_.value()))
  }

  @Test
  def testEarlyStartListeners(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())
    props.setProperty(ServerConfigs.EARLY_START_LISTENERS_CONFIG, "INTERNAL,INTERNAL2")
    props.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "INTERNAL")
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
      "INTERNAL:PLAINTEXT,INTERNAL2:PLAINTEXT,CONTROLLER:PLAINTEXT")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG,
      "INTERNAL://127.0.0.1:9092,INTERNAL2://127.0.0.1:9093")
    val config = new KafkaConfig(props)
    assertEquals(Set(new ListenerName("INTERNAL"), new ListenerName("INTERNAL2")),
      config.earlyStartListeners)
  }

  @Test
  def testEarlyStartListenersMustBeListeners(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())
    props.setProperty(ServerConfigs.EARLY_START_LISTENERS_CONFIG, "INTERNAL")
    assertEquals("early.start.listeners contains listener INTERNAL, but this is not " +
      "contained in listeners or controller.listener.names",
        assertThrows(classOf[ConfigException], () => new KafkaConfig(props)).getMessage)
  }

  @Test
  def testMetadataMaxSnapshotInterval(): Unit = {
    val validValue = 100
    val props = new Properties()
    props.putAll(kraftProps())
    props.setProperty(MetadataLogConfig.METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG, validValue.toString)

    val config = KafkaConfig.fromProps(props)
    assertEquals(validValue, config.metadataSnapshotMaxIntervalMs)

    props.setProperty(MetadataLogConfig.METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG, "-1")
    val errorMessage = assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props)).getMessage

    assertEquals(
      "Invalid value -1 for configuration metadata.log.max.snapshot.interval.ms: Value must be at least 0",
      errorMessage
    )
  }

  @Test
  def testConsumerGroupSessionTimeoutValidation(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())

    // Max should be greater than or equals to min.
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "20")
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, "10")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))

    // The timeout should be within the min-max range.
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "10")
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, "20")
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, "5")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, "25")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testConsumerGroupHeartbeatIntervalValidation(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())

    // Max should be greater than or equals to min.
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, "20")
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, "10")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))

    // The timeout should be within the min-max range.
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, "10")
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, "20")
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, "5")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, "25")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testGroupCoordinatorRebalanceProtocols(): Unit = {
    val props = new Properties()

    // Setting KRaft's properties.
    props.putAll(kraftProps())

    // Empty list is illegal.
    props.put(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "")
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))

    // Only classic, consumer and share are supported.
    props.put(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "foo")
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))

    // classic cannot be disabled.
    props.put(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "consumer")
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))

    // This is OK.
    props.put(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer")
    val config = KafkaConfig.fromProps(props)
    assertEquals(Set(GroupType.CLASSIC, GroupType.CONSUMER), config.groupCoordinatorRebalanceProtocols)

    props.put(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,streams")
    val config2 = KafkaConfig.fromProps(props)
    assertEquals(Set(GroupType.CLASSIC, GroupType.STREAMS), config2.groupCoordinatorRebalanceProtocols)

    // Including "share" is also OK
    props.put(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,share")
    val config3 = KafkaConfig.fromProps(props)
    assertEquals(Set(GroupType.CLASSIC, GroupType.CONSUMER, GroupType.SHARE), config3.groupCoordinatorRebalanceProtocols)
  }

  @Test
  def testConsumerGroupMigrationPolicy(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())

    // Invalid GroupProtocolMigrationPolicy name.
    props.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, "foo")
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))

    ConsumerGroupMigrationPolicy.values.foreach { policy =>
      props.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, policy.toString)
      val config = KafkaConfig.fromProps(props)
      assertEquals(policy, config.groupCoordinatorConfig.consumerGroupMigrationPolicy)
    }

    // The config is case-insensitive.
    ConsumerGroupMigrationPolicy.values.foreach { policy =>
      props.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, policy.toString.toUpperCase())
      val config = KafkaConfig.fromProps(props)
      assertEquals(policy, config.groupCoordinatorConfig.consumerGroupMigrationPolicy)
    }
  }

  @Test
  def testSingleLogDirectoryWithRemoteLogStorage(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, String.valueOf(true))
    props.put(ServerLogConfigs.LOG_DIRS_CONFIG, "/tmp/a")
    assertDoesNotThrow(() => KafkaConfig.fromProps(props))

    props.put(ServerLogConfigs.LOG_DIRS_CONFIG, "/tmp/a,/tmp/b")
    assertDoesNotThrow(() => KafkaConfig.fromProps(props))
  }

  @Test
  def testShareGroupSessionTimeoutValidation(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())

    // Max should be greater than or equals to min.
    props.put(GroupCoordinatorConfig.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "20")
    props.put(GroupCoordinatorConfig.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, "10")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))

    // The timeout should be within the min-max range.
    props.put(GroupCoordinatorConfig.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "10")
    props.put(GroupCoordinatorConfig.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, "20")
    props.put(GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG, "5")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
    props.put(GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG, "25")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testShareGroupHeartbeatIntervalValidation(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())

    // Max should be greater than or equals to min.
    props.put(GroupCoordinatorConfig.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, "20")
    props.put(GroupCoordinatorConfig.SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, "10")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))

    // The timeout should be within the min-max range.
    props.put(GroupCoordinatorConfig.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, "10")
    props.put(GroupCoordinatorConfig.SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, "20")
    props.put(GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, "5")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
    props.put(GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, "25")
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testShareGroupRecordLockDurationValidation(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())

    props.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, "10")
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
    props.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, "10000000")
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))

    // The duration should be within the min-max range.
    props.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, "1000")
    props.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, "3600000")
    props.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, "999")
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
    props.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, "3600001")
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
    props.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, "30000")
    assertDoesNotThrow(() => KafkaConfig.fromProps(props))
  }

  @Test
  def testLowercaseControllerListenerNames(): Unit = {
    val props = createDefaultConfig()
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "controller")
    val message = assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props)).getMessage
    assertEquals("requirement failed: controller.listener.names must contain at least one value appearing in the 'listeners' configuration when running the KRaft controller role", message)
  }

  @Test
  def testLogBrokerHeartbeatIntervalMsShouldBeLowerThanHalfOfBrokerSessionTimeoutMs(): Unit = {
    val props = createDefaultConfig()
    Using.resource(LogCaptureAppender.createAndRegister) { appender =>
      appender.setClassLogger(KafkaConfig.getClass, Level.ERROR)
      props.setProperty(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG, "4500")
      props.setProperty(KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG, "8999")
      KafkaConfig.fromProps(props)
      assertTrue(appender.getMessages.contains("broker.heartbeat.interval.ms (4500 ms) must be less than or equal to half of the broker.session.timeout.ms (8999 ms). " +
        "The broker.session.timeout.ms is configured on controller. The broker.heartbeat.interval.ms is configured on broker. " +
        "If a broker doesn't send heartbeat request within broker.session.timeout.ms, it loses broker lease. " +
        "Please increase broker.session.timeout.ms or decrease broker.heartbeat.interval.ms."))
    }
  }
}
