/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.time
import kafka.security.JaasTestUtils
import kafka.utils.TestUtils._
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl._
import org.apache.kafka.common.acl.AclOperation.{ALL, ALTER, ALTER_CONFIGS, CLUSTER_ACTION, CREATE, DELETE, DESCRIBE, IDEMPOTENT_WRITE}
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.config.{ConfigResource, SaslConfigs, TopicConfig}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, DelegationTokenExpiredException, DelegationTokenNotFoundException, InvalidRequestException, TimeoutException, TopicAuthorizationException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourceType.{GROUP, TOPIC}
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.SecurityUtils
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.apache.kafka.security.authorizer.AclEntry.{WILDCARD_HOST, WILDCARD_PRINCIPAL_STRING}
import org.apache.kafka.server.config.{DelegationTokenManagerConfigs, ServerConfigs, ServerLogConfigs}
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.apache.kafka.server.authorizer.{Authorizer => JAuthorizer}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo, Timeout}

import java.util
import java.util.Optional
import scala.collection.Seq
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

@Timeout(120)
class SaslSslAdminIntegrationTest extends BaseAdminIntegrationTest with SaslSetup {
  val clusterResourcePattern = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
  val kraftAuthorizerClassName = classOf[StandardAuthorizer].getName
  val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KAFKA_SERVER_PRINCIPAL_UNQUALIFIED_NAME)
  var superUserAdmin: Admin = _
  val secretKey = "secretKey"
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(TestUtils.tempFile("truststore", ".jks"))

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    this.serverConfig.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, kraftAuthorizerClassName)
    this.controllerConfig.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, kraftAuthorizerClassName)
    // controllers talk to brokers as User:ANONYMOUS therefore it needs to be super user
    this.serverConfig.setProperty(StandardAuthorizer.SUPER_USERS_CONFIG, kafkaPrincipal.toString + ";" + KafkaPrincipal.ANONYMOUS.toString)
    this.controllerConfig.setProperty(StandardAuthorizer.SUPER_USERS_CONFIG, kafkaPrincipal.toString)

    // Enable delegationTokenControlManager
    this.serverConfig.setProperty(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG, secretKey)
    this.serverConfig.setProperty(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_CONFIG, Long.MaxValue.toString)
    this.serverConfig.setProperty(DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFETIME_CONFIG, Long.MaxValue.toString)

    setUpSasl()
    super.setUp(testInfo)
    setInitialAcls()
  }

  def setUpSasl(): Unit = {
    startSasl(jaasSections(Seq("GSSAPI"), Some("GSSAPI"), JaasTestUtils.KAFKA_SERVER_CONTEXT_NAME))

    val loginContext = jaasAdminLoginModule("GSSAPI")
    superuserClientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, loginContext)
  }

  private def setInitialAcls(): Unit = {
    superUserAdmin = createSuperuserAdminClient()
    val ace = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, ALL, ALLOW)
    superUserAdmin.createAcls(java.util.List.of(new AclBinding(new ResourcePattern(TOPIC, "*", LITERAL), ace)))
    superUserAdmin.createAcls(java.util.List.of(new AclBinding(new ResourcePattern(GROUP, "*", LITERAL), ace)))

    val clusterAcls = List(clusterAcl(ALLOW, CREATE),
      clusterAcl(ALLOW, DELETE),
      clusterAcl(ALLOW, CLUSTER_ACTION),
      clusterAcl(ALLOW, ALTER_CONFIGS),
      clusterAcl(ALLOW, ALTER),
      clusterAcl(ALLOW, IDEMPOTENT_WRITE))

    superUserAdmin.createAcls(clusterAcls.map(ace => new AclBinding(clusterResourcePattern, ace)).asJava)

    brokers.foreach { b =>
      TestUtils.waitAndVerifyAcls(Set(ace), b.dataPlaneRequestProcessor.authorizerPlugin.get, new ResourcePattern(TOPIC, "*", LITERAL))
      TestUtils.waitAndVerifyAcls(Set(ace), b.dataPlaneRequestProcessor.authorizerPlugin.get, new ResourcePattern(GROUP, "*", LITERAL))
      TestUtils.waitAndVerifyAcls(clusterAcls.toSet, b.dataPlaneRequestProcessor.authorizerPlugin.get, clusterResourcePattern)
    }
  }

  private def clusterAcl(permissionType: AclPermissionType, operation: AclOperation): AccessControlEntry = {
    new AccessControlEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*").toString,
      WILDCARD_HOST, operation, permissionType)
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  val anyAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
    new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW))
  val acl2 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW))
  val acl3 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val fooAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "foobar", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val prefixAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic", PatternType.PREFIXED),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val transactionalIdAcl = new AclBinding(new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "transactional_id", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW))
  val groupAcl = new AclBinding(new ResourcePattern(ResourceType.GROUP, "*", PatternType.LITERAL),
    new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW))

  @Test
  @Timeout(30)
  def testAclOperationsWithOptionTimeoutMs(): Unit = {
    val config = createConfig
    // this will cause timeout connecting to broker
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${TestUtils.IncorrectBrokerPort}")
    val brokenClient = Admin.create(config)

    try {
      val acl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
      val exception = assertThrows(classOf[ExecutionException], () => {
      brokenClient.createAcls(util.Set.of(acl), new CreateAclsOptions().timeoutMs(0)).all().get()
      })
      assertInstanceOf(classOf[TimeoutException], exception.getCause)
    } finally brokenClient.close(time.Duration.ZERO)
  }

  @Test
  @Timeout(30)
  def testDeleteAclsWithOptionTimeoutMs(): Unit = {
    val config = createConfig
    // this will cause timeout connecting to broker
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${TestUtils.IncorrectBrokerPort}")
    val brokenClient = Admin.create(config)

    try {
      val exception = assertThrows(classOf[ExecutionException], () => {
        brokenClient.deleteAcls(util.Set.of(AclBindingFilter.ANY), new DeleteAclsOptions().timeoutMs(0)).all().get()
      })
      assertInstanceOf(classOf[TimeoutException], exception.getCause)
    } finally brokenClient.close(time.Duration.ZERO)
  }

  @Test
  def testExpireDelegationTokenWithOptionExpireTimePeriodMs(): Unit = {
    client = createAdminClient
    val renewer = java.util.List.of(SecurityUtils.parseKafkaPrincipal("User:renewer"))

    def generateTokenResult(maxLifeTimeMs: Int, expiryTimePeriodMs: Int, expectedTokenNum: Int): (CreateDelegationTokenResult, ExpireDelegationTokenResult) = {
      val createResult = client.createDelegationToken(new CreateDelegationTokenOptions().renewers(renewer).maxLifetimeMs(maxLifeTimeMs))
      val tokenCreated = createResult.delegationToken.get
      TestUtils.waitUntilTrue(() => brokers.forall(server => server.tokenCache.tokens().size() == expectedTokenNum),
            "Timed out waiting for token to propagate to all servers")
      val expireResult = client.expireDelegationToken(
        tokenCreated.hmac(),
        new ExpireDelegationTokenOptions().expiryTimePeriodMs(expiryTimePeriodMs)
      )
      (createResult, expireResult)
    }

    try {
      // Note that maxTimestamp = token created time + maxLifeTimeMs
      val (createResult1, expireResult1) = generateTokenResult(10000, -1, 1)
      // if expiryTimePeriodMs < 0, token will be expired immediately.
      assertTrue(createResult1.delegationToken().get().tokenInfo().maxTimestamp() > expireResult1.expiryTimestamp().get())

      // expireDelegationToken will decrease the value of expiryTimestamp, since this token is not expired,
      // expiryTimestamp will be set to min(now + expiryTimePeriodMs, maxTimestamp),
      // in this case, maxTimestamp is smaller, so expiryTimestamp will not be modified
      val (createResult2, expireResult2) = generateTokenResult(50000, 100000, 1)
      assert(createResult2.delegationToken().get().tokenInfo().expiryTimestamp() == expireResult2.expiryTimestamp().get())

      // since previous token is not expired yet, we need to set expectedTokenNum to 2
      val (createResult3, expireResult3) = generateTokenResult(5000, 2000, 2)
      assert(createResult3.delegationToken().get().tokenInfo().expiryTimestamp() > expireResult3.expiryTimestamp().get())
    } finally client.close(time.Duration.ZERO)
  }

  @Test
  def testAclOperations(): Unit = {
    client = createAdminClient
    val acl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
    assertEquals(8, getAcls(AclBindingFilter.ANY).size)
    val results = client.createAcls(java.util.List.of(acl2, acl3))
    assertEquals(Set(acl2, acl3), results.values.keySet().asScala)
    results.values.values.forEach(value => value.get)
    val aclUnknown = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.UNKNOWN, AclPermissionType.ALLOW))
    val results2 = client.createAcls(java.util.List.of(aclUnknown))
    assertEquals(Set(aclUnknown), results2.values.keySet().asScala)
    assertFutureThrows(classOf[InvalidRequestException], results2.all)
    val results3 = client.deleteAcls(java.util.List.of(acl.toFilter, acl2.toFilter, acl3.toFilter)).values
    assertEquals(Set(acl.toFilter, acl2.toFilter, acl3.toFilter), results3.keySet.asScala)
    assertEquals(0, results3.get(acl.toFilter).get.values.size())
    assertEquals(Set(acl2), results3.get(acl2.toFilter).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(acl3), results3.get(acl3.toFilter).get.values.asScala.map(_.binding).toSet)
  }

  @Test
  def testAclOperations2(): Unit = {
    client = createAdminClient
    val results = client.createAcls(java.util.List.of(acl2, acl2, transactionalIdAcl))
    assertEquals(Set(acl2, acl2, transactionalIdAcl), results.values.keySet.asScala)
    results.all.get()
    waitForDescribeAcls(client, acl2.toFilter, Set(acl2))
    waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set(transactionalIdAcl))

    val filterA = new AclBindingFilter(new ResourcePatternFilter(ResourceType.GROUP, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val filterB = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val filterC = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TRANSACTIONAL_ID, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)

    waitForDescribeAcls(client, filterA, Set(groupAcl))
    waitForDescribeAcls(client, filterC, Set(transactionalIdAcl))

    val results2 = client.deleteAcls(java.util.List.of(filterA, filterB, filterC), new DeleteAclsOptions())
    assertEquals(Set(filterA, filterB, filterC), results2.values.keySet.asScala)
    assertEquals(Set(groupAcl), results2.values.get(filterA).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(transactionalIdAcl), results2.values.get(filterC).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(acl2), results2.values.get(filterB).get.values.asScala.map(_.binding).toSet)

    waitForDescribeAcls(client, filterB, Set())
    waitForDescribeAcls(client, filterC, Set())
  }

  @Test
  def testAclDescribe(): Unit = {
    client = createAdminClient
    ensureAcls(Set(anyAcl, acl2, fooAcl, prefixAcl))

    val allTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.ANY), AccessControlEntryFilter.ANY)
    val allLiteralTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val allPrefixedTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.PREFIXED), AccessControlEntryFilter.ANY)
    val literalMyTopic2Acls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val prefixedMyTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic", PatternType.PREFIXED), AccessControlEntryFilter.ANY)
    val allMyTopic2Acls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.MATCH), AccessControlEntryFilter.ANY)
    val allFooTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "foobar", PatternType.MATCH), AccessControlEntryFilter.ANY)

    assertEquals(Set(anyAcl), getAcls(anyAcl.toFilter))
    assertEquals(Set(prefixAcl), getAcls(prefixAcl.toFilter))
    assertEquals(Set(acl2), getAcls(acl2.toFilter))
    assertEquals(Set(fooAcl), getAcls(fooAcl.toFilter))

    assertEquals(Set(acl2), getAcls(literalMyTopic2Acls))
    assertEquals(Set(prefixAcl), getAcls(prefixedMyTopicAcls))
    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(allLiteralTopicAcls))
    assertEquals(Set(prefixAcl), getAcls(allPrefixedTopicAcls))
    assertEquals(Set(anyAcl, acl2, prefixAcl), getAcls(allMyTopic2Acls))
    assertEquals(Set(anyAcl, fooAcl), getAcls(allFooTopicAcls))
    assertEquals(Set(anyAcl, acl2, fooAcl, prefixAcl), getAcls(allTopicAcls))
  }

  @Test
  def testAclDelete(): Unit = {
    client = createAdminClient
    ensureAcls(Set(anyAcl, acl2, fooAcl, prefixAcl))

    val allTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.MATCH), AccessControlEntryFilter.ANY)
    val allLiteralTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val allPrefixedTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.PREFIXED), AccessControlEntryFilter.ANY)

    // Delete only ACLs on literal 'mytopic2' topic
    var deleted = client.deleteAcls(java.util.List.of(acl2.toFilter)).all().get().asScala.toSet
    brokers.foreach { b =>
      waitAndVerifyRemovedAcl(acl2.entry(), b.dataPlaneRequestProcessor.authorizerPlugin.get, acl2.pattern())
    }
    assertEquals(Set(anyAcl, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete only ACLs on literal '*' topic
    deleted = client.deleteAcls(java.util.List.of(anyAcl.toFilter)).all().get().asScala.toSet
    brokers.foreach { b =>
      waitAndVerifyRemovedAcl(anyAcl.entry(), b.dataPlaneRequestProcessor.authorizerPlugin.get, anyAcl.pattern())
    }
    assertEquals(Set(acl2, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete only ACLs on specific prefixed 'mytopic' topics:
    deleted = client.deleteAcls(java.util.List.of(prefixAcl.toFilter)).all().get().asScala.toSet
    brokers.foreach { b =>
      waitAndVerifyRemovedAcl(prefixAcl.entry(), b.dataPlaneRequestProcessor.authorizerPlugin.get, prefixAcl.pattern())
    }
    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all literal ACLs:
    deleted = client.deleteAcls(java.util.List.of(allLiteralTopicAcls)).all().get().asScala.toSet
    brokers.foreach { b =>
      Set(anyAcl, acl2, fooAcl).foreach(acl =>
        waitAndVerifyRemovedAcl(acl.entry(), b.dataPlaneRequestProcessor.authorizerPlugin.get, acl.pattern())
      )
    }
    assertEquals(Set(prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all prefixed ACLs:
    deleted = client.deleteAcls(java.util.List.of(allPrefixedTopicAcls)).all().get().asScala.toSet
    brokers.foreach { b =>
      waitAndVerifyRemovedAcl(prefixAcl.entry(), b.dataPlaneRequestProcessor.authorizerPlugin.get, prefixAcl.pattern())
    }
    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all topic ACLs:
    deleted = client.deleteAcls(java.util.List.of(allTopicAcls)).all().get().asScala.toSet
    brokers.foreach { b =>
      Set(anyAcl, acl2, fooAcl, prefixAcl).foreach(acl =>
        waitAndVerifyRemovedAcl(acl.entry(), b.dataPlaneRequestProcessor.authorizerPlugin.get, acl.pattern())
      )
    }
    assertEquals(Set(), getAcls(allTopicAcls))
  }

  //noinspection ScalaDeprecation - test explicitly covers clients using legacy / deprecated constructors
  @Test
  def testLegacyAclOpsNeverAffectOrReturnPrefixed(): Unit = {
    client = createAdminClient
    ensureAcls(Set(anyAcl, acl2, fooAcl, prefixAcl))  // <-- prefixed exists, but should never be returned.

    val allTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.MATCH), AccessControlEntryFilter.ANY)
    val legacyAllTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val legacyMyTopic2Acls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val legacyAnyTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "*", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val legacyFooTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "foobar", PatternType.LITERAL), AccessControlEntryFilter.ANY)

    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(legacyAllTopicAcls))
    assertEquals(Set(acl2), getAcls(legacyMyTopic2Acls))
    assertEquals(Set(anyAcl), getAcls(legacyAnyTopicAcls))
    assertEquals(Set(fooAcl), getAcls(legacyFooTopicAcls))

    // Delete only (legacy) ACLs on 'mytopic2' topic
    var deleted = client.deleteAcls(java.util.List.of(legacyMyTopic2Acls)).all().get().asScala.toSet
    brokers.foreach { b =>
      waitAndVerifyRemovedAcl(acl2.entry(), b.dataPlaneRequestProcessor.authorizerPlugin.get, acl2.pattern())
    }
    assertEquals(Set(anyAcl, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete only (legacy) ACLs on '*' topic
    deleted = client.deleteAcls(java.util.List.of(legacyAnyTopicAcls)).all().get().asScala.toSet
    brokers.foreach { b =>
      waitAndVerifyRemovedAcl(anyAcl.entry(), b.dataPlaneRequestProcessor.authorizerPlugin.get, anyAcl.pattern())
    }
    assertEquals(Set(acl2, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all (legacy) topic ACLs:
    deleted = client.deleteAcls(java.util.List.of(legacyAllTopicAcls)).all().get().asScala.toSet
    brokers.foreach { b =>
      Set(anyAcl, acl2, fooAcl).foreach(acl =>
        waitAndVerifyRemovedAcl(acl.entry(), b.dataPlaneRequestProcessor.authorizerPlugin.get, acl.pattern())
      )
    }
    assertEquals(Set(), getAcls(legacyAllTopicAcls))
    assertEquals(Set(prefixAcl), getAcls(allTopicAcls))
  }

  @Test
  def testAttemptToCreateInvalidAcls(): Unit = {
    client = createAdminClient
    val clusterAcl = new AclBinding(new ResourcePattern(ResourceType.CLUSTER, "foobar", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
    val emptyResourceNameAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
    val results = client.createAcls(java.util.List.of(clusterAcl, emptyResourceNameAcl), new CreateAclsOptions())
    assertEquals(Set(clusterAcl, emptyResourceNameAcl), results.values.keySet().asScala)
    assertFutureThrows(classOf[InvalidRequestException], results.values.get(clusterAcl))
    assertFutureThrows(classOf[InvalidRequestException], results.values.get(emptyResourceNameAcl))
  }

  @Test
  def testCreateDelegationTokenWithSmallerTimeout(): Unit = {
    client = createAdminClient
    val timeout = 5000

    val options = new CreateDelegationTokenOptions().maxLifetimeMs(timeout)
    val tokenInfo = client.createDelegationToken(options).delegationToken.get.tokenInfo

    assertEquals(timeout, tokenInfo.maxTimestamp - tokenInfo.issueTimestamp)
    assertTrue(tokenInfo.maxTimestamp >= tokenInfo.expiryTimestamp)
  }

  @Test
  def testExpiredTimeStampLargerThanMaxLifeStamp(): Unit = {
    client = createAdminClient
    val timeout = 5000

    val createOptions = new CreateDelegationTokenOptions().maxLifetimeMs(timeout)
    val token = client.createDelegationToken(createOptions).delegationToken.get
    val tokenInfo = token.tokenInfo

    assertEquals(timeout, tokenInfo.maxTimestamp - tokenInfo.issueTimestamp)
    assertTrue(tokenInfo.maxTimestamp >= tokenInfo.expiryTimestamp)

    TestUtils.waitUntilTrue(() => brokers.forall(server => server.tokenCache.tokens.size == 1),
      "Timed out waiting for token to propagate to all servers")

    val expiredOptions = new ExpireDelegationTokenOptions().expiryTimePeriodMs(tokenInfo.maxTimestamp + 1)
    val expiredResult = client.expireDelegationToken(token.hmac, expiredOptions)

    assertEquals(tokenInfo.maxTimestamp, expiredResult.expiryTimestamp.get())
  }

  private def verifyCauseIsClusterAuth(e: Throwable): Unit = assertEquals(classOf[ClusterAuthorizationException], e.getCause.getClass)

  private def testAclCreateGetDelete(expectAuth: Boolean): Unit = {
    TestUtils.waitUntilTrue(() => {
      val result = client.createAcls(java.util.List.of(fooAcl, transactionalIdAcl), new CreateAclsOptions)
      if (expectAuth) {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(_) => true
        }
      } else {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
          case Success(_) => false
        }
      }
    }, "timed out waiting for createAcls to " + (if (expectAuth) "succeed" else "fail"))
    if (expectAuth) {
      waitForDescribeAcls(client, fooAcl.toFilter, Set(fooAcl))
      waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set(transactionalIdAcl))
    }
    TestUtils.waitUntilTrue(() => {
      val result = client.deleteAcls(java.util.List.of(fooAcl.toFilter, transactionalIdAcl.toFilter), new DeleteAclsOptions)
      if (expectAuth) {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(_) => true
        }
      } else {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
          case Success(_) =>
            assertEquals(Set(fooAcl, transactionalIdAcl), result.values.keySet)
            assertEquals(Set(fooAcl), result.values.get(fooAcl.toFilter).get.values.asScala.map(_.binding).toSet)
            assertEquals(Set(transactionalIdAcl),
              result.values.get(transactionalIdAcl.toFilter).get.values.asScala.map(_.binding).toSet)
            true
        }
      }
    }, "timed out waiting for deleteAcls to " + (if (expectAuth) "succeed" else "fail"))
    if (expectAuth) {
      waitForDescribeAcls(client, fooAcl.toFilter, Set.empty)
      waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set.empty)
    }
  }

  private def testAclGet(expectAuth: Boolean): Unit = {
    TestUtils.waitUntilTrue(() => {
      val userAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
        new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW))
      val results = client.describeAcls(userAcl.toFilter)
      if (expectAuth) {
        Try(results.values.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(acls) => Set(userAcl).equals(acls.asScala.toSet)
        }
      } else {
        Try(results.values.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
          case Success(_) => false
        }
      }
    }, "timed out waiting for describeAcls to " + (if (expectAuth) "succeed" else "fail"))
  }

  @Test
  def testAclAuthorizationDenied(): Unit = {
    client = createAdminClient

    // Test that we cannot create or delete ACLs when ALTER is denied.
    addClusterAcl(DENY, ALTER)
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we cannot do anything with ACLs when DESCRIBE and ALTER are denied.
    addClusterAcl(DENY, DESCRIBE)
    testAclGet(expectAuth = false)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we can create, delete, and get ACLs with the default ACLs.
    removeClusterAcl(DENY, DESCRIBE)
    removeClusterAcl(DENY, ALTER)
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = true)

    // Test that we can't do anything with ACLs without the ALLOW ALTER ACL in place.
    removeClusterAcl(ALLOW, ALTER)
    removeClusterAcl(ALLOW, DELETE)
    testAclGet(expectAuth = false)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we can describe, but not alter ACLs, with only the ALLOW DESCRIBE ACL in place.
    addClusterAcl(ALLOW, DESCRIBE)
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = false)
  }

  private def addClusterAcl(permissionType: AclPermissionType, operation: AclOperation): Unit = {
    val ace = clusterAcl(permissionType, operation)
    superUserAdmin.createAcls(java.util.List.of(new AclBinding(clusterResourcePattern, ace)))
    brokers.foreach { b =>
      waitAndVerifyAcl(ace, b.dataPlaneRequestProcessor.authorizerPlugin.get, clusterResourcePattern)
    }
  }

  private def removeClusterAcl(permissionType: AclPermissionType, operation: AclOperation): Unit = {
    val ace = clusterAcl(permissionType, operation)
    superUserAdmin.deleteAcls(java.util.List.of(new AclBinding(clusterResourcePattern, ace).toFilter)).values

    brokers.foreach { b =>
      waitAndVerifyRemovedAcl(ace, b.dataPlaneRequestProcessor.authorizerPlugin.get, clusterResourcePattern)
    }
  }

  @Test
  def testCreateTopicsResponseMetadataAndConfig(): Unit = {
    val topic1 = "mytopic1"
    val topic2 = "mytopic2"
    val denyAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic2, PatternType.LITERAL),
      new AccessControlEntry("User:*", "*", AclOperation.DESCRIBE_CONFIGS, AclPermissionType.DENY))

    client = createAdminClient
    client.createAcls(java.util.List.of(denyAcl), new CreateAclsOptions()).all().get()

    val topics = Seq(topic1, topic2)
    val configsOverride = java.util.Map.of(TopicConfig.SEGMENT_BYTES_CONFIG, "3000000")
    val newTopics = java.util.List.of(
      new NewTopic(topic1, 2, 3.toShort).configs(configsOverride),
      new NewTopic(topic2, Optional.empty[Integer], Optional.empty[java.lang.Short]).configs(configsOverride))
    val validateResult = client.createTopics(newTopics, new CreateTopicsOptions().validateOnly(true))
    validateResult.all.get()
    waitForTopics(client, List(), topics)

    def validateMetadataAndConfigs(result: CreateTopicsResult): Unit = {
      assertEquals(2, result.numPartitions(topic1).get())
      assertEquals(3, result.replicationFactor(topic1).get())
      val topicConfigs = result.config(topic1).get().entries.asScala
      assertTrue(topicConfigs.nonEmpty)
      val segmentBytesConfig = topicConfigs.find(_.name == TopicConfig.SEGMENT_BYTES_CONFIG).get
      assertEquals(3000000, segmentBytesConfig.value.toLong)
      assertEquals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, segmentBytesConfig.source)
      val compressionConfig = topicConfigs.find(_.name == TopicConfig.COMPRESSION_TYPE_CONFIG).get
      assertEquals(ServerLogConfigs.COMPRESSION_TYPE_DEFAULT, compressionConfig.value)
      assertEquals(ConfigEntry.ConfigSource.DEFAULT_CONFIG, compressionConfig.source)

      assertFutureThrows(classOf[TopicAuthorizationException], result.numPartitions(topic2))
      assertFutureThrows(classOf[TopicAuthorizationException], result.replicationFactor(topic2))
      assertFutureThrows(classOf[TopicAuthorizationException], result.config(topic2))
    }
    validateMetadataAndConfigs(validateResult)

    val createResult = client.createTopics(newTopics, new CreateTopicsOptions())
    createResult.all.get()
    waitForTopics(client, topics, List())
    validateMetadataAndConfigs(createResult)
    val topicIds = getTopicIds()
    assertNotEquals(Uuid.ZERO_UUID, createResult.topicId(topic1).get())
    assertEquals(topicIds(topic1), createResult.topicId(topic1).get())
    assertFutureThrows(classOf[TopicAuthorizationException], createResult.topicId(topic2))
    
    val createResponseConfig = createResult.config(topic1).get().entries.asScala

    val describeResponseConfig = describeConfigs(topic1)
    assertEquals(describeResponseConfig.map(_.name).toSet, createResponseConfig.map(_.name).toSet)
    describeResponseConfig.foreach { describeEntry =>
      val name = describeEntry.name
      val createEntry = createResponseConfig.find(_.name == name).get
      assertEquals(describeEntry.value, createEntry.value, s"Value mismatch for $name")
      assertEquals(describeEntry.isReadOnly, createEntry.isReadOnly, s"isReadOnly mismatch for $name")
      assertEquals(describeEntry.isSensitive, createEntry.isSensitive, s"isSensitive mismatch for $name")
      assertEquals(describeEntry.source, createEntry.source, s"Source mismatch for $name")
    }
  }

  @Test
  def testExpireDelegationToken(): Unit = {
    client = createAdminClient
    val createDelegationTokenOptions = new CreateDelegationTokenOptions().maxLifetimeMs(5000)

    // Test expiration for non-exists token
    assertFutureThrows(
      classOf[DelegationTokenNotFoundException],
      client.expireDelegationToken("".getBytes()).expiryTimestamp()
    )

    // Test expiring the token immediately
    val token1 = client.createDelegationToken(createDelegationTokenOptions).delegationToken().get()
    TestUtils.retry(maxWaitMs = 1000) { assertTrue(expireTokenOrFailWithAssert(token1, -1) < System.currentTimeMillis()) }

    // Test expiring the expired token
    val token2 = client.createDelegationToken(createDelegationTokenOptions.maxLifetimeMs(1000)).delegationToken().get()
    // Ensure current time > maxLifeTimeMs of token
    Thread.sleep(1000)
    assertFutureThrows(
      classOf[DelegationTokenExpiredException],
      client.expireDelegationToken(token2.hmac(),
      new ExpireDelegationTokenOptions().expiryTimePeriodMs(1)).expiryTimestamp()
    )

    // Ensure expiring the expired token with negative expiryTimePeriodMs will not throw exception
    assertDoesNotThrow(() => expireTokenOrFailWithAssert(token2, -1))

    // Test shortening the expiryTimestamp
    val token3 = client.createDelegationToken(createDelegationTokenOptions).delegationToken().get()
    TestUtils.retry(1000) { assertTrue(expireTokenOrFailWithAssert(token3, 200) < token3.tokenInfo().expiryTimestamp()) }
  }

  @Test
  def testCreateTokenWithOverflowTimestamp(): Unit = {
    client = createAdminClient
    val token = client.createDelegationToken(new CreateDelegationTokenOptions().maxLifetimeMs(Long.MaxValue)).delegationToken().get()
    assertEquals(Long.MaxValue, token.tokenInfo().expiryTimestamp())
  }

  @Test
  def testExpireTokenWithOverflowTimestamp(): Unit = {
    client = createAdminClient
    val token = client.createDelegationToken(new CreateDelegationTokenOptions().maxLifetimeMs(Long.MaxValue)).delegationToken().get()
    TestUtils.retry(1000) { assertTrue(expireTokenOrFailWithAssert(token, Long.MaxValue) == Long.MaxValue) }
  }

  private def expireTokenOrFailWithAssert(token: DelegationToken, expiryTimePeriodMs: Long): Long  = {
    try {
      client.expireDelegationToken(token.hmac(), new ExpireDelegationTokenOptions().expiryTimePeriodMs(expiryTimePeriodMs))
        .expiryTimestamp().get()
    } catch {
      // If metadata is not synced yet, the response will contain an errorCode, causing an exception to be thrown.
      // This wrapper is designed to work with TestUtils.retry
      case _: ExecutionException => throw new AssertionError("Metadata not sync yet.")
    }
  }

  private def describeConfigs(topic: String): Iterable[ConfigEntry] = {
    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    var configEntries: Iterable[ConfigEntry] = null

    TestUtils.waitUntilTrue(() => {
      try {
        val topicResponse = client.describeConfigs(java.util.List.of(topicResource)).all.get.get(topicResource)
        configEntries = topicResponse.entries.asScala
        true
      } catch {
        case e: ExecutionException if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => false
      }
    }, "Timed out waiting for describeConfigs")

    configEntries
  }

  private def waitForDescribeAcls(client: Admin, filter: AclBindingFilter, acls: Set[AclBinding]): Unit = {
    var lastResults: util.Collection[AclBinding] = null
    TestUtils.waitUntilTrue(() => {
      lastResults = client.describeAcls(filter).values.get()
      acls == lastResults.asScala.toSet
    }, s"timed out waiting for ACLs $acls.\nActual $lastResults")
  }

  private def ensureAcls(bindings: Set[AclBinding]): Unit = {
    client.createAcls(bindings.asJava).all().get()

    bindings.foreach(binding => waitForDescribeAcls(client, binding.toFilter, Set(binding)))
  }

  private def getAcls(allTopicAcls: AclBindingFilter) = {
    client.describeAcls(allTopicAcls).values.get().asScala.toSet
  }

  def waitAndVerifyRemovedAcl(expectedToRemoved: AccessControlEntry,
                              authorizerPlugin: Plugin[JAuthorizer],
                              resource: ResourcePattern,
                              accessControlEntryFilter: AccessControlEntryFilter = AccessControlEntryFilter.ANY): Unit = {
    val newLine = scala.util.Properties.lineSeparator
    val authorizer = authorizerPlugin.get
    val filter = new AclBindingFilter(resource.toFilter, accessControlEntryFilter)
    waitUntilTrue(() => !authorizer.acls(filter).asScala.map(_.entry).toSet.contains(expectedToRemoved),
      s"expected acl to be removed : $expectedToRemoved" +
        s"but got:${authorizer.acls(filter).asScala.map(_.entry).mkString(newLine + "\t", newLine + "\t", newLine)}",
      45000)
  }

  def waitAndVerifyAcl(expected: AccessControlEntry,
                       authorizerPlugin: Plugin[JAuthorizer],
                       resource: ResourcePattern,
                       accessControlEntryFilter: AccessControlEntryFilter = AccessControlEntryFilter.ANY): Unit = {
    val newLine = scala.util.Properties.lineSeparator
    val authorizer = authorizerPlugin.get
    val filter = new AclBindingFilter(resource.toFilter, accessControlEntryFilter)
    waitUntilTrue(() => authorizer.acls(filter).asScala.map(_.entry).toSet.contains(expected),
      s"expected to contain acl: $expected" +
        s"but got:${authorizer.acls(filter).asScala.map(_.entry).mkString(newLine + "\t", newLine + "\t", newLine)}",
      45000)
  }
}
