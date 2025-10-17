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

package org.apache.kafka.common.test;

import kafka.server.BrokerServer;
import kafka.server.ControllerServer;
import kafka.server.KafkaBroker;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.fault.FaultHandlerException;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.clients.consumer.GroupProtocol.CLASSIC;
import static org.apache.kafka.clients.consumer.GroupProtocol.CONSUMER;

public interface ClusterInstance {

    Type type();

    Map<Integer, KafkaBroker> brokers();

    default Map<Integer, KafkaBroker> aliveBrokers() {
        return brokers().entrySet().stream().filter(entry -> !entry.getValue().isShutdown())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    Map<Integer, ControllerServer> controllers();

    /**
     * The immutable cluster configuration used to create this cluster.
     */
    ClusterConfig config();

    /**
     * Return the set of all controller IDs configured for this test. For kraft, this
     * will return only the nodes which have the "controller" role enabled in `process.roles`.
     */
    Set<Integer> controllerIds();

    /**
     * Return the set of all broker IDs configured for this test.
     */
    default Set<Integer> brokerIds() {
        return brokers().keySet();
    }

    /**
     * The listener for this cluster as configured by {@link ClusterTest} or by {@link ClusterConfig}. If
     * unspecified by those sources, this will return the listener for the default security protocol PLAINTEXT
     */
    ListenerName clientListener();

    /**
     * The listener for the kraft cluster controller configured by controller.listener.names.
     */
    ListenerName controllerListenerName();

    /**
     * The broker connect string which can be used by clients for bootstrapping
     */
    String bootstrapServers();

    /**
     * The broker connect string which can be used by clients for bootstrapping to the controller quorum.
     */
    String bootstrapControllers();

    default List<Integer> controllerBoundPorts() {
        return controllers().values().stream()
            .map(ControllerServer::socketServer)
            .map(ss -> ss.boundPort(controllerListenerName()))
            .toList();
    }

    default List<Integer> brokerBoundPorts() {
        return brokers().values().stream()
            .map(KafkaBroker::socketServer)
            .map(ss -> ss.boundPort(clientListener()))
            .toList();
    }

    String clusterId();

    //---------------------------[producer/consumer/admin]---------------------------//

    default <K, V> Producer<K, V> producer(Map<String, Object> configs) {
        Map<String, Object> props = new HashMap<>(configs);
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        return new KafkaProducer<>(setClientSaslConfig(props));
    }

    default <K, V> Producer<K, V> producer() {
        return producer(Map.of());
    }

    default <K, V> Consumer<K, V> consumer(Map<String, Object> configs) {
        Map<String, Object> props = new HashMap<>(configs);
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "group_" + TestUtils.randomString(5));
        props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        return new KafkaConsumer<>(setClientSaslConfig(props));
    }

    default <K, V> Consumer<K, V> consumer() {
        return consumer(Map.of());
    }

    default <K, V> ShareConsumer<K, V> shareConsumer() {
        return shareConsumer(Map.of());
    }

    default <K, V> ShareConsumer<K, V> shareConsumer(Map<String, Object> configs) {
        return shareConsumer(configs, null, null);
    }

    default <K, V> ShareConsumer<K, V> shareConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        Map<String, Object> props = new HashMap<>(configs);
        if (keyDeserializer == null) {
            props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        }
        if (valueDeserializer == null) {
            props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        }
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "group_" + TestUtils.randomString(5));
        props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        return new KafkaShareConsumer<>(setClientSaslConfig(props), keyDeserializer, valueDeserializer);
    }

    default Admin admin(Map<String, Object> configs, boolean usingBootstrapControllers) {
        Map<String, Object> props = new HashMap<>(configs);
        if (usingBootstrapControllers) {
            props.putIfAbsent(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, bootstrapControllers());
            props.remove(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        } else {
            props.putIfAbsent(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
            props.remove(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG);
        }
        return Admin.create(setClientSaslConfig(props));
    }

    default Map<String, Object> setClientSaslConfig(Map<String, Object> configs) {
        Map<String, Object> props = new HashMap<>(configs);
        if (config().brokerSecurityProtocol() == SecurityProtocol.SASL_PLAINTEXT) {
            props.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
            props.putIfAbsent(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.putIfAbsent(
                SaslConfigs.SASL_JAAS_CONFIG,
                String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    JaasUtils.KAFKA_PLAIN_ADMIN, JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD
                )
            );
        }
        return props;
    }

    default Admin admin(Map<String, Object> configs) {
        return admin(configs, false);
    }

    default Admin admin() {
        return admin(Map.of(), false);
    }

    default Set<GroupProtocol> supportedGroupProtocols() {
        if (brokers().values().stream().allMatch(b -> b.dataPlaneRequestProcessor().isConsumerGroupProtocolEnabled())) {
            return Set.of(CLASSIC, CONSUMER);
        } else {
            return Set.of(CLASSIC);
        }
    }

    /**
     * Returns the first recorded fatal exception, if any.
     *
     */
    Optional<FaultHandlerException> firstFatalException();

    /**
     * Return the first recorded non-fatal exception, if any.
     */
    Optional<FaultHandlerException> firstNonFatalException();

    //---------------------------[modify]---------------------------//

    void start();

    boolean started();

    void stop();

    boolean stopped();

    void shutdownBroker(int brokerId);

    void startBroker(int brokerId);

    //---------------------------[wait]---------------------------//

    default void waitTopicDeletion(String topic) throws InterruptedException {
        Collection<KafkaBroker> brokers = aliveBrokers().values();
        // wait for metadata
        TestUtils.waitForCondition(
            () -> brokers.stream().allMatch(
                broker -> broker.metadataCache().numPartitions(topic).isEmpty()),
                60000L, topic + " metadata not propagated after 60000 ms");

        ensureConsistentMetadata(brokers, controllers().values());

        TopicPartition topicPartition = new TopicPartition(topic, 0);

        // Ensure that the topic-partition has been deleted from all brokers' replica managers
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                broker.replicaManager().onlinePartition(topicPartition).isEmpty()
        ), "Replica manager's should have deleted all of this topic's partitions");

        // Ensure that logs from all replicas are deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                broker.logManager().getLog(topicPartition, false).isEmpty()
        ), "Replica logs not deleted after delete topic is complete");

        // Ensure that the topic is removed from all cleaner offsets
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker -> {
            List<File> liveLogDirs = CollectionConverters.asJava(broker.logManager().liveLogDirs());
            return liveLogDirs.stream().allMatch(logDir -> {
                OffsetCheckpointFile checkpointFile;
                try {
                    checkpointFile = new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint"), null);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return !checkpointFile.read().containsKey(topicPartition);
            });
        }), "Cleaner offset for deleted partition should have been removed");

        // Ensure that the topic directories are soft-deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                broker.config().logDirs().stream().allMatch(logDir ->
                    !new File(logDir, topicPartition.topic() + "-" + topicPartition.partition()).exists())
        ), "Failed to soft-delete the data to a delete directory");

        // Ensure that the topic directories are hard-deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                broker.config().logDirs().stream().allMatch(logDir ->
                    Arrays.stream(Objects.requireNonNull(new File(logDir).list())).noneMatch(partitionDirectoryName ->
                        partitionDirectoryName.startsWith(topicPartition.topic() + "-" + topicPartition.partition()) &&
                            partitionDirectoryName.endsWith(UnifiedLog.DELETE_DIR_SUFFIX)))
        ), "Failed to hard-delete the delete directory");
    }

    default void createTopic(String topicName, int partitions, short replicas) throws InterruptedException {
        createTopic(topicName, partitions, replicas, Map.of());
    }

    default void createTopic(String topicName, int partitions, short replicas, Map<String, String> props) throws InterruptedException {
        try (Admin admin = admin()) {
            admin.createTopics(List.of(new NewTopic(topicName, partitions, replicas).configs(props)));
            waitTopicCreation(topicName, partitions);
        }
    }

    /**
     * Deletes a topic and waits for the deletion to complete.
     *
     * @param topicName The name of the topic to delete
     * @throws InterruptedException If the operation is interrupted
     */
    default void deleteTopic(String topicName) throws InterruptedException, ExecutionException {
        try (Admin admin = admin()) {
            admin.deleteTopics(List.of(topicName));
            waitTopicDeletion(topicName);
        }
    }

    void waitForReadyBrokers() throws InterruptedException;

    default void waitTopicCreation(String topic, int partitions) throws InterruptedException {
        if (partitions <= 0) {
            throw new IllegalArgumentException("Partition count must be > 0, but was " + partitions);
        }

        // wait for metadata
        Collection<KafkaBroker> brokers = aliveBrokers().values();
        TestUtils.waitForCondition(
            () -> brokers.stream().allMatch(broker -> broker.metadataCache().numPartitions(topic).filter(p -> p == partitions).isPresent()),
                60000L, topic + " metadata not propagated after 60000 ms");

        ensureConsistentMetadata(brokers, controllers().values());
    }

    default void ensureConsistentMetadata() throws InterruptedException  {
        ensureConsistentMetadata(aliveBrokers().values(), controllers().values());
    }

    default void ensureConsistentMetadata(Collection<KafkaBroker> brokers, Collection<ControllerServer> controllers) throws InterruptedException  {
        for (ControllerServer controller : controllers) {
            long controllerOffset = controller.raftManager().replicatedLog().endOffset().offset() - 1;
            TestUtils.waitForCondition(
                () -> brokers.stream().allMatch(broker -> ((BrokerServer) broker).sharedServer().loader().lastAppliedOffset() >= controllerOffset),
                60000L, "Timeout waiting for controller metadata propagating to brokers");
        }
    }

    default List<Authorizer> authorizers() {
        List<Authorizer> authorizers = new ArrayList<>();
        authorizers.addAll(brokers().values().stream()
                .filter(server -> server.authorizerPlugin().isDefined())
                .map(server -> server.authorizerPlugin().get().get()).toList());
        authorizers.addAll(controllers().values().stream()
                .filter(server -> server.authorizerPlugin().isDefined())
                .map(server -> server.authorizerPlugin().get().get()).toList());
        return authorizers;
    }

    default void waitAcls(AclBindingFilter filter, Collection<AccessControlEntry> entries) throws InterruptedException {
        for (Authorizer authorizer : authorizers()) {
            AtomicReference<Set<AccessControlEntry>> actualEntries = new AtomicReference<>(new HashSet<>());
            TestUtils.waitForCondition(() -> {
                Set<AccessControlEntry> accessControlEntrySet = new HashSet<>();
                authorizer.acls(filter).forEach(aclBinding -> accessControlEntrySet.add(aclBinding.entry()));
                actualEntries.set(accessControlEntrySet);
                return accessControlEntrySet.containsAll(entries) && entries.containsAll(accessControlEntrySet);
            }, "expected acls: " + entries + ", actual acls: " + actualEntries.get());
        }
    }

    /**
     * Returns the broker id of leader partition.
     */
    default int getLeaderBrokerId(TopicPartition topicPartition) throws ExecutionException, InterruptedException {
        try (var admin = admin()) {
            String topic = topicPartition.topic();
            TopicDescription description = admin.describeTopics(List.of(topic)).topicNameValues().get(topic).get();

            return description.partitions().stream()
                    .filter(tp -> tp.partition() == topicPartition.partition())
                    .mapToInt(tp -> tp.leader().id())
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Leader not found for tp " + topicPartition));
        }
    }

    /**
     * Wait for a leader to be elected or changed using the provided admin client.
     */
    default int waitUntilLeaderIsElectedOrChangedWithAdmin(Admin admin,
                                                           String topic,
                                                           int partitionNumber,
                                                           long timeoutMs) throws Exception {
        long startTime = System.currentTimeMillis();
        TopicPartition topicPartition = new TopicPartition(topic, partitionNumber);

        while (System.currentTimeMillis() < startTime + timeoutMs) {
            try {
                TopicDescription topicDescription = admin.describeTopics(List.of(topic))
                        .allTopicNames().get().get(topic);

                Optional<Integer> leader = topicDescription.partitions().stream()
                        .filter(partitionInfo -> partitionInfo.partition() == partitionNumber)
                        .findFirst()
                        .map(partitionInfo -> {
                            int leaderId = partitionInfo.leader().id();
                            return leaderId == Node.noNode().id() ? null : leaderId;
                        });

                if (leader.isPresent()) {
                    return leader.get();
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnknownTopicOrPartitionException ||
                        cause instanceof LeaderNotAvailableException) {
                    continue;
                } else {
                    throw e;
                }
            }

            TimeUnit.MILLISECONDS.sleep(Math.min(100L, timeoutMs));
        }

        throw new AssertionError("Timing out after " + timeoutMs +
                " ms since a leader was not elected for partition " + topicPartition);
    }
}
