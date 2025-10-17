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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ConnectedStoreProvider;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;

import java.time.Duration;

/**
 * {@code KStream} is an abstraction of a <em>record stream</em> of {@link Record key-value} pairs, i.e., each record is
 * an independent entity/event in the real world.
 * For example a user X might buy two items I1 and I2, and thus there might be two records {@code <K:I1>, <K:I2>}
 * in the stream.
 *
 * <p>A {@code KStream} is either {@link StreamsBuilder#stream(String) defined from one or multiple Kafka topics} that
 * are consumed message by message or the result of a {@code KStream} transformation.
 * A {@link KTable} can also be directly {@link KTable#toStream() converted} into a {@code KStream}.
 *
 * <p>A {@code KStream} can be transformed record by record, joined with another {@code KStream}, {@link KTable},
 * {@link GlobalKTable}, or can be aggregated into a {@link KTable}.
 * A {@link KStream} can also be directly {@link KStream#toTable() converted} into a {@code KTable}.
 * Kafka Streams DSL can be mixed-and-matched with the Processor API (PAPI) (cf. {@link Topology}) via
 * {@link #process(ProcessorSupplier, String...) process(...)} and {@link #processValues(FixedKeyProcessorSupplier,
 * String...) processValues(...)}.
 *
 * @param <K> the key type of this stream
 * @param <V> the value type of this stream
 */
public interface KStream<K, V> {

    /**
     * Create a new {@code KStream} that consists of all records of this stream which satisfy the given predicate.
     * All records that do not satisfy the predicate are dropped.
     * This is a stateless record-by-record operation (cf. {@link #processValues(FixedKeyProcessorSupplier, String...)}
     * for stateful record processing or if you need access to the record's timestamp, headers, or other metadata).
     *
     * @param predicate
     *        a filter {@link Predicate} that is applied to each record
     *
     * @return A {@code KStream} that contains only those records that satisfy the given predicate.
     *
     * @see #filterNot(Predicate)
     */
    KStream<K, V> filter(final Predicate<? super K, ? super V> predicate);

    /**
     * See {@link #filter(Predicate)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    KStream<K, V> filter(final Predicate<? super K, ? super V> predicate, final Named named);

    /**
     * Create a new {@code KStream} that consists all records of this stream which do <em>not</em> satisfy the given
     * predicate.
     * All records that <em>do</em> satisfy the predicate are dropped.
     * This is a stateless record-by-record operation (cf. {@link #processValues(FixedKeyProcessorSupplier, String...)}
     * for stateful record processing or if you need access to the record's timestamp, headers, or other metadata).
     *
     * @param predicate
     *        a filter {@link Predicate} that is applied to each record
     *
     * @return A {@code KStream} that contains only those records that do <em>not</em> satisfy the given predicate.
     *
     * @see #filter(Predicate)
     */
    KStream<K, V> filterNot(final Predicate<? super K, ? super V> predicate);

    /**
     * See {@link #filterNot(Predicate)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    KStream<K, V> filterNot(final Predicate<? super K, ? super V> predicate, final Named named);

    /**
     * Create a new {@code KStream} that consists of all records of this stream but with a modified key.
     * The provided {@link KeyValueMapper} is applied to each input record and computes a new key (possibly of a
     * different type) for it.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K':V>}.
     * This is a stateless record-by-record operation (cf. {@link #process(ProcessorSupplier, String...)} for
     * stateful record processing or if you need access to the record's timestamp, headers, or other metadata).
     *
     * <p>For example, you can use this transformation to set a key for a key-less input record {@code <null,V>}
     * by extracting a key from the value within your {@link KeyValueMapper}. The example below computes the new key
     * as the length of the value string.
     * <pre>{@code
     * KStream<Byte[], String> keyLessStream = builder.stream("key-less-topic");
     * KStream<Integer, String> keyedStream = keyLessStream.selectKey(new KeyValueMapper<Byte[], String, Integer> {
     *     Integer apply(Byte[] key, String value) {
     *         return value.length();
     *     }
     * });
     * }</pre>
     * Setting a new key might result in an internal data redistribution if a key-based operator (like an aggregation
     * or join) is applied to the result {@code KStream}.
     *
     * @param mapper
     *        a {@link KeyValueMapper} that computes a new key for each input record
     *
     * @param <KOut> the new key type of the result {@code KStream}
     *
     * @return A {@code KStream} that contains records with new key (possibly of a different type) and unmodified value.
     *
     * @see #map(KeyValueMapper)
     * @see #mapValues(ValueMapper)
     * @see #flatMap(KeyValueMapper)
     * @see #flatMapValues(ValueMapper)
     */
    <KOut> KStream<KOut, V> selectKey(final KeyValueMapper<? super K, ? super V, ? extends KOut> mapper);

    /**
     * See {@link #selectKey(KeyValueMapper)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <KOut> KStream<KOut, V> selectKey(final KeyValueMapper<? super K, ? super V, ? extends KOut> mapper,
                                      final Named named);

    /**
     * Create a new {@code KStream} that consists of all records of this stream but with a modified value.
     * The provided {@link ValueMapper} is applied to each input record value and computes a new value (possibly
     * of a different type) for it.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
     * If you need read access to the input record key, use {@link #mapValues(ValueMapperWithKey)}.
     * This is a stateless record-by-record operation (cf.
     * {@link #processValues(FixedKeyProcessorSupplier, String...)} for stateful value processing or if you need access
     * to the record's timestamp, headers, or other metadata).
     *
     * <p>The example below counts the number of token of the value string.
     * <pre>{@code
     * KStream<String, String> inputStream = builder.stream("topic");
     * KStream<String, Integer> outputStream = inputStream.mapValues(new ValueMapper<String, Integer> {
     *     Integer apply(String value) {
     *         return value.split(" ").length;
     *     }
     * });
     * }</pre>
     *
     * Setting a new value preserves data co-location with respect to the key.
     * Thus, <em>no</em> internal data redistribution is required if a key-based operator (like an aggregation
     * or join) is applied to the result {@code KStream} (cf. {@link #map(KeyValueMapper)}).
     *
     * @param mapper
     *        a {@link ValueMapper} that computes a new value for each input record
     *
     * @param <VOut> the value type of the result stream
     *
     * @return A {@code KStream} that contains records with unmodified key and new values (possibly of a different type).
     *
     * @see #selectKey(KeyValueMapper)
     * @see #map(KeyValueMapper)
     * @see #flatMap(KeyValueMapper)
     * @see #flatMapValues(ValueMapper)
     */
    <VOut> KStream<K, VOut> mapValues(final ValueMapper<? super V, ? extends VOut> mapper);

    /**
     * See {@link #mapValues(ValueMapper)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <VOut> KStream<K, VOut> mapValues(final ValueMapper<? super V, ? extends VOut> mapper,
                                      final Named named);

    /**
     * See {@link #mapValues(ValueMapper)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning and
     * incorrect results.
     */
    <VOut> KStream<K, VOut> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VOut> mapper);

    /**
     * See {@link #mapValues(ValueMapperWithKey)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <VOut> KStream<K, VOut> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VOut> mapper,
                                      final Named named);

    /**
     * Create a new {@code KStream} that consists of a modified record for each record in this stream.
     * The provided {@link KeyValueMapper} is applied to each input record and computes a new output record
     * (possibly of a different key and/or value type) for it.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K':V'>}.
     * This is a stateless record-by-record operation (cf. {@link #process(ProcessorSupplier, String...)} for
     * stateful record processing or if you need access to the record's timestamp, headers, or other metadata).
     *
     * <p>The example below normalizes the String key to upper-case letters and counts the number of token of the
     * value string.
     * <pre>{@code
     * KStream<String, String> inputStream = builder.stream("topic");
     * KStream<String, Integer> outputStream = inputStream.map(new KeyValueMapper<String, String, KeyValue<String, Integer>> {
     *     KeyValue<String, Integer> apply(String key, String value) {
     *         return new KeyValue<>(key.toUpperCase(), value.split(" ").length);
     *     }
     * });
     * }</pre>
     * The provided {@link KeyValueMapper} must return a {@link KeyValue} type and must not return {@code null}.
     *
     * <p>Mapping records might result in an internal data redistribution if a key-based operator (like an
     * aggregation or join) is applied to the result {@code KStream} (cf. {@link #mapValues(ValueMapper)}).
     *
     * @param mapper
     *        a {@link KeyValueMapper} that computes a new {@link KeyValue} pair for each input record
     *
     * @param <KOut> the key type of the result stream
     * @param <VOut> the value type of the result stream
     *
     * @return A {@code KStream} that contains records with new key and new value (possibly of different types).
     *
     * @see #selectKey(KeyValueMapper)
     * @see #flatMap(KeyValueMapper)
     * @see #mapValues(ValueMapper)
     * @see #flatMapValues(ValueMapper)
     */
    <KOut, VOut> KStream<KOut, VOut> map(final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KOut, ? extends VOut>> mapper);

    /**
     * See {@link #map(KeyValueMapper)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <KOut, VOut> KStream<KOut, VOut> map(final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KOut, ? extends VOut>> mapper,
                                         final Named named);

    /**
     * Create a new {@code KStream} that consists of zero or more records for each record in this stream.
     * The provided {@link KeyValueMapper} is applied to each input record and computes zero or more output records
     * (possibly of a different key and/or value type) for it.
     * Thus, an input record {@code <K,V>} can be transformed into output records {@code <K':V'>, <K':V'>, ...}.
     * This is a stateless record-by-record operation (cf. {@link #process(ProcessorSupplier, String...)} for
     * stateful record processing or if you need access to the record's timestamp, headers, or other metadata).
     *
     * <p>The example below splits input records {@code <null:String>} containing sentences as values into their words
     * and emit a record {@code <word:1>} for each word.
     * <pre>{@code
     * KStream<byte[], String> inputStream = builder.stream("topic");
     * KStream<String, Integer> outputStream = inputStream.flatMap(
     *     new KeyValueMapper<byte[], String, Iterable<KeyValue<String, Integer>>> {
     *         Iterable<KeyValue<String, Integer>> apply(byte[] key, String value) {
     *             String[] tokens = value.split(" ");
     *             List<KeyValue<String, Integer>> result = new ArrayList<>(tokens.length);
     *
     *             for(String token : tokens) {
     *                 result.add(new KeyValue<>(token, 1));
     *             }
     *
     *             return result;
     *         }
     *     });
     * }</pre>
     * The provided {@link KeyValueMapper} must return an {@link Iterable} (e.g., any {@link java.util.Collection}
     * type) and the return value must not be {@code null}.
     *
     * <p>Flat-mapping records might result in an internal data redistribution if a key-based operator (like an
     * aggregation or join) is applied to the result {@code KStream}. (cf. {@link #flatMapValues(ValueMapper)})
     *
     * @param mapper
     *        a {@link KeyValueMapper KeyValueMapper&lt;K, V, Iterable&lt;KeyValue&lt;K', V'&gt;&gt;&gt;} that
     *        computes zero of more new {@link KeyValue} pairs for each input record
     *
     * @param <KOut> the key type of the result stream
     * @param <VOut> the value type of the result stream
     *
     * @return A {@code KStream} that contains more or fewer records with new keys and values (possibly of different types).
     *
     * @see #selectKey(KeyValueMapper)
     * @see #map(KeyValueMapper)
     * @see #mapValues(ValueMapper)
     * @see #flatMapValues(ValueMapper)
     */
    <KOut, VOut> KStream<KOut, VOut> flatMap(final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KOut, ? extends VOut>>> mapper);

    /**
     * See {@link #flatMap(KeyValueMapper)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <KR, VOut> KStream<KR, VOut> flatMap(final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VOut>>> mapper,
                                         final Named named);

    /**
     * Create a new {@code KStream} that consists of zero or more records with modified value for each record
     * in this stream.
     * The provided {@link ValueMapper} is applied to each input record value and computes zero or more output values
     * (possibly of a different type) for it.
     * Thus, an input record {@code <K,V>} can be transformed into output records {@code <K:V'>, <K:V'>, ...}.
     * If you need read access to the input record key, use {@link #flatMapValues(ValueMapperWithKey)}.
     * This is a stateless record-by-record operation (cf. {@link #processValues(FixedKeyProcessorSupplier, String...)}
     * for stateful record processing or if you need access to the record's timestamp, headers, or other metadata).
     *
     * <p>The example below splits input records {@code <null:String>} containing sentences as values into their words.
     * <pre>{@code
     * KStream<byte[], String> inputStream = builder.stream("topic");
     * KStream<byte[], String> outputStream = inputStream.flatMapValues(new ValueMapper<String, Iterable<String>> {
     *     Iterable<String> apply(String value) {
     *         return Arrays.asList(value.split(" "));
     *     }
     * });
     * }</pre>
     * The provided {@link ValueMapper} must return an {@link Iterable} (e.g., any {@link java.util.Collection} type)
     * and the return value must not be {@code null}.
     *
     * <p>Splitting a record into multiple records with the same key preserves data co-location with respect to the key.
     * Thus, <em>no</em> internal data redistribution is required if a key-based operator (like an aggregation or join)
     * is applied to the result {@code KStream} (cf. {@link #flatMap(KeyValueMapper)}).
     *
     * @param mapper
     *        a {@link ValueMapper ValueMapper&lt;V, Iterable&lt;V&gt;&gt;} that computes zero or more new values
     *        for each input record
     *
     * @param <VOut> the value type of the result stream
     *
     * @return A {@code KStream} that contains more or fewer records with unmodified keys but new values (possibly of a different type).
     *
     * @see #selectKey(KeyValueMapper)
     * @see #map(KeyValueMapper)
     * @see #flatMap(KeyValueMapper)
     * @see #mapValues(ValueMapper)
     */
    <VOut> KStream<K, VOut> flatMapValues(final ValueMapper<? super V, ? extends Iterable<? extends VOut>> mapper);

    /**
     * See {@link #flatMapValues(ValueMapper)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <VOut> KStream<K, VOut> flatMapValues(final ValueMapper<? super V, ? extends Iterable<? extends VOut>> mapper,
                                          final Named named);

    /**
     * See {@link #flatMapValues(ValueMapper)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning and
     * incorrect results.
     */
    <VOut> KStream<K, VOut> flatMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VOut>> mapper);

    /**
     * See {@link #flatMapValues(ValueMapperWithKey)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <VOut> KStream<K, VOut> flatMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VOut>> mapper,
                                          final Named named);

    /**
     * Print the records of this {@code KStream} using the options provided by {@link Printed}.
     * Note that this is mainly for debugging/testing purposes, and it will try to flush on each record print.
     * It <em>SHOULD NOT</em> be used for production usage if performance requirements are concerned.
     *
     * @param printed options for printing
     */
    void print(final Printed<K, V> printed);

    /**
     * Perform an action on each record of this {@code KStream}.
     * This is a stateless record-by-record operation (cf. {@link #process(ProcessorSupplier, String...)} for
     * stateful record processing or if you need access to the record's timestamp, headers, or other metadata).
     *
     * <p>{@code Foreach} is a terminal operation that may triggers side effects (such as logging or statistics
     * collection) and returns {@code void} (cf. {@link #peek(ForeachAction)}).
     *
     * <p>Note that this operation may execute multiple times for a single record in failure cases,
     * and it is <em>not</em> guarded by "exactly-once processing guarantees".
     *
     * @param action
     *        an action to perform on each record
     */
    void foreach(final ForeachAction<? super K, ? super V> action);

    /**
     * See {@link #foreach(ForeachAction)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    void foreach(final ForeachAction<? super K, ? super V> action, final Named named);

    /**
     * Perform an action on each record of this {@code KStream}.
     * This is a stateless record-by-record operation (cf. {@link #process(ProcessorSupplier, String...)} for
     * stateful record processing or if you need access to the record's timestamp, headers, or other metadata).
     *
     * <p>{@code Peek} is a non-terminal operation that may triggers side effects (such as logging or statistics
     * collection) and returns an unchanged {@code KStream} (cf. {@link #foreach(ForeachAction)}).
     *
     * <p>Note that this operation may execute multiple times for a single record in failure cases,
     * and it is <em>not</em> guarded by "exactly-once processing guarantees".
     *
     * @param action
     *        an action to perform on each record
     *
     * @return An unmodified {@code KStream}.
     */
    KStream<K, V> peek(final ForeachAction<? super K, ? super V> action);

    /**
     * See {@link #peek(ForeachAction)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    KStream<K, V> peek(final ForeachAction<? super K, ? super V> action, final Named named);

    /**
     * Split this {@code KStream} into different branches. The returned {@link BranchedKStream} instance can be used
     * for routing the records to different branches depending on evaluation against the supplied predicates.
     * Records are evaluated against the predicates in the order they are provided with the first matching predicate
     * accepting the record. Branching is a stateless record-by-record operation.
     * See {@link BranchedKStream} for a detailed description and usage example.
     *
     * <p>Splitting a {@code KStream} guarantees that each input record is sent to at most one result {@code KStream}.
     * There is no operator for broadcasting/multicasting records into multiple result {@code KStream}.
     * If you want to broadcast records, you can apply multiple downstream operators to the same {@code KStream}
     * instance:
     * <pre>{@code
     * // Broadcasting: every record of `stream` is sent to all three operators for processing
     * KStream<...> stream1 = stream.map(...);
     * KStream<...> stream2 = stream.mapValue(...);
     * KStream<...> stream3 = stream.flatMap(...);
     * }</pre>
     *
     * Multicasting can be achieved with broadcasting into multiple filter operations:
     * <pre>{@code
     * // Multicasting: every record of `stream` is sent to all three filters, and thus, may be part of
     * // multiple result streams, `stream1`, `stream2`, and/or `stream3`
     * KStream<...> stream1 = stream.filter(predicate1);
     * KStream<...> stream2 = stream.filter(predicate2);
     * KStream<...> stream3 = stream.filter(predicate3);
     * }</pre>
     *
     * @return A {@link BranchedKStream} that provides methods for routing the records to different branches.
     *
     * @see #merge(KStream)
     */
    BranchedKStream<K, V> split();

    /**
     * See {@link #split()}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    BranchedKStream<K, V> split(final Named named);

    /**
     * Merge this {@code KStream} and the given {@code KStream}.
     *
     * <p>There is no ordering guarantee between records from this {@code KStream} and records from
     * the provided {@code KStream} in the merged stream.
     * Relative order is preserved within each input stream though (i.e., records within one input
     * stream are processed in order).
     *
     * @param otherStream
     *        a stream which is to be merged into this stream
     *
     * @return A merged stream containing all records from this and the provided {@code KStream}
     *
     * @see #split()
     */
    KStream<K, V> merge(final KStream<K, V> otherStream);

    /**
     * See {@link #merge(KStream)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    KStream<K, V> merge(final KStream<K, V> otherStream, final Named named);

    /**
     * Materialize this stream to an auto-generated repartition topic and create a new {@code KStream}
     * from the auto-generated topic.
     *
     * <p>The created topic is considered an internal topic and is meant to be used only by the current
     * Kafka Streams instance.
     * The topic will be named as "${applicationId}-&lt;name&gt;-repartition",
     * where "applicationId" is user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
     * "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
     * The number of partitions for the repartition topic is determined based on the upstream topics partition numbers.
     * Furthermore, the topic will be created with infinite retention time and data will be automatically purged
     * by Kafka Streams.
     *
     * <p>You can retrieve all generated internal topic names via {@link Topology#describe()}.
     * To explicitly set key/value serdes, specify the number of used partitions or the partitioning strategy,
     * or to customize the name of the repartition topic, use {@link #repartition(Repartitioned)}.
     *
     * @return A {@code KStream} that contains the exact same, but repartitioned records as this {@code KStream}.
     */
    KStream<K, V> repartition();

    /**
     * See {@link #repartition()}.
     */
    KStream<K, V> repartition(final Repartitioned<K, V> repartitioned);

    /**
     * Materialize this stream to a topic.
     * The topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     *
     * <p>To explicitly set key/value serdes or the partitioning strategy, use {@link #to(String, Produced)}.
     *
     * @param topic
     *        the output topic name
     *
     * @see #to(TopicNameExtractor)
     */
    void to(final String topic);

    /**
     * See {@link #to(String)}.
     */
    void to(final String topic,
            final Produced<K, V> produced);

    /**
     * Materialize the record of this stream to different topics.
     * The provided {@link TopicNameExtractor} is applied to each input record to compute the output topic name.
     * All topics should be manually created before they are used (i.e., before the Kafka Streams application is started).
     *
     * <p>To explicitly set key/value serdes or the partitioning strategy, use {@link #to(TopicNameExtractor, Produced)}.
     *
     * @param topicExtractor
     *        the extractor to determine the name of the Kafka topic to write to for each record
     *
     * @see #to(String)
     */
    void to(final TopicNameExtractor<K, V> topicExtractor);

    /**
     * See {@link #to(TopicNameExtractor)}.
     */
    void to(final TopicNameExtractor<K, V> topicExtractor,
            final Produced<K, V> produced);

    /**
     * Convert this stream to a {@link KTable}.
     * The conversion is a logical operation and only changes the "interpretation" of the records, i.e., each record of
     * this stream is a "fact/event" and is re-interpreted as a "change/update-per-key" now
     * (cf. {@link KStream} vs {@link KTable}). The resulting {@link KTable} is essentially a changelog stream.
     * To "upsert" the records of this stream into a materialized {@link KTable} (i.e., into a state store),
     * use {@link #toTable(Materialized)}.
     *
     * <p>Note that {@code null} keys are not supported by {@code KTables} and records with {@code null} key will be dropped.
     *
     * <p>If a key changing operator was used before this operation (e.g., {@link #selectKey(KeyValueMapper)},
     * {@link #map(KeyValueMapper)}, {@link #flatMap(KeyValueMapper)} or {@link #process(ProcessorSupplier, String...)})
     * Kafka Streams will automatically repartition the data, i.e., it will create an internal repartitioning topic in
     * Kafka and write and re-read the data via this topic such that the resulting {@link KTable} is correctly
     * partitioned by its key.
     *
     * <p>This internal repartitioning topic will be named "${applicationId}-&lt;name&gt;-repartition",
     * where "applicationId" is user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
     * "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
     * The number of partitions for the repartition topic is determined based on the upstream topics partition numbers.
     * Furthermore, the topic will be created with infinite retention time and data will be automatically purged
     * by Kafka Streams.
     *
     * <p>Note: If the result {@link KTable} is materialized, it is not possible to apply
     * {@link StreamsConfig#REUSE_KTABLE_SOURCE_TOPICS "source topic optimization"}, because
     * repartition topics are considered transient and don't allow to recover the result {@link KTable} in case of
     * a failure; hence, a dedicated changelog topic is required to guarantee fault-tolerance.
     *
     * <p>You can retrieve all generated internal topic names via {@link Topology#describe()}.
     * To customize the name of the repartition topic, use {@link #toTable(Named)}.
     * For more control over the repartitioning, use {@link #repartition(Repartitioned)} before {@code toTable()}.
     *
     * @return A {@link KTable} that contains the same records as this {@code KStream}.
     */
    KTable<K, V> toTable();

    /**
     * See {@link #toTable()}.
     */
    KTable<K, V> toTable(final Named named);

     /**
     * See {@link #toTable()}.
     */
    KTable<K, V> toTable(final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * See {@link #toTable()}.
     */
    KTable<K, V> toTable(final Named named,
                         final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Group the records by their current key into a {@link KGroupedStream} while preserving the original values.
     * {@link KGroupedStream} can be further grouped with other streams to form a {@link CogroupedKStream}.
     * (Co-)Grouping a stream on the record key is required before a windowing or aggregation operator can be applied
     * to the data (cf. {@link KGroupedStream}).
     * By default, the current key is used as grouping key, but a new grouping key can be set via
     * {@link #groupBy(KeyValueMapper)}.
     * In either case, if the grouping key is {@code null}, the record will be dropped.
     *
     * <p>If a key changing operator was used before this operation (e.g., {@link #selectKey(KeyValueMapper)},
     * {@link #map(KeyValueMapper)}, {@link #flatMap(KeyValueMapper)} or
     * {@link #process(ProcessorSupplier, String...)}) Kafka Streams will automatically repartition the data, i.e.,
     * it will create an internal repartitioning topic in Kafka and write and re-read the data via this topic such that
     * the resulting {@link KGroupedStream} is correctly partitioned by the grouping key, before the downstream
     * windowing/aggregation will be applied.
     *
     * <p>This internal repartition topic will be named "${applicationId}-&lt;name&gt;-repartition",
     * where "applicationId" is user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
     * "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
     * The number of partitions for the repartition topic is determined based on the upstream topics partition numbers.
     * Furthermore, the topic will be created with infinite retention time and data will be automatically purged
     * by Kafka Streams.
     *
     * <p>You can retrieve all generated internal topic names via {@link Topology#describe()}.
     * To explicitly set key/value serdes or to customize the name of the repartition topic, use {@link #groupByKey(Grouped)}.
     * For more control over the repartitioning, use {@link #repartition(Repartitioned)} before {@code groupByKey()}.
     *
     * @return A {@link KGroupedStream} that contains the grouped records of the original {@code KStream}.
     */
    KGroupedStream<K, V> groupByKey();

    /**
     * See {@link #groupByKey()}.
     *
     * <p>Takes an additional {@link Grouped} parameter, that allows to explicitly set key/value serdes or to customize
     * the name of the potentially created internal repartition topic.
     */
    KGroupedStream<K, V> groupByKey(final Grouped<K, V> grouped);

    /**
     * Group the records of this {@code KStream} on a new key (in contrast to {@link #groupByKey()}).
     * This operation is semantically equivalent to {@link #selectKey(KeyValueMapper)} followed by {@link #groupByKey()}.
     *
     * <p>Because a new key is selected, an internal repartitioning topic will be created in Kafka.
     * See {@link #groupByKey()} for more details about auto-repartitioning.
     *
     * @param keySelector
     *        a {@link KeyValueMapper} that computes a new key for grouping
     *
     * @param <KOut> the new key type of the result {@link KGroupedStream}
     */
    <KOut> KGroupedStream<KOut, V> groupBy(final KeyValueMapper<? super K, ? super V, KOut> keySelector);

    /**
     * See {@link #groupBy(KeyValueMapper)}.
     *
     * <p>Takes an additional {@link Grouped} parameter, that allows to explicitly set key/value serdes or to customize
     * the name of the created internal repartition topic.
     */
    <KOut> KGroupedStream<KOut, V> groupBy(final KeyValueMapper<? super K, ? super V, KOut> keySelector,
                                           final Grouped<KOut, V> grouped);

    /**
     * Join records of this (left) stream with another (right) {@code KStream}'s records using a windowed inner equi-join.
     * The join is computed using the records' key as join attribute, i.e., {@code leftRecord.key == rightRight.key}.
     * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
     * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
     *
     * <p>For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to
     * compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * If you need read access to the join key, use {@link #join(KStream, ValueJoinerWithKey, JoinWindows)}.
     * If an input record's key or value is {@code null} the input record will be dropped, and no join computation
     * is triggered.
     * Similarly, so-called late records, i.e., records with a timestamp belonging to an already closed window (based
     * on stream-time progress, window size, and grace period), will be dropped.
     *
     * <p>Example (assuming all input records belong to the correct windows):
     * <table border='1'>
     * <tr>
     * <th>left</th>
     * <th>right</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * </tr>
     * <tr>
     * <td>&lt;K2:B&gt;</td>
     * <td>&lt;K2:b&gt;</td>
     * <td>&lt;K2:ValueJoiner(B,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K3:c&gt;</td>
     * <td></td>
     * </tr>
     * </table>
     *
     * Both {@code KStreams} (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     * If this is not the case (and if not auto-repartitioning happens, see further below), you would need to call
     * {@link #repartition(Repartitioned)} (for at least one of the two {@code KStreams}) before doing the join and
     * specify the matching number of partitions via {@link Repartitioned} parameter to align the partition count for
     * both inputs to each other.
     * Furthermore, both {@code KStreams} need to be co-partitioned on the join key (i.e., use the same partitioner).
     * Note: Kafka Streams cannot verify the used partitioner, so it is the user's responsibility to ensure that the
     * same partitioner is used for both inputs for the join.
     *
     * <p>If a key changing operator was used before this operation on either input stream
     * (e.g., {@link #selectKey(KeyValueMapper)}, {@link #map(KeyValueMapper)}, {@link #flatMap(KeyValueMapper)} or
     * {@link #process(ProcessorSupplier, String...)}) Kafka Streams will automatically repartition the data of the
     * corresponding input stream, i.e., it will create an internal repartitioning topic in Kafka and write and re-read
     * the data via this topic such that data is correctly partitioned by the join key.
     *
     * <p>The repartitioning topic(s) will be named "${applicationId}-&lt;name&gt;-repartition",
     * where "applicationId" is user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
     * "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
     * The number of partitions for the repartition topic(s) is determined based on the partition numbers of both
     * upstream topics, and Kafka Streams will automatically align the number of partitions if required for
     * co-partitioning.
     * Furthermore, the topic(s) will be created with infinite retention time and data will be automatically purged
     * by Kafka Streams.
     *
     * <p>Both of the joined {@code KStream}s will be materialized in local state stores.
     * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
     * "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
     *
     * <p>You can retrieve all generated internal topic names via {@link Topology#describe()}.
     * To explicitly set key/value serdes, to customize the names of the repartition and changelog topic, or to
     * customize the used state store, use {@link #join(KStream, ValueJoiner, JoinWindows, StreamJoined)}.
     * For more control over the repartitioning, use {@link #repartition(Repartitioned)} on eiter input before {@code join()}.
     *
     * @param rightStream
     *        the {@code KStream} to be joined with this stream
     * @param joiner
     *        a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param windows
     *        the specification of the {@link JoinWindows}
     *
     * @param <VRight> the value type of the right stream
     * @param <VOut> the value type of the result stream
     *
     * @return A {@code KStream} that contains join-records, one for each matched record-pair, with the corresponding
     *         key and a value computed by the given {@link ValueJoiner}.
     *
     * @see #leftJoin(KStream, ValueJoiner, JoinWindows)
     * @see #outerJoin(KStream, ValueJoiner, JoinWindows)
     */
    <VRight, VOut> KStream<K, VOut> join(final KStream<K, VRight> rightStream,
                                         final ValueJoiner<? super V, ? super VRight, ? extends VOut> joiner,
                                         final JoinWindows windows);

    /**
     * See {@link #join(KStream, ValueJoiner, JoinWindows)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning and
     * incorrect results.
     */
    <VRight, VOut> KStream<K, VOut> join(final KStream<K, VRight> rightStream,
                                         final ValueJoinerWithKey<? super K, ? super V, ? super VRight, ? extends VOut> joiner,
                                         final JoinWindows windows);

    /**
     * See {@link #join(KStream, ValueJoiner, JoinWindows)}.
     */
    <VRight, VOut> KStream<K, VOut> join(final KStream<K, VRight> rightStream,
                                         final ValueJoiner<? super V, ? super VRight, ? extends VOut> joiner,
                                         final JoinWindows windows,
                                         final StreamJoined<K, V, VRight> streamJoined);

    /**
     * See {@link #join(KStream, ValueJoiner, JoinWindows)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning and
     * incorrect results.
     */
    <VRight, VOut> KStream<K, VOut> join(final KStream<K, VRight> rightStream,
                                         final ValueJoinerWithKey<? super K, ? super V, ? super VRight, ? extends VOut> joiner,
                                         final JoinWindows windows,
                                         final StreamJoined<K, V, VRight> streamJoined);

    /**
     * Join records of this (left) stream with another (right) {@code KStream}'s records using a windowed left equi-join.
     * In contrast to an {@link #join(KStream, ValueJoiner, JoinWindows) inner join}, all records from this stream will
     * produce at least one output record (more details below).
     * The join is computed using the records' key as join attribute, i.e., {@code leftRecord.key == rightRight.key}.
     * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
     * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
     *
     * <p>For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to
     * compute a value (with arbitrary type) for the result record.
     * Furthermore, for each input record of this {@code KStream} that does not have any join-partner in the right
     * stream (i.e., no record with the same key within the join interval), {@link ValueJoiner} will be called with a
     * {@code null} value for the right stream.
     *
     * <p>Note: By default, non-joining records from this stream are buffered until their join window closes, and
     * corresponding left-join results for these records are emitted with some delay.
     * If you want to get left-join results without any delay, you can use {@link JoinWindows#of(Duration)
     * JoinWindows#of(Duration) [deprecated]} instead.
     * However, such an "eager" left-join result could be a spurious result, because the same record may find actual
     * join partners later, producing additional inner-join results.
     *
     * <p>The key of the result record is the same as for both joining input records,
     * or the left input record's key for a left-join result.
     * If you need read access to the join key, use {@link #leftJoin(KStream, ValueJoinerWithKey, JoinWindows)}.
     * If a <em>left</em> input record's value is {@code null} the input record will be dropped, and no join computation
     * is triggered.
     * Note, that for <em>left</em> input records, {@code null} keys are supported (in contrast to
     * {@link #join(KStream, ValueJoiner, JoinWindows) inner join}), resulting in a left join result.
     * If a <em>right</em> input record's key or value is {@code null} the input record will be dropped, and no join
     * computation is triggered.
     * For input record of either side, so-called late records, i.e., records with a timestamp belonging to an already
     * closed window (based on stream-time progress, window size, and grace period), will be dropped.
     *
     * <p>Example (assuming all input records belong to the correct windows, not taking actual emit/window-close time
     * for left-join results, or eager/spurious results into account):
     * <table border='1'>
     * <tr>
     * <th>left</th>
     * <th>right</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K2:B&gt;</td>
     * <td>&lt;K2:b&gt;</td>
     * <td>&lt;K2:ValueJoiner(B,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K3:c&gt;</td>
     * <td></td>
     * </tr>
     * </table>
     *
     * For more details, about co-partitioning requirements, (auto-)repartitioning, and more see
     * {@link #join(KStream, ValueJoiner, JoinWindows)}.
     *
     * @return A {@code KStream} that contains join-records, one for each matched record-pair plus one for each
     *         non-matching record of this {@code KStream}, with the corresponding key and a value computed by the
     *         given {@link ValueJoiner}.
     *
     * @see #join(KStream, ValueJoiner, JoinWindows)
     * @see #outerJoin(KStream, ValueJoiner, JoinWindows)
     */
    <VRight, VOut> KStream<K, VOut> leftJoin(final KStream<K, VRight> rightStream,
                                             final ValueJoiner<? super V, ? super VRight, ? extends VOut> joiner,
                                             final JoinWindows windows);

    /**
     * See {@link #leftJoin(KStream, ValueJoiner, JoinWindows)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning and
     * incorrect results.
     */
    <VRight, VOut> KStream<K, VOut> leftJoin(final KStream<K, VRight> rightStream,
                                             final ValueJoinerWithKey<? super K, ? super V, ? super VRight, ? extends VOut> joiner,
                                             final JoinWindows windows);

    /**
     * See {@link #leftJoin(KStream, ValueJoiner, JoinWindows)}.
     */
    <VRight, VOut> KStream<K, VOut> leftJoin(final KStream<K, VRight> rightStream,
                                             final ValueJoiner<? super V, ? super VRight, ? extends VOut> joiner,
                                             final JoinWindows windows,
                                             final StreamJoined<K, V, VRight> streamJoined);

    /**
     * See {@link #leftJoin(KStream, ValueJoiner, JoinWindows)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning and
     * incorrect results.
     */
    <VRight, VOut> KStream<K, VOut> leftJoin(final KStream<K, VRight> rightStream,
                                             final ValueJoinerWithKey<? super K, ? super V, ? super VRight, ? extends VOut> joiner,
                                             final JoinWindows windows,
                                             final StreamJoined<K, V, VRight> streamJoined);

    /**
     * Join records of this (left) stream with another (right) {@code KStream}'s records using a windowed outer equi-join.
     * In contrast to an {@link #join(KStream, ValueJoiner, JoinWindows) inner join} or
     * {@link #leftJoin(KStream, ValueJoiner, JoinWindows) left join}, all records from both stream will produce at
     * least one output record (more details below).
     * The join is computed using the records' key as join attribute, i.e., {@code leftRecord.key == rightRight.key}.
     * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
     * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
     *
     * <p>For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to
     * compute a value (with arbitrary type) for the result record.
     * Furthermore, for each input record of either {@code KStream} that does not have any join-partner in the other
     * stream (i.e., no record with the same key within the join interval), {@link ValueJoiner} will be called with a
     * {@code null} value for right/left stream, respectively.
     *
     * <p>Note: By default, non-joining records from either stream are buffered until their join window closes, and
     * corresponding outer-join results for these records are emitted with some delay.
     * If you want to get outer-join results without any delay, you can use {@link JoinWindows#of(Duration)
     * JoinWindows#of(Duration) [deprecated]} instead.
     * However, such an "eager" outer-join result could be a spurious result, because the same record may find actual
     * join partners later, producing additional inner-join results.
     *
     * <p>The key of the result record is the same as for both joining input records,
     * or the left/right input record's key for an outer-join result, respectively.
     * If you need read access to the join key, use {@link #outerJoin(KStream, ValueJoinerWithKey, JoinWindows)}.
     * If an input record's value is {@code null} the input record will be dropped, and no join computation is triggered.
     * Note, that input records with {@code null} keys are supported (in contrast to
     * {@link #join(KStream, ValueJoiner, JoinWindows) inner join}), resulting in left/right join results.
     * For input record of either side, so-called late records, i.e., records with a timestamp belonging to an already
     * closed window (based on stream-time progress, window size, and grace period), will be dropped.
     *
     * <p>Example (assuming all input records belong to the correct windows, not taking actual emit/window-close time
     * for outer-join result, or eager/spurious results into account):
     * <table border='1'>
     * <tr>
     * <th>left</th>
     * <th>right</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K2:B&gt;</td>
     * <td>&lt;K2:b&gt;</td>
     * <td>&lt;K2:ValueJoiner(B,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K3:c&gt;</td>
     * <td>&lt;K3:ValueJoiner(null,c)&gt;</td>
     * </tr>
     * </table>
     *
     * For more details, about co-partitioning requirements, (auto-)repartitioning, and more see
     * {@link #join(KStream, ValueJoiner, JoinWindows)}.
     *
     * @return A {@code KStream} that contains join-records, one for each matched record-pair plus one for each
     *         non-matching record of either input {@code KStream}, with the corresponding key and a value computed
     *         by the given {@link ValueJoiner}.
     *
     * @see #join(KStream, ValueJoiner, JoinWindows)
     * @see #leftJoin(KStream, ValueJoiner, JoinWindows)
     */
    <VRight, VOut> KStream<K, VOut> outerJoin(final KStream<K, VRight> otherStream,
                                              final ValueJoiner<? super V, ? super VRight, ? extends VOut> joiner,
                                              final JoinWindows windows);

    /**
     * See {@link #outerJoin(KStream, ValueJoiner, JoinWindows)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning.
     */
    <VRight, VOut> KStream<K, VOut> outerJoin(final KStream<K, VRight> otherStream,
                                              final ValueJoinerWithKey<? super K, ? super V, ? super VRight, ? extends VOut> joiner,
                                              final JoinWindows windows);

    /**
     * See {@link #outerJoin(KStream, ValueJoiner, JoinWindows)}.
     */

    <VRight, VOut> KStream<K, VOut> outerJoin(final KStream<K, VRight> otherStream,
                                              final ValueJoiner<? super V, ? super VRight, ? extends VOut> joiner,
                                              final JoinWindows windows,
                                              final StreamJoined<K, V, VRight> streamJoined);

    /**
     * See {@link #outerJoin(KStream, ValueJoiner, JoinWindows)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning.
     */
    <VRight, VOut> KStream<K, VOut> outerJoin(final KStream<K, VRight> otherStream,
                                              final ValueJoinerWithKey<? super K, ? super V, ? super VRight, ? extends VOut> joiner,
                                              final JoinWindows windows,
                                              final StreamJoined<K, V, VRight> streamJoined);

    /**
     * Join records of this stream with {@link KTable}'s records using non-windowed inner equi-join.
     * The join is a primary key table lookup join with join attribute {@code streamRecord.key == tableRecord.key}.
     * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
     * This is done by performing a lookup for matching records into the internal {@link KTable} state.
     * In contrast, processing {@link KTable} input records will only update the internal {@link KTable} state and
     * will not produce any result records.
     *
     * <p>For each {@code KStream} record that finds a joining record in the {@link KTable} the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * If you need read access to the join key, use {@link #join(KTable, ValueJoinerWithKey)}.
     * If a {@code KStream} input record's key or value is {@code null} the input record will be dropped, and no join
     * computation is triggered.
     * If a {@link KTable} input record's key is {@code null} the input record will be dropped, and the table state
     * won't be updated.
     * {@link KTable} input records with {@code null} values are considered deletes (so-called tombstone) for the table.
     *
     * <p>Example:
     * <table border='1'>
     * <tr>
     * <th>KStream</th>
     * <th>KTable</th>
     * <th>state</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td></td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td></td>
     * </tr>
     * <tr>
     * <td>&lt;K1:C&gt;</td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
     * </tr>
     * </table>
     *
     * By default, {@code KStream} records are processed by performing a lookup for matching records in the
     * <em>current</em> (i.e., processing time) internal {@link KTable} state.
     * This default implementation does not handle out-of-order records in either input of the join well.
     * See {@link #join(KTable, ValueJoiner, Joined)} on how to configure a stream-table join to handle out-of-order
     * data.
     *
     * <p>{@code KStream} and {@link KTable} (or to be more precise, their underlying source topics) need to have the
     * same number of partitions (cf. {@link #join(GlobalKTable, KeyValueMapper, ValueJoiner)}).
     * If this is not the case (and if no auto-repartitioning happens for the {@code KStream}, see further below),
     * you would need to call {@link #repartition(Repartitioned)} for this {@code KStream} before doing the join,
     * specifying the same number of partitions via {@link Repartitioned} parameter as the given {@link KTable}.
     * Furthermore, {@code KStream} and {@link KTable} need to be co-partitioned on the join key
     * (i.e., use the same partitioner).
     * Note: Kafka Streams cannot verify the used partitioner, so it is the user's responsibility to ensure
     * that the same partitioner is used for both inputs of the join.
     *
     * <p>If a key changing operator was used on this {@code KStream} before this operation
     * (e.g., {@link #selectKey(KeyValueMapper)}, {@link #map(KeyValueMapper)}, {@link #flatMap(KeyValueMapper)} or
     * {@link #process(ProcessorSupplier, String...)}) Kafka Streams will automatically repartition the data of this
     * {@code KStream}, i.e., it will create an internal repartitioning topic in Kafka and write and re-read
     * the data via this topic such that data is correctly partitioned by the {@link KTable}'s key.
     *
     * <p>The repartitioning topic will be named "${applicationId}-&lt;name&gt;-repartition",
     * where "applicationId" is user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
     * "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
     * The number of partitions for the repartition topic is determined based on number of partitions of the
     * {@link KTable}.
     * Furthermore, the topic(s) will be created with infinite retention time and data will be automatically purged
     * by Kafka Streams.
     *
     * <p>You can retrieve all generated internal topic names via {@link Topology#describe()}.
     * To explicitly set key/value serdes or to customize the names of the repartition topic,
     * use {@link #join(KTable, ValueJoiner, Joined)}.
     * For more control over the repartitioning, use {@link #repartition(Repartitioned)} before {@code join()}.
     *
     * @param table
     *        the {@link KTable} to be joined with this stream
     * @param joiner
     *        a {@link ValueJoiner} that computes the join result for a pair of matching records
     *
     * @param <TableValue> the value type of the table
     * @param <VOut> the value type of the result stream
     *
     * @return A {@code KStream} that contains join-records, one for each matched stream record, with the corresponding
     *         key and a value computed by the given {@link ValueJoiner}.
     *
     * @see #leftJoin(KTable, ValueJoiner)
     */
    <TableValue, VOut> KStream<K, VOut> join(final KTable<K, TableValue> table,
                                             final ValueJoiner<? super V, ? super TableValue, ? extends VOut> joiner);

    /**
     * See {@link #join(KTable, ValueJoiner)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning and
     * incorrect results.
     */
    <TableValue, VOut> KStream<K, VOut> join(final KTable<K, TableValue> table,
                                             final ValueJoinerWithKey<? super K, ? super V, ? super TableValue, ? extends VOut> joiner);

    /**
     * Join records of this stream with {@link KTable}'s records using non-windowed inner equi-join.
     * In contrast to {@link #join(KTable, ValueJoiner)}, but only if the used {@link KTable} is backed by a
     * {@link org.apache.kafka.streams.state.VersionedKeyValueStore VersionedKeyValueStore}, the additional
     * {@link Joined} parameter allows to specify a join grace-period, to handle out-of-order data gracefully.
     *
     * <p>For details about stream-table semantics, including co-partitioning requirements, (auto-)repartitioning,
     * and more see {@link #join(KTable, ValueJoiner)}.
     * If you specify a grace-period to handle out-of-order data, see further details below.
     *
     * <p>To handle out-of-order records, the input {@link KTable} must use a
     * {@link org.apache.kafka.streams.state.VersionedKeyValueStore VersionedKeyValueStore} (specified via a
     * {@link Materialized} parameter when the {@link KTable} is created), and a join
     * {@link Joined#withGracePeriod(Duration) grace-period} must be specified.
     * For this case, {@code KStream} records are buffered until the end of the grace period and the {@link KTable}
     * lookup is performed with some delay.
     * Given that the {@link KTable} state is versioned, the lookup can use "event time", allowing out-of-order
     * {@code KStream} records, to join to the right (older) version of a {@link KTable} record with the same key.
     * Also, {@link KTable} out-of-order updates are handled correctly by the versioned state store.
     * Note, that using a join grace-period introduces the notion of late records, i.e., records with a timestamp
     * smaller than the defined grace-period allows; these late records will be dropped, and not join computation
     * is triggered.
     * Using a versioned state store for the {@link KTable} also implies that the defined
     * {@link VersionedBytesStoreSupplier#historyRetentionMs() history retention} provides
     * a cut-off point, and late records will be dropped, not updating the {@link KTable} state.
     *
     * <p>If a join grace-period is specified, the {@code KStream} will be materialized in a local state store.
     * For failure and recovery this store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog",
     * where "applicationId" is user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
     * "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
     *
     * <p>You can retrieve all generated internal topic names via {@link Topology#describe()}.
     * To customize the name of the changelog topic, use {@link Joined} input parameter.
     */
    <TableValue, VOut> KStream<K, VOut> join(final KTable<K, TableValue> table,
                                             final ValueJoiner<? super V, ? super TableValue, ? extends VOut> joiner,
                                             final Joined<K, V, TableValue> joined);

    /**
     * See {@link #join(KTable, ValueJoiner, Joined)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning and
     * incorrect results.
     */
    <TableValue, VOut> KStream<K, VOut> join(final KTable<K, TableValue> table,
                                             final ValueJoinerWithKey<? super K, ? super V, ? super TableValue, ? extends VOut> joiner,
                                             final Joined<K, V, TableValue> joined);

    /**
     * Join records of this stream with {@link KTable}'s records using non-windowed left equi-join.
     * In contrast to an {@link #join(KTable, ValueJoiner) inner join}, all records from this stream will produce an
     * output record (more details below).
     * The join is a primary key table lookup join with join attribute {@code streamRecord.key == tableRecord.key}.
     * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
     * This is done by performing a lookup for matching records into the internal {@link KTable} state.
     * In contrast, processing {@link KTable} input records will only update the internal {@link KTable} state and
     * will not produce any result records.
     *
     * <p>For each {@code KStream} record, regardless if it finds a joining record in the {@link KTable}, the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * If no {@link KTable} record with matching key was found during the lookup, {@link ValueJoiner} will be called
     * with a {@code null} value for the table record.
     * The key of the result record is the same as for both joining input records,
     * or the {@code KStreams} input record's key for a left-join result.
     * If you need read access to the join key, use {@link #leftJoin(KTable, ValueJoinerWithKey)}.
     * If a {@code KStream} input record's value is {@code null} the input record will be dropped, and no join
     * computation is triggered.
     * Note, that {@code null} keys for {@code KStream} input records are supported (in contrast to
     * {@link #join(KTable, ValueJoiner) inner join}) resulting in a left join result.
     * If a {@link KTable} input record's key is {@code null} the input record will be dropped, and the table state
     * won't be updated.
     * {@link KTable} input records with {@code null} values are considered deletes (so-called tombstone) for the table.
     *
     * <p>Example:
     * <table border='1'>
     * <tr>
     * <th>KStream</th>
     * <th>KTable</th>
     * <th>state</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td></td>
     * </tr>
     * <tr>
     * <td>&lt;K1:C&gt;</td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
     * </tr>
     * </table>
     *
     * By default, {@code KStream} records are processed by performing a lookup for matching records in the
     * <em>current</em> (i.e., processing time) internal {@link KTable} state.
     * This default implementation does not handle out-of-order records in either input of the join well.
     * See {@link #leftJoin(KTable, ValueJoiner, Joined)} on how to configure a stream-table join to handle out-of-order
     * data.
     *
     * <p>For more details, about co-partitioning requirements, (auto-)repartitioning, and more see
     * {@link #join(KStream, ValueJoiner, JoinWindows)}.
     *
     * @return A {@code KStream} that contains join-records, one for each matched stream record plus one for each
     *         non-matching stream record, with the corresponding key and a value computed by the given {@link ValueJoiner}.
     *
     * @see #join(KTable, ValueJoiner)
     */
    <VTable, VOut> KStream<K, VOut> leftJoin(final KTable<K, VTable> table,
                                             final ValueJoiner<? super V, ? super VTable, ? extends VOut> joiner);

    /**
     * See {@link #leftJoin(KTable, ValueJoiner)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning and
     * incorrect results.
     */
    <VTable, VOut> KStream<K, VOut> leftJoin(final KTable<K, VTable> table,
                                             final ValueJoinerWithKey<? super K, ? super V, ? super VTable, ? extends VOut> joiner);

    /**
     * Join records of this stream with {@link KTable}'s records using non-windowed left equi-join.
     * In contrast to {@link #leftJoin(KTable, ValueJoiner)}, but only if the used {@link KTable} is backed by a
     * {@link org.apache.kafka.streams.state.VersionedKeyValueStore VersionedKeyValueStore}, the additional
     * {@link Joined} parameter allows to specify a join grace-period, to handle out-of-order data gracefully.
     *
     * <p>For details about left-stream-table-join semantics see {@link #leftJoin(KTable, ValueJoiner)}.
     * For co-partitioning requirements, (auto-)repartitioning, and more see {@link #join(KTable, ValueJoiner)}.
     * If you specify a grace-period to handle out-of-order data, see {@link #join(KTable, ValueJoiner, Joined)}.
     */
    <VTable, VOut> KStream<K, VOut> leftJoin(final KTable<K, VTable> table,
                                             final ValueJoiner<? super V, ? super VTable, ? extends VOut> joiner,
                                             final Joined<K, V, VTable> joined);

    /**
     * See {@link #leftJoin(KTable, ValueJoiner, Joined)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning and
     * incorrect results.
     */
    <VTable, VOut> KStream<K, VOut> leftJoin(final KTable<K, VTable> table,
                                             final ValueJoinerWithKey<? super K, ? super V, ? super VTable, ? extends VOut> joiner,
                                             final Joined<K, V, VTable> joined);

    /**
     * Join records of this stream with {@link GlobalKTable}'s records using non-windowed inner equi-join.
     * The join is a primary key table lookup join with join attribute
     * {@code keyValueMapper.map(streamRecord) == tableRecord.key}.
     * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
     * This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time)
     * internal {@link GlobalKTable} state.
     * In contrast, processing {@link GlobalKTable} input records will only update the internal {@link GlobalKTable}
     * state and will not produce any result records.
     *
     * <p>For each {@code KStream} record that finds a joining record in the {@link GlobalKTable} the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as the stream record's key.
     * If you need read access to the {@code KStream} key, use {@link #join(GlobalKTable, KeyValueMapper, ValueJoinerWithKey)}.
     * If a {@code KStream} input record's value is {@code null} or if the provided {@link KeyValueMapper keySelector}
     * returns {@code null}, the input record will be dropped, and no join computation is triggered.
     * If a {@link GlobalKTable} input record's key is {@code null} the input record will be dropped, and the table
     * state won't be updated.
     * {@link GlobalKTable} input records with {@code null} values are considered deletes (so-called tombstone) for
     * the table.
     *
     * <p>Example, using the first value attribute as join key:
     * <table border='1'>
     * <tr>
     * <th>KStream</th>
     * <th>GlobalKTable</th>
     * <th>state</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:(GK1,A)&gt;</td>
     * <td></td>
     * <td></td>
     * <td></td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;GK1:b&gt;</td>
     * <td>&lt;GK1:b&gt;</td>
     * <td></td>
     * </tr>
     * <tr>
     * <td>&lt;K1:(GK1,C)&gt;</td>
     * <td></td>
     * <td>&lt;GK1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner((GK1,C),b)&gt;</td>
     * </tr>
     * </table>
     *
     * In contrast to {@link #join(KTable, ValueJoiner)}, there is no co-partitioning requirement between this
     * {@code KStream} and the {@link GlobalKTable}.
     * Also note that there are no ordering guarantees between the updates on the left and the right side of this join,
     * since updates to the {@link GlobalKTable} are in no way synchronized.
     * Therefore, the result of the join is inherently non-deterministic.
     *
     * @param globalTable
     *        the {@link GlobalKTable} to be joined with this stream
     * @param keySelector
     *        a {@link KeyValueMapper} that computes the join key for stream input records
     * @param joiner
     *        a {@link ValueJoiner} that computes the join result for a pair of matching records
     *
     * @param <GlobalKey> the key type of the global table
     * @param <GlobalValue> the value type of the global table
     * @param <VOut> the value type of the result stream
     *
     * @return A {@code KStream} that contains join-records, one for each matched stream record, with the corresponding
     *         key and a value computed by the given {@link ValueJoiner}.
     *
     * @see #leftJoin(GlobalKTable, KeyValueMapper, ValueJoiner)
     */
    <GlobalKey, GlobalValue, VOut> KStream<K, VOut> join(final GlobalKTable<GlobalKey, GlobalValue> globalTable,
                                                         final KeyValueMapper<? super K, ? super V, ? extends GlobalKey> keySelector,
                                                         final ValueJoiner<? super V, ? super GlobalValue, ? extends VOut> joiner);

    /**
     * See {@link #join(GlobalKTable, KeyValueMapper, ValueJoiner)}.
     *
     * <p>Note that the {@link KStream} key is read-only and must not be modified, as this can lead to corrupt
     * partitioning and incorrect results.
     */
    <GlobalKey, GlobalValue, VOut> KStream<K, VOut> join(final GlobalKTable<GlobalKey, GlobalValue> globalTable,
                                                         final KeyValueMapper<? super K, ? super V, ? extends GlobalKey> keySelector,
                                                         final ValueJoinerWithKey<? super K, ? super V, ? super GlobalValue, ? extends VOut> joiner);

    /**
     * See {@link #join(GlobalKTable, KeyValueMapper, ValueJoiner)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <GlobalKey, GlobalValue, VOut> KStream<K, VOut> join(final GlobalKTable<GlobalKey, GlobalValue> globalTable,
                                                         final KeyValueMapper<? super K, ? super V, ? extends GlobalKey> keySelector,
                                                         final ValueJoiner<? super V, ? super GlobalValue, ? extends VOut> joiner,
                                                         final Named named);

    /**
     * See {@link #join(GlobalKTable, KeyValueMapper, ValueJoinerWithKey)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <GlobalKey, GlobalValue, VOut> KStream<K, VOut> join(final GlobalKTable<GlobalKey, GlobalValue> globalTable,
                                                         final KeyValueMapper<? super K, ? super V, ? extends GlobalKey> keySelector,
                                                         final ValueJoinerWithKey<? super K, ? super V, ? super GlobalValue, ? extends VOut> joiner,
                                                         final Named named);

    /**
     * Join records of this stream with {@link GlobalKTable}'s records using non-windowed left equi-join.
     * In contrast to an {@link #join(GlobalKTable, KeyValueMapper, ValueJoiner) inner join}, all records from this
     * stream will produce an output record (more details below).
     * The join is a primary key table lookup join with join attribute
     * {@code keyValueMapper.map(streamRecord) == tableRecord.key}.
     * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
     * This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time)
     * internal {@link GlobalKTable} state.
     * In contrast, processing {@link GlobalKTable} input records will only update the internal {@link GlobalKTable}
     * state and will not produce any result records.
     *
     * <p>For each {@code KStream} record, regardless if it finds a joining record in the {@link GlobalKTable}, the
     * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * If no {@link GlobalKTable} record with matching key was found during the lookup, {@link ValueJoiner} will be
     * called with a {@code null} value for the global table record.
     * The key of the result record is the same as for both joining input records,
     * or the {@code KStreams} input record's key for a left-join result.
     * If you need read access to the {@code KStream} key, use
     * {@link #leftJoin(GlobalKTable, KeyValueMapper, ValueJoinerWithKey)}.
     * If a {@code KStream} input record's value is {@code null} or if the provided {@link KeyValueMapper keySelector}
     * returns {@code null}, the input record will be dropped, and no join computation is triggered.
     * Note, that {@code null} keys for {@code KStream} input records are supported (in contrast to
     * {@link #join(GlobalKTable, KeyValueMapper, ValueJoiner) inner join}) resulting in a left join result.
     * If a {@link GlobalKTable} input record's key is {@code null} the input record will be dropped, and the table
     * state won't be updated.
     * {@link GlobalKTable} input records with {@code null} values are considered deletes (so-called tombstone) for
     * the table.
     *
     * <p>Example, using the first value attribute as join key:
     * <table border='1'>
     * <tr>
     * <th>KStream</th>
     * <th>GlobalKTable</th>
     * <th>state</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:(GK1,A)&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner((GK1,A),null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;GK1:b&gt;</td>
     * <td>&lt;GK1:b&gt;</td>
     * <td></td>
     * </tr>
     * <tr>
     * <td>&lt;K1:(GK1,C)&gt;</td>
     * <td></td>
     * <td>&lt;GK1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner((GK1,C),b)&gt;</td>
     * </tr>
     * </table>
     *
     * In contrast to {@link #leftJoin(KTable, ValueJoiner)}, there is no co-partitioning requirement between this
     * {@code KStream} and the {@link GlobalKTable}.
     * Also note that there are no ordering guarantees between the updates on the left and the right side of this join,
     * since updates to the {@link GlobalKTable} are in no way synchronized.
     * Therefore, the result of the join is inherently non-deterministic.
     *
     * @param globalTable
     *        the {@link GlobalKTable} to be joined with this stream
     * @param keySelector
     *        a {@link KeyValueMapper} that computes the join key for stream input records
     * @param joiner
     *        a {@link ValueJoiner} that computes the join result for a pair of matching records
     *
     * @param <GlobalKey> the key type of the global table
     * @param <GlobalValue> the value type of the global table
     * @param <VOut> the value type of the result stream
     *
     * @return A {@code KStream} that contains join-records, one for each matched stream record plus one for each
     *         non-matching stream record, with the corresponding key and a value computed by the given {@link ValueJoiner}.
     *
     * @see #join(GlobalKTable, KeyValueMapper, ValueJoiner)
     */
    <GlobalKey, GlobalValue, VOut> KStream<K, VOut> leftJoin(final GlobalKTable<GlobalKey, GlobalValue> globalTable,
                                                             final KeyValueMapper<? super K, ? super V, ? extends GlobalKey> keySelector,
                                                             final ValueJoiner<? super V, ? super GlobalValue, ? extends VOut> joiner);

    /**
     * See {@link #leftJoin(GlobalKTable, KeyValueMapper, ValueJoiner)}.
     *
     * <p>Note that the key is read-only and must not be modified, as this can lead to corrupt partitioning and
     * incorrect results.
     */
    <GlobalKey, GlobalValue, VOut> KStream<K, VOut> leftJoin(final GlobalKTable<GlobalKey, GlobalValue> globalTable,
                                                             final KeyValueMapper<? super K, ? super V, ? extends GlobalKey> keySelector,
                                                             final ValueJoinerWithKey<? super K, ? super V, ? super GlobalValue, ? extends VOut> joiner);

    /**
     * See {@link #leftJoin(GlobalKTable, KeyValueMapper, ValueJoiner)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <GlobalKey, GlobalValue, VOut> KStream<K, VOut> leftJoin(final GlobalKTable<GlobalKey, GlobalValue> globalTable,
                                                             final KeyValueMapper<? super K, ? super V, ? extends GlobalKey> keySelector,
                                                             final ValueJoiner<? super V, ? super GlobalValue, ? extends VOut> joiner,
                                                             final Named named);

    /**
     * See {@link #leftJoin(GlobalKTable, KeyValueMapper, ValueJoinerWithKey)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <GlobalKey, GlobalValue, VOut> KStream<K, VOut> leftJoin(final GlobalKTable<GlobalKey, GlobalValue> globalTable,
                                                             final KeyValueMapper<? super K, ? super V, ? extends GlobalKey> keySelector,
                                                             final ValueJoinerWithKey<? super K, ? super V, ? super GlobalValue, ? extends VOut> joiner,
                                                             final Named named);

    /**
     * Process all records in this stream, one record at a time, by applying a {@link Processor} (provided by the given
     * {@link ProcessorSupplier}) to each input record.
     * The {@link Processor} can emit any number of result records via {@link ProcessorContext#forward(Record)}
     * (possibly of a different key and/or value type).
     *
     * <p>By default, the processor is stateless (similar to {@link #flatMap(KeyValueMapper, Named)}, however, it also
     * has access to the {@link Record record's} timestamp and headers), but previously added
     * {@link StateStore state stores} can be connected by providing their names as additional parameters, making
     * the processor stateful.
     * There is two different {@link StateStore state stores}, which can be added to the underlying {@link Topology}:
     * <ul>
     *   <li>{@link StreamsBuilder#addStateStore(StoreBuilder) state stores} for processing (i.e., read/write access)</li>
     *   <li>{@link StreamsBuilder#addGlobalStore(StoreBuilder, String, Consumed, ProcessorSupplier) read-only state stores}</li>
     * </ul>
     *
     * If the {@code processorSupplier} provides state stores via {@link ConnectedStoreProvider#stores()}, the
     * corresponding {@link StoreBuilder StoreBuilders} will be added to the topology and connected to this processor
     * automatically, without the need to provide the store names as parameter to this method.
     * Additionally, even if a processor is stateless, it can still access all
     * {@link StreamsBuilder#addGlobalStore global state stores} (read-only).
     * There is no need to connect global stores to processors.
     *
     * <p>All state stores which are connected to a processor and all global stores, can be accessed via
     * {@link ProcessorContext#getStateStore(String) context.getStateStore(String)}
     * using the context provided via
     * {@link Processor#init(ProcessorContext) Processor#init()}:
     *
     * <pre>{@code
     * public class MyProcessor implements Processor<String, Integer, String, Integer> {
     *     private ProcessorContext<String, Integer> context;
     *     private KeyValueStore<String, String> store;
     *
     *     @Override
     *     void init(final ProcessorContext<String, Integer> context) {
     *         this.context = context;
     *         this.store = context.getStateStore("myStore");
     *     }
     *
     *     @Override
     *     void process(final Record<String, Integer> record) {
     *         // can access this.context and this.store
     *     }
     * }
     * }</pre>
     *
     * Furthermore, the provided {@link ProcessorContext} gives access to topology, runtime, and
     * {@link RecordMetadata record metadata}, and allows to schedule {@link Punctuator punctuations} and to
     * <em>request</em> offset commits.
     *
     * <p>In contrast to grouping/aggregation and joins, even if the processor is stateful and an upstream operation
     * was key changing, no auto-repartition is triggered.
     * If repartitioning is required, a call to {@link #repartition()} should be performed before {@code process()}.
     * At the same time, this method is considered a key changing operation by itself, and might result in an internal
     * data redistribution if a key-based operator (like an aggregation or join) is applied to the result
     * {@code KStream} (cf. {@link #processValues(FixedKeyProcessorSupplier, String...)}).
     *
     * @param processorSupplier
     *        the supplier used to obtain {@link Processor} instances
     * @param stateStoreNames
     *        the names of state stores that the processor should be able to access
     */
    <KOut, VOut> KStream<KOut, VOut> process(
        final ProcessorSupplier<? super K, ? super V, ? extends KOut, ? extends VOut> processorSupplier,
        final String... stateStoreNames
    );

    /**
     * See {@link #process(ProcessorSupplier, String...)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <KOut, VOut> KStream<KOut, VOut> process(
        final ProcessorSupplier<? super K, ? super V, ? extends KOut, ? extends VOut> processorSupplier,
        final Named named,
        final String... stateStoreNames
    );

    /**
     * Process all records in this stream, one record at a time, by applying a {@link FixedKeyProcessor} (provided by
     * the given {@link FixedKeyProcessorSupplier}) to each input record.
     * This method is similar to {@link #process(ProcessorSupplier, String...)}, however the key of the input
     * {@link Record} cannot be modified.
     *
     * <p>Because the key cannot be modified, this method is <em>not</em> a key changing operation and preserves data
     * co-location with respect to the key (cf. {@link #flatMapValues(ValueMapper)}).
     * Thus, <em>no</em> internal data redistribution is required if a key-based operator (like an aggregation or join)
     * is applied to the result {@code KStream}.
     *
     * <p>However, because the key cannot be modified, some restrictions apply to a {@link FixedKeyProcessor} compared
     * to a {@link Processor}: for example, forwarding result records from a {@link Punctuator} is not possible.
     */
    <VOut> KStream<K, VOut> processValues(
        final FixedKeyProcessorSupplier<? super K, ? super V, ? extends VOut> processorSupplier,
        final String... stateStoreNames
    );

    /**
     * See {@link #processValues(FixedKeyProcessorSupplier, String...)}.
     *
     * <p>Takes an additional {@link Named} parameter that is used to name the processor in the topology.
     */
    <VOut> KStream<K, VOut> processValues(
        final FixedKeyProcessorSupplier<? super K, ? super V, ? extends VOut> processorSupplier,
        final Named named,
        final String... stateStoreNames
    );
}