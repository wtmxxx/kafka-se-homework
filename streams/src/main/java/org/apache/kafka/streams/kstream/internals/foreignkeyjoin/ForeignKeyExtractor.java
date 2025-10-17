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
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An interface for extracting foreign keys from input records during foreign key joins in Kafka Streams.
 * This extractor is used to determine the key of the foreign table to join with based on the primary
 * table's record key and value.
 * <p>
 * The interface provides two factory methods:
 * <ul>
 *   <li>{@link #fromFunction(Function)} - when the foreign key depends only on the value</li>
 *   <li>{@link #fromBiFunction(BiFunction)} - when the foreign key depends on both key and value</li>
 * </ul>
 *
 * @param <KLeft>  Type of primary table's key
 * @param <VLeft>  Type of primary table's value
 * @param <KRight> Type of the foreign key to extract
 */
@FunctionalInterface
public interface ForeignKeyExtractor<KLeft, VLeft, KRight> {
    KRight extract(KLeft key, VLeft value);

    static <KLeft, VLeft, KRight> ForeignKeyExtractor<? super KLeft, ? super VLeft, ? extends KRight> fromFunction(Function<? super VLeft, ? extends KRight> function) {
        return (key, value) -> function.apply(value);
    }

    static <KLeft, VLeft, KRight> ForeignKeyExtractor<? super KLeft, ? super VLeft, ? extends KRight> fromBiFunction(BiFunction<? super KLeft, ? super VLeft, ? extends KRight> biFunction) {
        return biFunction::apply;
    }
}
