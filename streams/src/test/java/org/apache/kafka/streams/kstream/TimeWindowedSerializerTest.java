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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimeWindowedSerializerTest {
    private final TimeWindowedSerializer<?> timeWindowedSerializer = new TimeWindowedSerializer<>(Serdes.String().serializer());
    private final Map<String, String> props = new HashMap<>();

    @Test
    public void testTimeWindowedSerializerConstructor() {
        timeWindowedSerializer.configure(props, true);
        final Serializer<?> inner = timeWindowedSerializer.innerSerializer();
        assertNotNull(inner, "Inner serializer should be not null");
        assertInstanceOf(StringSerializer.class, inner, "Inner serializer type should be StringSerializer");
    }

    @Deprecated
    @Test
    public void shouldSetSerializerThroughWindowedInnerClassSerdeConfig() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.ByteArraySerde.class.getName());
        try (final TimeWindowedSerializer<?> serializer = new TimeWindowedSerializer<>()) {
            serializer.configure(props, false);
            assertInstanceOf(ByteArraySerializer.class, serializer.innerSerializer());
        }
    }

    @Test
    public void shouldSetSerializerThroughWindowedInnerSerializerClassConfig() {
        props.put(TimeWindowedSerializer.WINDOWED_INNER_SERIALIZER_CLASS, Serdes.ByteArraySerde.class.getName());
        try (final TimeWindowedSerializer<?> serializer = new TimeWindowedSerializer<>()) {
            serializer.configure(props, false);
            assertInstanceOf(ByteArraySerializer.class, serializer.innerSerializer());
        }
    }

    @Deprecated
    @Test
    public void shouldIgnoreWindowedInnerClassSerdeConfigIfWindowedInnerSerializerClassConfigIsSet() {
        props.put(TimeWindowedSerializer.WINDOWED_INNER_SERIALIZER_CLASS, Serdes.ByteArraySerde.class.getName());
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, "some.non.existent.class");
        try (final TimeWindowedSerializer<?> serializer = new TimeWindowedSerializer<>()) {
            serializer.configure(props, false);
            assertInstanceOf(ByteArraySerializer.class, serializer.innerSerializer());
        }
    }

    @Test
    public void shouldThrowErrorIfWindowedInnerClassSerdeAndWindowedInnerSerializerClassAreNotSet() {
        try (final TimeWindowedSerializer<?> serializer = new TimeWindowedSerializer<>()) {
            assertThrows(IllegalArgumentException.class, () -> serializer.configure(props, false));
        }
    }

    @Deprecated
    @Test
    public void shouldThrowErrorIfSerializerConflictInConstructorAndWindowedInnerClassSerdeConfig() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.ByteArraySerde.class.getName());
        assertThrows(IllegalArgumentException.class, () -> timeWindowedSerializer.configure(props, false));
    }

    @Test
    public void shouldThrowErrorIfSerializerConflictInConstructorAndWindowedInnerSerializerClassConfig() {
        props.put(TimeWindowedSerializer.WINDOWED_INNER_SERIALIZER_CLASS, Serdes.ByteArraySerde.class.getName());
        assertThrows(IllegalArgumentException.class, () -> timeWindowedSerializer.configure(props, false));
    }

    @Deprecated
    @Test
    public void shouldThrowConfigExceptionWhenInvalidWindowedInnerClassSerdeSupplied() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, "some.non.existent.class");
        assertThrows(ConfigException.class, () -> timeWindowedSerializer.configure(props, false));
    }

    @Test
    public void shouldThrowConfigExceptionWhenInvalidWindowedInnerSerializerClassSupplied() {
        props.put(TimeWindowedSerializer.WINDOWED_INNER_SERIALIZER_CLASS, "some.non.existent.class");
        assertThrows(ConfigException.class, () -> timeWindowedSerializer.configure(props, false));
    }
}
