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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SessionWindowedDeserializerTest {
    private final SessionWindowedDeserializer<?> sessionWindowedDeserializer = new SessionWindowedDeserializer<>(new StringDeserializer());
    private final Map<String, String> props = new HashMap<>();

    @Test
    public void testSessionWindowedDeserializerConstructor() {
        sessionWindowedDeserializer.configure(props, true);
        final Deserializer<?> inner = sessionWindowedDeserializer.innerDeserializer();
        assertNotNull(inner, "Inner deserializer should be not null");
        assertInstanceOf(StringDeserializer.class, inner, "Inner deserializer type should be StringDeserializer");
    }

    @Deprecated
    @Test
    public void shouldSetSerializerThroughWindowedInnerClassSerdeConfig() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.ByteArraySerde.class.getName());
        try (final SessionWindowedDeserializer<?> deserializer = new SessionWindowedDeserializer<>()) {
            deserializer.configure(props, false);
            assertInstanceOf(ByteArrayDeserializer.class, deserializer.innerDeserializer());
        }
    }

    @Test
    public void shouldSetSerializerThroughWindowedInnerDeserializerClassConfig() {
        props.put(SessionWindowedDeserializer.WINDOWED_INNER_DESERIALIZER_CLASS, Serdes.ByteArraySerde.class.getName());
        try (final SessionWindowedDeserializer<?> deserializer = new SessionWindowedDeserializer<>()) {
            deserializer.configure(props, false);
            assertInstanceOf(ByteArrayDeserializer.class, deserializer.innerDeserializer());
        }
    }

    @Deprecated
    @Test
    public void shouldIgnoreWindowedInnerClassSerdeConfigIfWindowedInnerDeserializerClassConfigIsSet() {
        props.put(SessionWindowedDeserializer.WINDOWED_INNER_DESERIALIZER_CLASS, Serdes.ByteArraySerde.class.getName());
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, "some.non.existent.class");
        try (final SessionWindowedDeserializer<?> deserializer = new SessionWindowedDeserializer<>()) {
            deserializer.configure(props, false);
            assertInstanceOf(ByteArrayDeserializer.class, deserializer.innerDeserializer());
        }
    }

    @Test
    public void shouldThrowErrorIfWindowedInnerClassSerdeAndSessionWindowedDeserializerClassAreNotSet() {
        try (final SessionWindowedDeserializer<?> deserializer = new SessionWindowedDeserializer<>()) {
            assertThrows(IllegalArgumentException.class, () -> deserializer.configure(props, false));
        }
    }

    @Deprecated
    @Test
    public void shouldThrowErrorIfDeserializersConflictInConstructorAndWindowedInnerClassSerdeConfig() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.ByteArraySerde.class.getName());
        assertThrows(IllegalArgumentException.class, () -> sessionWindowedDeserializer.configure(props, false));
    }

    @Test
    public void shouldThrowErrorIfDeserializersConflictInConstructorAndWindowedInnerDeserializerClassConfig() {
        props.put(SessionWindowedDeserializer.WINDOWED_INNER_DESERIALIZER_CLASS, Serdes.ByteArraySerde.class.getName());
        assertThrows(IllegalArgumentException.class, () -> sessionWindowedDeserializer.configure(props, false));
    }

    @Deprecated
    @Test
    public void shouldThrowConfigExceptionWhenInvalidWindowedInnerClassSerdeSupplied() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, "some.non.existent.class");
        assertThrows(ConfigException.class, () -> sessionWindowedDeserializer.configure(props, false));
    }

    @Test
    public void shouldThrowConfigExceptionWhenInvalidWindowedInnerDeserializerClassSupplied() {
        props.put(SessionWindowedDeserializer.WINDOWED_INNER_DESERIALIZER_CLASS, "some.non.existent.class");
        assertThrows(ConfigException.class, () -> sessionWindowedDeserializer.configure(props, false));
    }
}
