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
package org.apache.kafka.common.record;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V0;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V1;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MemoryRecordsBuilderTest {

    private static class Args {
        final int bufferOffset;
        final Compression compression;
        final byte magic;

        public Args(int bufferOffset, Compression compression, byte magic) {
            this.bufferOffset = bufferOffset;
            this.compression = compression;
            this.magic = magic;
        }

        @Override
        public String toString() {
            return "magic=" + magic +
                ", bufferOffset=" + bufferOffset +
                ", compression=" + compression;
        }
    }

    private static class MemoryRecordsBuilderArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            List<Arguments> values = new ArrayList<>();
            for (int bufferOffset : Arrays.asList(0, 15))
                for (CompressionType type: CompressionType.values()) {
                    List<Byte> magics = type == CompressionType.ZSTD
                            ? Collections.singletonList(RecordBatch.MAGIC_VALUE_V2)
                            : asList(RecordBatch.MAGIC_VALUE_V0, MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2);
                    for (byte magic : magics)
                        values.add(Arguments.of(new Args(bufferOffset, Compression.of(type).build(), magic)));
                }
            return values.stream();
        }
    }

    @Test
    public void testUnsupportedCompress() {
        BiFunction<Byte, Compression, MemoryRecordsBuilder> builderBiFunction = (magic, compression) ->
                new MemoryRecordsBuilder(ByteBuffer.allocate(128), magic, compression, TimestampType.CREATE_TIME, 0L, 0L,
                RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, 128);

        Arrays.asList(MAGIC_VALUE_V0, MAGIC_VALUE_V1).forEach(magic -> {
            Exception e = assertThrows(IllegalArgumentException.class, () -> builderBiFunction.apply(magic, Compression.zstd().build()));
            assertEquals(e.getMessage(), "ZStandard compression is not supported for magic " + magic);
        });
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteEmptyRecordSet(Args args) {
        byte magic = args.magic;
        ByteBuffer buffer = allocateBuffer(128, args);

        MemoryRecords records = new MemoryRecordsBuilder(buffer, magic,
            args.compression, TimestampType.CREATE_TIME, 0L, 0L,
            RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
            false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity()).build();

        assertEquals(0, records.sizeInBytes());
        assertEquals(args.bufferOffset, buffer.position());
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteTransactionalRecordSet(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);
        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        Supplier<MemoryRecordsBuilder> supplier = () ->  new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L, 0L, pid, epoch, sequence, true, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        if (args.magic < MAGIC_VALUE_V2) {
            assertThrows(IllegalArgumentException.class, supplier::get);
        } else {
            MemoryRecordsBuilder builder = supplier.get();
            builder.append(System.currentTimeMillis(), "foo".getBytes(), "bar".getBytes());
            MemoryRecords records = builder.build();

            List<MutableRecordBatch> batches = Utils.toList(records.batches().iterator());
            assertEquals(1, batches.size());
            assertTrue(batches.get(0).isTransactional());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteTransactionalWithInvalidPID(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);
        long pid = RecordBatch.NO_PRODUCER_ID;
        short epoch = 15;
        int sequence = 2342;

        Supplier<MemoryRecordsBuilder> supplier = () ->  new MemoryRecordsBuilder(buffer, args.magic, args.compression, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        if (args.magic < MAGIC_VALUE_V2) {
            assertThrows(IllegalArgumentException.class, supplier::get);
        } else {
            MemoryRecordsBuilder builder = supplier.get();
            assertThrows(IllegalArgumentException.class, builder::close);
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteIdempotentWithInvalidEpoch(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);
        long pid = 9809;
        short epoch = RecordBatch.NO_PRODUCER_EPOCH;
        int sequence = 2342;

        Supplier<MemoryRecordsBuilder> supplier = () -> new MemoryRecordsBuilder(buffer, args.magic, args.compression, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        if (args.magic < MAGIC_VALUE_V2) {
            assertThrows(IllegalArgumentException.class, supplier::get);
        } else {
            MemoryRecordsBuilder builder = supplier.get();
            assertThrows(IllegalArgumentException.class, builder::close);
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteIdempotentWithInvalidBaseSequence(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);
        long pid = 9809;
        short epoch = 15;
        int sequence = RecordBatch.NO_SEQUENCE;

        Supplier<MemoryRecordsBuilder> supplier = () -> new MemoryRecordsBuilder(buffer, args.magic, args.compression, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        if (args.magic < MAGIC_VALUE_V2) {
            assertThrows(IllegalArgumentException.class, supplier::get);
        } else {
            MemoryRecordsBuilder builder = supplier.get();
            assertThrows(IllegalArgumentException.class, builder::close);
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteEndTxnMarkerNonTransactionalBatch(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);
        long pid = 9809;
        short epoch = 15;
        int sequence = RecordBatch.NO_SEQUENCE;

        Supplier<MemoryRecordsBuilder> supplier = () -> new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L, 0L, pid, epoch, sequence, false, true,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        if (args.magic < MAGIC_VALUE_V2) {
            assertThrows(IllegalArgumentException.class, supplier::get);
        } else {
            MemoryRecordsBuilder builder = supplier.get();
            assertThrows(IllegalArgumentException.class, () -> builder.appendEndTxnMarker(RecordBatch.NO_TIMESTAMP,
                new EndTransactionMarker(ControlRecordType.ABORT, 0)));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteEndTxnMarkerNonControlBatch(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);
        long pid = 9809;
        short epoch = 15;
        int sequence = RecordBatch.NO_SEQUENCE;

        Supplier<MemoryRecordsBuilder> supplier = () -> new MemoryRecordsBuilder(buffer, args.magic, args.compression, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        if (args.magic < MAGIC_VALUE_V2) {
            assertThrows(IllegalArgumentException.class, supplier::get);
        } else {
            MemoryRecordsBuilder builder = supplier.get();
            assertThrows(IllegalArgumentException.class, () -> builder.appendEndTxnMarker(RecordBatch.NO_TIMESTAMP,
                    new EndTransactionMarker(ControlRecordType.ABORT, 0)));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteLeaderChangeControlBatchWithoutLeaderEpoch(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);
        Supplier<MemoryRecordsBuilder> supplier = () -> new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L, 0L,
                RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        if (args.magic < MAGIC_VALUE_V2) {
            assertThrows(IllegalArgumentException.class, supplier::get);
        } else {
            final int leaderId = 1;
            MemoryRecordsBuilder builder = supplier.get();
            assertThrows(IllegalArgumentException.class, () -> builder.appendLeaderChangeMessage(RecordBatch.NO_TIMESTAMP,
                    new LeaderChangeMessage().setLeaderId(leaderId).setVoters(Collections.emptyList())));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteLeaderChangeControlBatch(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);
        final int leaderId = 1;
        final int leaderEpoch = 5;
        final List<Integer> voters = Arrays.asList(2, 3);

        Supplier<MemoryRecordsBuilder> supplier = () ->  new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, true, leaderEpoch, buffer.capacity());

        if (args.magic < MAGIC_VALUE_V2) {
            assertThrows(IllegalArgumentException.class, supplier::get);
        } else {
            MemoryRecordsBuilder builder = supplier.get();
            builder.appendLeaderChangeMessage(RecordBatch.NO_TIMESTAMP,
                new LeaderChangeMessage()
                    .setLeaderId(leaderId)
                    .setVoters(voters.stream().map(
                        voterId -> new Voter().setVoterId(voterId)).collect(Collectors.toList())));

            MemoryRecords built = builder.build();
            List<Record> records = TestUtils.toList(built.records());
            assertEquals(1, records.size());
            LeaderChangeMessage leaderChangeMessage = ControlRecordUtils.deserializeLeaderChangeMessage(records.get(0));

            assertEquals(leaderId, leaderChangeMessage.leaderId());
            assertEquals(voters, leaderChangeMessage.voters().stream().map(Voter::voterId).collect(Collectors.toList()));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testLegacyCompressionRate(Args args) {
        byte magic = args.magic;
        ByteBuffer buffer = allocateBuffer(1024, args);

        Supplier<LegacyRecord[]> supplier = () -> new LegacyRecord[]{
                LegacyRecord.create(magic, 0L, "a".getBytes(), "1".getBytes()),
                LegacyRecord.create(magic, 1L, "b".getBytes(), "2".getBytes()),
                LegacyRecord.create(magic, 2L, "c".getBytes(), "3".getBytes()),
        };

        if (magic >= MAGIC_VALUE_V2) {
            assertThrows(IllegalArgumentException.class, supplier::get);
        } else {
            LegacyRecord[] records = supplier.get();

            MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compression,
                    TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                    false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

            int uncompressedSize = 0;
            for (LegacyRecord record : records) {
                uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD;
                builder.append(record);
            }

            MemoryRecords built = builder.build();
            if (args.compression.type() == CompressionType.NONE) {
                assertEquals(1.0, builder.compressionRatio(), 0.00001);
            } else {
                int recordHead = magic == MAGIC_VALUE_V0 ? LegacyRecord.RECORD_OVERHEAD_V0 : LegacyRecord.RECORD_OVERHEAD_V1;
                int compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - recordHead;
                double computedCompressionRate = (double) compressedSize / uncompressedSize;
                assertEquals(computedCompressionRate, builder.compressionRatio(), 0.00001);
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testEstimatedSizeInBytes(Args args) {
        ByteBuffer buffer = allocateBuffer(1024, args);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        int previousEstimate = 0;
        for (int i = 0; i < 10; i++) {
            builder.append(new SimpleRecord(i, ("" + i).getBytes()));
            int currentEstimate = builder.estimatedSizeInBytes();
            assertTrue(currentEstimate > previousEstimate);
            previousEstimate = currentEstimate;
        }

        int bytesWrittenBeforeClose = builder.estimatedSizeInBytes();
        MemoryRecords records = builder.build();
        assertEquals(records.sizeInBytes(), builder.estimatedSizeInBytes());
        if (args.compression.type() == CompressionType.NONE)
            assertEquals(records.sizeInBytes(), bytesWrittenBeforeClose);
    }


    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void buildUsingLogAppendTime(Args args) {
        byte magic = args.magic;
        ByteBuffer buffer = allocateBuffer(1024, args);
        long logAppendTime = System.currentTimeMillis();

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compression,
                TimestampType.LOG_APPEND_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(0L, "b".getBytes(), "2".getBytes());
        builder.append(0L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(logAppendTime, info.maxTimestamp);

        if (args.compression.type() == CompressionType.NONE && magic <= MAGIC_VALUE_V1)
            assertEquals(0L, info.shallowOffsetOfMaxTimestamp);
        else
            assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        for (RecordBatch batch : records.batches()) {
            if (magic == MAGIC_VALUE_V0) {
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
            } else {
                assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType());
                for (Record record : batch)
                    assertEquals(logAppendTime, record.timestamp());
            }
        }
    }
    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void buildUsingCreateTime(Args args) {
        byte magic = args.magic;
        ByteBuffer buffer = allocateBuffer(1024, args);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compression,
                TimestampType.CREATE_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(2L, "b".getBytes(), "2".getBytes());
        builder.append(1L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        if (magic == MAGIC_VALUE_V0) {
            assertEquals(-1, info.maxTimestamp);
        } else {
            assertEquals(2L, info.maxTimestamp);
        }

        if (magic == MAGIC_VALUE_V0)
            assertEquals(-1, info.shallowOffsetOfMaxTimestamp);
        else if (args.compression.type() == CompressionType.NONE && magic == MAGIC_VALUE_V1)
            assertEquals(1L, info.shallowOffsetOfMaxTimestamp);
        else
            assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        int i = 0;
        long[] expectedTimestamps = new long[] {0L, 2L, 1L};
        for (RecordBatch batch : records.batches()) {
            if (magic == MAGIC_VALUE_V0) {
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
            } else {
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                for (Record record : batch)
                    assertEquals(expectedTimestamps[i++], record.timestamp());
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testAppendedChecksumConsistency(Args args) {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L, LegacyRecord.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(1L, "key".getBytes(), "value".getBytes());
        MemoryRecords memoryRecords = builder.build();
        List<Record> records = TestUtils.toList(memoryRecords.records());
        assertEquals(1, records.size());
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testSmallWriteLimit(Args args) {
        // with a small write limit, we always allow at least one record to be added

        byte[] key = "foo".getBytes();
        byte[] value = "bar".getBytes();
        int writeLimit = 0;
        ByteBuffer buffer = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L, LegacyRecord.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, writeLimit);

        assertFalse(builder.isFull());
        assertTrue(builder.hasRoomFor(0L, key, value, Record.EMPTY_HEADERS));
        builder.append(0L, key, value);

        assertTrue(builder.isFull());
        assertFalse(builder.hasRoomFor(0L, key, value, Record.EMPTY_HEADERS));

        MemoryRecords memRecords = builder.build();
        List<Record> records = TestUtils.toList(memRecords.records());
        assertEquals(1, records.size());

        Record record = records.get(0);
        assertEquals(ByteBuffer.wrap(key), record.key());
        assertEquals(ByteBuffer.wrap(value), record.value());
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void writePastLimit(Args args) {
        byte magic = args.magic;
        ByteBuffer buffer = allocateBuffer(64, args);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compression,
                TimestampType.CREATE_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.setEstimatedCompressionRatio(0.5f);
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(1L, "b".getBytes(), "2".getBytes());

        assertFalse(builder.hasRoomFor(2L, "c".getBytes(), "3".getBytes(), Record.EMPTY_HEADERS));
        builder.append(2L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        if (magic == MAGIC_VALUE_V0) {
            assertEquals(-1, info.shallowOffsetOfMaxTimestamp);
            assertEquals(-1, info.maxTimestamp);
        } else {
            assertEquals(2L, info.shallowOffsetOfMaxTimestamp);
            assertEquals(2L, info.maxTimestamp);
        }

        long i = 0L;
        for (RecordBatch batch : records.batches()) {
            if (magic == MAGIC_VALUE_V0) {
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
            } else {
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                for (Record record : batch)
                    assertEquals(i++, record.timestamp());
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testAppendAtInvalidOffset(Args args) {
        ByteBuffer buffer = allocateBuffer(1024, args);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        builder.appendWithOffset(0L, System.currentTimeMillis(), "a".getBytes(), null);

        // offsets must increase monotonically
        assertThrows(IllegalArgumentException.class, () -> builder.appendWithOffset(0L, System.currentTimeMillis(),
            "b".getBytes(), null));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void shouldThrowIllegalStateExceptionOnBuildWhenAborted(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.abort();
        assertThrows(IllegalStateException.class, builder::build);
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void shouldResetBufferToInitialPositionOnAbort(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                                                                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                                                                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.abort();
        assertEquals(args.bufferOffset, builder.buffer().position());
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void shouldThrowIllegalStateExceptionOnCloseWhenAborted(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                                                                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                                                                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.abort();
        assertThrows(IllegalStateException.class, builder::close, "Should have thrown IllegalStateException");
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void shouldThrowIllegalStateExceptionOnAppendWhenAborted(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                                                                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                                                                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.abort();
        assertThrows(IllegalStateException.class, () -> builder.append(0L, "a".getBytes(), "1".getBytes()), "Should have thrown IllegalStateException");
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void shouldThrowIllegalStateExceptionOnAppendWhenClosed(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.build();

        assertEquals("Tried to append a record, but MemoryRecordsBuilder is closed for record appends",
                assertThrows(IllegalStateException.class, () -> builder.append(0L, "a".getBytes(), "1".getBytes())).getMessage());
    }

    @ParameterizedTest
    @ArgumentsSource(V2MemoryRecordsBuilderArgumentsProvider.class)
    public void testRecordTimestampsWithDeleteHorizon(Args args) {
        long deleteHorizon = 100;
        int payloadLen = 1024 * 1024;
        ByteBuffer buffer = ByteBuffer.allocate(payloadLen * 2);
        ByteBufferOutputStream byteBufferOutputStream = new ByteBufferOutputStream(buffer);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(byteBufferOutputStream, args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, 0, deleteHorizon);

        builder.append(50L, "0".getBytes(), "0".getBytes());
        builder.append(100L, "1".getBytes(), null);
        builder.append(150L, "2".getBytes(), "2".getBytes());

        MemoryRecords records = builder.build();
        List<MutableRecordBatch> batches = TestUtils.toList(records.batches());
        assertEquals(OptionalLong.of(deleteHorizon), batches.get(0).deleteHorizonMs());

        CloseableIterator<Record> recordIterator = batches.get(0).streamingIterator(BufferSupplier.create());
        Record record = recordIterator.next();
        assertEquals(50L, record.timestamp());
        record = recordIterator.next();
        assertEquals(100L, record.timestamp());
        record = recordIterator.next();
        assertEquals(150L, record.timestamp());
        recordIterator.close();
    }

    private static class V2MemoryRecordsBuilderArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            List<Arguments> values = new ArrayList<>();
            for (int bufferOffset : Arrays.asList(0, 15))
                for (CompressionType type: CompressionType.values()) {
                    values.add(Arguments.of(new Args(bufferOffset, Compression.of(type).build(), MAGIC_VALUE_V2)));
                }
            return values.stream();
        }
    }

    private ByteBuffer allocateBuffer(int size, Args args) {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.position(args.bufferOffset);
        return buffer;
    }
}
