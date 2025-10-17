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
package org.apache.kafka.tools;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.ThroughputThrottler;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Scanner;
import java.util.SplittableRandom;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class ProducerPerformance {

    public static final String DEFAULT_TRANSACTION_ID_PREFIX = "performance-producer-";
    public static final long DEFAULT_TRANSACTION_DURATION_MS = 3000L;

    public static void main(String[] args) throws Exception {
        ProducerPerformance perf = new ProducerPerformance();
        perf.start(args);
    }

    void start(String[] args) throws IOException {
        ArgumentParser parser = argParser();

        try {
            ConfigPostProcessor config = new ConfigPostProcessor(parser, args);
            KafkaProducer<byte[], byte[]> producer = createKafkaProducer(config.producerProps);

            if (config.transactionsEnabled)
                producer.initTransactions();

            /* setup perf test */
            byte[] payload = null;
            if (config.recordSize != null) {
                payload = new byte[config.recordSize];
            }
            // not thread-safe, do not share with other threads
            SplittableRandom random = new SplittableRandom(0);
            ProducerRecord<byte[], byte[]> record;

            if (config.warmupRecords > 0) {
                System.out.println("Warmup first " + config.warmupRecords + " records. Steady state results will print after the complete test summary.");
            }
            boolean isSteadyState = false;
            stats = new Stats(config.numRecords, config.reportingInterval, isSteadyState);
            long startMs = System.currentTimeMillis();

            ThroughputThrottler throttler = new ThroughputThrottler(config.throughput, startMs);

            int currentTransactionSize = 0;
            long transactionStartTime = 0;
            for (long i = 0; i < config.numRecords; i++) {

                payload = generateRandomPayload(config.recordSize, config.payloadByteList, payload, random, config.payloadMonotonic, i);

                if (config.transactionsEnabled && currentTransactionSize == 0) {
                    producer.beginTransaction();
                    transactionStartTime = System.currentTimeMillis();
                }

                record = new ProducerRecord<>(config.topicName, payload);

                long sendStartMs = System.currentTimeMillis();
                if ((isSteadyState = config.warmupRecords > 0) && i == config.warmupRecords) {
                    steadyStateStats = new Stats(config.numRecords - config.warmupRecords, config.reportingInterval, isSteadyState);
                    stats.suppressPrinting();
                }
                cb = new PerfCallback(sendStartMs, payload.length, stats, steadyStateStats);
                producer.send(record, cb);

                currentTransactionSize++;
                if (config.transactionsEnabled && config.transactionDurationMs <= (sendStartMs - transactionStartTime)) {
                    producer.commitTransaction();
                    currentTransactionSize = 0;
                }

                if (throttler.shouldThrottle(i, sendStartMs)) {
                    throttler.throttle();
                }
            }

            if (config.transactionsEnabled && currentTransactionSize != 0)
                producer.commitTransaction();

            if (!config.shouldPrintMetrics) {
                producer.close();

                /* print final results */
                stats.printTotal();
                /* print steady-state stats if relevant */
                if (steadyStateStats != null) {
                    steadyStateStats.printTotal();
                }
            } else {
                // Make sure all records are sent before printing out the stats and the metrics
                // We need to do this in a different branch for now since tests/kafkatest/sanity_checks/test_performance_services.py
                // expects this class to work with older versions of the client jar that don't support flush().
                producer.flush();

                /* print final results */
                stats.printTotal();
                /* print steady-state stats if relevant */
                if (steadyStateStats != null) {
                    steadyStateStats.printTotal();
                }

                /* print out metrics */
                ToolsUtils.printMetrics(producer.metrics());
                producer.close();
            }
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }

    }

    KafkaProducer<byte[], byte[]> createKafkaProducer(Properties props) {
        return new KafkaProducer<>(props);
    }

    Callback cb;
    Stats stats;
    Stats steadyStateStats;

    static byte[] generateRandomPayload(Integer recordSize, List<byte[]> payloadByteList, byte[] payload,
            SplittableRandom random, boolean payloadMonotonic, long recordValue) {
        if (!payloadByteList.isEmpty()) {
            payload = payloadByteList.get(random.nextInt(payloadByteList.size()));
        } else if (recordSize != null) {
            for (int j = 0; j < payload.length; ++j)
                payload[j] = (byte) (random.nextInt(26) + 65);
        } else if (payloadMonotonic) {
            payload = Long.toString(recordValue).getBytes(StandardCharsets.UTF_8);
        } else {
            throw new IllegalArgumentException("No payload file, record size or payload-monotonic option provided.");
        }
        return payload;
    }

    static Properties readProps(List<String> producerProps, String producerConfig) throws IOException {
        Properties props = new Properties();
        if (producerConfig != null) {
            props.putAll(Utils.loadProps(producerConfig));
        }
        if (producerProps != null)
            for (String prop : producerProps) {
                String[] pieces = prop.split("=");
                if (pieces.length != 2)
                    throw new IllegalArgumentException("Invalid property: " + prop);
                props.put(pieces[0], pieces[1]);
            }

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        if (props.getProperty(ProducerConfig.CLIENT_ID_CONFIG) == null) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "perf-producer-client");
        }
        return props;
    }

    static List<byte[]> readPayloadFile(String payloadFilePath, String payloadDelimiter) throws IOException {
        List<byte[]> payloadByteList = new ArrayList<>();
        if (payloadFilePath != null) {
            Path path = Paths.get(payloadFilePath);
            System.out.println("Reading payloads from: " + path.toAbsolutePath());
            if (Files.notExists(path) || Files.size(path) == 0)  {
                throw new IllegalArgumentException("File does not exist or empty file provided.");
            }

            try (Scanner payLoadScanner = new Scanner(path, StandardCharsets.UTF_8)) {
                //setting the delimiter while parsing the file, avoids loading entire data in memory before split
                payLoadScanner.useDelimiter(payloadDelimiter);
                while (payLoadScanner.hasNext()) {
                    byte[] payloadBytes = payLoadScanner.next().getBytes(StandardCharsets.UTF_8);
                    payloadByteList.add(payloadBytes);
                }
            }

            System.out.println("Number of records read: " + payloadByteList.size());

        }
        return payloadByteList;
    }

    /** Get the command-line argument parser. */
    static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("kafka-producer-perf-test")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance. To enable transactions, " +
                        "you can specify a transactional id or set a transaction duration using --transaction-duration-ms. " +
                        "There are three ways to specify the transactional id: set transactional.id=<id> via --command-property, " +
                        "set transactional.id=<id> in the config file via --command-config, or use --transactional-id <id>.");

        parser.addArgument("--bootstrap-server")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("BOOTSTRAP-SERVERS")
                .dest("bootstrapServers")
                .help("The server(s) to connect to. This config takes precedence over bootstrap.servers specified " +
                        "via --command-property or --command-config.");

        MutuallyExclusiveGroup payloadOptions = parser
                .addMutuallyExclusiveGroup()
                .required(true)
                .description("Note that you must provide exactly one of --record-size, --payload-file " +
                        "or --payload-monotonic.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("Produce records to this topic.");

        parser.addArgument("--num-records")
                .action(store())
                .required(true)
                .type(Long.class)
                .metavar("NUM-RECORDS")
                .dest("numRecords")
                .help("Number of records to produce.");

        payloadOptions.addArgument("--record-size")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("RECORD-SIZE")
                .dest("recordSize")
                .help("Record size in bytes. Note that you must provide exactly one of --record-size, --payload-file " +
                        "or --payload-monotonic.");

        payloadOptions.addArgument("--payload-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-FILE")
                .dest("payloadFile")
                .help("File to read the record payloads from. This works only for UTF-8 encoded text files. " +
                        "Payloads will be read from this file and a payload will be randomly selected when sending records. " +
                        "Note that you must provide exactly one of --record-size, --payload-file or --payload-monotonic.");

        payloadOptions.addArgument("--payload-monotonic")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("PAYLOAD-MONOTONIC")
                .dest("payloadMonotonic")
                .help("Payload is a monotonically increasing integer. Note that you must provide exactly one of --record-size, " +
                        "--payload-file or --payload-monotonic.");

        parser.addArgument("--payload-delimiter")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-DELIMITER")
                .dest("payloadDelimiter")
                .setDefault("\\n")
                .help("Provides the delimiter to be used when --payload-file is provided. Defaults to new line. " +
                        "Note that this parameter will be ignored if --payload-file is not provided.");

        parser.addArgument("--throughput")
                .action(store())
                .required(true)
                .type(Double.class)
                .metavar("THROUGHPUT")
                .help("Throttle maximum record throughput to *approximately* THROUGHPUT records/sec. Set this to -1 to disable throttling.");

        parser.addArgument("--producer-props")
                .nargs("+")
                .required(false)
                .metavar("PROP-NAME=PROP-VALUE")
                .type(String.class)
                .dest("producerConfig")
                .help("(DEPRECATED) Kafka producer related configuration properties like client.id. " +
                        "These configs take precedence over those passed via --command-config or --producer.config. " +
                        "This option will be removed in a future version. Use --command-property instead.");

        parser.addArgument("--command-property")
                .nargs("+")
                .required(false)
                .metavar("PROP-NAME=PROP-VALUE")
                .type(String.class)
                .dest("commandProperties")
                .help("Kafka producer related configuration properties like client.id. " +
                        "These configs take precedence over those passed via --command-config or --producer.config.");

        parser.addArgument("--producer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG-FILE")
                .dest("producerConfigFile")
                .help("(DEPRECATED) Producer config properties file. " +
                        "This option will be removed in a future version. Use --command-config instead.");

        parser.addArgument("--command-config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG-FILE")
                .dest("commandConfigFile")
                .help("Producer config properties file.");

        parser.addArgument("--print-metrics")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("PRINT-METRICS")
                .dest("printMetrics")
                .help("Print out metrics at the end of the test.");

        parser.addArgument("--transactional-id")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("TRANSACTIONAL-ID")
                .dest("transactionalId")
                .help("The transactional id to use. This config takes precedence over the transactional.id " +
                        "specified via --command-property or --command-config. Note that if the transactional id " +
                        "is not specified while --transaction-duration-ms is provided, the default value for the " +
                        "transactional id will be performance-producer- followed by a random uuid.");

        parser.addArgument("--transaction-duration-ms")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("TRANSACTION-DURATION")
                .dest("transactionDurationMs")
                .help("The maximum duration of each transaction. The commitTransaction will be called after this time has elapsed. " +
                        "The value should be greater than 0. If the transactional id is specified via --command-property, " +
                        "--command-config or --transactional-id but --transaction-duration-ms is not specified, " +
                        "the default value will be 3000.");

        parser.addArgument("--warmup-records")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("WARMUP-RECORDS")
                .dest("warmupRecords")
                .setDefault(0L)
                .help("The number of records to treat as warmup. These initial records will not be included in steady-state statistics. " +
                        "An additional summary line will be printed describing the steady-state statistics.");

        parser.addArgument("--reporting-interval")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("INTERVAL-MS")
                .dest("reportingInterval")
                .setDefault(5_000L)
                .help("Interval in milliseconds at which to print progress info.");

        return parser;
    }

    // Visible for testing
    static class Stats {
        private final long start;
        private final int[] latencies;
        private final long sampling;
        private final long reportingInterval;
        private long iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long windowStart;
        private final boolean isSteadyState;
        private boolean suppressPrint;

        public Stats(long numRecords, long reportingInterval, boolean isSteadyState) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = numRecords / Math.min(numRecords, 500000);
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
            this.isSteadyState = isSteadyState;
            this.suppressPrint = false;
        }

        public void record(int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (this.iteration % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                if (this.isSteadyState && count == windowCount) {
                    System.out.println("In steady state.");
                }
                if (!this.suppressPrint) {
                    printWindow();
                }
                newWindow();
            }
            this.iteration++;
        }

        public long totalCount() {
            return this.count;
        }

        public long currentWindowCount() {
            return this.windowCount;
        }

        public long iteration() {
            return this.iteration;
        }

        public long bytes() {
            return this.bytes;
        }

        public int index() {
            return this.index;
        }

        public void printWindow() {
            long elapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f ms max latency.%n",
                              windowCount,
                              recsPerSec,
                              mbPerSec,
                              windowTotalLatency / (double) windowCount,
                              (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d%s records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                              count,
                              this.isSteadyState ? " steady state" : "",
                              recsPerSec,
                              mbPerSec,
                              totalLatency / (double) count,
                              (double) maxLatency,
                              percs[0],
                              percs[1],
                              percs[2],
                              percs[3]);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }

        public void suppressPrinting() {
            this.suppressPrint = true;
        }
    }

    static final class PerfCallback implements Callback {
        private final long start;
        private final int bytes;
        private final Stats stats;
        private final Stats steadyStateStats;

        public PerfCallback(long start, int bytes, Stats stats, Stats steadyStateStats) {
            this.start = start;
            this.stats = stats;
            this.steadyStateStats = steadyStateStats;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            // It will only be counted when the sending is successful, otherwise the number of sent records may be
            // magically printed when the sending fails.
            if (exception == null) {
                this.stats.record(latency, bytes, now);
                if (steadyStateStats != null) {
                    this.steadyStateStats.record(latency, bytes, now);
                }
            }
            if (exception != null)
                exception.printStackTrace();
        }
    }

    static final class ConfigPostProcessor {
        final String bootstrapServers;
        final String topicName;
        final long numRecords;
        final long warmupRecords;
        final Integer recordSize;
        final double throughput;
        final boolean payloadMonotonic;
        final Properties producerProps;
        final boolean shouldPrintMetrics;
        final Long transactionDurationMs;
        final boolean transactionsEnabled;
        final List<byte[]> payloadByteList;
        final long reportingInterval;

        public ConfigPostProcessor(ArgumentParser parser, String[] args) throws IOException, ArgumentParserException {
            Namespace namespace = parser.parseArgs(args);
            this.bootstrapServers = namespace.getString("bootstrapServers");
            this.topicName = namespace.getString("topic");
            this.numRecords = namespace.getLong("numRecords");
            this.warmupRecords = Math.max(namespace.getLong("warmupRecords"), 0);
            this.recordSize = namespace.getInt("recordSize");
            this.throughput = namespace.getDouble("throughput");
            this.payloadMonotonic = namespace.getBoolean("payloadMonotonic");
            this.shouldPrintMetrics = namespace.getBoolean("printMetrics");
            this.reportingInterval = namespace.getLong("reportingInterval");

            List<String> producerConfigs = namespace.getList("producerConfig");
            String producerConfigFile = namespace.getString("producerConfigFile");
            List<String> commandProperties = namespace.getList("commandProperties");
            String commandConfigFile = namespace.getString("commandConfigFile");
            String payloadFilePath = namespace.getString("payloadFile");
            Long transactionDurationMsArg = namespace.getLong("transactionDurationMs");
            String transactionIdArg = namespace.getString("transactionalId");
            if (numRecords <= 0) {
                throw new ArgumentParserException("--num-records should be greater than zero.", parser);
            }
            if (warmupRecords >= numRecords) {
                throw new ArgumentParserException("The value for --warmup-records must be strictly fewer than the number of records in the test, --num-records.", parser);
            }
            if (recordSize != null && recordSize <= 0) {
                throw new ArgumentParserException("--record-size should be greater than zero.", parser);
            }
            if (bootstrapServers == null && commandProperties == null && producerConfigs == null && producerConfigFile == null && commandConfigFile == null) {
                throw new ArgumentParserException("At least one of --bootstrap-server, --command-property, --producer-props, --producer.config or --command-config must be specified.", parser);
            }
            if (commandProperties != null && producerConfigs != null) {
                throw new ArgumentParserException("--command-property and --producer-props cannot be specified together.", parser);
            }
            if (commandConfigFile != null && producerConfigFile != null) {
                throw new ArgumentParserException("--command-config and --producer.config cannot be specified together.", parser);
            }
            if (transactionDurationMsArg != null && transactionDurationMsArg <= 0) {
                throw new ArgumentParserException("--transaction-duration-ms should be greater than zero.", parser);
            }
            if (reportingInterval <= 0) {
                throw new ArgumentParserException("--reporting-interval should be greater than zero.", parser);
            }

            // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
            String payloadDelimiter = namespace.getString("payloadDelimiter").equals("\\n")
                    ? "\n" : namespace.getString("payloadDelimiter");
            this.payloadByteList = readPayloadFile(payloadFilePath, payloadDelimiter);
            if (producerConfigs != null) {
                System.out.println("Option --producer-props has been deprecated and will be removed in a future version. Use --command-property instead.");
                commandProperties = producerConfigs;
            }
            if (producerConfigFile != null) {
                System.out.println("Option --producer.config has been deprecated and will be removed in a future version. Use --command-config instead.");
                commandConfigFile = producerConfigFile;
            }
            this.producerProps = readProps(commandProperties, commandConfigFile);
            if (bootstrapServers != null) {
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            }
            // setup transaction related configs
            this.transactionsEnabled = transactionDurationMsArg != null
                    || transactionIdArg != null
                    || producerProps.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            if (transactionsEnabled) {
                Optional<String> txIdInProps =
                        Optional.ofNullable(producerProps.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
                                .map(Object::toString);
                String transactionId = Optional.ofNullable(transactionIdArg).orElse(txIdInProps.orElse(DEFAULT_TRANSACTION_ID_PREFIX + Uuid.randomUuid().toString()));
                producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

                if (transactionDurationMsArg == null) {
                    transactionDurationMsArg = DEFAULT_TRANSACTION_DURATION_MS;
                }
            }
            this.transactionDurationMs = transactionDurationMsArg;
        }
    }
}
