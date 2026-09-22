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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.query.MultiVersionedKeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class IQv2VersionedStoreIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final String STORE_NAME = "versioned-store";
    private static final Duration HISTORY_RETENTION = Duration.ofDays(1);
    private static final Duration SEGMENT_INTERVAL = Duration.ofHours(1);

    private static final int RECORD_KEY = 2;
    private static final int NON_EXISTING_KEY = 3;

    private static final Instant BASE_TIMESTAMP = Instant.parse("2023-01-01T10:00:00.00Z");
    private static final Long BASE_TIMESTAMP_LONG = BASE_TIMESTAMP.getLong(ChronoField.INSTANT_SECONDS);
    private static final Integer[] RECORD_VALUES = {2, 20, 200, 2000};
    private static final Long[] RECORD_TIMESTAMPS = {BASE_TIMESTAMP_LONG, BASE_TIMESTAMP_LONG + 10, BASE_TIMESTAMP_LONG + 20, BASE_TIMESTAMP_LONG + 30};
    private static final int RECORD_NUMBER = RECORD_VALUES.length;
    private static final int LAST_INDEX = RECORD_NUMBER - 1;
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "true")));

    private static TestContext classicContext;
    private static TestContext streamsContext;
    private static EnumMap<TestGroupProtocol, TestContext> contexts;

    private enum TestGroupProtocol {
        CLASSIC("classic", "iqv2_classic"),
        STREAMS("streams", "iqv2_streams");

        private final String configValue;
        private final String testName;

        TestGroupProtocol(final String configValue, final String testName) {
            this.configValue = configValue;
            this.testName = testName;
        }
    }

    private static final class TestContext {
        private final TestGroupProtocol groupProtocol;
        private final String topicName;
        private final String applicationId;
        private KafkaStreams kafkaStreams;
        private Position inputPosition;
        private boolean stateReady;

        private TestContext(final TestGroupProtocol groupProtocol, final String topicName, final String applicationId) {
            this.groupProtocol = groupProtocol;
            this.topicName = topicName;
            this.applicationId = applicationId;
        }
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        CLUSTER.start();
        final String classicTestName = safeUniqueTestName(TestGroupProtocol.CLASSIC.testName);
        final String streamsTestName = safeUniqueTestName(TestGroupProtocol.STREAMS.testName);
        classicContext = createContext(TestGroupProtocol.CLASSIC, "input-topic-" + classicTestName, "app-" + classicTestName);
        streamsContext = createContext(TestGroupProtocol.STREAMS, "input-topic-" + streamsTestName, "app-" + streamsTestName);
        contexts = new EnumMap<>(TestGroupProtocol.class);
        contexts.put(TestGroupProtocol.CLASSIC, classicContext);
        contexts.put(TestGroupProtocol.STREAMS, streamsContext);
        prepareTopicAndRecords(classicContext);
        prepareTopicAndRecords(streamsContext);

        startStreams(classicContext);
        startStreams(streamsContext);
        awaitStateStoreReady(classicContext);
        awaitStateStoreReady(streamsContext);
    }

    private static TestContext createContext(final TestGroupProtocol groupProtocol, final String topicName, final String applicationId) {
        return new TestContext(groupProtocol, topicName, applicationId);
    }

    private static void prepareTopicAndRecords(final TestContext context) throws Exception {
        CLUSTER.createTopic(context.topicName, 1, 1);
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        try (final KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(context.topicName, 0, RECORD_TIMESTAMPS[0], RECORD_KEY, RECORD_VALUES[0])).get();
            producer.send(new ProducerRecord<>(context.topicName, 0, RECORD_TIMESTAMPS[1], RECORD_KEY, RECORD_VALUES[1])).get();
            producer.send(new ProducerRecord<>(context.topicName, 0, RECORD_TIMESTAMPS[2], RECORD_KEY, RECORD_VALUES[2])).get();
            producer.send(new ProducerRecord<>(context.topicName, 0, RECORD_TIMESTAMPS[3], RECORD_KEY, RECORD_VALUES[3])).get();
        }
        context.inputPosition = Position.emptyPosition().withComponent(context.topicName, 0, 3);
    }

    private static void startStreams(final TestContext context) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(context.topicName,
            Materialized.as(Stores.persistentVersionedKeyValueStore(STORE_NAME, HISTORY_RETENTION, SEGMENT_INTERVAL)));
        final Properties configs = new Properties();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, context.applicationId);
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        configs.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, context.groupProtocol.configValue);
        context.kafkaStreams = IntegrationTestUtils.getStartedStreams(configs, builder, true);
    }

    @AfterAll
    public static void after() {
        closeStreams(classicContext);
        closeStreams(streamsContext);
        CLUSTER.stop();
    }

    private static void closeStreams(final TestContext context) {
        if (context != null && context.kafkaStreams != null) {
            context.kafkaStreams.close(Duration.ofSeconds(60));
            context.kafkaStreams.cleanUp();
        }
    }

    private static Stream<Arguments> groupProtocolParameters() {
        return Stream.of(
            Arguments.of(TestGroupProtocol.CLASSIC, "CLASSIC protocol"),
            Arguments.of(TestGroupProtocol.STREAMS, "STREAMS protocol")
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("groupProtocolParameters")
    public void verifyStore(final TestGroupProtocol groupProtocol, final String testName) throws Exception {
        final TestContext context = contexts.get(groupProtocol);
        /* Test Versioned Key Queries */
        // retrieve the latest value
        shouldHandleVersionedKeyQuery(context, Optional.empty(), RECORD_VALUES[3], RECORD_TIMESTAMPS[3], Optional.empty());
        shouldHandleVersionedKeyQuery(context, Optional.of(Instant.now()), RECORD_VALUES[3], RECORD_TIMESTAMPS[3], Optional.empty());
        shouldHandleVersionedKeyQuery(context, Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[3])), RECORD_VALUES[3], RECORD_TIMESTAMPS[3], Optional.empty());
        // retrieve the old value
        shouldHandleVersionedKeyQuery(context, Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0])), RECORD_VALUES[0], RECORD_TIMESTAMPS[0], Optional.of(RECORD_TIMESTAMPS[1]));
        // there is no record for the provided timestamp
        shouldVerifyGetNullForVersionedKeyQuery(context, RECORD_KEY, Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 50));
        // there is no record with this key
        shouldVerifyGetNullForVersionedKeyQuery(context, NON_EXISTING_KEY, Instant.now());

        /* Test Multi Versioned Key Queries */
        // retrieve all existing values
        shouldHandleMultiVersionedKeyQuery(context, Optional.empty(), Optional.empty(), ResultOrder.ANY, 0, LAST_INDEX);
        // retrieve all existing values in ascending order
        shouldHandleMultiVersionedKeyQuery(context, Optional.empty(), Optional.empty(), ResultOrder.ASCENDING, 0, LAST_INDEX);
        // retrieve existing values in query defined time range
        shouldHandleMultiVersionedKeyQuery(context, Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[1] + 5)), Optional.of(Instant.now()),
                                           ResultOrder.ANY, 1, LAST_INDEX);
        // there is no record in the query specified time range
        shouldVerifyGetNullForMultiVersionedKeyQuery(context, RECORD_KEY,
                                                     Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 100)), Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 50)),
                                                     ResultOrder.ANY);
        // there is no record in the query specified time range even retrieving results in ascending order
        shouldVerifyGetNullForMultiVersionedKeyQuery(context, RECORD_KEY,
                                                     Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 100)), Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 50)),
                                                     ResultOrder.ASCENDING);
        // there is no record with this key
        shouldVerifyGetNullForMultiVersionedKeyQuery(context, NON_EXISTING_KEY, Optional.empty(), Optional.empty(), ResultOrder.ANY);
        // there is no record with this key even retrieving results in ascending order
        shouldVerifyGetNullForMultiVersionedKeyQuery(context, NON_EXISTING_KEY, Optional.empty(), Optional.empty(), ResultOrder.ASCENDING);
        // test concurrent write while retrieving records
        shouldHandleRaceCondition(context);
    }

    private static void awaitStateStoreReady(final TestContext context) {
        if (context.stateReady) {
            return;
        }

        final VersionedKeyQuery<Integer, Integer> query = defineQuery(RECORD_KEY, Optional.empty());
        final StateQueryRequest<VersionedRecord<Integer>> request = StateQueryRequest.inStore(STORE_NAME)
            .withQuery(query)
            .withPositionBound(PositionBound.at(context.inputPosition));
        final StateQueryResult<VersionedRecord<Integer>> result =
            IntegrationTestUtils.iqv2WaitForResult(context.kafkaStreams, request);
        assertTrue(result.getOnlyPartitionResult().isSuccess());
        context.stateReady = true;
    }

    private void shouldHandleVersionedKeyQuery(final TestContext context,
                                               final Optional<Instant> queryTimestamp,
                                               final Integer expectedValue,
                                               final Long expectedTimestamp,
                                               final Optional<Long> expectedValidToTime) {

        final VersionedKeyQuery<Integer, Integer> query = defineQuery(RECORD_KEY, queryTimestamp);

        final QueryResult<VersionedRecord<Integer>> queryResult = sendRequestAndReceiveResults(context, query);

        // verify results
        if (queryResult == null) {
            throw new AssertionError("The query returned null.");
        }
        if (queryResult.isFailure()) {
            throw new AssertionError(queryResult.toString());
        }
        if (queryResult.getResult() == null) {
            throw new AssertionError("The query returned null.");
        }

        assertTrue(queryResult.isSuccess());
        final VersionedRecord<Integer> result1 = queryResult.getResult();
        assertEquals(expectedValue, result1.value());
        assertEquals(expectedTimestamp, result1.timestamp());
        assertEquals(expectedValidToTime, result1.validTo());
        assertTrue(queryResult.getExecutionInfo().isEmpty());
    }

    private void shouldVerifyGetNullForVersionedKeyQuery(final TestContext context, final Integer key, final Instant queryTimestamp) {
        final VersionedKeyQuery<Integer, Integer> query = defineQuery(key, Optional.of(queryTimestamp));
        assertNull(sendRequestAndReceiveResults(context, query));
    }

    private void shouldHandleMultiVersionedKeyQuery(final TestContext context,
                                                    final Optional<Instant> fromTime, final Optional<Instant> toTime,
                                                    final ResultOrder order, final int expectedArrayLowerBound, final int expectedArrayUpperBound) {

        final MultiVersionedKeyQuery<Integer, Integer> query = defineQuery(RECORD_KEY, fromTime, toTime, order);

        final Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResults = sendRequestAndReceiveResults(context, query);

        // verify results
        for (final Entry<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResultsEntry : partitionResults.entrySet()) {
            verifyPartitionResult(partitionResultsEntry.getValue());
            try (final VersionedRecordIterator<Integer> iterator = partitionResultsEntry.getValue().getResult()) {
                int i = order.equals(ResultOrder.ASCENDING) ? 0 : expectedArrayUpperBound;
                int iteratorSize = 0;
                while (iterator.hasNext()) {
                    final VersionedRecord<Integer> record = iterator.next();
                    final Long timestamp = record.timestamp();
                    final Optional<Long> validTo = record.validTo();
                    final Integer value = record.value();

                    final Optional<Long> expectedValidTo = i < expectedArrayUpperBound ? Optional.of(RECORD_TIMESTAMPS[i + 1]) : Optional.empty();
                    assertEquals(RECORD_VALUES[i], value);
                    assertEquals(RECORD_TIMESTAMPS[i], timestamp);
                    assertEquals(expectedValidTo, validTo);
                    i = order.equals(ResultOrder.ASCENDING) ? i + 1 : i - 1;
                    iteratorSize++;
                }
                // The number of returned records by query is equal to expected number of records
                assertEquals(expectedArrayUpperBound - expectedArrayLowerBound + 1, iteratorSize);
            }
        }
    }

    private void shouldVerifyGetNullForMultiVersionedKeyQuery(final TestContext context,
                                                              final Integer key, final Optional<Instant> fromTime,
                                                              final Optional<Instant> toTime, final ResultOrder order) {
        final MultiVersionedKeyQuery<Integer, Integer> query = defineQuery(key, fromTime, toTime, order);

        final Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResults = sendRequestAndReceiveResults(context, query);

        // verify results
        for (final Entry<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResultsEntry : partitionResults.entrySet()) {
            try (final VersionedRecordIterator<Integer> iterator = partitionResultsEntry.getValue().getResult()) {
                assertFalse(iterator.hasNext());
            }
        }
    }

    /**
     * This method updates a record value in an existing timestamp, while it is retrieving records.
     * Since IQv2 guarantees snapshot semantics, we expect that the old value is retrieved.
     */
    private void shouldHandleRaceCondition(final TestContext context) {
        final MultiVersionedKeyQuery<Integer, Integer> query = defineQuery(RECORD_KEY, Optional.empty(), Optional.empty(), ResultOrder.ANY);

        // For race condition test, we don't use position bounds since we're testing concurrent updates
        final StateQueryRequest<VersionedRecordIterator<Integer>> request = StateQueryRequest.inStore(STORE_NAME).withQuery(query);
        final StateQueryResult<VersionedRecordIterator<Integer>> result = IntegrationTestUtils.iqv2WaitForResult(context.kafkaStreams, request);
        final Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResults = result.getPartitionResults();

        // verify results in two steps
        for (final Entry<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResultsEntry : partitionResults.entrySet()) {
            try (final VersionedRecordIterator<Integer> iterator = partitionResultsEntry.getValue().getResult()) {
                int i = LAST_INDEX;
                int iteratorSize = 0;

                // step 1:
                while (iterator.hasNext()) {
                    final VersionedRecord<Integer> record = iterator.next();
                    final Long timestamp = record.timestamp();
                    final Optional<Long> validTo = record.validTo();
                    final Integer value = record.value();

                    final Optional<Long> expectedValidTo = i < LAST_INDEX ? Optional.of(RECORD_TIMESTAMPS[i + 1]) : Optional.empty();
                    assertEquals(RECORD_VALUES[i], value);
                    assertEquals(RECORD_TIMESTAMPS[i], timestamp);
                    assertEquals(expectedValidTo, validTo);
                    i--;
                    iteratorSize++;
                    if (i == 2) {
                        break;
                    }
                }

                // update the value of the oldest record
                updateRecordValue(context);

                // step 2: continue reading records from through the already opened iterator
                while (iterator.hasNext()) {
                    final VersionedRecord<Integer> record = iterator.next();
                    final Long timestamp = record.timestamp();
                    final Optional<Long> validTo = record.validTo();
                    final Integer value = record.value();

                    final Optional<Long> expectedValidTo = Optional.of(RECORD_TIMESTAMPS[i + 1]);
                    assertEquals(RECORD_VALUES[i], value);
                    assertEquals(RECORD_TIMESTAMPS[i], timestamp);
                    assertEquals(expectedValidTo, validTo);
                    i--;
                    iteratorSize++;
                }

                // The number of returned records by query is equal to expected number of records
                assertEquals(RECORD_NUMBER, iteratorSize);
            }
        }
    }

    private static VersionedKeyQuery<Integer, Integer> defineQuery(final Integer key, final Optional<Instant> queryTimestamp) {
        VersionedKeyQuery<Integer, Integer> query = VersionedKeyQuery.withKey(key);
        if (queryTimestamp.isPresent()) {
            query = query.asOf(queryTimestamp.get());
        }
        return query;
    }

    private static MultiVersionedKeyQuery<Integer, Integer> defineQuery(final Integer key, final Optional<Instant> fromTime, final Optional<Instant> toTime, final ResultOrder order) {
        MultiVersionedKeyQuery<Integer, Integer> query = MultiVersionedKeyQuery.withKey(key);
        if (fromTime.isPresent()) {
            query = query.fromTime(fromTime.get());
        }
        if (toTime.isPresent()) {
            query = query.toTime(toTime.get());
        }
        if (order.equals(ResultOrder.ASCENDING)) {
            query = query.withAscendingTimestamps();
        }
        return query;
    }

    private Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> sendRequestAndReceiveResults(final TestContext context,
                                                                                                      final MultiVersionedKeyQuery<Integer, Integer> query) {
        final StateQueryRequest<VersionedRecordIterator<Integer>> request = StateQueryRequest.inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(context.inputPosition));
        final StateQueryResult<VersionedRecordIterator<Integer>> result = IntegrationTestUtils.iqv2WaitForResult(context.kafkaStreams, request);
        return result.getPartitionResults();
    }

    private QueryResult<VersionedRecord<Integer>> sendRequestAndReceiveResults(final TestContext context,
                                                                                final VersionedKeyQuery<Integer, Integer> query) {
        final StateQueryRequest<VersionedRecord<Integer>> request = StateQueryRequest.inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(context.inputPosition));
        final StateQueryResult<VersionedRecord<Integer>> result = IntegrationTestUtils.iqv2WaitForResult(context.kafkaStreams, request);
        return result.getOnlyPartitionResult();
    }

    private static void verifyPartitionResult(final QueryResult<VersionedRecordIterator<Integer>> result) {
        assertTrue(result.getExecutionInfo().isEmpty());
        if (result.isFailure()) {
            throw new AssertionError(result.toString());
        }
        assertTrue(result.isSuccess());
        assertThrows(IllegalArgumentException.class, result::getFailureReason);
        assertThrows(IllegalArgumentException.class, result::getFailureMessage);
    }

    /**
     * This method inserts a new value (999999) for the key in the oldest timestamp (RECORD_TIMESTAMPS[0]).
     */
    private void updateRecordValue(final TestContext context) {
        // update the record value at RECORD_TIMESTAMPS[0]
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        try (final KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(context.topicName, 0, RECORD_TIMESTAMPS[0], RECORD_KEY, 999999));
        }

        context.inputPosition = context.inputPosition.withComponent(context.topicName, 0, 4);
        assertEquals(Position.emptyPosition().withComponent(context.topicName, 0, 4), context.inputPosition);

        // make sure that the new value is picked up by the store
        final Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "foo");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try {
            IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProps, context.topicName, RECORD_NUMBER + 1);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
