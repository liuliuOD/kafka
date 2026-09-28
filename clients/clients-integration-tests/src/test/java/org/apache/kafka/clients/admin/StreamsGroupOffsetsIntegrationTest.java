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
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData;
import org.apache.kafka.clients.consumer.internals.StreamsRebalanceListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = {Type.KRAFT}, brokers = 3)
public class StreamsGroupOffsetsIntegrationTest {
    private static final int PARTITION_COUNT = 3;
    private static final int RECORD_COUNT = 20;

    private final ClusterInstance clusterInstance;

    StreamsGroupOffsetsIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest
    public void testListStreamsGroupOffsets() throws Exception {
        String groupId = "streams_group_list";
        String topic = "test_list_streams_group_offsets";
        clusterInstance.createTopic(topic, PARTITION_COUNT, (short) 1);
        try (Admin admin = clusterInstance.admin();
             Producer<byte[], byte[]> producer = clusterInstance.producer();
             AsyncKafkaConsumer<byte[], byte[]> streamsConsumer = createStreamsGroupConsumer(groupId, topic)) {
            produceRecords(producer, topic);
            consumeAndCommit(streamsConsumer);
            awaitStableGroup(admin, groupId);

            Map<TopicPartition, OffsetAndMetadata> offsets = listOffsets(admin, groupId);

            assertNotNull(offsets);
            assertEquals(PARTITION_COUNT, offsets.size());
            offsets.forEach((topicPartition, offsetAndMetadata) -> {
                assertNotNull(topicPartition);
                assertNotNull(offsetAndMetadata);
                assertTrue(topicPartition.topic().startsWith(topic));
                assertTrue(offsetAndMetadata.offset() >= 0);
            });
        }
    }

    @ClusterTest
    public void testDeleteStreamsGroupOffsets() throws Exception {
        String groupId = "streams_group_delete";
        String topic = "test_delete_streams_group_offsets";
        clusterInstance.createTopic(topic, PARTITION_COUNT, (short) 1);
        try (Admin admin = clusterInstance.admin();
             Producer<byte[], byte[]> producer = clusterInstance.producer();
             AsyncKafkaConsumer<byte[], byte[]> streamsConsumer = createStreamsGroupConsumer(groupId, topic)) {
            produceRecords(producer, topic);
            consumeAndCommit(streamsConsumer);
            awaitOffsets(admin, groupId);

            assertFutureThrows(GroupSubscribedToTopicException.class,
                admin.deleteStreamsGroupOffsets(groupId, Set.of(new TopicPartition(topic, 0))).all());

            streamsConsumer.close();
            awaitGroupState(admin, groupId, GroupState.EMPTY);

            TopicPartition deletedPartition = new TopicPartition(topic, 0);
            assertNull(admin.deleteStreamsGroupOffsets(groupId, Set.of(deletedPartition))
                .partitionResult(deletedPartition).get());
            assertEquals(PARTITION_COUNT - 1, listOffsets(admin, groupId).size());

            assertFutureThrows(UnknownTopicOrPartitionException.class,
                admin.deleteStreamsGroupOffsets(groupId, Set.of(new TopicPartition("mock-topic", 1))).all());
            assertFutureThrows(UnknownTopicOrPartitionException.class,
                admin.deleteStreamsGroupOffsets(groupId, Set.of(new TopicPartition(topic, PARTITION_COUNT))).all());
        }
    }

    @ClusterTest
    public void testAlterStreamsGroupOffsets() throws Exception {
        String groupId = "streams_group_alter";
        String topic = "test_alter_streams_group_offsets";
        clusterInstance.createTopic(topic, PARTITION_COUNT, (short) 1);
        try (Admin admin = clusterInstance.admin();
             Producer<byte[], byte[]> producer = clusterInstance.producer();
             AsyncKafkaConsumer<byte[], byte[]> streamsConsumer = createStreamsGroupConsumer(groupId, topic)) {
            produceRecords(producer, topic);
            consumeAndCommit(streamsConsumer);
            awaitOffsets(admin, groupId);

            streamsConsumer.close();
            awaitGroupState(admin, groupId, GroupState.EMPTY);

            TopicPartition partition0 = new TopicPartition(topic, 0);
            TopicPartition partition1 = new TopicPartition(topic, 1);
            Map<TopicPartition, OffsetAndMetadata> changedOffsets = Map.of(
                partition0, new OffsetAndMetadata(1L),
                partition1, new OffsetAndMetadata(10L)
            );
            var result = admin.alterStreamsGroupOffsets(groupId, changedOffsets);
            assertNull(result.partitionResult(partition0).get());
            assertNull(result.partitionResult(partition1).get());

            Map<TopicPartition, OffsetAndMetadata> offsets = listOffsets(admin, groupId);
            assertNotNull(offsets);
            assertEquals(PARTITION_COUNT, offsets.size());
            assertEquals(1L, offsets.get(partition0).offset());
            assertEquals(10L, offsets.get(partition1).offset());
        }
    }

    private AsyncKafkaConsumer<byte[], byte[]> createStreamsGroupConsumer(String groupId, String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsRebalanceData streamsRebalanceData = new StreamsRebalanceData(
            UUID.randomUUID(),
            Optional.empty(),
            Optional.empty(),
            Map.of("subtopology-0", new StreamsRebalanceData.Subtopology(
                Set.of(topic),
                Set.of(),
                Map.of(),
                Map.of(topic + "-changelog", new StreamsRebalanceData.TopicInfo(
                    Optional.empty(), Optional.empty(), Map.of())),
                List.of()
            )),
            Map.of(),
            Map::of,
            Map::of
        );

        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<>(
            new ConsumerConfig(properties),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer(),
            Optional.of(streamsRebalanceData)
        );
        consumer.subscribe(Set.of(topic), new StreamsRebalanceListener() {
            @Override
            public void onTasksRevoked(Set<StreamsRebalanceData.TaskId> tasks) {
            }

            @Override
            public void onTasksAssigned(StreamsRebalanceData.Assignment assignment) {
            }

            @Override
            public void onAllTasksLost() {
            }
        });
        return consumer;
    }

    private void produceRecords(Producer<byte[], byte[]> producer, String topic) throws Exception {
        List<Future<?>> sends = new ArrayList<>(RECORD_COUNT);
        // These records create the offsets exercised below; a separate partition-0 seed is unnecessary.
        for (int i = 1; i <= RECORD_COUNT; i++) {
            sends.add(producer.send(new ProducerRecord<>(topic,
                ("key-" + i).getBytes(), ("value-" + i).getBytes())));
        }
        for (Future<?> send : sends) {
            send.get(60, TimeUnit.SECONDS);
        }
    }

    private void consumeAndCommit(AsyncKafkaConsumer<byte[], byte[]> consumer) throws Exception {
        AtomicInteger consumedRecordCount = new AtomicInteger(0);
        TestUtils.waitForCondition(() -> {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            return consumedRecordCount.addAndGet(records.count()) >= RECORD_COUNT;
        }, 60_000L, "Streams consumer did not receive all produced records");
        consumer.commitSync();
    }

    private void awaitStableGroup(Admin admin, String groupId) throws Exception {
        awaitGroupState(admin, groupId, GroupState.STABLE);
    }

    private void awaitGroupState(Admin admin, String groupId, GroupState state) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                return admin.listGroups().all().get().stream()
                    .filter(group -> group.groupId().equals(groupId))
                    .anyMatch(group -> group.groupState().orElse(null) == state);
            } catch (Exception e) {
                return false;
            }
        }, 60_000L, "Streams group " + groupId + " did not transition to " + state + " before timeout");
    }

    private void awaitOffsets(Admin admin, String groupId) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                Map<TopicPartition, OffsetAndMetadata> offsets = listOffsets(admin, groupId);
                return offsets != null && offsets.size() == PARTITION_COUNT;
            } catch (Exception e) {
                return false;
            }
        }, 60_000L, "Streams group offsets are not ready to list");
    }

    private Map<TopicPartition, OffsetAndMetadata> listOffsets(Admin admin, String groupId) throws Exception {
        return admin.listStreamsGroupOffsets(
            Map.of(groupId, new ListStreamsGroupOffsetsSpec())
        ).partitionsToOffsetAndMetadata(groupId).get();
    }
}
