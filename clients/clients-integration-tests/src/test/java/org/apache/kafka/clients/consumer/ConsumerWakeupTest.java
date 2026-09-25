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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConsumerWakeupTest {

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("unavailable-topic", 15);

    @Test
    public void testClassicConsumerPositionWithErrorConnectionRespectsWakeup() {
        testPositionWithErrorConnectionRespectsWakeup(GroupProtocol.CLASSIC);
    }

    @Test
    public void testAsyncConsumerPositionWithErrorConnectionRespectsWakeup() {
        testPositionWithErrorConnectionRespectsWakeup(GroupProtocol.CONSUMER);
    }

    private void testPositionWithErrorConnectionRespectsWakeup(GroupProtocol groupProtocol) {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            BOOTSTRAP_SERVERS_CONFIG, "localhost:12345",
            KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            GROUP_ID_CONFIG, "consumer-wakeup-test"
        );
        try (var consumer = new KafkaConsumer<byte[], byte[]>(config)) {
            consumer.assign(List.of(TOPIC_PARTITION));
            CompletableFuture.runAsync(() -> {
                try {
                    TimeUnit.SECONDS.sleep(1);
                    consumer.wakeup();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThrows(WakeupException.class,
                    () -> consumer.position(TOPIC_PARTITION, Duration.ofSeconds(100)));
        }
    }
}
