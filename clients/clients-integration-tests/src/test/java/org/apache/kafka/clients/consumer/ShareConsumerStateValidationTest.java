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

import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShareConsumerStateValidationTest {

    @Test
    public void testPollNoSubscribeFails() {
        Map<String, Object> config = Map.of(
            BOOTSTRAP_SERVERS_CONFIG, "localhost:12345",
            GROUP_ID_CONFIG, "share-consumer-state-validation",
            KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()
        );
        try (ShareConsumer<byte[], byte[]> shareConsumer = new KafkaShareConsumer<>(config)) {
            assertEquals(Set.of(), shareConsumer.subscription());
            // This precondition is validated before any broker request is required.
            assertThrows(IllegalStateException.class,
                    () -> shareConsumer.poll(Duration.ofMillis(500)));
        }
    }
}
