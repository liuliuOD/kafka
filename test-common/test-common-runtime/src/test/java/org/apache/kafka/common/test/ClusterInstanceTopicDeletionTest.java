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

package org.apache.kafka.common.test;

import kafka.server.KafkaBroker;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import scala.jdk.javaapi.OptionConverters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClusterInstanceTopicDeletionTest {
    private static final String TOPIC = "topic";

    @TempDir
    Path logDir;

    private ClusterInstance cluster;
    private KafkaBroker broker;

    @BeforeEach
    void setUp() {
        cluster = mock(ClusterInstance.class, CALLS_REAL_METHODS);
        broker = mock(KafkaBroker.class, RETURNS_DEEP_STUBS);
        when(cluster.aliveBrokers()).thenReturn(Map.of(0, broker));
        when(cluster.controllers()).thenReturn(Map.of());
        when(broker.metadataCache().numPartitions(TOPIC)).thenReturn(Optional.empty());
        when(broker.replicaManager().onlinePartition(new TopicPartition(TOPIC, 0)))
            .thenReturn(OptionConverters.toScala(Optional.empty()));
        when(broker.logManager().getLog(new TopicPartition(TOPIC, 0), false)).thenReturn(Optional.empty());
        when(broker.logManager().logsByTopic(TOPIC)).thenReturn(List.of());
        when(broker.logManager().liveLogDirs()).thenReturn(List.of(logDir.toFile()));
        when(broker.config().logDirs()).thenReturn(List.of(logDir.toString()));
    }

    @Test
    void testWaitsForNonzeroPartitionLog() throws Exception {
        UnifiedLog remainingLog = mock(UnifiedLog.class);
        when(broker.logManager().logsByTopic(TOPIC)).thenReturn(List.of(remainingLog)).thenReturn(List.of());

        cluster.waitTopicDeletion(TOPIC);

        verify(broker.logManager(), times(2)).logsByTopic(TOPIC);
    }

    @Test
    void testWaitsForNonzeroPartitionCleanerOffset() throws Exception {
        OffsetCheckpointFile checkpoint = new OffsetCheckpointFile(logDir.resolve("cleaner-offset-checkpoint").toFile(), null);
        checkpoint.write(Map.of(new TopicPartition(TOPIC, 1), 42L));
        AtomicInteger reads = new AtomicInteger();
        when(broker.logManager().liveLogDirs()).thenAnswer(invocation -> {
            if (reads.incrementAndGet() > 1) {
                checkpoint.write(Map.of());
            }
            return List.of(logDir.toFile());
        });

        cluster.waitTopicDeletion(TOPIC);

        assertTrue(checkpoint.read().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", ".test-delete"})
    void testWaitsForNonzeroPartitionDirectory(String suffix) throws Exception {
        verifyDirectoryDeletion(TOPIC, TOPIC + "-1" + suffix, TOPIC + "-other-1" + suffix);
    }

    @Test
    void testWaitsForTruncatedTopicDeletionDirectory() throws Exception {
        String topic = "t".repeat(249);
        when(broker.metadataCache().numPartitions(topic)).thenReturn(Optional.empty());
        when(broker.logManager().logsByTopic(topic)).thenReturn(List.of());
        verifyDirectoryDeletion(topic, UnifiedLog.logDeleteDirName(new TopicPartition(topic, 1)), "other-1.test-delete");
    }

    private void verifyDirectoryDeletion(String topic, String directoryName, String otherDirectoryName) throws Exception {
        Path remainingDirectory = Files.createDirectory(logDir.resolve(directoryName));
        Path otherTopicDirectory = Files.createDirectory(logDir.resolve(otherDirectoryName));
        AtomicInteger checks = new AtomicInteger();
        when(broker.config().logDirs()).thenAnswer(invocation -> {
            if (checks.incrementAndGet() >= 3) {
                Files.deleteIfExists(remainingDirectory);
            }
            return List.of(logDir.toString());
        });

        cluster.waitTopicDeletion(topic);

        assertFalse(Files.exists(remainingDirectory));
        assertTrue(Files.exists(otherTopicDirectory));
    }
}
