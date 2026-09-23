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

import org.apache.kafka.common.errors.TimeoutException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AdminMetadataTimeoutTest {

    // Reserved port that should not be used by any other service.
    private static final int INCORRECT_BROKER_PORT = 225;
    private static final String TXN_ID = "mytxnid";

    private Admin createInvalidAdminClient() {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + INCORRECT_BROKER_PORT
        );
        return Admin.create(config);
    }

    @Test
    public void testListTopicsWithOptionTimeoutMs() {
        Admin admin = createInvalidAdminClient();
        try {
            ListTopicsOptions timeoutOption = new ListTopicsOptions().timeoutMs(0);
            ExecutionException exception = assertThrows(ExecutionException.class,
                    () -> admin.listTopics(timeoutOption).names().get());
            assertInstanceOf(TimeoutException.class, exception.getCause());
        } finally {
            admin.close(Duration.ZERO);
        }
    }

    @Test
    public void testDeleteTopicsWithOptionTimeoutMs() {
        Admin admin = createInvalidAdminClient();
        try {
            DeleteTopicsOptions timeoutOption = new DeleteTopicsOptions().timeoutMs(0);
            ExecutionException exception = assertThrows(ExecutionException.class,
                    () -> admin.deleteTopics(List.of("test-topic"), timeoutOption).all().get());
            assertInstanceOf(TimeoutException.class, exception.getCause());
        } finally {
            admin.close(Duration.ZERO);
        }
    }

    @Test
    public void testDescribeTopicsWithOptionTimeoutMs() {
        Admin admin = createInvalidAdminClient();
        try {
            DescribeTopicsOptions timeoutOption = new DescribeTopicsOptions().timeoutMs(0);
            ExecutionException exception = assertThrows(ExecutionException.class,
                    () -> admin.describeTopics(List.of("test-topic"), timeoutOption).allTopicNames().get());
            assertInstanceOf(TimeoutException.class, exception.getCause());
        } finally {
            admin.close(Duration.ZERO);
        }
    }

    @Test
    public void testFenceProducerTimeoutMs() {
        Admin adminClient = createInvalidAdminClient();
        try {
            ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> adminClient.fenceProducers(Collections.singletonList(TXN_ID),
                            new FenceProducersOptions().timeoutMs(0)).all().get());
            assertInstanceOf(TimeoutException.class, exception.getCause());
        } finally {
            adminClient.close(Duration.ZERO);
        }
    }
}
