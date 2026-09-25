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

import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.common.utils.internals.Exit;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetOffsetShellUnitTest {

    @Test
    public void testPrintHelp() {
        Exit.setExitProcedure((statusCode, message) -> { });
        try {
            String out = ToolsTestUtils.captureStandardErr(() -> GetOffsetShell.mainNoExit("--help"));
            assertTrue(out.startsWith(GetOffsetShell.USAGE_TEXT));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testPrintVersion() {
        String out = ToolsTestUtils.captureStandardOut(() -> GetOffsetShell.mainNoExit("--version"));
        assertEquals(AppInfoParser.getVersion(), out);
    }

    @Test
    public void testTopicPartitionsFlagWithTopicFlagCauseExit() {
        assertExitCodeIsOne("--topic-partitions", "__consumer_offsets", "--topic", "topic1");
    }

    @Test
    public void testTopicPartitionsFlagWithPartitionsFlagCauseExit() {
        assertExitCodeIsOne("--topic-partitions", "__consumer_offsets", "--partitions", "0");
    }

    private void assertExitCodeIsOne(String... args) {
        final int[] exitStatus = new int[1];

        Exit.setExitProcedure((statusCode, message) -> {
            exitStatus[0] = statusCode;
            throw new RuntimeException();
        });

        List<String> command = new ArrayList<>(List.of(args));
        command.add("--bootstrap-server");
        command.add("localhost:9092");
        try {
            GetOffsetShell.main(command.toArray(new String[0]));
        } catch (RuntimeException ignored) {
            // The exit procedure throws to stop command execution.
        } finally {
            Exit.resetExitProcedure();
        }

        assertEquals(1, exitStatus[0]);
    }
}
