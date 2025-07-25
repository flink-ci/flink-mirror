/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.MockChannelStateWriter;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestSubtaskCheckpointCoordinator;
import org.apache.flink.streaming.util.MockStreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the behaviors of the {@link
 * org.apache.flink.streaming.runtime.io.checkpointing.InputProcessorUtil}.
 */
class InputProcessorUtilTest {

    @Test
    void testCreateCheckpointedMultipleInputGate() throws Exception {
        try (CloseableRegistry registry = new CloseableRegistry()) {
            MockEnvironment environment = new MockEnvironmentBuilder().build();
            MockStreamTask streamTask = new MockStreamTaskBuilder(environment).build();
            Configuration jobConf = streamTask.getJobConfiguration();
            jobConf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
            jobConf.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
            StreamConfig streamConfig = new StreamConfig(environment.getJobConfiguration());

            // First input gate has index larger than the second
            List<IndexedInputGate>[] inputGates =
                    new List[] {
                        Collections.singletonList(getGate(1, 4)),
                        Collections.singletonList(getGate(0, 2)),
                    };

            CheckpointBarrierHandler barrierHandler =
                    InputProcessorUtil.createCheckpointBarrierHandler(
                            streamTask,
                            jobConf,
                            streamConfig,
                            new TestSubtaskCheckpointCoordinator(new MockChannelStateWriter()),
                            streamTask.getName(),
                            inputGates,
                            Collections.emptyList(),
                            new SyncMailboxExecutor(),
                            new TestProcessingTimeService());

            CheckpointedInputGate[] checkpointedMultipleInputGate =
                    InputProcessorUtil.createCheckpointedMultipleInputGate(
                            new SyncMailboxExecutor(),
                            inputGates,
                            environment.getMetricGroup().getIOMetricGroup(),
                            barrierHandler,
                            streamConfig);

            for (CheckpointedInputGate checkpointedInputGate : checkpointedMultipleInputGate) {
                registry.registerCloseable(checkpointedInputGate);
            }

            List<IndexedInputGate> allInputGates =
                    Arrays.stream(inputGates)
                            .flatMap(gates -> gates.stream())
                            .collect(Collectors.toList());
            for (IndexedInputGate inputGate : allInputGates) {
                for (int channelId = 0;
                        channelId < inputGate.getNumberOfInputChannels();
                        channelId++) {
                    barrierHandler.processBarrier(
                            new CheckpointBarrier(
                                    1,
                                    42,
                                    CheckpointOptions.unaligned(
                                            CheckpointType.CHECKPOINT,
                                            CheckpointStorageLocationReference.getDefault())),
                            new InputChannelInfo(inputGate.getGateIndex(), channelId),
                            false);
                }
            }
            assertThat(barrierHandler.getAllBarriersReceivedFuture(1)).isDone();
        }
    }

    private SingleInputGate getGate(int index, int numChannels) {
        return new SingleInputGateBuilder()
                .setNumberOfChannels(numChannels)
                .setSingleInputGateIndex(index)
                .setChannelFactory(InputChannelBuilder::buildLocalChannel)
                .build();
    }
}
