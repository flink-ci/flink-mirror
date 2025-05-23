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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Mock {@link IndexedInputGate}. */
public class MockIndexedInputGate extends IndexedInputGate {
    private final int gateIndex;
    private final int numberOfInputChannels;

    public MockIndexedInputGate() {
        this(0, 1);
    }

    public MockIndexedInputGate(int gateIndex, int numberOfInputChannels) {
        this.gateIndex = gateIndex;
        this.numberOfInputChannels = numberOfInputChannels;
    }

    @Override
    public void setup() {}

    @Override
    public CompletableFuture<Void> getStateConsumedFuture() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void finishReadRecoveredState() {}

    @Override
    public void requestPartitions() {}

    @Override
    public void resumeConsumption(InputChannelInfo channelInfo) {}

    @Override
    public void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo) throws IOException {
        throw new UnsupportedEncodingException();
    }

    @Override
    public int getNumberOfInputChannels() {
        return numberOfInputChannels;
    }

    @Override
    public InputChannel getChannel(int channelIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {}

    @Override
    public List<InputChannelInfo> getChannelInfos() {
        return IntStream.range(0, numberOfInputChannels)
                .mapToObj(channelIndex -> new InputChannelInfo(gateIndex, channelIndex))
                .collect(Collectors.toList());
    }

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public EndOfDataStatus hasReceivedEndOfData() {
        return EndOfDataStatus.NOT_END_OF_DATA;
    }

    @Override
    public Optional<BufferOrEvent> getNext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<BufferOrEvent> pollNext() {
        return getNext();
    }

    @Override
    public void sendTaskEvent(TaskEvent event) {}

    @Override
    public void resumeGateConsumption() throws IOException {}

    @Override
    public void close() {}

    @Override
    public int getGateIndex() {
        return gateIndex;
    }

    @Override
    public List<InputChannelInfo> getUnfinishedChannels() {
        return Collections.emptyList();
    }

    @Override
    public ResultPartitionType getConsumedPartitionType() {
        return ResultPartitionType.PIPELINED;
    }

    @Override
    public void triggerDebloating() {}
}
