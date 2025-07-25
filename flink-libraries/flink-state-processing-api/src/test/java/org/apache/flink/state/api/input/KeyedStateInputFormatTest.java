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

package org.apache.flink.state.api.input;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.asyncprocessing.operators.AsyncStreamFlatMap;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.input.operator.KeyedStateReaderOperator;
import org.apache.flink.state.api.input.splits.KeyGroupRangeInputSplit;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.asyncprocessing.AsyncKeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for keyed state input format. */
@RunWith(Parameterized.class)
class KeyedStateInputFormatTest {
    private static ValueStateDescriptor<Integer> stateDescriptor =
            new ValueStateDescriptor<>("state", Types.INT);

    @ParameterizedTest(name = "Enable async state = {0}")
    @ValueSource(booleans = {false, true})
    void testCreatePartitionedInputSplits(boolean asyncState) throws Exception {
        OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

        OperatorSubtaskState state =
                createOperatorSubtaskState(createFlatMap(asyncState), asyncState);
        OperatorState operatorState = new OperatorState(null, null, operatorID, 1, 128);
        operatorState.putState(0, state);

        KeyedStateInputFormat<?, ?, ?> format =
                new KeyedStateInputFormat<>(
                        operatorState,
                        new HashMapStateBackend(),
                        new Configuration(),
                        new KeyedStateReaderOperator<>(new ReaderFunction(), Types.INT),
                        new ExecutionConfig());
        KeyGroupRangeInputSplit[] splits = format.createInputSplits(4);
        Assert.assertEquals(
                "Failed to properly partition operator state into input splits", 4, splits.length);
    }

    @ParameterizedTest(name = "Enable async state = {0}")
    @ValueSource(booleans = {false, true})
    void testMaxParallelismRespected(boolean asyncState) throws Exception {
        OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

        OperatorSubtaskState state =
                createOperatorSubtaskState(createFlatMap(asyncState), asyncState);
        OperatorState operatorState = new OperatorState(null, null, operatorID, 1, 128);
        operatorState.putState(0, state);

        KeyedStateInputFormat<?, ?, ?> format =
                new KeyedStateInputFormat<>(
                        operatorState,
                        new HashMapStateBackend(),
                        new Configuration(),
                        new KeyedStateReaderOperator<>(new ReaderFunction(), Types.INT),
                        new ExecutionConfig());
        KeyGroupRangeInputSplit[] splits = format.createInputSplits(129);
        Assert.assertEquals(
                "Failed to properly partition operator state into input splits",
                128,
                splits.length);
    }

    @ParameterizedTest(name = "Enable async state = {0}")
    @ValueSource(booleans = {false, true})
    void testReadState(boolean asyncState) throws Exception {
        OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

        OperatorSubtaskState state =
                createOperatorSubtaskState(createFlatMap(asyncState), asyncState);
        OperatorState operatorState = new OperatorState(null, null, operatorID, 1, 128);
        operatorState.putState(0, state);

        KeyedStateInputFormat<?, ?, ?> format =
                new KeyedStateInputFormat<>(
                        operatorState,
                        new HashMapStateBackend(),
                        new Configuration(),
                        new KeyedStateReaderOperator<>(new ReaderFunction(), Types.INT),
                        new ExecutionConfig());
        KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];

        KeyedStateReaderFunction<Integer, Integer> userFunction = new ReaderFunction();

        List<Integer> data = readInputSplit(split, userFunction);

        Assert.assertEquals("Incorrect data read from input split", Arrays.asList(1, 2, 3), data);
    }

    @ParameterizedTest(name = "Enable async state = {0}")
    @ValueSource(booleans = {false, true})
    void testReadMultipleOutputPerKey(boolean asyncState) throws Exception {
        OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

        OperatorSubtaskState state =
                createOperatorSubtaskState(createFlatMap(asyncState), asyncState);
        OperatorState operatorState = new OperatorState(null, null, operatorID, 1, 128);
        operatorState.putState(0, state);

        KeyedStateInputFormat<?, ?, ?> format =
                new KeyedStateInputFormat<>(
                        operatorState,
                        new HashMapStateBackend(),
                        new Configuration(),
                        new KeyedStateReaderOperator<>(new ReaderFunction(), Types.INT),
                        new ExecutionConfig());
        KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];

        KeyedStateReaderFunction<Integer, Integer> userFunction = new DoubleReaderFunction();

        List<Integer> data = readInputSplit(split, userFunction);

        Assert.assertEquals(
                "Incorrect data read from input split", Arrays.asList(1, 1, 2, 2, 3, 3), data);
    }

    @ParameterizedTest(name = "Enable async state = {0}")
    @ValueSource(booleans = {false, true})
    void testInvalidProcessReaderFunctionFails(boolean asyncState) throws Exception {
        OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

        OperatorSubtaskState state =
                createOperatorSubtaskState(createFlatMap(asyncState), asyncState);
        OperatorState operatorState = new OperatorState(null, null, operatorID, 1, 128);
        operatorState.putState(0, state);

        KeyedStateInputFormat<?, ?, ?> format =
                new KeyedStateInputFormat<>(
                        operatorState,
                        new HashMapStateBackend(),
                        new Configuration(),
                        new KeyedStateReaderOperator<>(new ReaderFunction(), Types.INT),
                        new ExecutionConfig());
        KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];

        KeyedStateReaderFunction<Integer, Integer> userFunction = new InvalidReaderFunction();

        assertThatThrownBy(() -> readInputSplit(split, userFunction))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testReadTime() throws Exception {
        OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

        OperatorSubtaskState state =
                createOperatorSubtaskState(
                        new KeyedProcessOperator<>(new StatefulFunctionWithTime()), false);
        OperatorState operatorState = new OperatorState(null, null, operatorID, 1, 128);
        operatorState.putState(0, state);

        KeyedStateInputFormat<?, ?, ?> format =
                new KeyedStateInputFormat<>(
                        operatorState,
                        new HashMapStateBackend(),
                        new Configuration(),
                        new KeyedStateReaderOperator<>(new TimerReaderFunction(), Types.INT),
                        new ExecutionConfig());
        KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];

        KeyedStateReaderFunction<Integer, Integer> userFunction = new TimerReaderFunction();

        List<Integer> data = readInputSplit(split, userFunction);

        Assert.assertEquals(
                "Incorrect data read from input split", Arrays.asList(1, 1, 2, 2, 3, 3), data);
    }

    @Nonnull
    private List<Integer> readInputSplit(
            KeyGroupRangeInputSplit split, KeyedStateReaderFunction<Integer, Integer> userFunction)
            throws IOException {
        KeyedStateInputFormat<Integer, VoidNamespace, Integer> format =
                new KeyedStateInputFormat<>(
                        new OperatorState(null, null, OperatorIDGenerator.fromUid("uid"), 1, 4),
                        new HashMapStateBackend(),
                        new Configuration(),
                        new KeyedStateReaderOperator<>(userFunction, Types.INT),
                        new ExecutionConfig());

        List<Integer> data = new ArrayList<>();

        format.setRuntimeContext(new MockStreamingRuntimeContext(1, 0));

        format.openInputFormat();
        format.open(split);

        while (!format.reachedEnd()) {
            data.add(format.nextRecord(0));
        }

        format.close();
        format.closeInputFormat();

        data.sort(Comparator.comparingInt(id -> id));
        return data;
    }

    private OneInputStreamOperator<Integer, Void> createFlatMap(boolean asyncState) {
        return asyncState
                ? new AsyncStreamFlatMap<>(new AsyncStatefulFunction())
                : new StreamFlatMap<>(new StatefulFunction());
    }

    private OperatorSubtaskState createOperatorSubtaskState(
            OneInputStreamOperator<Integer, Void> operator, boolean async) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                async
                        ? AsyncKeyedOneInputStreamOperatorTestHarness.create(
                                operator, id -> id, Types.INT, 128, 1, 0)
                        : new KeyedOneInputStreamOperatorTestHarness<>(
                                operator, id -> id, Types.INT, 128, 1, 0)) {

            testHarness.setup(VoidSerializer.INSTANCE);
            testHarness.open();

            testHarness.processElement(1, 0);
            testHarness.processElement(2, 0);
            testHarness.processElement(3, 0);

            return testHarness.snapshot(0, 0);
        }
    }

    static class ReaderFunction extends KeyedStateReaderFunction<Integer, Integer> {
        ValueState<Integer> state;

        @Override
        public void open(OpenContext openContext) {
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void readKey(
                Integer key, KeyedStateReaderFunction.Context ctx, Collector<Integer> out)
                throws Exception {
            out.collect(state.value());
        }
    }

    static class DoubleReaderFunction extends KeyedStateReaderFunction<Integer, Integer> {
        ValueState<Integer> state;

        @Override
        public void open(OpenContext openContext) {
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void readKey(
                Integer key, KeyedStateReaderFunction.Context ctx, Collector<Integer> out)
                throws Exception {
            out.collect(state.value());
            out.collect(state.value());
        }
    }

    static class InvalidReaderFunction extends KeyedStateReaderFunction<Integer, Integer> {

        @Override
        public void open(OpenContext openContext) {
            getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void readKey(
                Integer key, KeyedStateReaderFunction.Context ctx, Collector<Integer> out)
                throws Exception {
            ValueState<Integer> state = getRuntimeContext().getState(stateDescriptor);
            out.collect(state.value());
        }
    }

    static class StatefulFunction extends RichFlatMapFunction<Integer, Void> {
        ValueState<Integer> state;

        @Override
        public void open(OpenContext openContext) {
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void flatMap(Integer value, Collector<Void> out) throws Exception {
            state.update(value);
        }
    }

    static class AsyncStatefulFunction extends RichFlatMapFunction<Integer, Void> {
        org.apache.flink.api.common.state.v2.ValueState<Integer> state;
        org.apache.flink.api.common.state.v2.ValueStateDescriptor<Integer> asyncStateDescriptor;

        @Override
        public void open(OpenContext openContext) {
            asyncStateDescriptor =
                    new org.apache.flink.api.common.state.v2.ValueStateDescriptor<>(
                            "state", Types.INT);
            state =
                    ((StreamingRuntimeContext) getRuntimeContext())
                            .getValueState(asyncStateDescriptor);
        }

        @Override
        public void flatMap(Integer value, Collector<Void> out) throws Exception {
            state.asyncUpdate(value);
        }
    }

    static class StatefulFunctionWithTime extends KeyedProcessFunction<Integer, Integer, Void> {
        ValueState<Integer> state;

        @Override
        public void open(OpenContext openContext) {
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<Void> out)
                throws Exception {
            state.update(value);
            ctx.timerService().registerEventTimeTimer(value);
            ctx.timerService().registerProcessingTimeTimer(value);
        }
    }

    static class TimerReaderFunction extends KeyedStateReaderFunction<Integer, Integer> {
        ValueState<Integer> state;

        @Override
        public void open(OpenContext openContext) {
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void readKey(
                Integer key, KeyedStateReaderFunction.Context ctx, Collector<Integer> out)
                throws Exception {
            Set<Long> eventTimers = ctx.registeredEventTimeTimers();
            Assert.assertEquals(
                    "Each key should have exactly one event timer for key " + key,
                    1,
                    eventTimers.size());

            out.collect(eventTimers.iterator().next().intValue());

            Set<Long> procTimers = ctx.registeredProcessingTimeTimers();
            Assert.assertEquals(
                    "Each key should have exactly one processing timer for key " + key,
                    1,
                    procTimers.size());

            out.collect(procTimers.iterator().next().intValue());
        }
    }
}
