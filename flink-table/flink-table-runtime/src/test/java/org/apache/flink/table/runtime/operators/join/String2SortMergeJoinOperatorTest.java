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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTaskTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.join.String2HashJoinOperatorTest.MyProjection;
import org.apache.flink.table.runtime.operators.sort.StringNormalizedKeyComputer;
import org.apache.flink.table.runtime.operators.sort.StringRecordComparator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.runtime.operators.join.String2HashJoinOperatorTest.newRow;
import static org.apache.flink.table.runtime.operators.join.String2HashJoinOperatorTest.transformToBinary;

/** Test for {@link SortMergeJoinOperator}. */
@ExtendWith(ParameterizedTestExtension.class)
class String2SortMergeJoinOperatorTest {

    private boolean leftIsSmall;
    InternalTypeInfo<RowData> typeInfo =
            InternalTypeInfo.ofFields(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE);
    private InternalTypeInfo<RowData> joinedInfo =
            InternalTypeInfo.ofFields(
                    VarCharType.STRING_TYPE,
                    VarCharType.STRING_TYPE,
                    VarCharType.STRING_TYPE,
                    VarCharType.STRING_TYPE);

    String2SortMergeJoinOperatorTest(boolean leftIsSmall) {
        this.leftIsSmall = leftIsSmall;
    }

    @Parameters(name = "leftIsSmall = {0}")
    private static Collection<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    @TestTemplate
    void testInnerJoin() throws Exception {
        StreamOperator joinOperator = newOperator(FlinkJoinType.INNER, leftIsSmall);
        TwoInputStreamTaskTestHarness<BinaryRowData, BinaryRowData, JoinedRowData> testHarness =
                buildSortMergeJoin(joinOperator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(newRow("a", "02")));
        expectedOutput.add(new StreamRecord<>(newRow("b", "14")));
        testHarness.waitForTaskCompletion();
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.",
                expectedOutput,
                transformToBinary(testHarness.getOutput()));
    }

    @TestTemplate
    void testLeftOuterJoin() throws Exception {
        StreamOperator joinOperator = newOperator(FlinkJoinType.LEFT, leftIsSmall);
        TwoInputStreamTaskTestHarness<BinaryRowData, BinaryRowData, JoinedRowData> testHarness =
                buildSortMergeJoin(joinOperator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(newRow("a", "02")));
        expectedOutput.add(new StreamRecord<>(newRow("b", "14")));
        expectedOutput.add(new StreamRecord<>(newRow("d", "0null")));
        testHarness.waitForTaskCompletion();
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.",
                expectedOutput,
                transformToBinary(testHarness.getOutput()));
    }

    @TestTemplate
    void testRightOuterJoin() throws Exception {
        StreamOperator joinOperator = newOperator(FlinkJoinType.RIGHT, leftIsSmall);
        TwoInputStreamTaskTestHarness<BinaryRowData, BinaryRowData, JoinedRowData> testHarness =
                buildSortMergeJoin(joinOperator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(newRow("a", "02")));
        expectedOutput.add(new StreamRecord<>(newRow("b", "14")));
        expectedOutput.add(new StreamRecord<>(newRow("c", "2null")));
        testHarness.waitForTaskCompletion();
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.",
                expectedOutput,
                transformToBinary(testHarness.getOutput()));
    }

    @TestTemplate
    void testFullJoin() throws Exception {
        StreamOperator joinOperator = newOperator(FlinkJoinType.FULL, leftIsSmall);
        TwoInputStreamTaskTestHarness<BinaryRowData, BinaryRowData, JoinedRowData> testHarness =
                buildSortMergeJoin(joinOperator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(newRow("a", "02")));
        expectedOutput.add(new StreamRecord<>(newRow("b", "14")));
        expectedOutput.add(new StreamRecord<>(newRow("c", "2null")));
        expectedOutput.add(new StreamRecord<>(newRow("d", "0null")));

        testHarness.waitForTaskCompletion();
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.",
                expectedOutput,
                transformToBinary(testHarness.getOutput()));
    }

    private TwoInputStreamTaskTestHarness<BinaryRowData, BinaryRowData, JoinedRowData>
            buildSortMergeJoin(StreamOperator operator) throws Exception {
        final TwoInputStreamTaskTestHarness<BinaryRowData, BinaryRowData, JoinedRowData>
                testHarness =
                        new TwoInputStreamTaskTestHarness<>(
                                TwoInputStreamTask::new,
                                2,
                                2,
                                new int[] {1, 2},
                                typeInfo,
                                (TypeInformation) typeInfo,
                                joinedInfo);

        testHarness.memorySize = 36 * 1024 * 1024;
        testHarness.setupOutputForSingletonOperatorChain();
        testHarness.getStreamConfig().setStreamOperator(operator);
        testHarness.getStreamConfig().setOperatorID(new OperatorID());
        testHarness
                .getStreamConfig()
                .setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 0.99);

        long initialTime = 0L;

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        testHarness.processElement(new StreamRecord<>(newRow("a", "0"), initialTime), 0, 0);
        testHarness.processElement(new StreamRecord<>(newRow("d", "0"), initialTime), 0, 0);
        testHarness.processElement(new StreamRecord<>(newRow("a", "2"), initialTime), 1, 1);
        testHarness.processElement(new StreamRecord<>(newRow("b", "1"), initialTime), 0, 1);
        testHarness.processElement(new StreamRecord<>(newRow("c", "2"), initialTime), 1, 1);
        testHarness.processElement(new StreamRecord<>(newRow("b", "4"), initialTime), 1, 0);
        testHarness.waitForInputProcessing();

        testHarness.endInput();
        return testHarness;
    }

    static StreamOperator newOperator(FlinkJoinType type, boolean leftIsSmaller) {
        int maxNumFileHandles =
                ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES.defaultValue();
        boolean compressionEnable =
                ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED.defaultValue();
        int compressionBlockSize =
                (int)
                        ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE
                                .defaultValue()
                                .getBytes();
        boolean asyncMergeEnable =
                ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED.defaultValue();
        SortMergeJoinFunction sortMergeJoinFunction =
                new SortMergeJoinFunction(
                        0,
                        type,
                        leftIsSmaller,
                        maxNumFileHandles,
                        compressionEnable,
                        compressionBlockSize,
                        asyncMergeEnable,
                        new GeneratedJoinCondition("", "", new Object[0]) {
                            @Override
                            public JoinCondition newInstance(ClassLoader classLoader) {
                                return new Int2HashJoinOperatorTest.TrueCondition();
                            }
                        },
                        new GeneratedProjection("", "", new Object[0]) {
                            @Override
                            public Projection newInstance(ClassLoader classLoader) {
                                return new MyProjection();
                            }
                        },
                        new GeneratedProjection("", "", new Object[0]) {
                            @Override
                            public Projection newInstance(ClassLoader classLoader) {
                                return new MyProjection();
                            }
                        },
                        new GeneratedNormalizedKeyComputer("", "") {
                            @Override
                            public NormalizedKeyComputer newInstance(ClassLoader classLoader) {
                                return new StringNormalizedKeyComputer();
                            }
                        },
                        new GeneratedRecordComparator("", "", new Object[0]) {
                            @Override
                            public RecordComparator newInstance(ClassLoader classLoader) {
                                return new StringRecordComparator();
                            }
                        },
                        new GeneratedNormalizedKeyComputer("", "") {
                            @Override
                            public NormalizedKeyComputer newInstance(ClassLoader classLoader) {
                                return new StringNormalizedKeyComputer();
                            }
                        },
                        new GeneratedRecordComparator("", "", new Object[0]) {
                            @Override
                            public RecordComparator newInstance(ClassLoader classLoader) {
                                return new StringRecordComparator();
                            }
                        },
                        new GeneratedRecordComparator("", "", new Object[0]) {
                            @Override
                            public RecordComparator newInstance(ClassLoader classLoader) {
                                return new StringRecordComparator();
                            }
                        },
                        new boolean[] {true});
        return new SortMergeJoinOperator(sortMergeJoinFunction);
    }
}
