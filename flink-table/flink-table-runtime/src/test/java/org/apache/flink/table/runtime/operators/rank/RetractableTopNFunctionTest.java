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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.TestTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/** Tests for {@link RetractableTopNFunction}. */
public class RetractableTopNFunctionTest extends TopNFunctionTestBase {

    @Override
    protected AbstractTopNFunction createFunction(
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber,
            boolean enableAsyncState) {
        return new RetractableTopNFunction(
                ttlConfig,
                inputRowType,
                comparableRecordComparator,
                sortKeySelector,
                rankType,
                rankRange,
                generatedEqualiser,
                generateUpdateBefore,
                outputRankNumber);
    }

    @Override
    boolean supportedAsyncState() {
        return false;
    }

    @TestTemplate
    void testProcessRetractMessageWithNotGenerateUpdateBefore() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 4L, 11));
        testHarness.processElement(updateBeforeRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 5L, 11));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        testHarness.processElement(insertRecord("fruit", 3L, 44));
        testHarness.processElement(insertRecord("fruit", 5L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        // ("book", 1L, 12)
        // ("book", 2L, 19)
        expectedOutput.add(insertRecord("book", 1L, 12, 1L));
        expectedOutput.add(insertRecord("book", 2L, 19, 2L));
        // ("book", 4L, 11)
        expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
        expectedOutput.add(updateAfterRecord("book", 1L, 12, 2L));
        // UB ("book", 1L, 12)
        expectedOutput.add(updateAfterRecord("book", 2L, 19, 2L));
        // ("book", 5L, 11)
        expectedOutput.add(updateAfterRecord("book", 5L, 11, 2L));
        // ("fruit", 4L, 33)
        // ("fruit", 3L, 44)
        expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(insertRecord("fruit", 3L, 44, 2L));
        // ("fruit", 5L, 22)
        expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 4L, 33, 2L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testProcessRetractMessageWithGenerateUpdateBefore() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 4L, 11));
        testHarness.processElement(updateBeforeRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 5L, 11));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        testHarness.processElement(insertRecord("fruit", 3L, 44));
        testHarness.processElement(insertRecord("fruit", 5L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        // ("book", 1L, 12)
        // ("book", 2L, 19)
        expectedOutput.add(insertRecord("book", 1L, 12, 1L));
        expectedOutput.add(insertRecord("book", 2L, 19, 2L));
        // ("book", 4L, 11)
        expectedOutput.add(updateBeforeRecord("book", 1L, 12, 1L));
        expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19, 2L));
        expectedOutput.add(updateAfterRecord("book", 1L, 12, 2L));
        // UB ("book", 1L, 12)
        expectedOutput.add(updateBeforeRecord("book", 1L, 12, 2L));
        expectedOutput.add(updateAfterRecord("book", 2L, 19, 2L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19, 2L));
        expectedOutput.add(updateAfterRecord("book", 5L, 11, 2L));
        // ("fruit", 4L, 33)
        // ("fruit", 3L, 44)
        expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(insertRecord("fruit", 3L, 44, 2L));
        // ("fruit", 5L, 22)
        expectedOutput.add(updateBeforeRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
        expectedOutput.add(updateBeforeRecord("fruit", 3L, 44, 2L));
        expectedOutput.add(updateAfterRecord("fruit", 4L, 33, 2L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testConstantRankRangeWithoutOffsetWithRowNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 4L, 11));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        testHarness.processElement(insertRecord("fruit", 3L, 44));
        testHarness.processElement(insertRecord("fruit", 5L, 22));

        List<Object> expectedOutput = new ArrayList<>();
        // ("book", 1L, 12)
        // ("book", 2L, 19)
        expectedOutput.add(insertRecord("book", 1L, 12, 1L));
        expectedOutput.add(insertRecord("book", 2L, 19, 2L));
        // ("book", 4L, 11)
        expectedOutput.add(updateBeforeRecord("book", 1L, 12, 1L));
        expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19, 2L));
        expectedOutput.add(updateAfterRecord("book", 1L, 12, 2L));
        // ("fruit", 4L, 33)
        // ("fruit", 3L, 44)
        expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(insertRecord("fruit", 3L, 44, 2L));
        // ("fruit", 5L, 22)
        expectedOutput.add(updateBeforeRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
        expectedOutput.add(updateBeforeRecord("fruit", 3L, 44, 2L));
        expectedOutput.add(updateAfterRecord("fruit", 4L, 33, 2L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());

        // do a snapshot, data could be recovered from state
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();
        expectedOutput.clear();

        func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, true);
        testHarness = createTestHarness(func);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 10));

        expectedOutput.add(updateBeforeRecord("book", 4L, 11, 1L));
        expectedOutput.add(updateAfterRecord("book", 1L, 10, 1L));
        expectedOutput.add(updateBeforeRecord("book", 1L, 12, 2L));
        expectedOutput.add(updateAfterRecord("book", 4L, 11, 2L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @TestTemplate
    void testConstantRankRangeWithoutOffsetWithoutRowNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 4L, 11));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        testHarness.processElement(insertRecord("fruit", 3L, 44));
        testHarness.processElement(insertRecord("fruit", 5L, 22));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(deleteRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("book", 4L, 11));
        expectedOutput.add(insertRecord("fruit", 4L, 33));
        expectedOutput.add(insertRecord("fruit", 3L, 44));
        expectedOutput.add(deleteRecord("fruit", 3L, 44));
        expectedOutput.add(insertRecord("fruit", 5L, 22));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());

        // do a snapshot, data could be recovered from state
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();
        expectedOutput.clear();

        func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, false);
        testHarness = createTestHarness(func);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 10));

        expectedOutput.add(deleteRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 1L, 10));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @TestTemplate
    void testVariableRankRangeWithRowNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("fruit", 1L, 33));
        testHarness.processElement(insertRecord("fruit", 1L, 44));
        testHarness.processElement(insertRecord("fruit", 1L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        // ("book", 2L, 12)
        // ("book", 2L, 19)
        expectedOutput.add(insertRecord("book", 2L, 12, 1L));
        expectedOutput.add(insertRecord("book", 2L, 19, 2L));
        // ("book", 2L, 11)
        expectedOutput.add(updateBeforeRecord("book", 2L, 12, 1L));
        expectedOutput.add(updateAfterRecord("book", 2L, 11, 1L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19, 2L));
        expectedOutput.add(updateAfterRecord("book", 2L, 12, 2L));
        // ("fruit", 1L, 33)
        expectedOutput.add(insertRecord("fruit", 1L, 33, 1L));

        // ("fruit", 1L, 44)
        // nothing, because it's Top-1

        // ("fruit", 1L, 22)
        expectedOutput.add(updateBeforeRecord("fruit", 1L, 33, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 1L, 22, 1L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testVariableRankRangeWithoutRowNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("fruit", 1L, 33));
        testHarness.processElement(insertRecord("fruit", 1L, 44));
        testHarness.processElement(insertRecord("fruit", 1L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 12));
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(deleteRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("book", 2L, 11));
        expectedOutput.add(insertRecord("fruit", 1L, 33));
        expectedOutput.add(deleteRecord("fruit", 1L, 33));
        expectedOutput.add(insertRecord("fruit", 1L, 22));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testDisableGenerateUpdateBeforeWithRowNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 4L, 11));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        testHarness.processElement(insertRecord("fruit", 3L, 44));
        testHarness.processElement(insertRecord("fruit", 5L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        // ("book", 1L, 12)
        // ("book", 2L, 19)
        expectedOutput.add(insertRecord("book", 1L, 12, 1L));
        expectedOutput.add(insertRecord("book", 2L, 19, 2L));
        // ("book", 4L, 11)
        expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
        expectedOutput.add(updateAfterRecord("book", 1L, 12, 2L));
        // ("fruit", 4L, 33)
        // ("fruit", 3L, 44)
        expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(insertRecord("fruit", 3L, 44, 2L));
        // ("fruit", 5L, 22)
        expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 4L, 33, 2L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testDisableGenerateUpdateBeforeWithoutRowNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 4L, 11));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        testHarness.processElement(insertRecord("fruit", 3L, 44));
        testHarness.processElement(insertRecord("fruit", 5L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(deleteRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("book", 4L, 11));
        expectedOutput.add(insertRecord("fruit", 4L, 33));
        expectedOutput.add(insertRecord("fruit", 3L, 44));
        expectedOutput.add(deleteRecord("fruit", 3L, 44));
        expectedOutput.add(insertRecord("fruit", 5L, 22));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testCleanIdleState() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        // register cleanup timer with 20L
        testHarness.setStateTtlProcessingTime(0L);
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("fruit", 5L, 22));

        // register cleanup timer with 29L
        testHarness.setStateTtlProcessingTime(9_000_000L);
        testHarness.processElement(updateBeforeRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("fruit", 4L, 11));

        // trigger the first cleanup timer and register cleanup timer with 4000
        testHarness.setStateTtlProcessingTime(20_000_000L);
        testHarness.processElement(insertRecord("fruit", 8L, 100));
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        // ("book", 1L, 12)
        // ("fruit", 5L, 22)
        expectedOutput.add(insertRecord("book", 1L, 12, 1L));
        expectedOutput.add(insertRecord("fruit", 5L, 22, 1L));
        // UB ("book", 1L, 12)
        expectedOutput.add(deleteRecord("book", 1L, 12, 1L));
        // ("fruit", 4L, 11)
        expectedOutput.add(updateBeforeRecord("fruit", 5L, 22, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 4L, 11, 1L));
        expectedOutput.add(insertRecord("fruit", 5L, 22, 2L));

        // after idle state expired
        // ("fruit", 8L, 100)
        // ("book", 1L, 12)
        expectedOutput.add(insertRecord("fruit", 8L, 100, 1L));
        expectedOutput.add(insertRecord("book", 1L, 12, 1L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testConstantRankRangeWithoutRowNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 3), false, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("a", 1L, 1));
        testHarness.processElement(insertRecord("a", 2L, 2));
        testHarness.processElement(insertRecord("a", 3L, 2));
        testHarness.processElement(insertRecord("a", 4L, 2));
        testHarness.processElement(insertRecord("a", 5L, 3));
        testHarness.processElement(insertRecord("a", 6L, 4));
        testHarness.processElement(updateBeforeRecord("a", 2L, 2));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("a", 1L, 1));
        expectedOutput.add(insertRecord("a", 2L, 2));
        expectedOutput.add(insertRecord("a", 3L, 2));
        expectedOutput.add(deleteRecord("a", 2L, 2));
        expectedOutput.add(insertRecord("a", 4L, 2));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testConstantRankRangeWithRowNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 3), false, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("a", 1L, 1));
        testHarness.processElement(insertRecord("a", 2L, 2));
        testHarness.processElement(insertRecord("a", 3L, 2));
        testHarness.processElement(insertRecord("a", 4L, 2));
        testHarness.processElement(insertRecord("a", 5L, 3));
        testHarness.processElement(insertRecord("a", 6L, 4));
        testHarness.processElement(updateBeforeRecord("a", 2L, 2));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("a", 1L, 1, 1L));
        expectedOutput.add(insertRecord("a", 2L, 2, 2L));
        expectedOutput.add(insertRecord("a", 3L, 2, 3L));
        expectedOutput.add(updateAfterRecord("a", 3L, 2, 2L));
        expectedOutput.add(updateAfterRecord("a", 4L, 2, 3L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testRetractRecordOutOfRankRangeWithoutRowNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("a", 1L, 1));
        testHarness.processElement(insertRecord("a", 2L, 2));
        testHarness.processElement(insertRecord("a", 3L, 2));
        testHarness.processElement(insertRecord("a", 4L, 4));
        testHarness.processElement(insertRecord("a", 5L, 4));

        // delete records from out of rank range
        testHarness.processElement(deleteRecord("a", 4L, 4));
        testHarness.processElement(deleteRecord("a", 1L, 1));
        testHarness.processElement(deleteRecord("a", 2L, 2));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("a", 1L, 1));
        expectedOutput.add(insertRecord("a", 2L, 2));
        expectedOutput.add(deleteRecord("a", 1L, 1));
        expectedOutput.add(insertRecord("a", 3L, 2));
        expectedOutput.add(deleteRecord("a", 2L, 2));
        expectedOutput.add(insertRecord("a", 5L, 4));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testRetractRecordOutOfRankRangeWithRowNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("a", 1L, 1));
        testHarness.processElement(insertRecord("a", 2L, 2));
        testHarness.processElement(insertRecord("a", 3L, 2));
        testHarness.processElement(insertRecord("a", 4L, 4));
        testHarness.processElement(insertRecord("a", 5L, 4));

        // delete records from out of rank range
        testHarness.processElement(deleteRecord("a", 4L, 4));
        testHarness.processElement(deleteRecord("a", 1L, 1));
        testHarness.processElement(deleteRecord("a", 2L, 2));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("a", 1L, 1, 1L));
        expectedOutput.add(insertRecord("a", 2L, 2, 2L));
        expectedOutput.add(updateAfterRecord("a", 2L, 2, 1L));
        expectedOutput.add(updateAfterRecord("a", 3L, 2, 2L));
        expectedOutput.add(updateAfterRecord("a", 3L, 2, 1L));
        expectedOutput.add(updateAfterRecord("a", 5L, 4, 2L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testRetractAndThenDeleteRecordWithoutRowNumber() throws Exception {
        AbstractTopNFunction func =
                new RetractableTopNFunction(
                        ttlConfig,
                        InternalTypeInfo.ofFields(
                                VarCharType.STRING_TYPE,
                                new BigIntType(),
                                new IntType(),
                                new IntType()),
                        comparableRecordComparator,
                        sortKeySelector,
                        RankType.ROW_NUMBER,
                        new ConstantRankRange(1, 1),
                        generatedEqualiser,
                        true,
                        false);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("a", 1L, 10, 0));
        testHarness.processElement(insertRecord("a", 1L, 9, 0));
        testHarness.processElement(deleteRecord("a", 1L, 10, 0));
        testHarness.processElement(deleteRecord("a", 1L, 9, 0));
        testHarness.processElement(insertRecord("a", 1L, 10, 1));
        testHarness.processElement(insertRecord("a", 1L, 9, 1));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("a", 1L, 10, 0));
        expectedOutput.add(deleteRecord("a", 1L, 10, 0));
        expectedOutput.add(insertRecord("a", 1L, 9, 0));
        expectedOutput.add(deleteRecord("a", 1L, 9, 0));
        expectedOutput.add(insertRecord("a", 1L, 10, 1));
        expectedOutput.add(deleteRecord("a", 1L, 10, 1));
        expectedOutput.add(insertRecord("a", 1L, 9, 1));

        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testRetractAnStaledRecordWithRowNumber() throws Exception {
        StateTtlConfig ttlConfig = StateConfigUtil.createTtlConfig(1_000);
        AbstractTopNFunction func =
                new RetractableTopNFunction(
                        ttlConfig,
                        InternalTypeInfo.ofFields(
                                VarCharType.STRING_TYPE, new BigIntType(), new IntType()),
                        comparableRecordComparator,
                        sortKeySelector,
                        RankType.ROW_NUMBER,
                        new ConstantRankRange(1, 2),
                        generatedEqualiser,
                        true,
                        true);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.setStateTtlProcessingTime(0);
        testHarness.processElement(insertRecord("a", 1L, 10));
        testHarness.setStateTtlProcessingTime(1001);
        testHarness.processElement(insertRecord("a", 2L, 11));
        testHarness.processElement(deleteRecord("a", 1L, 10));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("a", 1L, 10, 1L));
        expectedOutput.add(insertRecord("a", 2L, 11, 1L));
        // the following delete record should not be sent because the left row is null which is
        // illegal.
        // -D{row1=null, row2=+I(1)};

        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }
}
