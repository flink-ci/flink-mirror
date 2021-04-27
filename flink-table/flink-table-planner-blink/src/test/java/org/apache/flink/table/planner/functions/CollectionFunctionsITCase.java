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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.array;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.map;
import static org.apache.flink.table.api.Expressions.mapFromArrays;
import static org.apache.flink.table.api.Expressions.row;

/** Tests for collections {@link BuiltInFunctionDefinitions}. */
public class CollectionFunctionsITCase extends BuiltInFunctionTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.MAP_KEYS, "Null inputs")
                        .onFieldsWithData(null, null, "item")
                        .andDataTypes(
                                DataTypes.BOOLEAN().nullable(),
                                DataTypes.INT().nullable(),
                                DataTypes.STRING())
                        .testTableApiError(
                                call("MAP_KEYS", $("f0"), $("f1")),
                                "Invalid function call:\nMAP_KEYS(BOOLEAN, INT)")
                        .testResult(
                                map(
                                                $("f0").cast(DataTypes.BOOLEAN()),
                                                $("f1").cast(DataTypes.INT()))
                                        .mapKeys(),
                                "MAP_KEYS(map[cast(f0 as boolean), cast(f1 as int)])",
                                new Boolean[] {null},
                                DataTypes.ARRAY(DataTypes.BOOLEAN()))
                        .testTableApiResult(
                                map($("f2"), $("f1").cast(DataTypes.INT())).mapKeys(),
                                new String[] {"item"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testSqlResult(
                                "MAP_KEYS(map[cast(NULL as int), 'value'])",
                                new Integer[] {null},
                                DataTypes.ARRAY(DataTypes.INT())),
                TestSpec.forFunction(BuiltInFunctionDefinitions.MAP_KEYS)
                        .onFieldsWithData(1, "one", 2, "two")
                        .andDataTypes(
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.INT(),
                                DataTypes.STRING())
                        .testResult(
                                map($("f0"), $("f1"), $("f2"), $("f3")).mapKeys(),
                                "MAP_KEYS(map[f0, f1, f2, f3])",
                                new Integer[] {1, 2},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiResult(
                                map($("f1"), $("f0"), $("f3"), $("f2")).mapKeys(),
                                new String[] {"one", "two"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                map(map($("f0"), $("f1")), map($("f2"), $("f3"))).mapKeys(),
                                "MAP_KEYS(map[map[f0, f1], map[f2, f3]])",
                                new Map[] {ImmutableMap.of(1, "one")},
                                DataTypes.ARRAY(
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())
                                                .notNull()))
                        .testResult(
                                map(array($("f0"), $("f2")), array($("f1"), $("f3"))).mapKeys(),
                                "MAP_KEYS(map[array[f0, f2], array[f1, f3]])",
                                new Integer[][] {new Integer[] {1, 2}},
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()).notNull()))
                        .testResult(
                                map(
                                                row($("f0"), $("f1"))
                                                        .cast(
                                                                DataTypes.ROW(
                                                                        DataTypes.FIELD(
                                                                                "EXPR$0",
                                                                                DataTypes.INT()),
                                                                        DataTypes.FIELD(
                                                                                "EXPR$1",
                                                                                DataTypes
                                                                                        .STRING()))),
                                                map($("f2"), $("f3")))
                                        .mapKeys(),
                                "MAP_KEYS(map[row(f0, f1), map[f2, f3]])",
                                new Row[] {Row.of(1, "one")},
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                        DataTypes.FIELD("EXPR$0", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "EXPR$1", DataTypes.STRING()))
                                                .notNull())),
                TestSpec.forFunction(BuiltInFunctionDefinitions.MAP_VALUES, "Null inputs")
                        .onFieldsWithData(null, null, "item")
                        .andDataTypes(
                                DataTypes.BOOLEAN().nullable(),
                                DataTypes.INT().nullable(),
                                DataTypes.STRING())
                        .testTableApiError(
                                call("MAP_VALUES", $("f0"), $("f1")),
                                "Invalid function call:\nMAP_VALUES(BOOLEAN, INT)")
                        .testResult(
                                map(
                                                $("f0").cast(DataTypes.BOOLEAN()),
                                                $("f1").cast(DataTypes.INT()))
                                        .mapValues(),
                                "MAP_VALUES(map[cast(f0 as boolean), cast(f1 as int)])",
                                new Integer[] {null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiResult(
                                map($("f1").cast(DataTypes.INT()), $("f2")).mapValues(),
                                new String[] {"item"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testSqlResult(
                                "MAP_VALUES(map['key', cast(NULL as int)])",
                                new Integer[] {null},
                                DataTypes.ARRAY(DataTypes.INT())),
                TestSpec.forFunction(BuiltInFunctionDefinitions.MAP_VALUES)
                        .onFieldsWithData(1, "one", 2, "two")
                        .andDataTypes(
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.INT(),
                                DataTypes.STRING())
                        .testResult(
                                map($("f0"), $("f1"), $("f2"), $("f3")).mapValues(),
                                "MAP_VALUES(map[f0, f1, f2, f3])",
                                new String[] {"one", "two"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testTableApiResult(
                                map($("f1"), $("f0"), $("f3"), $("f2")).mapValues(),
                                new Integer[] {1, 2},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                map(map($("f0"), $("f1")), map($("f2"), $("f3"))).mapValues(),
                                "MAP_VALUES(map[map[f0, f1], map[f2, f3]])",
                                new Map[] {ImmutableMap.of(2, "two")},
                                DataTypes.ARRAY(
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())
                                                .notNull()))
                        .testResult(
                                map(array($("f0"), $("f2")), array($("f1"), $("f3"))).mapValues(),
                                "MAP_VALUES(map[array[f0, f2], array[f1, f3]])",
                                new String[][] {new String[] {"one", "two"}},
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING()).notNull()))
                        .testResult(
                                map(
                                                map($("f0"), $("f1")),
                                                row($("f2"), $("f3"))
                                                        .cast(
                                                                DataTypes.ROW(
                                                                        DataTypes.FIELD(
                                                                                "EXPR$0",
                                                                                DataTypes.INT()),
                                                                        DataTypes.FIELD(
                                                                                "EXPR$1",
                                                                                DataTypes
                                                                                        .STRING()))))
                                        .mapValues(),
                                "MAP_VALUES(map[map[f0, f1], row(f2, f3)])",
                                new Row[] {Row.of(2, "two")},
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                        DataTypes.FIELD("EXPR$0", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "EXPR$1", DataTypes.STRING()))
                                                .notNull())),
                TestSpec.forFunction(BuiltInFunctionDefinitions.MAP_FROM_ARRAYS, "Invalid input")
                        .onFieldsWithData(null, null, 1)
                        .andDataTypes(
                                DataTypes.BOOLEAN().nullable(),
                                DataTypes.INT().nullable(),
                                DataTypes.INT())
                        .testTableApiError(
                                mapFromArrays($("f0"), $("f1")),
                                "Invalid function call:\nMAP_FROM_ARRAYS(BOOLEAN, INT)")
                        .testTableApiError(
                                mapFromArrays(array($("f2")), array($("f2"), $("f2"))),
                                "Invalid function MAP_FROM_ARRAYS call:\n"
                                        + "The length of the keys array 1 is not equal to the length of the values array 2")
                        .testSqlError(
                                "MAP_FROM_ARRAYS(array[1], array[1, 2])",
                                "Invalid function MAP_FROM_ARRAYS call:\n"
                                        + "The length of the keys array 1 is not equal to the length of the values array 2")
                        .testSqlError(
                                "MAP_FROM_ARRAYS(array[1, 2, 3], array[1, 2])",
                                "Invalid function MAP_FROM_ARRAYS call:\n"
                                        + "The length of the keys array 3 is not equal to the length of the values array 2"),
                TestSpec.forFunction(BuiltInFunctionDefinitions.MAP_FROM_ARRAYS)
                        .onFieldsWithData(1, "one", 2, "two")
                        .andDataTypes(
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.INT(),
                                DataTypes.STRING())
                        .testTableApiResult(
                                call(
                                        "cardinality",
                                        mapFromArrays(
                                                        array($("f1"), $("f3")),
                                                        array($("f0"), $("f2")))
                                                .mapValues()),
                                2,
                                DataTypes.INT())
                        .testResult(
                                mapFromArrays(array($("f0"), $("f2")), array($("f1"), $("f3"))),
                                "MAP_FROM_ARRAYS(array[f0, f2], array[f1, f3])",
                                ImmutableMap.of(1, "one", 2, "two"),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        .testTableApiResult(
                                mapFromArrays(array($("f2"), $("f0")), array($("f3"), $("f1"))),
                                ImmutableMap.of(1, "one", 2, "two"),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        .testResult(
                                mapFromArrays(
                                        array(map($("f0"), $("f2"))), array(map($("f1"), $("f3")))),
                                "MAP_FROM_ARRAYS(array[map[f0, f2]], array[map[f1, f3]])",
                                ImmutableMap.of(
                                        ImmutableMap.of(1, 2), ImmutableMap.of("one", "two")),
                                DataTypes.MAP(
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.INT()).notNull(),
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())
                                                .notNull()))
                        .testResult(
                                mapFromArrays(array($("f0")), array(map($("f1"), $("f3")))),
                                "MAP_FROM_ARRAYS(array[f0], array[map[f1, f3]])",
                                ImmutableMap.of(1, ImmutableMap.of("one", "two")),
                                DataTypes.MAP(
                                        DataTypes.INT(),
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())
                                                .notNull())));
    }
}
