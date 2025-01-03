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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.junit.jupiter.api.{BeforeEach, Test}

/** Test for [[FlinkAggregateJoinTransposeRule]]. */
class FlinkAggregateJoinTransposeRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @BeforeEach
  def setup(): Unit = {
    val program = new FlinkChainedProgram[BatchOptimizeContext]()
    program.addLast(
      "rules",
      FlinkGroupProgramBuilder
        .newBuilder[BatchOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(RuleSets.ofList(AggregateReduceGroupingRule.INSTANCE))
            .build(),
          "reduce useless grouping"
        )
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(RuleSets.ofList(
              AggregateReduceGroupingRule.INSTANCE,
              CoreRules.FILTER_INTO_JOIN,
              CoreRules.JOIN_CONDITION_PUSH,
              CoreRules.FILTER_AGGREGATE_TRANSPOSE,
              CoreRules.FILTER_PROJECT_TRANSPOSE,
              CoreRules.FILTER_MERGE,
              CoreRules.AGGREGATE_PROJECT_MERGE,
              FlinkAggregateJoinTransposeRule.EXTENDED
            ))
            .build(),
          "aggregate join transpose"
        )
        .build()
    )
    util.replaceBatchProgram(program)

    util.addTableSource[(Int, Int, String)]("T", 'a, 'b, 'c)
    util.tableEnv.executeSql(s"""
                                |CREATE TABLE T2 (
                                |  a2 INT,
                                |  b2 INT PRIMARY KEY NOT ENFORCED,
                                |  c2 STRING
                                |) WITH (
                                |  'connector' = 'values',
                                |  'bounded' = 'true',
                                |  'enable-projection-push-down' = 'false'
                                |)
                                |""".stripMargin)
  }

  @Test
  def testPushCountAggThroughJoinOverUniqueColumn(): Unit = {
    util.verifyRelPlan(
      "SELECT COUNT(A.a) FROM (SELECT DISTINCT a FROM T) AS A JOIN T AS B ON A.a=B.a")
  }

  @Test
  def testPushSumAggThroughJoinOverUniqueColumn(): Unit = {
    util.verifyRelPlan(
      "SELECT SUM(A.a) FROM (SELECT DISTINCT a FROM T) AS A JOIN T AS B ON A.a=B.a")
  }

  @Test
  def testPushAggThroughJoinWithUniqueJoinKey(): Unit = {
    val sqlQuery =
      """
        |WITH T1 AS (SELECT a AS a1, COUNT(b) AS b1 FROM T GROUP BY a),
        |     T2 AS (SELECT COUNT(a) AS a2, b AS b2 FROM T GROUP BY b)
        |SELECT MIN(a1), MIN(b1), MIN(a2), MIN(b2), a, b, COUNT(c) FROM
        |  (SELECT * FROM T1, T2, T WHERE a1 = b2 AND a1 = a) t GROUP BY a, b
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testSomeAggCallColumnsAndJoinConditionColumnsIsSame(): Unit = {
    val sqlQuery =
      """
        |SELECT MIN(a2), MIN(b2), a, b, COUNT(c2) FROM
        |    (SELECT * FROM T2, T WHERE b2 = a) t GROUP BY a, b
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateWithAuxGroup_JoinKeyIsUnique1(): Unit = {
    val sqlQuery =
      """
        |SELECT a2, b2, c2, SUM(a) FROM (SELECT * FROM T2, T WHERE b2 = b) GROUP BY a2, b2, c2
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateWithAuxGroup_JoinKeyIsUnique2(): Unit = {
    val sqlQuery =
      """
        |SELECT a2, b2, c, SUM(a) FROM (SELECT * FROM T2, T WHERE b2 = b) GROUP BY a2, b2, c
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateWithAuxGroup_JoinKeyIsNotUnique1(): Unit = {
    val sqlQuery =
      """
        |SELECT a2, b2, c2, SUM(a) FROM (SELECT * FROM T2, T WHERE a2 = a) GROUP BY a2, b2, c2
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateWithAuxGroup_JoinKeyIsNotUnique2(): Unit = {
    val sqlQuery =
      """
        |SELECT a2, b2, c, SUM(a) FROM (SELECT * FROM T2, T WHERE a2 = a) GROUP BY a2, b2, c
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

}
