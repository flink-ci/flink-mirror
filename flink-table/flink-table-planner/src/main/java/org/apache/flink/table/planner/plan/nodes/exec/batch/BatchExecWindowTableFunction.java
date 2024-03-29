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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;

/** Batch {@link ExecNode} for window table-valued function. */
public class BatchExecWindowTableFunction extends CommonExecWindowTableFunction
        implements BatchExecNode<RowData> {

    public BatchExecWindowTableFunction(
            ReadableConfig tableConfig,
            TimeAttributeWindowingStrategy windowingStrategy,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecWindowTableFunction.class),
                ExecNodeContext.newPersistedConfig(BatchExecWindowTableFunction.class, tableConfig),
                windowingStrategy,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (windowingStrategy.isProctime()) {
            throw new TableException("Processing time Window TableFunction is not supported yet.");
        }
        return super.translateToPlanInternal(planner, config);
    }

    @Override
    protected Transformation<RowData> translateWithUnalignedWindow(
            PlannerBase planner,
            ExecNodeConfig config,
            RowType inputRowType,
            Transformation<RowData> inputTransform) {
        throw new TableException(
                "Unaligned windows like session are not supported in batch mode yet.");
    }
}
