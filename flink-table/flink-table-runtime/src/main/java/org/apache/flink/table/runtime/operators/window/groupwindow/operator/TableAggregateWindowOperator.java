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

package org.apache.flink.table.runtime.operators.window.groupwindow.operator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceTableAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceTableAggsHandleFunction;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.Trigger;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import java.time.ZoneId;

/**
 * A {@link WindowOperator} for grouped and windowed table aggregates.
 *
 * <p>When an element arrives it gets assigned a key using a {@link KeySelector} and it gets
 * assigned to zero or more windows using a {@link GroupWindowAssigner}. Based on this, the element
 * is put into panes. A pane is the bucket of elements that have the same key and same {@code
 * Window}. An element can be in multiple panes if it was assigned to multiple windows by the {@code
 * WindowAssigner}.
 *
 * <p>Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires, the given
 * {@link NamespaceTableAggsHandleFunction#emitValue(Object, RowData, Collector)} is invoked to
 * produce the results that are emitted for the pane to which the {@code Trigger} belongs.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class TableAggregateWindowOperator<K, W extends Window> extends WindowOperator<K, W> {

    private static final long serialVersionUID = 1L;

    private NamespaceTableAggsHandleFunction<W> tableAggWindowAggregator;
    private GeneratedNamespaceTableAggsHandleFunction<W> generatedTableAggWindowAggregator;

    TableAggregateWindowOperator(
            NamespaceTableAggsHandleFunction<W> windowTableAggregator,
            GroupWindowAssigner<W> windowAssigner,
            Trigger<W> trigger,
            TypeSerializer<W> windowSerializer,
            LogicalType[] inputFieldTypes,
            LogicalType[] accumulatorTypes,
            LogicalType[] aggResultTypes,
            LogicalType[] windowPropertyTypes,
            int rowtimeIndex,
            boolean produceUpdates,
            long allowedLateness,
            ZoneId shiftTimeZone,
            int inputCountIndex) {
        super(
                windowTableAggregator,
                windowAssigner,
                trigger,
                windowSerializer,
                inputFieldTypes,
                accumulatorTypes,
                aggResultTypes,
                windowPropertyTypes,
                rowtimeIndex,
                produceUpdates,
                allowedLateness,
                shiftTimeZone,
                inputCountIndex);
        this.tableAggWindowAggregator = windowTableAggregator;
    }

    TableAggregateWindowOperator(
            GeneratedNamespaceTableAggsHandleFunction<W> generatedTableAggWindowAggregator,
            GroupWindowAssigner<W> windowAssigner,
            Trigger<W> trigger,
            TypeSerializer<W> windowSerializer,
            LogicalType[] inputFieldTypes,
            LogicalType[] accumulatorTypes,
            LogicalType[] aggResultTypes,
            LogicalType[] windowPropertyTypes,
            int rowtimeIndex,
            boolean sendRetraction,
            long allowedLateness,
            ZoneId shiftTimeZone,
            int inputCountIndex) {
        super(
                windowAssigner,
                trigger,
                windowSerializer,
                inputFieldTypes,
                accumulatorTypes,
                aggResultTypes,
                windowPropertyTypes,
                rowtimeIndex,
                sendRetraction,
                allowedLateness,
                shiftTimeZone,
                inputCountIndex);
        this.generatedTableAggWindowAggregator = generatedTableAggWindowAggregator;
    }

    @Override
    protected void compileGeneratedCode() {
        if (generatedTableAggWindowAggregator != null) {
            tableAggWindowAggregator =
                    generatedTableAggWindowAggregator.newInstance(
                            getRuntimeContext().getUserCodeClassLoader());
            windowAggregator = tableAggWindowAggregator;
        }
    }

    @Override
    protected void emitWindowResult(W window) throws Exception {
        windowFunction.prepareAggregateAccumulatorForEmit(window);
        RowData acc = tableAggWindowAggregator.getAccumulators();
        if (!recordCounter.recordCountIsZero(acc)) {
            tableAggWindowAggregator.emitValue(window, (RowData) getCurrentKey(), collector);
        }
    }
}
