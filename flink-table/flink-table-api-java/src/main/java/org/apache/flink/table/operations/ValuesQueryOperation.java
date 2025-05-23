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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Table operation that computes new table using given {@link Expression}s from its input relational
 * operation.
 */
@Internal
public class ValuesQueryOperation implements QueryOperation {

    private static final String INPUT_ALIAS = "$$T_VAL";
    private final List<List<ResolvedExpression>> values;
    private final ResolvedSchema resolvedSchema;

    public ValuesQueryOperation(
            List<List<ResolvedExpression>> values, ResolvedSchema resolvedSchema) {
        this.values = values;
        this.resolvedSchema = resolvedSchema;
    }

    public List<List<ResolvedExpression>> getValues() {
        return values;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("values", values);

        return OperationUtils.formatWithChildren(
                "Values", args, getChildren(), Operation::asSummaryString);
    }

    @Override
    public String asSerializableString(SqlFactory sqlFactory) {
        return String.format(
                "SELECT %s FROM (VALUES %s\n) %s(%s)",
                OperationUtils.formatSelectColumns(resolvedSchema, INPUT_ALIAS),
                OperationUtils.indent(
                        values.stream()
                                .map(
                                        row ->
                                                row.stream()
                                                        .map(
                                                                resolvedExpression ->
                                                                        resolvedExpression
                                                                                .asSerializableString(
                                                                                        sqlFactory))
                                                        .collect(
                                                                Collectors.joining(", ", "(", ")")))
                                .collect(Collectors.joining(",\n"))),
                INPUT_ALIAS,
                OperationUtils.formatSelectColumns(resolvedSchema, null));
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ValuesQueryOperation that = (ValuesQueryOperation) o;
        return Objects.equals(values, that.values)
                && Objects.equals(resolvedSchema, that.resolvedSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, resolvedSchema);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
