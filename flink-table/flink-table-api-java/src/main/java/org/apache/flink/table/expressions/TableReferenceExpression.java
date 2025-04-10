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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.operations.OperationUtils;
import org.apache.flink.table.operations.PartitionQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Expression that references another table.
 *
 * <p>This is a pure API expression that is translated into uncorrelated sub-queries by the planner.
 */
@PublicEvolving
public final class TableReferenceExpression implements ResolvedExpression {

    private final String name;
    private final QueryOperation queryOperation;
    // The environment is optional but serves validation purposes
    // to ensure that all referenced tables belong to the same
    // environment.
    private final @Nullable TableEnvironment env;

    @Deprecated
    TableReferenceExpression(String name, QueryOperation queryOperation) {
        this.name = Preconditions.checkNotNull(name);
        this.queryOperation = Preconditions.checkNotNull(queryOperation);
        this.env = null;
    }

    TableReferenceExpression(String name, QueryOperation queryOperation, TableEnvironment env) {
        this.name = Preconditions.checkNotNull(name);
        this.queryOperation = Preconditions.checkNotNull(queryOperation);
        this.env = Preconditions.checkNotNull(env);
    }

    public String getName() {
        return name;
    }

    public QueryOperation getQueryOperation() {
        return queryOperation;
    }

    public @Nullable TableEnvironment getTableEnvironment() {
        return env;
    }

    @Override
    public DataType getOutputDataType() {
        return DataTypeUtils.fromResolvedSchemaPreservingTimeAttributes(
                queryOperation.getResolvedSchema());
    }

    @Override
    public List<ResolvedExpression> getResolvedChildren() {
        return Collections.emptyList();
    }

    @Override
    public String asSerializableString(SqlFactory sqlFactory) {
        if (queryOperation instanceof PartitionQueryOperation) {
            return OperationUtils.indent(queryOperation.asSerializableString(sqlFactory));
        }
        return String.format(
                "(%s\n)", OperationUtils.indent(queryOperation.asSerializableString(sqlFactory)));
    }

    @Override
    public String asSummaryString() {
        return name;
    }

    @Override
    public List<Expression> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
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
        TableReferenceExpression that = (TableReferenceExpression) o;
        return Objects.equals(name, that.name)
                && Objects.equals(queryOperation, that.queryOperation)
                // Effectively means reference equality
                && Objects.equals(env, that.env);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, queryOperation, env);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
