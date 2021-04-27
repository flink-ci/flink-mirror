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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link InputTypeStrategy} specific for {@link BuiltInFunctionDefinitions#MAP_FROM_ARRAYS}.
 *
 * <p>It expects exactly two array arguments. The length of input arrays must be the same.
 */
@Internal
public class MapFromArraysInputTypeStrategy implements InputTypeStrategy {
    private static final ArgumentCount TWO =
            new ArgumentCount() {
                @Override
                public boolean isValidCount(int count) {
                    return count == 2;
                }

                @Override
                public Optional<Integer> getMinCount() {
                    return Optional.of(2);
                }

                @Override
                public Optional<Integer> getMaxCount() {
                    return Optional.of(2);
                }
            };

    @Override
    public ArgumentCount getArgumentCount() {
        return TWO;
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        if (argumentDataTypes.size() != 2) {
            return Optional.empty();
        }

        LogicalType keyLogicalType = argumentDataTypes.get(0).getLogicalType();
        LogicalType valueLogicalType = argumentDataTypes.get(1).getLogicalType();

        if (keyLogicalType.getTypeRoot() != LogicalTypeRoot.ARRAY
                || valueLogicalType.getTypeRoot() != LogicalTypeRoot.ARRAY) {
            return Optional.empty();
        }

        DataType keyType = TypeConversions.fromLogicalToDataType(keyLogicalType);
        DataType valueType = TypeConversions.fromLogicalToDataType(valueLogicalType);
        return Optional.of(ImmutableList.of(keyType, valueType));
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(Signature.of(Signature.Argument.of("*")));
    }
}
