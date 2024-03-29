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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.state.changelog.restore.ChangelogApplierFactory;
import org.apache.flink.state.changelog.restore.StateChangeApplier;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;

/**
 * Delegated partitioned {@link ValueState} that forwards changes to {@link StateChange} upon {@link
 * ValueState} is updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
class ChangelogValueState<K, N, V>
        extends AbstractChangelogState<K, N, V, InternalValueState<K, N, V>>
        implements InternalValueState<K, N, V> {

    private final InternalKeyContext<K> keyContext;

    ChangelogValueState(
            InternalValueState<K, N, V> delegatedState,
            KvStateChangeLogger<V, N> changeLogger,
            InternalKeyContext<K> keyContext) {
        super(delegatedState, changeLogger);
        this.keyContext = keyContext;
    }

    @Override
    public V value() throws IOException {
        return delegatedState.value();
    }

    @Override
    public void update(V value) throws IOException {
        delegatedState.update(value);
        changeLogger.valueUpdated(value, getCurrentNamespace());
    }

    @Override
    public void clear() {
        delegatedState.clear();
        try {
            changeLogger.valueCleared(getCurrentNamespace());
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            InternalKvState<K, N, SV> valueState,
            KvStateChangeLogger<SV, N> changeLogger,
            InternalKeyContext<K> keyContext) {
        return (IS)
                new ChangelogValueState<>(
                        (InternalValueState<K, N, SV>) valueState, changeLogger, keyContext);
    }

    @Override
    public StateChangeApplier getChangeApplier(ChangelogApplierFactory factory) {
        return factory.forValue(delegatedState, keyContext);
    }
}
