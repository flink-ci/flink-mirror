/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

class PriorityQueueStateChangeLoggerImpl<K, T> extends AbstractStateChangeLogger<K, T, Void> {
    private TypeSerializer<T> serializer;

    PriorityQueueStateChangeLoggerImpl(
            TypeSerializer<T> serializer,
            InternalKeyContext<K> keyContext,
            StateChangelogWriter<?> stateChangelogWriter,
            RegisteredPriorityQueueStateBackendMetaInfo<T> meta,
            short stateId) {
        super(stateChangelogWriter, keyContext, meta, stateId);
        this.serializer = checkNotNull(serializer);
    }

    @Override
    protected void serializeValue(T t, DataOutputView out) throws IOException {
        serializer.serialize(t, out);
    }

    @Override
    protected void serializeScope(Void unused, DataOutputView out) throws IOException {}

    @Override
    protected PriorityQueueStateChangeLoggerImpl<K, T> setMetaInfo(
            RegisteredStateMetaInfoBase metaInfo) {
        super.setMetaInfo(metaInfo);
        @SuppressWarnings("unchecked")
        RegisteredPriorityQueueStateBackendMetaInfo<T> pqMetaInfo =
                (RegisteredPriorityQueueStateBackendMetaInfo<T>) metaInfo;
        this.serializer = pqMetaInfo.getElementSerializer();
        return this;
    }
}
