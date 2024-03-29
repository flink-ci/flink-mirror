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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Point-in-time configuration of a {@link GenericArraySerializer}.
 *
 * @param <C> The component type.
 * @deprecated this is deprecated and no longer used by the {@link GenericArraySerializer}. It has
 *     been replaced by {@link GenericArraySerializerSnapshot}.
 */
@Internal
@Deprecated
public final class GenericArraySerializerConfigSnapshot<C> implements TypeSerializerSnapshot<C[]> {

    private static final int CURRENT_VERSION = 2;

    /** The class of the components of the serializer's array type. */
    @Nullable private Class<C> componentClass;

    /** Snapshot handling for the component serializer snapshot. */
    @Nullable private NestedSerializersSnapshotDelegate nestedSnapshot;

    /** Constructor for read instantiation. */
    @SuppressWarnings("unused")
    public GenericArraySerializerConfigSnapshot() {}

    /** Constructor to create the snapshot for writing. */
    public GenericArraySerializerConfigSnapshot(GenericArraySerializer<C> serializer) {
        this.componentClass = serializer.getComponentClass();
        this.nestedSnapshot =
                new NestedSerializersSnapshotDelegate(serializer.getComponentSerializer());
    }

    // ------------------------------------------------------------------------

    @Override
    public int getCurrentVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        checkState(componentClass != null && nestedSnapshot != null);
        out.writeUTF(componentClass.getName());
        nestedSnapshot.writeNestedSerializerSnapshots(out);
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader)
            throws IOException {
        switch (readVersion) {
            case 1:
                throw new UnsupportedOperationException(
                        String.format(
                                "No longer supported version [%d]. Please upgrade first to Flink 1.16. ",
                                readVersion));
            case 2:
                readV2(in, classLoader);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized version: " + readVersion);
        }
    }

    private void readV2(DataInputView in, ClassLoader classLoader) throws IOException {
        componentClass = InstantiationUtil.resolveClassByName(in, classLoader);
        nestedSnapshot =
                NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(in, classLoader);
    }

    @Override
    public GenericArraySerializer<C> restoreSerializer() {
        checkState(componentClass != null && nestedSnapshot != null);
        return new GenericArraySerializer<>(
                componentClass, nestedSnapshot.getRestoredNestedSerializer(0));
    }

    @Nullable
    public TypeSerializerSnapshot<?>[] getNestedSerializerSnapshots() {
        return nestedSnapshot == null ? null : nestedSnapshot.getNestedSerializerSnapshots();
    }

    @Nullable
    public Class<C> getComponentClass() {
        return componentClass;
    }
}
