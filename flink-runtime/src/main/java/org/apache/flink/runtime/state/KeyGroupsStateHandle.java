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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * A handle to the partitioned stream operator state after it has been checkpointed. This state
 * consists of a range of key group snapshots. A key group is subset of the available key space. The
 * key groups are identified by their key group indices.
 */
public class KeyGroupsStateHandle implements StreamStateHandle, KeyedStateHandle {

    private static final long serialVersionUID = -8070326169926626355L;

    /** Range of key-groups with their respective offsets in the stream state */
    private final KeyGroupRangeOffsets groupRangeOffsets;

    /** Inner stream handle to the actual states of the key-groups in the range */
    private final StreamStateHandle stateHandle;

    private final StateHandleID stateHandleId;

    /**
     * @param groupRangeOffsets range of key-group ids that in the state of this handle
     * @param streamStateHandle handle to the actual state of the key-groups
     */
    public KeyGroupsStateHandle(
            KeyGroupRangeOffsets groupRangeOffsets, StreamStateHandle streamStateHandle) {
        this(groupRangeOffsets, streamStateHandle, new StateHandleID(UUID.randomUUID().toString()));
    }

    private KeyGroupsStateHandle(
            KeyGroupRangeOffsets groupRangeOffsets,
            StreamStateHandle streamStateHandle,
            StateHandleID stateHandleId) {
        Preconditions.checkNotNull(groupRangeOffsets);
        Preconditions.checkNotNull(streamStateHandle);
        Preconditions.checkNotNull(stateHandleId);

        this.groupRangeOffsets = groupRangeOffsets;
        this.stateHandle = streamStateHandle;
        this.stateHandleId = stateHandleId;
    }

    public static KeyGroupsStateHandle restore(
            KeyGroupRangeOffsets groupRangeOffsets,
            StreamStateHandle streamStateHandle,
            StateHandleID stateHandleId) {
        return new KeyGroupsStateHandle(groupRangeOffsets, streamStateHandle, stateHandleId);
    }

    /**
     * @return the internal key-group range to offsets metadata
     */
    public KeyGroupRangeOffsets getGroupRangeOffsets() {
        return groupRangeOffsets;
    }

    /**
     * @return The handle to the actual states
     */
    public StreamStateHandle getDelegateStateHandle() {
        return stateHandle;
    }

    /**
     * @param keyGroupId the id of a key-group. the id must be contained in the range of this
     *     handle.
     * @return offset to the position of data for the provided key-group in the stream referenced by
     *     this state handle
     */
    public long getOffsetForKeyGroup(int keyGroupId) {
        return groupRangeOffsets.getKeyGroupOffset(keyGroupId);
    }

    /**
     * @param keyGroupRange a key group range to intersect.
     * @return key-group state over a range that is the intersection between this handle's key-group
     *     range and the provided key-group range.
     */
    @Override
    public KeyGroupsStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        KeyGroupRangeOffsets offsets = groupRangeOffsets.getIntersection(keyGroupRange);
        if (offsets.getKeyGroupRange().getNumberOfKeyGroups() <= 0) {
            return null;
        }
        return new KeyGroupsStateHandle(offsets, stateHandle, stateHandleId);
    }

    @Override
    public StateHandleID getStateHandleId() {
        return stateHandleId;
    }

    @Override
    public PhysicalStateHandleID getStreamStateHandleID() {
        return stateHandle.getStreamStateHandleID();
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return groupRangeOffsets.getKeyGroupRange();
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
        // No shared states
    }

    @Override
    public void discardState() throws Exception {
        stateHandle.discardState();
    }

    @Override
    public long getStateSize() {
        return stateHandle.getStateSize();
    }

    @Override
    public void collectSizeStats(StateObjectSizeStatsCollector collector) {
        // TODO: for now this ignores that only some key groups might be accessed when reading the
        //  state, so this only reports the upper bound. We could introduce
        //  #collectSizeStats(StateObjectSizeStatsCollector, KeyGroupRange) in KeyedStateHandle
        //  that computes which groups where actually touched and computes the size, depending on
        //  the exact state handle type, from either the offsets (e.g. here) or for the full size
        //  (e.g. remote incremental) when we restore from managed/raw keyed state.
        stateHandle.collectSizeStats(collector);
    }

    @Override
    public long getCheckpointedSize() {
        return getStateSize();
    }

    @Override
    public FSDataInputStream openInputStream() throws IOException {
        return stateHandle.openInputStream();
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
        return stateHandle.asBytesIfInMemory();
    }

    @Override
    public Optional<org.apache.flink.core.fs.Path> maybeGetPath() {
        return stateHandle.maybeGetPath();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof KeyGroupsStateHandle)) {
            return false;
        }

        KeyGroupsStateHandle that = (KeyGroupsStateHandle) o;

        if (!groupRangeOffsets.equals(that.groupRangeOffsets)) {
            return false;
        }
        return stateHandle.equals(that.stateHandle);
    }

    @Override
    public int hashCode() {
        int result = groupRangeOffsets.hashCode();
        result = 31 * result + stateHandle.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "KeyGroupsStateHandle{"
                + "groupRangeOffsets="
                + groupRangeOffsets
                + ", stateHandle="
                + stateHandle
                + '}';
    }
}
