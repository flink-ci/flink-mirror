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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

/**
 * This interface is used by {@link HashSubpartitionBufferAccumulator} to operate {@link
 * HashBufferAccumulator}.
 */
public interface HashSubpartitionBufferAccumulatorContext {

    /**
     * Request {@link BufferBuilder} from the {@link BufferPool}.
     *
     * @return the requested buffer
     */
    BufferBuilder requestBufferBlocking();

    /**
     * Flush the accumulated {@link Buffer}s of the subpartition.
     *
     * @param subpartitionId the subpartition id
     * @param accumulatedBuffer the accumulated buffer
     * @param numRemainingConsecutiveBuffers number of buffers that would be passed in the following
     *     invocations and should be written to the same segment as this one
     */
    void flushAccumulatedBuffers(
            TieredStorageSubpartitionId subpartitionId,
            Buffer accumulatedBuffer,
            int numRemainingConsecutiveBuffers);
}
