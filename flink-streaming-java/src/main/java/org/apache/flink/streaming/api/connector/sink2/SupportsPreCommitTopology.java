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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Allows expert users to implement a custom topology after {@link SinkWriter} and before {@link
 * Committer}.
 *
 * <p>It is recommended to use immutable committables because mutating committables can have
 * unexpected side-effects.
 */
@Experimental
public interface SupportsPreCommitTopology<WriterResultT, CommittableT> {

    /**
     * Intercepts and modifies the committables sent on checkpoint or at end of input. Implementers
     * need to ensure to modify all {@link CommittableMessage}s appropriately.
     *
     * @param committables the stream of committables.
     * @return the custom topology before {@link Committer}.
     */
    DataStream<CommittableMessage<CommittableT>> addPreCommitTopology(
            DataStream<CommittableMessage<WriterResultT>> committables);

    /** Returns the serializer of the WriteResult type. */
    SimpleVersionedSerializer<WriterResultT> getWriteResultSerializer();
}
