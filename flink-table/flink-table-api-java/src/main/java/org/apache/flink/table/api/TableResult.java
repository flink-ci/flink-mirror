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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** A TableResult is the representation of the statement execution result. */
@PublicEvolving
public interface TableResult {

    /**
     * For DML and DQL statement, return the {@link JobClient} which associates the submitted Flink
     * job. For other statements (e.g. DDL, DCL) return empty.
     */
    Optional<JobClient> getJobClient();

    /**
     * Wait if necessary until the data is ready.
     *
     * <p>For a select operation, this method will wait until the first row can be accessed locally.
     * For an insert operation, this method will wait for the job to finish, because the result
     * contains only one row. For other operations, this method will return immediately, because the
     * result is already available locally.
     *
     * @throws ExecutionException if a problem occurred
     * @throws InterruptedException if the operation was interrupted while waiting
     */
    void await() throws InterruptedException, ExecutionException;

    /**
     * Wait if necessary for at most the given time for the data to be ready.
     *
     * <p>For a select operation, this method will wait until the first row can be accessed locally.
     * For an insert operation, this method will wait for the job to finish, because the result
     * contains only one row. For other operations, this method will return immediately, because the
     * result is already available locally.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @throws ExecutionException if a problem occurred
     * @throws InterruptedException if the operation was interrupted while waiting
     * @throws TimeoutException if the wait timed out
     */
    void await(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException;

    /** Returns the schema of the result. */
    ResolvedSchema getResolvedSchema();

    /**
     * Return the {@link ResultKind} which represents the result type.
     *
     * <p>For DDL operation and USE operation, the result kind is always {@link ResultKind#SUCCESS}.
     * For other operations, the result kind is always {@link ResultKind#SUCCESS_WITH_CONTENT}.
     */
    ResultKind getResultKind();

    /**
     * Get the result contents as a closeable row iterator.
     *
     * <p><strong>NOTE:</strong>
     *
     * <ul>
     *   <li>For SELECT operation, the job will not be finished unless all result data has been
     *       collected. So we should actively close the job to avoid resource leak through
     *       CloseableIterator#close method. Calling CloseableIterator#close method will cancel the
     *       job and release related resources.
     *   <li>For DML operation, Flink does not support getting the real affected row count now. So
     *       the affected row count is always -1 (unknown) for every sink, and them will be returned
     *       until the job is finished. Calling CloseableIterator#close method will cancel the job.
     *   <li>For other operations, no flink job will be submitted ({@link #getJobClient()} is always
     *       empty), and the result is bounded. Do nothing when calling CloseableIterator#close
     *       method.
     * </ul>
     *
     * <p>Recommended code to call CloseableIterator#close method looks like:
     *
     * <pre>{@code
     * TableResult result = tEnv.execute("select ...");
     * // using try-with-resources statement
     * try (CloseableIterator<Row> it = result.collect()) {
     *     it... // collect same data
     * }
     * }</pre>
     *
     * <p>This method has slightly different behaviors under different checkpointing settings (to
     * enable checkpointing for a streaming job, set checkpointing properties through {@link
     * TableConfig}).
     *
     * <ul>
     *   <li>For batch jobs or streaming jobs without checkpointing, this method has neither
     *       exactly-once nor at-least-once guarantee. Query results are immediately accessible by
     *       the clients once they're produced, but exceptions will be thrown when the job fails and
     *       restarts.
     *   <li>For streaming jobs with exactly-once checkpointing, this method guarantees an
     *       end-to-end exactly-once record delivery. A result will be accessible by clients only
     *       after its corresponding checkpoint completes.
     *   <li>For streaming jobs with at-least-once checkpointing, this method guarantees an
     *       end-to-end at-least-once record delivery. Query results are immediately accessible by
     *       the clients once they're produced, but it is possible for the same result to be
     *       delivered multiple times.
     * </ul>
     *
     * <p>In order to fetch result to local, you can call either {@link #collect()} and {@link
     * #print()}. But, they can't be called both on the same {@link TableResult} instance, because
     * the result can only be accessed once.
     */
    CloseableIterator<Row> collect();

    /**
     * Print the result contents as tableau form to client console.
     *
     * <p>This method has slightly different behaviors under different checkpointing settings (to
     * enable checkpointing for a streaming job, set checkpointing properties through {@link
     * TableConfig}).
     *
     * <ul>
     *   <li>For batch jobs or streaming jobs without checkpointing, this method has neither
     *       exactly-once nor at-least-once guarantee. Query results are immediately accessible by
     *       the clients once they're produced, but exceptions will be thrown when the job fails and
     *       restarts.
     *   <li>For streaming jobs with exactly-once checkpointing, this method guarantees an
     *       end-to-end exactly-once record delivery. A result will be accessible by clients only
     *       after its corresponding checkpoint completes.
     *   <li>For streaming jobs with at-least-once checkpointing, this method guarantees an
     *       end-to-end at-least-once record delivery. Query results are immediately accessible by
     *       the clients once they're produced, but it is possible for the same result to be
     *       delivered multiple times.
     * </ul>
     *
     * <p>In order to fetch result to local, you can call either {@link #collect()} and {@link
     * #print()}. But, they can't be called both on the same {@link TableResult} instance, because
     * the result can only be accessed once.
     */
    void print();
}
