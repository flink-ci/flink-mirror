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

package org.apache.flink.runtime.scheduler.exceptionhistory;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * {@code RootExceptionHistoryEntry} extending {@link ExceptionHistoryEntry} by providing a list of
 * {@code ExceptionHistoryEntry} instances to store concurrently caught failures.
 */
@NotThreadSafe
public class RootExceptionHistoryEntry extends ExceptionHistoryEntry {

    private static final long serialVersionUID = -7647332765867297434L;

    private final Collection<ExceptionHistoryEntry> concurrentExceptions;

    /**
     * Creates a {@code RootExceptionHistoryEntry} based on the passed {@link
     * FailureHandlingResultSnapshot}.
     *
     * @param snapshot The reason for the failure.
     * @return The {@code RootExceptionHistoryEntry} instance.
     * @throws NullPointerException if {@code cause} or {@code failingTaskName} are {@code null}.
     * @throws IllegalArgumentException if the {@code timestamp} of the passed {@code
     *     FailureHandlingResult} is not bigger than {@code 0}.
     */
    public static RootExceptionHistoryEntry fromFailureHandlingResultSnapshot(
            FailureHandlingResultSnapshot snapshot) {
        String failingTaskName = null;
        TaskManagerLocation taskManagerLocation = null;
        if (snapshot.getRootCauseExecution().isPresent()) {
            final Execution rootCauseExecution = snapshot.getRootCauseExecution().get();
            failingTaskName = rootCauseExecution.getVertexWithAttempt();
            taskManagerLocation = rootCauseExecution.getAssignedResourceLocation();
        }

        return createRootExceptionHistoryEntry(
                snapshot.getRootCause(),
                snapshot.getTimestamp(),
                snapshot.getFailureLabels(),
                failingTaskName,
                taskManagerLocation,
                snapshot.getConcurrentlyFailedExecution());
    }

    /**
     * Creates a {@code RootExceptionHistoryEntry} representing a global failure from the passed
     * {@code Throwable} and timestamp. If any of the passed {@link Execution Executions} failed, it
     * will be added to the {@code RootExceptionHistoryEntry}'s concurrently caught failures.
     *
     * @param cause The reason for the failure.
     * @param timestamp The time the failure was caught.
     * @param failureLabels Map of string labels associated with the failure.
     * @param executions The {@link Execution} instances that shall be analyzed for failures.
     * @return The {@code RootExceptionHistoryEntry} instance.
     * @throws NullPointerException if {@code failure} is {@code null}.
     * @throws IllegalArgumentException if the passed {@code timestamp} is not bigger than {@code
     *     0}.
     */
    public static RootExceptionHistoryEntry fromGlobalFailure(
            Throwable cause,
            long timestamp,
            CompletableFuture<Map<String, String>> failureLabels,
            Iterable<Execution> executions) {
        return createRootExceptionHistoryEntry(
                cause, timestamp, failureLabels, null, null, executions);
    }

    public static RootExceptionHistoryEntry fromExceptionHistoryEntry(
            ExceptionHistoryEntry entry, Collection<ExceptionHistoryEntry> entries) {
        return new RootExceptionHistoryEntry(
                entry.getException(),
                entry.getTimestamp(),
                entry.getFailureLabelsFuture(),
                null,
                null,
                entries);
    }

    /**
     * Creates a {@code RootExceptionHistoryEntry} based on the passed {@link ErrorInfo}. No
     * concurrent failures will be added.
     *
     * @param errorInfo The failure information that shall be used to initialize the {@code
     *     RootExceptionHistoryEntry}.
     * @return The {@code RootExceptionHistoryEntry} instance.
     * @throws NullPointerException if {@code errorInfo} is {@code null} or the passed info does not
     *     contain a {@code Throwable}.
     * @throws IllegalArgumentException if the passed {@code timestamp} is not bigger than {@code
     *     0}.
     */
    public static RootExceptionHistoryEntry fromGlobalFailure(ErrorInfo errorInfo) {
        Preconditions.checkNotNull(errorInfo, "errorInfo");
        return fromGlobalFailure(
                errorInfo.getException(),
                errorInfo.getTimestamp(),
                FailureEnricherUtils.EMPTY_FAILURE_LABELS,
                Collections.emptyList());
    }

    private static RootExceptionHistoryEntry createRootExceptionHistoryEntry(
            Throwable cause,
            long timestamp,
            CompletableFuture<Map<String, String>> failureLabels,
            @Nullable String failingTaskName,
            @Nullable TaskManagerLocation taskManagerLocation,
            Iterable<Execution> executions) {
        return new RootExceptionHistoryEntry(
                cause,
                timestamp,
                failureLabels,
                failingTaskName,
                taskManagerLocation,
                createExceptionHistoryEntries(executions));
    }

    private static Collection<ExceptionHistoryEntry> createExceptionHistoryEntries(
            Iterable<Execution> executions) {
        return StreamSupport.stream(executions.spliterator(), false)
                .filter(execution -> execution.getFailureInfo().isPresent())
                .map(
                        execution ->
                                ExceptionHistoryEntry.create(
                                        execution,
                                        execution.getVertexWithAttempt(),
                                        FailureEnricherUtils.EMPTY_FAILURE_LABELS))
                .collect(Collectors.toList());
    }

    /**
     * Instantiates a {@code RootExceptionHistoryEntry}.
     *
     * @param cause The reason for the failure.
     * @param timestamp The time the failure was caught.
     * @param failureLabels labels associated with the failure.
     * @param failingTaskName The name of the task that failed.
     * @param taskManagerLocation The host the task was running on.
     * @throws NullPointerException if {@code cause} is {@code null}.
     * @throws IllegalArgumentException if the passed {@code timestamp} is not bigger than {@code
     *     0}.
     */
    @VisibleForTesting
    public RootExceptionHistoryEntry(
            Throwable cause,
            long timestamp,
            CompletableFuture<Map<String, String>> failureLabels,
            @Nullable String failingTaskName,
            @Nullable TaskManagerLocation taskManagerLocation,
            Collection<ExceptionHistoryEntry> concurrentExceptions) {
        super(cause, timestamp, failureLabels, failingTaskName, taskManagerLocation);
        this.concurrentExceptions = concurrentExceptions;
    }

    public void addConcurrentExceptions(Iterable<Execution> concurrentlyExecutions) {
        this.concurrentExceptions.addAll(createExceptionHistoryEntries(concurrentlyExecutions));
    }

    public Iterable<ExceptionHistoryEntry> getConcurrentExceptions() {
        return concurrentExceptions;
    }
}
