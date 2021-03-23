/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import java.util.Arrays;

/** Utilities for creating {@link JobGraph JobGraphs} for testing purposes. */
public class JobGraphTestUtils {

    public static JobGraph emptyJobGraph() {
        return JobGraphBuilder.newStreamingJobGraphBuilder().build();
    }

    public static JobGraph singleNoOpJobGraph() {
        JobVertex jobVertex = createJobVertex("jobVertex", 1, JobVertex.MAX_PARALLELISM_DEFAULT);
        jobVertex.setInvokableClass(NoOpInvokable.class);

        return streamingJobGraph(jobVertex);
    }

    public static JobGraph streamingJobGraph(JobVertex... jobVertices) {
        JobGraph graph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertices(Arrays.asList(jobVertices))
                        .build();
        JobGraphConfigurationUtils.autoConfigureMaxParallelism(graph);
        return graph;
    }

    public static JobGraph batchJobGraph(JobVertex... jobVertices) {
        JobGraph graph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .addJobVertices(Arrays.asList(jobVertices))
                        .build();
        JobGraphConfigurationUtils.autoConfigureMaxParallelism(graph);
        return graph;
    }

    public static JobVertex createJobVertex(int parallelism, int preconfiguredMaxParallelism) {
        return createJobVertex("testVertex", parallelism, preconfiguredMaxParallelism);
    }

    public static JobVertex createJobVertex(
            String name, int parallelism, int preconfiguredMaxParallelism) {
        JobVertex jobVertex = new JobVertex(name);
        jobVertex.setInvokableClass(AbstractInvokable.class);
        jobVertex.setParallelism(parallelism);

        if (JobVertex.MAX_PARALLELISM_DEFAULT != preconfiguredMaxParallelism) {
            jobVertex.setMaxParallelism(preconfiguredMaxParallelism);
        }

        return jobVertex;
    }

    private JobGraphTestUtils() {
        throw new UnsupportedOperationException("This class should never be instantiated.");
    }
}
