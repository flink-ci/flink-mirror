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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for configuring {@link JobGraph JobGraphs} before their execution. */
public final class JobGraphConfigurationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JobGraphConfigurationUtils.class);

    /**
     * Configures a {@link JobGraph} for the reactive {@link SchedulerExecutionMode}.
     *
     * @param jobGraph The JobGraph.
     */
    public static void configureJobGraphForReactiveMode(JobGraph jobGraph) {
        LOG.info("Modifying job parallelism for running in reactive mode.");
        for (JobVertex vertex : jobGraph.getVertices()) {
            if (vertex.getMaxParallelism() == JobVertex.MAX_PARALLELISM_DEFAULT) {
                autoConfigureDefaultParallelism(vertex);
            }

            vertex.setParallelism(vertex.getMaxParallelism());
        }
    }

    /**
     * Configures a {@link JobGraph} for the default scheduling mode.
     *
     * @param jobGraph The JobGraph.
     */
    public static void configureJobGraphForDefaultMode(JobGraph jobGraph) {
        LOG.info("Modifying job parallelism for running in default mode.");
        for (JobVertex vertex : jobGraph.getVertices()) {
            // if no max parallelism was configured by the user, we calculate and set a default
            if (vertex.getMaxParallelism() == JobVertex.MAX_PARALLELISM_DEFAULT) {
                autoConfigureDefaultParallelism(vertex);
            }
        }
    }

    /**
     * Set the system-defined default for a {@link JobVertex}.
     *
     * @param vertex The JobVertex.
     */
    private static void autoConfigureDefaultParallelism(JobVertex vertex) {
        int defaultMax =
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(vertex.getParallelism());
        vertex.setAutoConfiguredMaxParallelism(defaultMax);
    }

    private JobGraphConfigurationUtils() {}
}
