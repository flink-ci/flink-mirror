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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.createJobVertex;
import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.streamingJobGraph;

@RunWith(Parameterized.class)
public class JobGraphConfigurationUtilsAutoConfigurtionTest {

    @Parameterized.Parameters(name = "parallelism = {0}, maxParallelism = {1}, expected max = {2}")
    public static Object[][] data() {
        return new Object[][] {
            // default minimum
            {1, JobVertex.MAX_PARALLELISM_DEFAULT, 128},
            // test round up part 1
            {171, JobVertex.MAX_PARALLELISM_DEFAULT, 256},
            // test round up part 2
            {172, JobVertex.MAX_PARALLELISM_DEFAULT, 512},
            // test round up limit
            {1 << 15, JobVertex.MAX_PARALLELISM_DEFAULT, 1 << 15},
            // test configured / trumps computed default
            {4, 1 << 15, 1 << 15},
            // test override trumps test configured 2
            {4, 7, 7},
        };
    }

    @Parameterized.Parameter(0)
    public int parallelism;

    @Parameterized.Parameter(1)
    public int maxParallelism;

    @Parameterized.Parameter(2)
    public int expectedMaxParallelism;

    @Test
    public void testMaxParallelismDefaulting() {
        JobVertex jobVertex = createJobVertex(parallelism, maxParallelism);
        JobGraph jobGraph = streamingJobGraph(jobVertex);
        JobGraphConfigurationUtils.autoConfigureMaxParallelism(jobGraph);
        Assert.assertEquals(expectedMaxParallelism, jobVertex.getMaxParallelism());
    }

    @Test
    public void testMaxParallelismDefaultingReactiveMode() {
        JobVertex jobVertex = createJobVertex(parallelism, maxParallelism);
        JobGraph jobGraph = streamingJobGraph(jobVertex);
        JobGraphConfigurationUtils.configureJobGraphForReactiveMode(jobGraph);
        Assert.assertEquals(expectedMaxParallelism, jobVertex.getMaxParallelism());
        Assert.assertEquals(expectedMaxParallelism, jobVertex.getParallelism());
    }
}
