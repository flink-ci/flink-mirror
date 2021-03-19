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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.JobVertex;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;

public class AllVerticesIteratorTest {

    @Test
    public void testAllVertices() throws Exception {
        JobVertex v1 = createNoOpVertex("v1", 1);
        JobVertex v2 = createNoOpVertex("v2", 7);
        JobVertex v3 = createNoOpVertex("v3", 3);
        JobVertex v4 = createNoOpVertex("v4", 2);

        ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(v1, v2, v3, v4);
        ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
        ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
        ExecutionJobVertex ejv3 = eg.getJobVertex(v3.getID());
        ExecutionJobVertex ejv4 = eg.getJobVertex(v4.getID());

        AllVerticesIterator iter =
                new AllVerticesIterator(Arrays.asList(ejv1, ejv2, ejv3, ejv4).iterator());

        int numReturned = 0;
        while (iter.hasNext()) {
            iter.hasNext();
            Assert.assertNotNull(iter.next());
            numReturned++;
        }

        Assert.assertEquals(13, numReturned);
    }
}
