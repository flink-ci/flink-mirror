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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;

import java.util.Locale;

/**
 * This test uses a third-party dependency (eclipse-collections) with the following properties:
 *
 * <ul>
 *   <li>not a part of JDK
 *   <li>not a part of Flink distribution
 * </ul>
 *
 * <p>The test is packaged as an ordinary jar (not a fat-jar). <br>
 * The resulting application is packaged as a tarball including:
 *
 * <ul>
 *   <li>the application jar
 *   <li>the jars of dependencies
 *   <li>the shell launcher
 * </ul>
 *
 * <p>Command-line argument: --output /path/to/write/output
 */
public class NoFatjarApp {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String outputPath = params.getRequired("output");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /* Use some API outside of JDK - eclipse-collections for example. */
        ImmutableList<String> inputData = Lists.immutable.of("one", "two", "three");
        DataStream<String> input = env.fromCollection(inputData.castToCollection());
        DataStream<String> output = input.map(value -> value.toUpperCase(Locale.ROOT));
        StreamingFileSink<String> sink =
                StreamingFileSink.forRowFormat(
                                new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                        .build();
        output.addSink(sink).setParallelism(1);
        env.execute(NoFatjarApp.class.getSimpleName());
    }

    /* TODO Remove */
}
