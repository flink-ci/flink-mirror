#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# shellcheck source=common.sh
source "$(dirname "$0")"/common.sh

# shellcheck source=common_yarn_docker.sh
source "$(dirname "$0")"/common_yarn_docker.sh

start_hadoop_cluster_and_prepare_flink

# TODO Remove. Smoke test to see if Flink is operable.
INPUT_ARGS=""
OUTPUT_PATH=hdfs:///user/hadoop-user/wc-out-$RANDOM
echo "=== Run smoke test"
docker exec master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
   echo \$HADOOP_CLASSPATH \
   /home/hadoop-user/$FLINK_DIRNAME/bin/flink run -m yarn-cluster -ys 1 -ytm 1000 -yjm 1000 -p 3 \
   -yD taskmanager.memory.jvm-metaspace.size=128m \
   /home/hadoop-user/$FLINK_DIRNAME/examples/streaming/WordCount.jar $INPUT_ARGS --output $OUTPUT_PATH"
OUTPUT=$(get_output "$OUTPUT_PATH/*")
echo "$OUTPUT"
echo "=== Done smoke test"

# Configure Flink
docker exec master bash -c "cat <<END_CONF >> \"/home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml\"
taskmanager.numberOfTaskSlots: 1
parallelism.default: 2
jobmanager.memory.process.size: 1g
taskmanager.memory.process.size: 1g
restart-strategy: none
yarn.application-attempts: 1
END_CONF"
echo "=== Flink config after configuring:"
docker exec master bash -c "cat /home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml"

# Install the application distribution to the cluster
APP_DISTR_PATH="$(ls "$END_TO_END_DIR"/flink-yarn-no-fatjar-test-parent/flink-yarn-no-fatjar-test/target/*.tar.gz)"
if [[ ! -f $APP_DISTR_PATH ]]
then
  echo "The distribution not found or not a file: $APP_DISTR_PATH. Is the project built?"
  exit 1
fi
APP_DISTR_FILE="$(basename $APP_DISTR_PATH)"
docker cp "$APP_DISTR_PATH" master:/home/hadoop-user/
docker exec master bash -c "tar xzf /home/hadoop-user/$APP_DISTR_FILE --directory /home/hadoop-user"
APP_HOME_DIR="/home/hadoop-user/flink-yarn-no-fatjar"
# TODO Remove debug
echo "=== Content of $APP_HOME_DIR:"
docker exec master bash -c "ls -l $APP_HOME_DIR"

# make the output path random, just in case it already exists, for example if we
# had cached docker containers
OUTPUT_PATH="hdfs:///user/hadoop-user/wc-out-$RANDOM"

# Generate the application configuration
docker exec master bash -c "cat <<END_CONF > \"$APP_HOME_DIR/conf/app-conf.sh\"
HADOOP_CLASSPATH=\"\\\$(hadoop classpath)\"
export HADOOP_CLASSPATH
export FLINK_HOME_DIR=\"/home/hadoop-user/$FLINK_DIRNAME\"
export OUTPUT_LOCATION=\"$OUTPUT_PATH\"
END_CONF"
# TODO Remove debug
echo "=== Content of app-conf.sh:"
docker exec master bash -c "cat $APP_HOME_DIR/conf/app-conf.sh"

if docker exec master bash -c "$APP_HOME_DIR/bin/flink-yarn-no-fatjar-test.sh"
then
    OUTPUT=$(get_output "$OUTPUT_PATH/*")
    echo "$OUTPUT"
else
    echo "Running the job failed."
    exit 1
fi

# TODO Checks on the output vs expected output

# TODO Remove vars
# END_TO_END_DIR=/home/vsts/work/1/s/flink-end-to-end-tests
# TEST_DATA_DIR=/home/vsts/work/1/s/flink-end-to-end-tests/test-scripts/temp-test-directory-10101014562
# FLINK_DIR=/home/vsts/work/1/s/flink-dist/target/flink-1.15-SNAPSHOT-bin/flink-1.15-SNAPSHOT
# FLINK_DIRNAME=flink-1.15-SNAPSHOT
# FLINK_TARBALL=flink.tar.gz
