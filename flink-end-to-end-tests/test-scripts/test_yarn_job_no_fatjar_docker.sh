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

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/common_yarn_docker.sh

start_hadoop_cluster_and_prepare_flink

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
docker exec master bash -c "ls -l $APP_HOME_DIR"

# make the output path random, just in case it already exists, for example if we
# had cached docker containers
OUTPUT_PATH="hdfs:///user/hadoop-user/wc-out-$RANDOM"

# Generate the application configuration
docker exec master bash -c "cat <<END_CONF > \"$APP_HOME_DIR/lib/app-conf.sh\"
HADOOP_CLASSPATH=\"\\\$(hadoop classpath)\"
export HADOOP_CLASSPATH
export FLINK_HOME_DIR=\"/home/hadoop-user/$FLINK_DIRNAME\"
export OUTPUT_LOCATION=\"$OUTPUT_PATH\"
END_CONF"
# TODO Remove debug
docker exec master bash -c "cat $APP_HOME_DIR/lib/app-conf.sh"

if docker exec master bash -c "$APP_HOME_DIR/bin/flink-jar-no-fatjar-test.sh"
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
