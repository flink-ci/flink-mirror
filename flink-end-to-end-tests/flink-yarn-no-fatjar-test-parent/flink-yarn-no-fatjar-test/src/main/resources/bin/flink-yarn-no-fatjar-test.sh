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

set -o nounset
set -o errexit
set -o pipefail

bin_dir="$(cd "$(dirname -- "$0")" ; pwd)"
product_dir="$(dirname -- "$bin_dir")"
lib_dir="$product_dir/lib"
conf_dir="$product_dir/conf"

source "$conf_dir/app-conf.sh"

app_main_class="org.apache.flink.streaming.tests.NoFatjarApp"

app_jar_name="flink-yarn-no-fatjar-app-@{project.version}.jar"
app_jar="$lib_dir/$app_jar_name"

jars=$(find "$lib_dir" -type f -name \*.jar | grep -v "$app_jar_name")
classpath_jars="${jars//[[:space:]]/:}"
yarn_jars="${jars//[[:space:]]/;}"

# Inject the jars into the classpath of the command-line client 'flink'
export FLINK_CLIENT_ADD_CLASSPATH="$classpath_jars"
# TODO Remove debug
echo $FLINK_CLIENT_ADD_CLASSPATH

# TODO Remove debug
echo "$FLINK_HOME_DIR/bin/flink" \
  run \
  --target yarn-per-job \
  --class "$app_main_class" \
  -Dyarn.ship-files="$yarn_jars" \
  "$app_jar" \
  --output "$OUTPUT_LOCATION"
