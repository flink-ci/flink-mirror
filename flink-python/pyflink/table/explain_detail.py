################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

__all__ = ['ExplainDetail']

from pyflink.util.api_stability_decorators import PublicEvolving


@PublicEvolving()
class ExplainDetail(object):
    """
    ExplainDetail defines the types of details for explain result.

    .. versionadded:: 1.11.0
    """

    # The cost information on physical rel node estimated by optimizer.
    # e.g. TableSourceScan(..., cumulative cost = {1.0E8 rows, 1.0E8 cpu, 2.4E9 io, 0.0 network,
    # 0.0 memory}
    ESTIMATED_COST = 0

    # The changelog mode produced by a physical rel node.
    # e.g. GroupAggregate(..., changelogMode=[I,UA,D])
    CHANGELOG_MODE = 1

    # The execution plan in json format of the program.
    JSON_EXECUTION_PLAN = 2

    # The potential risk warnings and SQL optimizer tuning advice analyzed from the physical plan.
    PLAN_ADVICE = 3
