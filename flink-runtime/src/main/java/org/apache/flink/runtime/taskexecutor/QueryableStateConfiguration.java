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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.util.NetUtils;

import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Simple configuration object for the parameters for the server-side of queryable state. */
public class QueryableStateConfiguration {

    private final Iterator<Integer> proxyPortRange;

    private final Iterator<Integer> qserverPortRange;

    private final int numProxyThreads;

    private final int numPQueryThreads;

    private final int numServerThreads;

    private final int numSQueryThreads;

    public QueryableStateConfiguration(
            Iterator<Integer> proxyPortRange,
            Iterator<Integer> qserverPortRange,
            int numProxyThreads,
            int numPQueryThreads,
            int numServerThreads,
            int numSQueryThreads) {

        checkArgument(proxyPortRange != null && proxyPortRange.hasNext());
        checkArgument(qserverPortRange != null && qserverPortRange.hasNext());
        checkArgument(
                numProxyThreads >= 0,
                "queryable state number of proxy threads must be zero or larger");
        checkArgument(
                numPQueryThreads >= 0,
                "queryable state number of proxy query threads must be zero or larger");
        checkArgument(
                numServerThreads >= 0,
                "queryable state number of server threads must be zero or larger");
        checkArgument(
                numSQueryThreads >= 0,
                "queryable state number of query threads must be zero or larger");

        this.proxyPortRange = proxyPortRange;
        this.qserverPortRange = qserverPortRange;
        this.numProxyThreads = numProxyThreads;
        this.numPQueryThreads = numPQueryThreads;
        this.numServerThreads = numServerThreads;
        this.numSQueryThreads = numSQueryThreads;
    }

    // ------------------------------------------------------------------------

    /**
     * Returns the port range where the queryable state client proxy can listen. See {@link
     * org.apache.flink.configuration.QueryableStateOptions#PROXY_PORT_RANGE
     * QueryableStateOptions.PROXY_PORT_RANGE}.
     */
    public Iterator<Integer> getProxyPortRange() {
        return proxyPortRange;
    }

    /**
     * Returns the port range where the queryable state server can listen. See {@link
     * org.apache.flink.configuration.QueryableStateOptions#SERVER_PORT_RANGE
     * QueryableStateOptions.SERVER_PORT_RANGE}.
     */
    public Iterator<Integer> getStateServerPortRange() {
        return qserverPortRange;
    }

    /**
     * Returns the number of threads for the query proxy NIO event loop. These threads only process
     * network events and dispatch query requests to the query threads.
     */
    public int numProxyServerThreads() {
        return numProxyThreads;
    }

    /** Returns the number of query threads for the queryable state client proxy. */
    public int numProxyQueryThreads() {
        return numPQueryThreads;
    }

    /**
     * Returns the number of threads for the query server NIO event loop. These threads only process
     * network events and dispatch query requests to the query threads.
     */
    public int numStateServerThreads() {
        return numServerThreads;
    }

    /**
     * Returns the number of threads for the thread pool that performs the actual state lookup.
     * These threads perform the actual state lookup.
     */
    public int numStateQueryThreads() {
        return numSQueryThreads;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "QueryableStateConfiguration{"
                + "numProxyServerThreads="
                + numProxyThreads
                + ", numProxyQueryThreads="
                + numPQueryThreads
                + ", numStateServerThreads="
                + numServerThreads
                + ", numStateQueryThreads="
                + numSQueryThreads
                + '}';
    }

    // ------------------------------------------------------------------------

    /** Gets the configuration describing the queryable state as deactivated. */
    public static QueryableStateConfiguration disabled() {
        final Iterator<Integer> proxyPorts =
                NetUtils.getPortRangeFromString(
                        QueryableStateOptions.PROXY_PORT_RANGE.defaultValue());
        final Iterator<Integer> serverPorts =
                NetUtils.getPortRangeFromString(
                        QueryableStateOptions.SERVER_PORT_RANGE.defaultValue());
        return new QueryableStateConfiguration(proxyPorts, serverPorts, 0, 0, 0, 0);
    }

    /** Creates the {@link QueryableStateConfiguration} from the given Configuration. */
    public static QueryableStateConfiguration fromConfiguration(Configuration config) {
        if (!config.get(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER)) {
            return null;
        }

        final Iterator<Integer> proxyPorts =
                NetUtils.getPortRangeFromString(config.get(QueryableStateOptions.PROXY_PORT_RANGE));
        final Iterator<Integer> serverPorts =
                NetUtils.getPortRangeFromString(
                        config.get(QueryableStateOptions.SERVER_PORT_RANGE));

        final int numProxyServerNetworkThreads =
                config.get(QueryableStateOptions.PROXY_NETWORK_THREADS);
        final int numProxyServerQueryThreads =
                config.get(QueryableStateOptions.PROXY_ASYNC_QUERY_THREADS);

        final int numStateServerNetworkThreads =
                config.get(QueryableStateOptions.SERVER_NETWORK_THREADS);
        final int numStateServerQueryThreads =
                config.get(QueryableStateOptions.SERVER_ASYNC_QUERY_THREADS);

        return new QueryableStateConfiguration(
                proxyPorts,
                serverPorts,
                numProxyServerNetworkThreads,
                numProxyServerQueryThreads,
                numStateServerNetworkThreads,
                numStateServerQueryThreads);
    }
}
