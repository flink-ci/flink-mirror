---
title: ZooKeeper HA Services
weight: 2
type: docs
aliases:
  - /deployment/ha/zookeeper_ha.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# ZooKeeper HA Services

Flink's ZooKeeper HA services use [ZooKeeper](http://zookeeper.apache.org) for high availability services.

Flink leverages **[ZooKeeper](http://zookeeper.apache.org)** for *distributed coordination* between all running JobManager instances. 
ZooKeeper is a separate service from Flink, which provides highly reliable distributed coordination via leader election and light-weight consistent state storage. 
Check out [ZooKeeper's Getting Started Guide](http://zookeeper.apache.org/doc/current/zookeeperStarted.html) for more information about ZooKeeper. 
Flink includes scripts to [bootstrap a simple ZooKeeper](#bootstrap-zookeeper) installation.

## Configuration

In order to start an HA-cluster you have to configure the following configuration keys:

- [high-availability.type]({{< ref "docs/deployment/config" >}}#high-availability-type) (required): 
The `high-availability.type` option has to be set to `zookeeper`.

  <pre>high-availability.type: zookeeper</pre>

- [high-availability.storageDir]({{< ref "docs/deployment/config" >}}#high-availability-storagedir) (required): 
JobManager metadata is persisted in the file system `high-availability.storageDir` and only a pointer to this state is stored in ZooKeeper.

  <pre>high-availability.storageDir: hdfs:///flink/recovery</pre>

  The `storageDir` stores all metadata needed to recover a JobManager failure.

- [high-availability.zookeeper.quorum]({{< ref "docs/deployment/config.md" >}}#high-availability-zookeeper-quorum) (required): 
A *ZooKeeper quorum* is a replicated group of ZooKeeper servers, which provide the distributed coordination service.

  <pre>high-availability.zookeeper.quorum: address1:2181[,...],addressX:2181</pre>

  Each `addressX:port` refers to a ZooKeeper server, which is reachable by Flink at the given address and port.

- [high-availability.zookeeper.path.root]({{< ref "docs/deployment/config" >}}#high-availability-zookeeper-path-root) (recommended): 
The *root ZooKeeper node*, under which all cluster nodes are placed.

  <pre>high-availability.zookeeper.path.root: /flink</pre>

- [high-availability.cluster-id]({{< ref "docs/deployment/config" >}}#high-availability-cluster-id) (recommended): 
The *cluster-id ZooKeeper node*, under which all required coordination data for a cluster is placed.

  <pre>high-availability.cluster-id: /default_ns # important: customize per cluster</pre>

  **Important**: 
  You should not set this value manually when running on YARN, native Kubernetes or on another cluster manager. 
  In those cases a cluster-id is being automatically generated. 
  If you are running multiple Flink HA clusters on bare metal, you have to manually configure separate cluster-ids for each cluster.

### Example configuration

Configure high availability mode and ZooKeeper quorum in [Flink configuration file]({{< ref "docs/deployment/config#flink-configuration-file" >}}):

```bash
high-availability.type: zookeeper
high-availability.zookeeper.quorum: localhost:2181
high-availability.zookeeper.path.root: /flink
high-availability.cluster-id: /cluster_one # important: customize per cluster
high-availability.storageDir: hdfs:///flink/recovery
```

{{< top >}}

## Configuring for ZooKeeper Security

If ZooKeeper is running in secure mode with Kerberos, you can override the following configurations in [Flink configuration file]({{< ref "docs/deployment/config#flink-configuration-file" >}}) as necessary:

```bash
# default is "zookeeper". If the ZooKeeper quorum is configured
# with a different service name then it can be supplied here.

zookeeper.sasl.service-name: zookeeper 

# default is "Client". The value needs to match one of the values
# configured in "security.kerberos.login.contexts".   
zookeeper.sasl.login-context-name: Client  
```

For more information on Flink configuration for Kerberos security, please refer to the [security section of the Flink configuration page]({{< ref "docs/deployment/config" >}}#security).
You can also find further details on [how Flink sets up Kerberos-based security internally]({{< ref "docs/deployment/security/security-kerberos" >}}).

{{< top >}}

## Advanced Configuration

### Tolerating Suspended ZooKeeper Connections

Per default, Flink's ZooKeeper client treats suspended ZooKeeper connections as an error.
This means that Flink will invalidate all leaderships of its components and thereby triggering a failover if a connection is suspended.

This behaviour might be too disruptive in some cases (e.g., unstable network environment).
If you are willing to take a more aggressive approach, then you can tolerate suspended ZooKeeper connections and only treat lost connections as an error via [high-availability.zookeeper.client.tolerate-suspended-connections]({{< ref "docs/deployment/config" >}}#high-availability-zookeeper-client-tolerate-suspended-connection).
Enabling this feature will make Flink more resilient against temporary connection problems but also increase the risk of running into ZooKeeper timing problems.

For more information take a look at [Curator's error handling](https://curator.apache.org/errors.html).

## Bootstrap ZooKeeper

If you don't have a running ZooKeeper installation, you can use the helper scripts, which ship with Flink.

There is a ZooKeeper configuration template in `conf/zoo.cfg`. 
You can configure the hosts to run ZooKeeper on with the `server.X` entries, where X is a unique ID of each server:

```bash
server.X=addressX:peerPort:leaderPort
[...]
server.Y=addressY:peerPort:leaderPort
```

The script `bin/start-zookeeper-quorum.sh` will start a ZooKeeper server on each of the configured hosts. 
The started processes start ZooKeeper servers via a Flink wrapper, which reads the configuration from `conf/zoo.cfg` and makes sure to set some required configuration values for convenience. 
In production setups, it is recommended to manage your own ZooKeeper installation.

{{< top >}} 
