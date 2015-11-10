PostgreSQL failover managed by ZooKeeper
========================================

Leverage the co-ordination capability of ZooKeeper
(http://zookeeper.apache.org) to provide a robust implementation of automated
failover for PostgreSQL.

Basic Design
------------

All inter-server communication occurs via ZooKeeper. No SSH or network
connections of any kind.

There are 2 different daemons which make up the system: "deadman" and
the "sync" daemons. On the nodes which actually use the PostgreSQL
cluster, the "deadman" daemon is run to control the PostgreSQL process
and optionally the sync daemon to update PostgreSQL configuration files
(e.g. pg\_hba.conf). On application servers or proxies the "sync" daemon
is run.

Terminology
-----------

- database cluster or cluster:
  http://www.postgresql.org/docs/9.4/static/creating-cluster.html
- znode: a ZooKeeper data node in the ZooKeeper database
- group, cluster group, database group: A group of master/replica providing one logical
  database, made up of one or more database clusters.


ZooKeeper Directory Layout
--------------------------

In ZooKeeper, we have one directory which contains the information on
all the nodes in multiple master-replica database groups. It is a
directory with 4 sub-directories types:

    /state/{DATABASE_GROUP_NAME}_{IPADDR}:{PORT}
        This contains all the information about a database cluster nodes,
        healthy or not. It is updated frequently with data such as the WAL log
        replay status. It is an ephemeral node and will dissapear if
        the connection to zookeeper is lost.
        Ephemeral, created/maintained by the "deadman" daemon.

    /conn/{DATABASE_GROUP_NAME}_{IPADDR}:{PORT}
        This znode contains a subset of information from the state-
        node. It is the static connection information/metadata about a single
        healthy (i.e. connectable) cluster. If the node is not "healthy", this
        entry will not exist. The information in this znode is not vollatile and
        is gaurenteed not to change over the lifespan of the znode.  Ephemeral,
        created/maintained by the "deadman" daemon.

    /lock/{DATABASE_GROUP_NAME}
        This contains the IPADDR:PORT of the current master for the
        database group. Connection info should be looked up in the
        "_conn_" node (if it exists).
        Created/maintained by the "deadman" daemon on the current

    /static/{DATABASE_GROUP_NAME}-db-id
        Contains the database identifier of the database group.

Most of the above znodes contain a JSON encoded
dictionary/object/hashmap.

Sync Daemon
-----------

This daemon runs on any node which wishes to connect to a database group
and maintains the local machine's configuration files. For example, it
can rewrite a pgbouncer configuration if the master of one database
group fails over. It can also start a new pgbouncer daemon if a new
database group is created. Another example is dynamically changing
HAProxy weight according to the node location (e.g. availability zone)
or replication lag.

Actually applying the configuration changes is the job of plugins, the
sync daemon will not apply any changes by itself. Plugins can be
specified in 2 ways:

    * Using setuptools entry points to subscribe in-process to the
      changes. This allows subscribers to subscribe to events from
      either state-, master- or healthy- znodes.
    * Provide an executable which will be called with the path to a
      JSON encoded file containing the information from the healthy-
      and master- znodes. This is provided by the zgres-apply package
      which plugs into zgres-sync using the previous plugin method.
      This plugin does not recieve state- events for performance
      reasons.

These plugins MUST be idempotent, they will be called repeatedly with
the same data.

Deadman Daemon
--------------

This daemon controls one PostgreSQL database cluster and registers it in
zookeeper (creating/maintaining the state-, conn- and master-
znodes). It must run on the same machine as the database cluster.

It is responsible for promoting or shutting down it's postgresql
database cluster.

Currently, remastering and starting PostgreSQL should be handled outside
before deadman is started.

Plugins for the deadman daemon should be able to do 2 things:
    * Provide extra metadata (i.e. availability-zone or replication lag)
    * Veto the cluster being up (aside from a builtin SELECT 1)

Challenges
----------

 * multiple replicas: How to fail over to the "best" replica.
 * multiple replicas: How to connect replicas to new master.
 * Would it be a good idea to know where the basebackups are and initialize new replicas on startup?

 * PLUGINS PLUGINS PLUGINS:
    - Provide a lot of plugins builtin, allow them to be seen and enabled via the
      "config" in zookeeper? EEEK: dynamic reconfiguration of daemons?
    - What happens if a few nodes don't have some plugins?
    - Configuration on a "cluster group" level

The Good
--------

 * Relatively simple configuration. No ssh config.
 * Fully distributed.

Implementation Thoughts
-----------------------

 * Implement daemons in python, log to stdout and stderr. Have them be
   run via systemd with configured to restart on fail. Fail noisily!

Dependencies
------------

 * systemd
 * kazoo - ZooKeeper client
 * psycopg2 - connections to PostgreSQL

Acknowledgment
--------------

Zgres is heavily influenced by HandyRep
(https://github.com/pgexperts/handyrep) and Patroni
(https://github.com/zalando/patroni). Many thanks to the developers of
those for some great ideas.
