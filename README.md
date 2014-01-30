PostgreSQL failover managed by ZooKeeper
========================================

Leverage the co-ordination capability of ZooKeeper
(http://zookeeper.apache.org) to provide a robust implementation of automated
failover for PostgreSQL.

Basic Design
------------

All inter-node communication occurs via ZooKeeper or psycopg2. No SSH

There are 2 differnt daemons which make up the system: "database" and
"application" daemons. On the nodes which actually use the PostgreSQL cluster,
the "application" daemon is run. The "database" dameons are unsurprisingly run
on the PostgreSQL master and replicas.

Application Daemon
------------------

This daemon checks the connection to the master and replica servers and writes
the results to ZooKeeper. When a failover happens this daemon is responsible
for triggering connections failover. This daemon can be responsible for
starting or stopping the database-using services on the node if the primary
cannot be reached.

Database Daemon
---------------

This daemon controls the PostgreSQL server on it's node.

On the master it is responsible only for shutting down PostgreSQL if a failover
is or has occured.

On the replicas it is responsible for periodically checking the information
written by application daemon.  If it decides that the master PostgreSQL server
is not viable, it will trigger a failover and become the master itself.

Challenges
----------

 * multiple replicas: How to fail over to the "best" replica.
 * multiple replicas: How to connect replicas to new master.
 * Datastructure in ZooKeeper (gonna be complex!)
 * Would it be a good idea to know where the basebackups are and initialize new replicas on startup?

The Good
--------

 * Relatively simple configuration. No ssh config.
 * Fully distributed.

Implementation Thoughts
-----------------------

Implement daemons in python, log to stdout and stderr.

Dependencies:
    kazoo - ZooKeeper client
    psycopg2 - connections to PostgreSQL
