# Meteora

Meteora is a distributed key-value storage system built over RocksDB, inspired by Dynamo, and written in Scala with Akka.

Meteora is not suitable for any production deployment. It was developed as an attempt to learn various concepts applicable when building a distributed system.

**This project is still under active development.**

## Overview

- A 128-bit MurmurHash3 Hash Function.
- Consistent Hashing with the `Q/S tokens per node, equal-sized partitions` Strategy.
- Tunable NRW Eventual Consistency.
- Last-Write-Wins Conflict Resolution Strategy.
- Read-Repair.
- Merkle Trees for Anti-Entropy.
- RocksDB for Persistence.

## Requirements

1. Java 8.
2. Scala 2.11.8.
3. SBT 0.13.8.

## Quick Start Guide

Currently, Meteora does not assemble into an executable JAR.

```bash
# 1. Clone this repository.
$ git clone git@github.com:jparkie/Meteora.git
$ cd Meteora

# 2. Build the project.
$ sbt compile

# 3. Run the project.
$ sbt run

# 4. Get Request:
$ curl -X GET 'http://127.0.0.1:9000/meteora/get/1?readConsistency=1'
{"key":"1","value":"2","timestamp":0,"readConsistency":1}

# 5. Set Request:
$ curl -X PUT -H 'Content-Type: application/json' -d '{"key":"1", "value":"1"}' 'http://127.0.0.1:9000/meteora/set?writeConsistency=1'
{"key":"1","value":"1","timestamp":0,"writeConsistency":1}

# 6. Repair
$ curl -X POST 'http://127.0.0.1:9000/meteora/repair'
{"repairId":"404b9fc9-7450-4c84-99af-0de325048625"}
```

## Improvements:
The following is a list of future improvements:
- Hinted Handoff.
- Adding Nodes.
- Removing Nodes.
- A FSM for Anti-Entropy Node Repair.

## References

- [Dynamo: amazon's highly available key-value store](http://dl.acm.org/citation.cfm?id=1294281)

> Giuseppe DeCandia, Deniz Hastorun, Madan Jampani, Gunavardhan Kakulapati, Avinash Lakshman, Alex Pilchin, Swaminathan Sivasubramanian, Peter Vosshall, and Werner Vogels. 2007. Dynamo: amazon's highly available key-value store. In Proceedings of twenty-first ACM SIGOPS symposium on Operating systems principles (SOSP '07). ACM, New York, NY, USA, 205-220. DOI=http://dx.doi.org/10.1145/1294261.1294281

- [Cassandra: a decentralized structured storage system](http://dl.acm.org/citation.cfm?id=1773922)

> Avinash Lakshman and Prashant Malik. 2010. Cassandra: a decentralized structured storage system. SIGOPS Oper. Syst. Rev. 44, 2 (April 2010), 35-40. DOI=http://dx.doi.org/10.1145/1773912.1773922