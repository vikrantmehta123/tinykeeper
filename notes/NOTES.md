## ClickHouse is Multi-Master But Keeper is Master-Slave

ClickHouse is multi-master. We can send `INSERT` queries to any node in ClickHouse cluster, and it will accept it.

But multi-master doesn't really mean that all nodes are independently operating.

### Background Merges

While any node can accept an INSERT, ClickHouse constantly performs background operations, like merging small data parts into larger ones (the core mechanic of MergeTree). If every node in a cluster independently decided to merge the same data parts at the same time, it would waste a lot of CPU and disk I/O.

Instead, the ClickHouse nodes use Keeper to elect one specific node as the "leader replica" for that specific background merge. That leader replica coordinates the merge and tells the others what to do.

### ClickHouse Keeper is a Separate System

ClickHouse Keeper is a separate system (even if it runs inside the same process). While ClickHouse is designed to be "eventually consistent" (data gets replicated in the background over time), Keeper must be strongly consistent. Every node must agree on the exact state of the metadata at all times.

To guarantee this strong consistency, the Keeper nodes use an algorithm (like Raft). This algorithm dictates that all write operations (creating a node, updating data) must be routed through a single, elected Keeper Leader. The Keeper Leader decides the exact strict order of operations and ensures all other Keeper nodes apply them in that exact same order.

## ZNodes

At its core, ClickHouse Keeper (which is a drop-in replacement for ZooKeeper) is a coordination service for distributed systems.

When you have a distributed database like ClickHouse running on multiple servers, those servers need a central, highly reliable place to store small pieces of important metadata. They use this central place to answer questions like:

* "Which server is currently the leader?"
* "Which servers are currently alive and reachable?"
* "What is the latest schema for this table?"

To store this information, ClickHouse Keeper provides a hierarchical, in-memory data store. Everything is simply a node—historically called a ZNode (ZooKeeper Node).

A ZNode can hold a small amount of binary data and it can also have several child ZNodes.

A node is identified by its name which is a string. It's derived from the path, like in a filesystem, to the node from the root ZNode.

For example, the following are nodes in the data-store identified by their path:

```
/clickhouse/tables/01/default/hits/replicas/r1/is_active
/clickhouse/tables/01/default/hits/log/log-0000000042
```

## Wire Protocol

Think about how ClickHouse Keeper will work with ClickHouse Server.

Typically, ClickHouse Servers will be running on a separate machine than the ClickHouse Keepers. So ClickHouse servers will act as clients to the Keepers.

So Keepers and Servers have to communicate. Forget leader/slave/multi-master and what not for a moment. But there is a need for these two to communicate. That's why there needs to be a protocol.

That protocol is a standard ZooKeeper protocol. It's called the "Wire Protocol". It's a binary protocol. Both requests and responses have a protocol.

We, as the writers of Keeper, we need to serve requests from the ClickHouse Server clients.

The ZooKeeper protocol is detailed in this file [here](https://github.com/apache/zookeeper/blob/master/zookeeper-jute/src/main/resources/zookeeper.jute).

ZooKeeper itself is a protocol over TCP, like HTTP. We don't send ZooKeeper requests/responses over HTTP. Since ZooKeeper is a protocol of its own, it also has some handshake, etc.

Like HTTP has methods like GET or POST, ZooKeeper Wire Protocol also has different types of "requests". Each type of request is mapped to an integer code called "OpCode" or "type".

The wire protocol can be summarized as below:

```
CLIENT → SERVER  (handshake):
  [int32] packet length
  [int32] protocol version
  [int64] last zxid seen
  [int32] timeout ms
  [int64] previous session id
  [bytes] password (fixed 16 bytes)

SERVER → CLIENT  (handshake response):
  [int32] packet length
  [int32] protocol version
  [int32] negotiated timeout ms
  [int64] assigned session id
  [bytes] password echo

CLIENT → SERVER  (each request):
  [int32] packet length
  [int32/64] XID
  [int32] opcode
  [variable] opcode payload

SERVER → CLIENT  (each response):
  [int32] packet length
  [int32/64] XID (matches the request)
  [int64] zxid (transaction ID assigned by Raft)
  [int32] error code
  [variable] opcode payload
```

## KeeperStateMachine and the LogEntry

This is not really a state machine in the FSM sense of the word. The terminology comes from the Raft paper. 

It doesn't mean an FSM but a Replicated State Machine. The idea is following:

```text
If you start multiple machines in the same initial state, and feed them the same sequence of operations in the same order, they will all end up in the same state.
```

* "state" is the entire ZooKeeper data tree at any given point of time.
* "operations" correspond to ZooKeeper commands.
* "machine" is the code that we write (i.e. the Keeper), which applies those operations deterministically.

So think about the division of responsibility here:
* Raft tells each Keeper node what log entries to apply and in what order. So Raft is guaranteeing that each Keeper node will see the same log entries and in the same order.
* The Keeper State Machine is just the code that actually applies those log entries deterministically.

And what are the Log Entries? 
* The log entries are simply the ZooKeeper commands with their log index, parameters and payload.
* Since the log entries need to be ordered, we need a log index. This is the zxid.

