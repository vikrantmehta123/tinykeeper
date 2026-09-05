## ClickHouse is Multi-Master But Keeper is Master-Slave

ClickHouse is multi-master. We can send `INSERT` queries to any node in ClickHouse cluster, and it will accept it.

But multi-master doesn't really mean that all nodes are independently operating.

### Background Merges

While any node can accept an INSERT, ClickHouse constantly performs background operations, like merging small data parts into larger ones (the core mechanic of MergeTree). If every node in a cluster independently decided to merge the same data parts at the same time, it would waste a lot of CPU and disk I/O.

Instead, the ClickHouse nodes use Keeper to elect one specific node as the "leader replica" for that specific background merge. That leader replica coordinates the merge and tells the others what to do.

### ClickHouse Keeper is a Separate System

ClickHouse Keeper is a separate system (even if it runs inside the same process). While ClickHouse is designed to be "eventually consistent" (data gets replicated in the background over time), Keeper must be strongly consistent. Every node must agree on the exact state of the metadata at all times.

To guarantee this strong consistency, the Keeper nodes use an algorithm (like Raft). This algorithm dictates that all write operations (creating a node, updating data) must be routed through a single, elected Keeper Leader. The Keeper Leader decides the exact strict order of operations and ensures all other Keeper nodes apply them in that exact same order.

## Concurrency in Keeper

Let's assume a setup where we have three keeper nodes. And we will assume something like NuRaft or openraft that handles the Raft consensus for us. This is from a ClickHouse and C++ point of view. In Rust, this discussion can be slightly different.

* Each keeper node has its own Raft log, which is stored on disk like a WAL. So each node can have its own disk for the log. 
* The in-memory keeper tree is the "result" of applying that log.
* Raft guarantees that the on disk WAL will be correct. We have to guarantee that the result state is correct.

How does ClickHouse maintain the tree and provide the guarantee that there is no data race?
* Keeper is a leader-follower setup. So there is only one node that accepts writes in Keeper. In ClickHouse, if the clients connect to a follower, the write is rejected and the client has to try connecting to a different node i.e. the leader node.
* Now, multiple clients can concurrently send writes to the leader as well! So ClickHouse has a queue where the clients put their write requests. This is non-deterministic. We cannot guarantee which thread will put their request first and we are okay with this.
* There is another thread that is pulling these write requests from the queue and sending it to NuRaft to append to log. Note that this is a single threaded operation! A single thread calls NuRaft with a write request and then NuRaft takes over.
* NuRaft then does its thing, writes the "request" to the Raft log on its disk. This happens on the leader node only!
* Then the leader node calls `pre_commit()` and then the log entry is sent to the follower nodes to apply to their records.
* The `pre_commit()` method takes an exclusive lock on the `delta` linked list. In this, the node records the deltas for that zxid in one order. Again this is single-threaded and under a lock. This record is not yet applied to the keeper tree. The deltas are like a staging area.
* When the leader node's NuRaft gets back acknowledgements from majority of the nodes, then it calls `commit()`. This method takes an exclusive lock on the actual keeper tree and applies the deltas one by one for that zxid. Again this is single threaded!
* So, by design, the write-write conflicts are entirely eliminated. 
* Only read-write conflicts are possible but they are also eliminated by taking the locks.
* Note that while committing, we need only a Read lock on the `delta` list.

Thanks to Rust, this whole class of problems itself goes away! We can be quite relaxed and think that this issue won't be there in our Rust implementation. We can simply use the RwLock and we're sorted. There is a very nice Claude session where I explored this more.

Refer to this session: `claude --resume "ClickHouse Keeper Concurrency"`

#### TODO Section

Let's think about how this might be implemented in Rust:
1. We have tokio running asynchronous tasks. Tokio receives incoming client requests.
2. We first parse the incoming request based on the opcode.
3. Then we match the opcode to the appropriate handler.
4. If the request is valid, then we call openraft in between. TODO: Need to check whether the validation happens before openraft or after.
5. Each handler is an async function, which gets registered as a task with our tokio runtime.
6. Each handler has to get either a read or a write lock on the keeper tree. Since we let tokio control all the thread spawning and execution in tinykeeper, we don't control the fact that there will be a dedicated thread handling write requests.
7. So we rely on the RwLock on the keeper tree to prevent the data races.
8. TODO: In Rust design, we need to check what happens when a write task is picked up by another thread. That is, we need to check whether we implement `Send` for our handler.
9. TODO: For the moment, we are not having a delta mechanism. We are simply cloning the tree. I think we can rely on openraft to send sequential requests and not concurrent write requests. So we will only have one clone of the tree. But still we need to check how the locking mechanism will work in this cloned tree.
10. All this is to support the `multi` opcode. A cloned copy acts as a proxy for the deltas and can be used to simulate a transaction.
11. In v2, we need to add support for deltas. That's for sure.

---

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

## Persistent and Ephemeral Nodes

Ephemeral Nodes have a lifetime. They live only as long as the session that created that node. 

But they are still stored on disk and on the data tree like other nodes. It's just that a background thread cleans up these ephemeral
nodes once the session is closed. This too has a log entry and has to go through Raft and are captured in the snapshots.

Persistent nodes have no such lifetime. They persist until someone calls `delete()` on them explicitly.

Why do we even have ephemeral nodes?
Because there are several things, i.e. state, that is session scoped. For example, liveness of a client. Another example would be
lock acquisition- locks acquired by clients are session scoped.

Often, ephemeral nodes work together with "watchers", which we will cover later. Watchers are a more efficient way to detect changes in an ephemeral node.

## Sequential Nodes

Whether a node is sequential or not is independent of whether the node is ephemeral or persistent.

A sequential node is a node whose final path is not exactly what you asked for — Keeper appends a number to it.

You provide a path prefix, say `/queue/task-`. Keeper takes that prefix, looks at the parent node's sequence counter,
and appends it as a 10-digit zero-padded number. So the actual created node might be `/queue/task-0000000000`. The next sequential create under `/queue/` produces `/queue/task-0000000001`, then `0000000002`, and so on.

The sequence counter belongs to the parent node, and it only goes up — it never resets or reuses numbers, even if earlier nodes are deleted. This means every sequential node created under the same parent gets a unique, monotonically increasing number.

Why do we need Sequential nodes?
Without sequential nodes, if two clients both try to create `/queue/task-1` at the same time, one succeeds and the other gets a `ZNODEEXISTS` error. 
The failing client has to pick a new name and retry. This is coordination overhead. With sequential nodes, both clients can create the nodes with prefix and then keeper assigns a different number.

This is specifically used during leader election and a distributed queue.

## Sessions

A session is a logical, stateful connection between a client and the Keeper cluster. It starts when a 
client connects, stays alive as long as the client sends heartbeats, and dies when the client goes silent for too long.

Sessions are used for three things:

1. Ephemeral nodes: A client can connect and create nodes and say, "Delete this node when I disconnect". Distributed locks and all come here.
2. Watches: When a client sets a watch on a node ("tell me when this changes"), the cluster needs to know which client to notify. That binding is per-session.
3. Authentication and ACL 

How are sessions expired?
We don't keep track of expiry of each session. We time bucket the sessions based on their expiry. All sessions are rounded up to some
value and sessions expiring around similar time land in same expiration bucket.
Heartbeats move session from one bucket to another.

### How Are Sessions Created?

Each session is uniquely identified by a session ID. The keeper is a distributed system. There can be multiple keeper servers to
which clients can connect and send requests to. 

We need to ensure that the clients are getting unique session ID regardless of the server they connect to.

That's why the session ID generation has to go through Raft. The leader has to assign the session ID and then the ID has to be
forwarded to other nodes.

## Watches

Watches are part of ZooKeeper protocol. It's for event-drive notifications.

Instead of clients polling for changes, a client says "give me this value, and also notify me if it changes." The server remembers the interest and pushes a small notification when the value changes.

In initial versions of ZooKeeper, there were only one-shot watches. The server pushes notification for one change event and then it stops. If the client wants to keep listening to events, it had to re-register.

This led to some problems in high-churn systems, where change events would get missed in the small window of re-registering. So ZooKeeper introduced persistent watches in later versions of the protocol.

### How is a Watch part of the protocol?

There is no separate request for Watch. Watch is always piggybacked onto a read request- it's typically a flag in the read request. So the interpretation is: read this node and then watch it. 

We don't have watches in write requests. Not all read requests have watches as well. The following read requests have watch flags:

1. getData
2. exists
3. getChildren

However, persistent watches ( introduced in later versions ) have a separate request for them.

## Sequential Nodes

When you create a znode, you can pass a flag called `SEQUENTIAL`. When you do, the server doesn't create the node with exactly the name you asked for — instead, it appends a monotonically increasing counter to the name you gave.

The core problem sequential nodes solve is: multiple clients need to independently create entries under the same path, and they need a global, conflict-free ordering of who came first.

The Keeper server is the single authority that assigns the number, so there's no conflict and no ambiguity about who was first.

For ClickHouse, sequential nodes are used for distributed DDL queue (e.g. ALTER TABLE) and for replicated merge tree table's entries.

The sequential counter is stored on the parent node- not the node itself! 

Ephemeral nodes cannot have children so they don't also need a sequential counter. 

ClickHouse does bit-packing using the above information. It either packs the ephemeral_owner or the num_children + seq_num.

In clickhouse, regardless of whether you create a sequential node or non-sequential, you increment counter when you create the child.

The counter reflects all the child creations- not just the sequential ones. This is done because it guarantees that the counter never 
produces collisions. Otherwise, you need some other mechanism to ensure that there is never a collision between names of sequential and
non-sequential nodes.

## Multi

ZooKeeper gives you basic operations: create, delete, setData, check. Each one is atomic. 
But in real systems often need several operations to happen together.

That's why ZooKeeper offers a `multi` primitive. This accepts a list of operations and ZooKeeper guarantees atomicity and isolation.

That implies:
* Every operation in the list either succeeds or none succeed.
* The whole batch is applied as a single atomic unit in global order. No other client sees an intermediate state, and no other operation interleaves inside your batch.

Only four operations are allowed in the `multi` operations list:

* Create: There are multiple create operations allowed in zookeeper: Create2, CreateTTL, Create, and createContainer. All are allowed.
* setData
* delete
* check: "make sure this node is still at version X". This changes nothing.

A WatchEvent can also be registered nodes being changed in the `multi`. Let's say four nodes are already being watched and then `multi` changes those nodes. The watch events should be emitted only if the `multi` commits.
