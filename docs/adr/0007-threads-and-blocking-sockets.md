# 0007: Threads and blocking sockets, and a node that knows no network

Status: accepted, 2026-11-18

## Context

Up to here quorate ran only inside its simulator. Milestone 7 puts it on real sockets, as a process that can be started, killed and started again. The design document had planned for tokio.

Three things had to be decided: how the code that ties the core, the storage and the state machine together is shaped; what moves the bytes; and what the bytes look like.

## Decision

**A node driver with no I/O of its own.** `Node` owns the Raft core, the storage and the state machine. It takes events (a tick, a frame from a peer, a request from a client, a client that went away) and returns effects (a frame for a peer, a response for a client). It never touches a socket. All the ordering rules live there: persist before the core is told, checkpoint before compaction, install before the core hears of a snapshot, answer a client when its entry is applied.

**Threads and blocking sockets, no async runtime.** One thread owns the node. One thread accepts connections, one per incoming connection reads frames, one per peer writes frames and reconnects when the link breaks. They talk to the node's thread over a channel.

**A wire format written by hand.** Frames are length, CRC32C, payload. Every message is encoded field by field, big-endian. A connection opens with a magic number and a version. Decoding accepts a buffer only if it is exactly one value.

**Crash-only.** There is no shutdown path. A node is stopped by killing it, and that has to be safe at any instant, so it is the only way a node is ever stopped, in the tests as well.

## Consequences

- No new dependency beyond a checksum crate. The server is a few hundred lines that can be read top to bottom, and nothing in it is needed for correctness: a frame that is dropped, late or doubled is what the core is built for.
- A cluster has a handful of peers, so the thread per peer costs nothing. The thread per client connection is the ceiling: thousands of clients mean thousands of threads. An event loop for the client side is the first thing to change if this ever serves real traffic, and it can change without touching the node.
- The node's thread writes responses to clients itself and can wait up to two seconds on a client that does not read. A writer thread per client removes that; it is noted in the code.
- Storage is synchronous: the node's thread waits for the disk on every write that changes the term, the vote or the log. The core's contract allows writes to complete later, and the simulator uses that; the server does not yet. Throughput is bounded by one fsync at a time.
- The driver for the real server and the driver inside the simulator are two pieces of code that do the same job. The simulator's has slow disks, power cuts and message faults woven through it and stays as it is; the server's is tested by an in-process cluster and by the kill test. A bug could live in one and not the other. Moving the simulator onto `Node` would close that gap and is not done.
- Peer addresses are given at start and never looked up again. A node whose address changes needs a restart of its peers. The compose file gives the containers fixed addresses for that reason.
- There is no authentication and no encryption. Anyone who can reach the port can read, write and change the membership.
- The kill test ends a process; it does not cut the power. What the operating system had been given survives a killed process, so a storage layer that forgot to sync would pass it. Power loss is the simulator's job, on a filesystem that drops what was not synced.
- The first version of the kill test would have missed a cluster that carried out retried requests twice: a request was only ever repeated when a leader died, and one repeat rarely shows in a history. With the deduplication switched off by hand the test passed twice in a row. Half the clients are now impatient and give up after four milliseconds, so most of their requests reach the log more than once, and the same mutation fails the test three times out of three.
