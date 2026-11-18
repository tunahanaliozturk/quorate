# quorate design

This document is the contract. Code that disagrees with it is a bug in one of the two, and the fix says which.

## Goals

1. A replicated key-value store with `get`, `put`, `delete` and compare-and-set, where every operation is linearizable.
2. The cluster stays available while a majority of nodes can talk to each other.
3. No acknowledged write is lost when any minority of nodes fails, or when every node loses power at once.
4. Every bug found by the tests is reproducible from a single seed.

## Non-goals

- Sharding or multi-Raft. One Raft group holds all keys.
- Transactions over several keys. Compare-and-set on one key is the only conditional write.
- Byzantine faults. Nodes crash, pause and lose messages; they do not lie.
- Tolerating disk corruption beyond what katman already detects. A node whose database reports corruption stops.
- Production use. This is a study of how to build and test consensus, with honest numbers.

## Shape

```
client ── TCP ──> server ──> Node (driver) ──> raft core (pure)
                               │   │
                               │   └──> Transport (TCP or simulated)
                               └──────> Storage (katman)
```

Three layers, with one rule: the core never touches the outside world.

### Raft core

A struct with one entry point:

```rust
fn step(&mut self, input: Input) -> Vec<Output>;
```

`Input` is one of: a message from a peer, a clock tick, a client proposal, a read request, or a confirmation that earlier state reached disk. `Output` is one of: send a message, persist entries or hard state, apply committed entries, answer a client.

The core has no threads, no sockets, no files, no `Instant::now()` and no global randomness. Election timeouts come from a seeded generator passed in at construction. See [ADR 0001](adr/0001-sans-io-core-and-own-simulator.md).

Protocol features, in the order they are built:

- Leader election and log replication as in the Raft paper. A follower that refuses an append names the first index of its conflicting term, so the leader skips back a term per round trip.
- Pre-vote and check-quorum, so a partitioned node does not disrupt the cluster when it returns.
- Linearizable reads with ReadIndex: the leader confirms leadership with a heartbeat round before answering, without writing to the log.
- Snapshots and log truncation, with InstallSnapshot for followers that fall behind.
- Membership change one server at a time, with new servers joining as non-voting learners until they catch up.

### Storage

Two katman databases per node.

- **Log database**: the hard state (current term, vote) under one key, and the Raft log with entry `i` under `l` plus `i` as eight big-endian bytes. Always opened with `SyncMode::Always`. Each write request from the core is one atomic batch. A conflicting tail is replaced in the same batch that writes the new entries. See [ADR 0002](adr/0002-raft-log-in-katman.md).
- **State database**: the key-value data, one row per client session and the index of the last entry applied. Each apply writes all three in one `WriteBatch`, so a crash cannot apply an entry twice or skip one. This database is not synced: after a power cut it holds the result of some prefix of the log, and the node applies the rest again. See [ADR 0003](adr/0003-state-machine-unsynced-with-client-chosen-sessions.md).

The core does not wait for the disk. It emits `Output::Persist` with a sequence number and is told with `Input::Persisted` when that write is durable. Until then it holds back every message, counts its own log towards a quorum only up to its durable index, and applies nothing past that index.

katman is not yet published on crates.io. Until it is, quorate depends on it by git revision.

### Snapshots and log compaction

There is no snapshot file: the state database is the snapshot. Compacting the log means forcing the state database to disk and then dropping the log up to the index that is now durable. A follower that needs entries the leader no longer holds is sent every row of the leader's state database, installs them in one atomic batch, and continues from the log after that. The core never sees the data, only the index and term a snapshot ends with.

The state machine always becomes durable before the log is cut. A node that restarts in between finds a log that disagrees with its state machine, tells by the term at the state machine's position, and starts its log over after it. See [ADR 0005](adr/0005-the-state-database-is-the-snapshot.md).

### Membership

The cluster changes one server at a time: a new server joins as a learner, which receives the log and counts for nothing, is promoted to voter once it has caught up, and an old server is removed. A change is a log entry and is in force on a node as soon as it is in that node's log. The leader allows one change in flight, and none before it has committed an entry of its own term. The state machine records the membership, so it travels in snapshots and survives compaction. See [ADR 0006](adr/0006-membership-one-server-at-a-time.md).

### Client sessions

A client picks a random 64-bit session id. Each request carries the session id and a sequence number, and a session has one request in flight at a time. The state machine stores the highest sequence number and its reply per session. A request with that same number gets the stored reply; one with a lower number is ignored. Without this, a retry after a lost reply would apply twice and break linearizability.

Sessions do not expire yet, so the table only grows.

### Reads

A read does not go through the log. The leader notes its commit index, has a quorum confirm that it still leads, and the node then answers from its state machine once that has applied the index (ReadIndex). No clock is involved. A leader that has not committed an entry of its own term yet, or that steps down while a read waits, refuses the read and the client tries again. See [ADR 0004](adr/0004-reads-by-read-index-no-leases.md).

A get can still be sent through the log like any other operation; the simulator does both.

### Transport and wire format

A `Transport` trait with two implementations: simulated (in-process queues owned by the simulator) and TCP on tokio. Frames are length-prefixed with a CRC32C. Message encoding is written by hand, with a version byte, and fuzzed.

### Server and client

A `quorate` binary that runs one node, and a small client library plus CLI. The client follows leader redirects and retries with its session.

## Simulation

The simulator is a single-threaded loop that owns every node, a virtual clock, a message queue and a seeded generator. Each iteration picks the next event: deliver a message, tick a node, issue a client request, or inject a fault.

Faults:

- Message drop, duplication, reordering and delay.
- Partitions: symmetric, one-way, and a leader isolated from the majority.
- Power cuts. The node loses its memory and every write that had not completed. Storage runs on katman's `SimFs`, which discards unsynced bytes; the node restarts later from what the database holds.
- Slow disks: a write completes some ticks after it was requested, in request order.
- Log compaction: every few applied entries a node checkpoints its state machine and drops the log before it, so followers that fall behind need snapshots. Often a node loses power right after it installed one.
- Membership: two spare nodes run from the start, and at random moments one joins as a learner, a learner is promoted or dropped, or a voter is removed.
- Clients: requests and replies are lost, a client that waits too long sends the same request to another node, and old requests are sent again at random.
- Bytes that are not a request are proposed, to check that every node ignores them the same way.
- Clock drift: now and then a node ticks only half or a fifth of the time, and a leader that is cut off often slows to a tenth.
- Power cuts are aimed at a leader half the time.

Checks, after every step:

- **Election safety**: at most one leader per term.
- **Leader completeness**: a committed entry appears in the log of every later leader.
- **State machine safety**: no two nodes apply different entries at the same index.
- **Durable before applied**: no node applies an entry while the write that stores it is still in flight.

Checks, every 32 ticks and at the end (they compare whole logs, which is too slow for every step):

- **Log matching**: two logs with the same index and term agree on everything before it.
- **Committed entries stay**: the committed part of every node's log equals the record of applied entries.
- **Bounded log**: no node holds more applied entries in its log than the compaction policy allows.

Checks, at the end of a run:

- **Linearizability**: the history of each key (invocations, returns and results, on one shared clock) is searched for an order that a single-threaded store could have produced. The checker is the Wing and Gong search with memoisation, and has its own tests.
- **Identical stores**: the state machines of all nodes hold the same bytes.
- **Liveness and durability**: after faults stop and every node is back, all nodes converge on one fully committed log that holds every entry any node ever applied, and a new write then commits everywhere within 50 ticks.

A failing run prints its seed. Setting `QUORATE_SEED=N` replays that run step for step.

### Does the simulator catch bugs

A simulator that never fails proves nothing. The test suite includes known-wrong variants of the core, behind a test-only switch, and asserts that the simulator rejects each within a seed budget:

- Commit an entry from an earlier term by counting replicas (the bug in Figure 8 of the Raft paper).
- Grant a vote without checking that the candidate's log is up to date.
- Acknowledge an append before it is durable.
- Carry out a retried request a second time.
- Compact the log before the state machine is on disk.
- Accept a membership change while an earlier one is uncommitted.
- Answer a read from a leader that has not confirmed its leadership.

## Errors

A node stops, loudly, when storage reports corruption or when an invariant fails. It does not try to continue with state it cannot trust. Client-facing errors are `NotLeader { hint }` and `Timeout`; a `Timeout` means the outcome is unknown and the client retries with the same sequence number.

## Benchmarks

Published in `docs/benchmarks.md` at M8, with the hardware and the commands:

- Write throughput and p50/p99 latency for a three-node and a five-node cluster on one machine, with fsync on.
- Read throughput with ReadIndex.
- Time to elect a new leader after the old one is killed.
- Simulator speed: simulated cluster seconds per wall-clock second.

No target numbers are claimed before they are measured.

## Dependencies

Kept small and permissive: katman, crc32c, tokio (transport and server only, never in the core), proptest for tests. `cargo deny` checks licences and advisories in CI.
