# kurultay design

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

- Leader election and log replication as in the Raft paper.
- Pre-vote and check-quorum, so a partitioned node does not disrupt the cluster when it returns.
- Linearizable reads with ReadIndex: the leader confirms leadership with a heartbeat round before answering, without writing to the log.
- Snapshots and log truncation, with InstallSnapshot for followers that fall behind.
- Membership change one server at a time, with new servers joining as non-voting learners until they catch up.

### Storage

Two katman databases per node.

- **Log database**: Raft log entries keyed by big-endian index, plus the hard state (current term, vote). Opened with `SyncMode::Always`. An entry is acknowledged to the leader only after katman reports it durable. katman's group commit batches concurrent appends into one fsync.
- **State database**: the key-value data, the client session table and `applied_index`. Each apply writes all three in one `WriteBatch`, so a crash cannot apply an entry twice or skip one.

Open question for M2: log truncation deletes a range of keys. If katman has no cheap range delete, either katman gains one or the log moves to its own segment files. The answer becomes an ADR.

katman is not yet published on crates.io. Until it is, kurultay depends on it by git revision.

### Client sessions

A client registers and gets a session id. Each request carries the session id and a sequence number. The state machine stores the last sequence number and reply per session, and returns the stored reply for a duplicate. Without this, a retry after a lost reply would apply twice and break linearizability.

### Transport and wire format

A `Transport` trait with two implementations: simulated (in-process queues owned by the simulator) and TCP on tokio. Frames are length-prefixed with a CRC32C. Message encoding is written by hand, with a version byte, and fuzzed.

### Server and client

A `kurultay` binary that runs one node, and a small client library plus CLI. The client follows leader redirects and retries with its session.

## Simulation

The simulator is a single-threaded loop that owns every node, a virtual clock, a message queue and a seeded generator. Each iteration picks the next event: deliver a message, tick a node, issue a client request, or inject a fault.

Faults:

- Message drop, duplication, reordering and delay.
- Partitions: symmetric, one-way, and a leader isolated from the majority.
- Node crash and restart. Storage runs on katman's `SimFs`, which discards everything not yet synced.
- Clock skew between nodes, within a configured bound.
- Slow disks: persistence confirmations delayed by many ticks.

Checks, after every step:

- **Election safety**: at most one leader per term.
- **Log matching**: two logs with the same index and term agree on everything before it.
- **Leader completeness**: a committed entry appears in the log of every later leader.
- **State machine safety**: no two nodes apply different entries at the same index.

Checks, at the end of a run:

- **Linearizability**: the recorded client history (invoke and return times, results) is checked against a key-value model.
- **Liveness**: after faults stop, the cluster commits a new write within a bounded number of ticks.

A failing run prints its seed. Setting `KURULTAY_SEED=N` replays that run step for step.

### Does the simulator catch bugs

A simulator that never fails proves nothing. The test suite includes known-wrong variants of the core, behind a test-only switch, and asserts that the simulator rejects each within a seed budget:

- Commit an entry from an earlier term by counting replicas (the bug in Figure 8 of the Raft paper).
- Grant a vote without checking that the candidate's log is up to date.
- Acknowledge an append before it is durable.
- Answer a read from a leader that has not confirmed its leadership.

## Errors

A node stops, loudly, when storage reports corruption or when an invariant fails. It does not try to continue with state it cannot trust. Client-facing errors are `NotLeader { hint }`, `Timeout` and `SessionExpired`; a `Timeout` means the outcome is unknown and the client retries with the same sequence number.

## Benchmarks

Published in `docs/benchmarks.md` at M8, with the hardware and the commands:

- Write throughput and p50/p99 latency for a three-node and a five-node cluster on one machine, with fsync on.
- Read throughput with ReadIndex.
- Time to elect a new leader after the old one is killed.
- Simulator speed: simulated cluster seconds per wall-clock second.

No target numbers are claimed before they are measured.

## Dependencies

Kept small and permissive: katman, crc32c, tokio (transport and server only, never in the core), proptest for tests. `cargo deny` checks licences and advisories in CI.
