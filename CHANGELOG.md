# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows [Semantic Versioning](https://semver.org/).

## Unreleased

## 0.1.0 - 2026-11-18

The first release. It is tagged on GitHub and not published to crates.io, because katman is a git dependency.

- Repository scaffold, design document and roadmap.
- Raft core as a pure state machine: leader election, log replication, a no-op entry on election and a conflict hint for fast log back-up. State is in memory only.
- Durability: the core issues write requests and holds messages until they are reported durable. `Storage` keeps the term, vote and log in a katman database.
- `quorate bench`, which measures clusters of real processes on the machine it runs on, and `docs/benchmarks.md` with the results.
- Group commit: a node handles events in batches and syncs once per batch.
- Frame limits per kind of connection; a frame's memory is taken as its bytes arrive.
- A `quorate` binary: `serve` runs a node over TCP, the other commands are a client (`get`, `put`, `delete`, `cas`, `member`, `status`). `Client` is the same thing as a library.
- A wire format with checksummed frames, and a test that kills the leader of a real cluster under load.
- The linearizability checker is public under the `testkit` feature.
- Dockerfile and a three-node `compose.yaml`.
- Membership changes one server at a time: `Membership`, `Change`, `Input::ChangeMembership`, learners, `Config::joining` for a node that joins an existing cluster. The state machine records the membership and snapshots carry it.
- Log compaction and snapshots: `Input::Compact`, `Output::SendSnapshot`, `Input::Snapshot`; `StateMachine::checkpoint`, `snapshot` and `install`; storage removes the entries before a new log base.
- A restarting node whose log disagrees with its state machine starts its log over.
- A step-by-step test of figure 8 of the Raft paper, and a weekly job that runs 200,000 simulator seeds.
- Reads without a log entry: the leader confirms itself with a quorum round (`Input::Read`, `Output::ReadReady`).
- Pre-vote and check-quorum.
- Simulator faults: drifting clocks, power cuts aimed at leaders.
- Key-value layer: `Request`, `Op` and `Reply`, and a `StateMachine` on katman with one row per client session so that a retried request is carried out once.
- A restarting node resumes from the state machine's applied index.
- Simulator: clients with retries, a linearizability checker per key, and a comparison of all state machines.
- Simulator faults: slow disks and power cuts with restart from storage.
- Deterministic simulator with message drop, duplication, delay and partitions, checking election safety, log matching, leader completeness, state machine safety and recovery after faults.
