# kurultay

A linearizable key-value store for Rust, replicated with Raft and stored in [katman](https://github.com/tunahanaliozturk/katman).

*Kurultay* is the Turkic word for a council that meets to reach a decision. A cluster of nodes does the same for every write.

## Status

Planning. The repository holds the design and the roadmap; no functional code yet.

- [docs/design.md](docs/design.md): what it does, what it does not do, and how each promise will be tested.
- [docs/roadmap.md](docs/roadmap.md): milestones, each with an exit test.
- [docs/adr/](docs/adr/): decisions that are hard to reverse.

## What it will promise

| Promise | How it will be tested |
| --- | --- |
| Every operation is linearizable, also during leader changes and partitions. | Client histories from the simulator are checked by a linearizability checker. |
| A write that was acknowledged survives the crash of any minority of nodes, and a power cut on all of them. | Simulated nodes run on katman's `SimFs`, which forgets unsynced bytes when a node is cut. |
| A retried request is applied once. | Client sessions with sequence numbers; the simulator duplicates and replays messages. |
| The Raft safety properties hold after every step. | The simulator asserts election safety, log matching, leader completeness and state machine safety on every tick. |
| Any failure is reproducible from one integer. | The Raft core does no I/O and reads no clock. A seed decides message order, delays, drops, partitions and crashes. |

## Why another Raft

The point is the testing method, not the algorithm. The core is a pure state machine: inputs in, outputs out. That makes it possible to run years of cluster time per CI minute on one thread, with faults that are rare in production happening thousands of times per run.

## Licence

Dual licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
