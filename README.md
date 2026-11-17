# quorate

A linearizable key-value store for Rust, replicated with Raft and stored in [katman](https://github.com/tunahanaliozturk/katman).

A meeting is *quorate* when enough members are present to make a binding decision. A cluster of nodes needs the same for every write.

## Status

Milestones 1 and 2 of 8 are done: the Raft core (leader election and log replication), durable storage in katman, and the simulator that tests both. There is no key-value layer and no network yet, so this is not usable as a database.

What the simulator shows today, all reproducible with `cargo test`:

- 10,000 seeds, each 1,500 ticks on a cluster of three or five nodes, break no safety property. The faults are dropped, duplicated and delayed messages, changing partitions, slow disks and power cuts. Two thirds of the seeds cut the power to nodes; the first 1,000 seeds commit 221 entries each on average.
- After the faults stop and every node is back, every seed converges on one fully committed log that holds every entry any node ever applied, and then commits a new write. No acknowledged entry is lost.
- Three rules are switched off one at a time to prove the simulator would notice. Of the first 1,000 seeds, the missing log check on votes fails 984, sending a message before its write is durable fails 152, and committing an earlier term's entry by counting replicas (figure 8 of the Raft paper) fails 32. The last one needs a long chain of events, so it is rare.

A failing seed replays exactly with `QUORATE_SEED=n cargo test`.

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
