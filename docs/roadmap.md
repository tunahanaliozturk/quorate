# Roadmap

Each milestone ends with a test that has to pass. A milestone is not done because the code exists.

| # | Milestone | Scope | Exit test |
| --- | --- | --- | --- |
| M0 | Scaffold | Repository, lints, CI, design, roadmap. | CI green on an empty crate. |
| M1 | Raft core and simulator | Election and replication in memory. Simulator with drop, duplicate, reorder, delay and partitions. The four safety checks. | 10,000 seeds pass. The Figure 8 commit bug and the stale-log vote bug are each caught within 1,000 seeds. |
| M2 | Durability | katman-backed log and hard state. Crash and restart in the simulator on `SimFs`. Decide where the log lives (ADR). | No acknowledged entry is lost over 10,000 seeds with power cuts. The ack-before-durable bug is caught. |
| M3 | Key-value state machine | `get`, `put`, `delete`, compare-and-set. Client sessions. Linearizability checker. | Client histories from 10,000 seeds are linearizable, with duplicated and replayed requests. |
| M4 | Reads and stability | ReadIndex, pre-vote, check-quorum. | The stale-read bug is caught. A returning partitioned node does not force an election. |
| M5 | Snapshots | State snapshots, log truncation, InstallSnapshot. | A node that was down for longer than the retained log catches up by snapshot. Log size stays bounded in a long run. |
| M6 | Membership | Learners, add and remove one voter at a time. | Random membership changes during faults keep every check green. Replacing every node one by one loses nothing. |
| M7 | Real network | TCP transport, server binary, client library and CLI, three-node compose file. | Real processes: `kill -9` of the leader under load loses no acknowledged write; the recorded history is linearizable. |
| M8 | Numbers and release | Benchmarks, architecture diagrams, README with measured results. | `docs/benchmarks.md` reproducible from the listed commands. Tag v0.1.0. |

## Progress

- **M0**, 2026-11-17: done.
- **M1**, 2026-11-17: done. 10,000 seeds pass in about 35 seconds on a laptop. The figure 8 commit bug is caught on 13 of the first 1,000 seeds (first at seed 67), the missing vote check on 995. Three more mutations tried by hand (follower commit index past the last new entry, two votes in one term, truncating on a duplicate append) were each caught on seed 0. The simulator also found that a follower with a long conflicting log was probed one entry per round trip and never caught up under load; rejections now carry the first index of the conflicting term.
- **M2**, 2026-11-17: done. Storage is a katman database; the log question is settled in ADR 0002 (katman, no range delete needed yet). 10,000 seeds pass in about 30 seconds, two thirds of them with power cuts, and every seed ends with all nodes holding every entry that was ever applied. Sending before the write is durable is caught on 152 of the first 1,000 seeds (first at seed 2). The figure 8 bug is now caught on 32, the missing vote check on 984. Five mutations tried by hand were all caught: applying before the local write, keeping the durable index across a truncation, storage that keeps a stale tail, storage that does not sync, and a leader counting its own unwritten entries (a unit test, since with several nodes another rule masks it). The unsynced-storage mutation also exposed a message storm: a leader answered every refusal from a follower that had lost data with the same request at once. It now retries at once only when the refusal moved it back.
- **M3**, 2026-11-18: done. A key-value state machine on a second katman database, client sessions, and a linearizability checker. 10,000 seeds pass in about 25 seconds; the first 1,000 complete 90 client operations each on average, with lost requests and replies, retries to other nodes and stray copies of old requests. Every history is linearizable and all state machines end up byte for byte identical. Carrying out a retried request twice is caught on 328 of the first 1,000 seeds, always as a history that is not linearizable. Decisions are in ADR 0003: the state database is not synced, the client names its session, reads go through the log for now. Writing the state machine exposed a bug in the simulator itself: a disk write that finished at once ran a second step inside the first, and entries reached the state machine out of order.
- **M4**, 2026-11-18: done. Reads by ReadIndex, pre-vote and check-quorum (ADR 0004). 10,000 seeds pass in about 16 seconds. Answering a read without the quorum round is caught on 22 of the first 1,000 seeds, as a history that is not linearizable. A node that returns from a partition does not force an election; the same test with pre-vote switched off shows the term rising. Five mutations tried by hand were all caught, one of them only after a test was added for a node that hears everyone but the leader. Pre-vote and check-quorum made the earlier bugs rarer in the simulator: the stale read was not caught at all until clocks could drift, and figure 8 fell to 2 seeds in 1,000. Drifting clocks and power cuts aimed at leaders brought the rates to 22 and 5. Current rates for the others: retried request applied twice 360, send before durable 143, missing vote check 676.

## Order of work inside a milestone

1. Write the check or the fault first, and see it fail.
2. Implement until it passes.
3. Add the known-wrong variant if the milestone has one, and see the simulator reject it.
4. Record any decision that is hard to reverse as an ADR.

## After 0.1

Not planned, listed so they are not forgotten: session expiry, merging the quorum rounds of reads that arrive together, leader leases for reads without a heartbeat round, joint consensus, batching and pipelining of appends, a disk-fault model.
