# 0005: The state database is the snapshot

Status: accepted, 2026-11-18

## Context

A log that only grows fills the disk and makes a restarting node replay everything. Raft's answer is the snapshot: a copy of the state machine at some index, after which the log before that index can go. A follower that needs entries the leader no longer has is sent the snapshot.

Four things had to be decided.

1. What a snapshot is, physically.
2. How the core, which knows nothing about the state machine, takes part.
3. In what order things become durable, given that the state database is not synced ([ADR 0003](0003-state-machine-unsynced-with-client-chosen-sessions.md)).
4. How entries are removed from katman, which has no range delete ([ADR 0002](0002-raft-log-in-katman.md) left this open).

## Decision

**There is no snapshot file. The state database is the snapshot.** It already holds the state at the applied index, atomically, with that index and its term stored beside the data.

- *Compacting the log* is: force the state database to disk (`StateMachine::checkpoint`, a katman flush), then tell the core `Input::Compact { index }` with the index that is now durable. The core drops its log up to there and asks storage to do the same.
- *Sending a snapshot* is: read every row of the state database as it is now and send them, with the applied index and term (`StateMachine::snapshot`).
- *Installing a snapshot* is: replace every row in one atomic batch and force it to disk (`StateMachine::install`).

**The core never sees snapshot data.** It emits `Output::SendSnapshot { to, term, round }` when a follower needs entries the log no longer holds. The receiving driver installs the snapshot and then tells its core `Input::Snapshot { from, term, round, snapshot }`, where `snapshot` is only the index and term. The core adjusts its log and acknowledges through the ordinary `AppendEntries` answer.

**The log keeps what still matches.** If the log holds the snapshot's last entry with the same term, only the part before it goes. Otherwise the whole log is replaced by an empty one that starts after the snapshot.

**Order of durability, on every path: state machine first, log second.**

- Compaction: the checkpoint is on disk before the core is told it may drop entries.
- Install: the snapshot is on disk before the core is told it arrived.

So there is a window in which the state machine is ahead of what the log on disk says. A node that restarts in that window finds a log that does not agree with its state machine. The rule on restart is the same as on install: compare the term at the state machine's position. If the log holds a different term there, or nothing, the log starts over after that position.

**Entries are removed with one delete per key**, in the same batch that moves the start of the log. No range delete.

## Consequences

- Nothing new to keep consistent: no snapshot file, no format, no separate durability story. A snapshot can be taken at any moment without preparation.
- A snapshot is the whole store, read into memory and sent as one message. That is fine for a store that fits in memory and wrong for a large one; streaming it in chunks is noted in the code and on the roadmap.
- The leader sends its state as it is when asked, not as it was at the start of its log. A follower can therefore receive a snapshot that ends later than the leader's log base, and the core takes the index from what was actually installed.
- A snapshot from a leader the node no longer follows is still installed: it is committed state, and refusing it would need the driver to ask the core first. The core does not move its log for it. The state machine can then be ahead of the core's own applied index, and the compaction policy has to go by the core's index. The simulator found this as a stack overflow in its own policy.
- The delete batch grows with the distance between two compactions. With a compaction every few hundred entries that is small. A range delete in katman would make it constant and is worth adding once compactions are far apart.
- The restart rule compares terms, and the first version compared lengths. A log that was long enough but had gone another way was kept, and the node came back with entries that contradicted its own state machine. Ten thousand seeds did not show it; a sweep of fifty thousand showed it three times. The fix is two lines, the lesson is the sweep: the simulator now cuts the power right after an install on purpose, which makes the same bug show within 250 seeds, and a weekly job runs many more seeds than the test suite does.
- After a snapshot install the commit index was at first allowed to stay ahead of the log. That is harmless for safety and broke the simulator's assumption that committed entries are in the log; keeping the matching tail removes the case.
