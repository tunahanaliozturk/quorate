# 0006: Membership changes one server at a time

Status: accepted, 2026-11-18

## Context

A cluster that cannot change its members cannot replace a broken machine. Changing them is where consensus implementations most often go wrong: for a moment two different sets of servers each believe they are the cluster, and if a majority of one does not overlap a majority of the other, both can elect a leader.

Raft offers two ways through.

1. **Joint consensus.** An intermediate configuration needs majorities of the old and the new set at once. Any change in one go, at the price of a second kind of configuration and two log entries per change.
2. **Single-server changes** (Raft thesis, chapter 4). Add or remove one voter at a time. Any majority of a set and any majority of that set plus or minus one server share a member, so no intermediate configuration is needed.

## Decision

**Single-server changes, with learners.**

- The membership is a set of voters and a set of learners. A learner receives the log and counts for nothing: not for elections, not for commits, not for confirming reads.
- Three changes exist: add a learner, promote a learner to voter, remove a member. Replacing a server is three steps.
- A change is a log entry holding the whole new membership. **It is in force on a node from the moment the entry is in that node's log**, committed or not. If the entry is later overwritten, the node goes back to the membership before it.
- The leader refuses a change while an earlier one is uncommitted, and until it has committed an entry of its own term. The second rule is the fix for the bug found in the thesis algorithm in 2015: a new leader cannot tell whether a change it inherited is committed.
- The leader refuses to promote a learner that has not caught up with its commit index.
- A leader that removes itself keeps leading until that entry commits, without counting itself towards the majority, and then steps down.
- A node that is not a voter in its own membership does not start elections. That covers learners, removed nodes that know they were removed, and nodes started with `Config::joining`, which know of no members at all until a leader tells them.

**The state machine records the membership.** A membership entry is applied like any other and stored beside the data. It therefore travels in snapshots, and a restarting node asks its state machine who is in the cluster, since the entry that said so may have been compacted away. Founding nodes write the founding membership into the state machine when they first start.

## Consequences

- No joint configuration, one entry per step, and the safety argument fits in a sentence.
- Several changes take several rounds. Swapping every node of a three-node cluster is nine committed entries. The simulator does exactly that under client load, 1,000 times per test run.
- With an even number of voters in between (four while going from three to three), the cluster tolerates no more failures than before and needs one more acknowledgement. That is the usual price of this method.
- A node that was removed and did not hear of it still believes it is a voter and keeps asking for pre-votes. The members refuse while they have a leader, so it disturbs nobody; pre-vote from milestone 4 is what makes this harmless. It never learns that it is out. Telling it is left for later.
- The core keeps a "floor": a committed position and the membership as of it, used when the log holds no membership entry. It moves when the log is compacted or a snapshot is installed. Getting this wrong loses the membership silently, so three unit tests pin it: compaction, restart, and an overwritten entry.
- Votes are counted only from voters of the candidate's own membership. Nothing in the tests could reach the opposite case, which showed when the check was removed by hand and every test still passed; a unit test now feeds a stranger's vote directly.
- Membership changes ride on the same replication as everything else, which sends one batch and waits for the answer. With a batch of one entry and clients that never pause, a leader's log grows faster than it commits and a change waits behind a backlog that never clears. The replacement run therefore uses full batches. Sending ahead is on the roadmap.
- Accepting a second change while the first is in flight is caught by the simulator on 17 of the first 1,000 seeds, as two leaders in one term or a leader missing a committed entry.
