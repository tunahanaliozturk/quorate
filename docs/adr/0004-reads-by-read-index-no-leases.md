# 0004: Reads confirmed by a quorum round, not by a lease

Status: accepted, 2026-11-18

## Context

Until now a read was a log entry: correct, and as expensive as a write. A leader could answer from its own state for free, but only if it is still the leader, and it cannot know that by itself. A newer leader may exist that it has not heard of, and may already have acknowledged writes this node has never seen.

There are two established ways to close that gap.

1. **ReadIndex.** The leader notes its commit index, asks a quorum to confirm that it still leads, and then answers from any state that includes that index. One round of messages per batch of reads, no disk write.
2. **Leader leases.** The leader assumes that it leads for some time after its last successful heartbeat round, because followers promise not to vote for anyone else in that time. No messages per read, and correctness depends on clocks running at bounded rates.

The same milestone adds two rules that make a node with a bad connection less disruptive: pre-vote and check-quorum.

## Decision

**Reads use ReadIndex. There are no leases.**

- The leader keeps a round counter, raised by one for every read it accepts. Every `AppendEntries` carries the current round and the follower repeats it in its answer.
- A read is confirmed when a quorum, counting the leader, has answered a message of the read's round or a later one. Any answer in the leader's term counts, a refusal included: it shows the follower had seen no newer term when it answered a message sent after the read arrived.
- A leader that has not yet committed an entry of its own term refuses reads. Its commit index may be behind what the previous leader acknowledged.
- A leader that steps down refuses the reads it was still confirming.
- The core emits `ReadReady { id, index }`. The driver answers from its state machine once that has applied `index`.

**Pre-vote**: before an election a node asks whether it would win. Asking changes nothing on either side. A node says no while it still hears from a leader.

**Check-quorum**: a leader that has heard from fewer than a quorum for a whole election timeout steps down.

## Consequences

- A read costs one round trip to a quorum and no fsync. Reads that arrive while a round is in flight each start their own round; they are not merged yet.
- Correctness of reads does not depend on any clock. The simulator checks exactly that: clocks drift, and a leader that is cut off often has its clock slowed to a tenth, so that it keeps believing it leads long after a new leader exists. With the quorum round removed, 22 of the first 1,000 seeds produce a history that is not linearizable.
- Leases are left for after 0.1. They would make reads free, and they would add an assumption about clocks that nothing in this design needs so far.
- Pre-vote and check-quorum are about availability, not safety, but they change what the simulator can find. With all clocks in step, a cut-off leader steps down before a new one is elected, and the stale-read bug was not caught in 1,000 seeds. Fewer needless elections also mean fewer logs that diverge, and the figure 8 bug dropped from 15 failing seeds in 1,000 to 2. The simulator needed stronger faults to keep its tests meaningful: drifting clocks, and power cuts aimed at leaders. Rules that make faults rarer make the bugs behind them harder to find, not absent.
- A pre-vote and a granted answer to one carry a term that nobody is in. The step that adopts a higher term from any message has to skip them; forgetting that turns pre-vote back into the disruption it is meant to prevent. A unit test fails if it does.
- `AppendEntries` and its answer each grow by eight bytes for the round.
