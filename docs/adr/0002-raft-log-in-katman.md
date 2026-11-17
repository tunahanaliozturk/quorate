# 0002: The Raft log lives in katman, and writes are reported back to the core

Status: accepted, 2026-11-17

## Context

A node has to keep its term, its vote and its log across a power cut. Two questions were open after milestone 1.

**Where does the log live?** The design proposed katman, with a doubt: truncating a log deletes a range of keys, and katman has no range delete. The alternative was a purpose-built log in its own segment files.

**How does the core learn that a write is durable?** Two contracts are common. In the synchronous one, a step returns "persist this, then send these", and the driver must finish the write before it sends anything. In the asynchronous one, the core issues a write request, carries on, and is told later that the write completed.

## Decision

**The log is stored in katman**, in the same database as the hard state. Entry `i` is under the key `l` plus `i` as eight big-endian bytes; the hard state is under `h`. Each request from the core becomes one atomic, synced `WriteBatch`.

Range delete turned out not to be needed yet. The only truncation so far is of the tail, when a follower's entries conflict with the leader's. Storage knows its own last index, so it deletes exactly the stale keys, in the same batch that writes the new entries. A crash leaves either the old tail or the new one, never a mix.

**The contract is asynchronous.** The core emits `Output::Persist` with a sequence number and continues. The driver reports completion with `Input::Persisted`. Until then the core holds back every message it produces, uses only its durable index when it counts its own log towards a quorum, and applies nothing beyond that index.

## Consequences

- One storage engine to test and reason about. katman's own crash tests cover the file formats; quorate's simulator covers what quorate does with them.
- A synced katman batch per request is the cost of every vote and every append. Requests are not merged yet. katman's group commit helps only when several threads write, and a node has one.
- Removing the head of the log after a snapshot (milestone 5) will delete many keys. That is where a range delete in katman, or deleting in bounded batches, has to be decided. This ADR does not settle it.
- The asynchronous contract lets a real driver keep serving the network while the disk works, and lets the simulator model slow disks by delaying the completion. It costs state in the core: the sequence numbers, the in-flight list and the queue of held messages.
- Holding every message while any write is in flight is stricter than Raft needs. A leader could send entries to followers in parallel with its own write. That is left out until a benchmark shows the disk is the bottleneck; the unit test `a_leader_counts_its_own_log_only_up_to_what_is_durable` guards the rule that change would lean on.
- A node replays `Output::Apply` from index 1 after a restart, because nothing stores how far it had applied. The state database of milestone 3 stores that index together with the data.
- quorate now depends on katman by git revision, and its minimum Rust version rises to 1.89 to match katman's. It cannot be published to crates.io until katman is.
