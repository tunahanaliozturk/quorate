# 0003: An unsynced state machine, and sessions the client names

Status: accepted, 2026-11-18

## Context

Milestone 3 adds the key-value store that committed entries are applied to, and the sessions that make a retried request take effect once. Three choices had to be made.

**Does the state database sync its writes?** The log database syncs every batch, because Raft is wrong without it. Syncing the state database as well would double the fsyncs per write.

**Where do sessions come from?** The Raft thesis has the client register through the log and get the log index back as its session id. That costs a round trip before the first request, and the reply to a registration has no session to be matched by.

**How are reads served?** Through the log like writes, or from the leader's local state.

## Decision

**The state database is never synced.** Each applied entry is one atomic katman batch holding the data change, the session row and the index of the entry. katman promises that without sync a power cut loses only the most recent batches and never splits one. So after any crash the database is the result of applying some prefix of the log. It records which prefix, and the node applies the rest again from its log, which is synced.

This leans on a rule from milestone 2: a node applies an entry only after its own log has it durably. Without that rule the state database could survive a crash holding an entry the log had lost.

A restarting node passes the stored applied index to `Raft::recover`, which starts with its commit index there. Those entries were committed when they were applied, so this is safe, and the node does not apply its whole log again.

**The client picks its session id**, a random 64-bit number. A session exists from its first request; there is no registration. The state machine keeps one row per session: the highest request number seen and the reply to it.

- A request with a higher number than the row is carried out and replaces the row.
- A request with the same number gets the stored reply and changes nothing.
- A request with a lower number is ignored. The client has moved on and nobody is waiting for it.

**Every operation goes through the log, reads included**, until milestone 4 adds ReadIndex.

## Consequences

- One fsync per write, not two.
- The state database can be deleted and rebuilt from the log at any time. That will stop being true when the log is truncated after a snapshot in milestone 5, and the snapshot then has to be durable before the log it replaces is removed.
- Sessions never expire, so the session table only grows. A client that picks a new id for every request leaks a row each time. Expiry needs a notion of time that all nodes agree on, which means putting it in the log; that is left for later and noted in the roadmap.
- Two clients that pick the same id corrupt each other's deduplication. With random 64-bit ids that is not a practical concern, and there is no authentication anyway.
- A session supports one request in flight. A client that wants concurrency opens several sessions.
- Bytes in the log that do not decode as a request change nothing. Every node reaches that verdict the same way, so the stores stay identical; the simulator proposes such bytes on purpose and compares the stores at the end.
- A read costs a log entry and an fsync on a quorum until milestone 4.
