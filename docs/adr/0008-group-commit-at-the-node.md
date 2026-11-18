# 0008: Group commit at the node

Status: accepted, 2026-11-18

## Context

The first benchmark of the server showed that write throughput did not grow with the number of clients: about 170 puts a second on three nodes with one client, and about 170 with sixteen. Each request was handled alone, and handling a request meant a synced write. Sixteen clients were sixteen requests queuing for sixteen syncs.

The core already allowed better. It asks for a write and carries on; it is told later that the write is durable, and holds its messages until then ([ADR 0002](0002-raft-log-in-katman.md)). Nothing in that contract says a write has to be synced by itself.

## Decision

**The node handles events in batches and writes once per batch.**

- The server's thread takes the event it was woken for and everything else already in its inbox, up to 1,024 events.
- The node feeds all of them to the core, one input at a time as before. Write requests the core issues on the way are collected, not carried out.
- When the core has nothing left to do, the collected requests go to storage as one katman batch with one sync. The core is then told that the last of them is durable, which by its contract covers all of them.
- That may set the core going again: held messages are released, entries become ready to apply. The loop repeats until nothing is left.

Storage merges the requests in order into one batch. A later request overwrites what an earlier one in the same batch stored, exactly as it would across two writes, and only the newest term and vote are written.

## Consequences

- Sixteen clients on three nodes went from about 170 puts a second to about 2,600, at a median latency of about 5 ms. A single client gains nothing: its requests never overlap. The numbers and their spread are in [docs/benchmarks.md](../benchmarks.md).
- Nothing about durability changed. The core still hears of a write only after it is synced, and still sends nothing that depends on it before. The batch is atomic, so a crash leaves all of it or none.
- The batch size adapts by itself. An idle node writes each request alone and adds no delay; a busy node writes as many as arrived during the last sync. There is no timer and nothing to tune.
- The node's thread still blocks on the sync. While it does, it does not tick and does not answer heartbeats. A sync that takes longer than an election timeout can therefore cost the leadership; the long tail at 64 clients in the benchmarks is probably that. Moving the write off the node's thread, which the core's contract also allows, removes it and is not done.
- The simulator does not go through this code: it has its own driver, with one write per request ([ADR 0007](0007-threads-and-blocking-sockets.md)). Merging is covered by a storage test that writes the same requests one by one and together and compares the disks, and by the kill test, which runs the real server.
