# Benchmarks

Measured on 2026-11-18 with `quorate bench`, which is in the binary so that anyone can run the same thing:

```
cargo build --release
target/release/quorate bench --seconds 5 --runs 3 --rounds 7
```

## What is measured, and what is not

Every node is its own `quorate serve` process. They talk over loopback, each has its own directory, and all directories are on one disk. Clients are threads in the benchmark process, each with its own connection, each sending one request after another. A put writes a 100-byte value to one of 1,000 keys; a get reads one of them.

That setup runs everything a deployment would run: the wire format, the sockets, and a synced write on every node for every batch. Two things make the numbers flatter than real life:

- **There is no network.** A round trip over loopback takes microseconds. Across machines, every write pays at least one real round trip and every read one more.
- **All nodes share one disk and one machine.** Five nodes do not have five disks here; they queue behind each other for the same one, and for the same 16 cores as the clients.

So read these as what the code and one laptop SSD cost, not as what a cluster would do.

The machine: Intel Core Ultra 7 255H (16 cores), 31 GB of memory, KIOXIA KBG60ZNV1T02 NVMe SSD, Windows 11 Enterprise build 26100, Rust 1.99.1, release build.

**The numbers move a lot from run to run.** Each row is the median of three runs of five seconds, and two full passes of the whole benchmark still disagree by a factor of two or three in places. Both passes are printed below for that reason. Trust the order of magnitude and the shape, not the digits.

## Throughput and latency

Latencies are in milliseconds, from the moment the client sends to the moment it has the answer. No request failed in any run.

| nodes | operation | clients | ops/s, pass 1 | ops/s, pass 2 | p50 | p99 |
| --- | --- | --- | --- | --- | --- | --- |
| 3 | put | 1 | 167 | 554 | 1.6 to 5.6 | 2.8 to 13 |
| 3 | put | 16 | 2,679 | 2,508 | 5.0 to 5.3 | 17 to 32 |
| 3 | put | 64 | 4,258 | 3,277 | 8.5 to 11 | 191 to 227 |
| 3 | get | 1 | 6,175 | 2,725 | 0.15 to 0.25 | 0.3 to 2.0 |
| 3 | get | 16 | 18,913 | 14,992 | 0.8 to 0.9 | 1.3 to 5.4 |
| 3 | get | 64 | 13,752 | 18,324 | 3.1 to 4.5 | 9.5 to 9.7 |
| 5 | put | 1 | 493 | 502 | 2.0 | 2.9 to 3.9 |
| 5 | put | 16 | 1,840 | 1,933 | 7.6 to 7.8 | 14 to 26 |
| 5 | put | 64 | 1,818 | 974 | 22 to 33 | 91 to 2,137 |
| 5 | get | 1 | 2,271 | 4,355 | 0.20 to 0.25 | 0.7 to 4.9 |
| 5 | get | 16 | 8,212 | 16,138 | 0.9 to 1.0 | 2.6 to 18 |
| 5 | get | 64 | 1,958 | 14,834 | 3.9 to 23 | 15 to 163 |

What stands behind the shape:

- **A single client is bound by the disk.** One put is one synced write on the leader and one on each follower, in sequence for that client. On this SSD that lands between 1.6 and 5.6 ms, so a few hundred puts a second.
- **More clients share the sync.** Requests that arrive while one write is on its way to disk go out together in the next one. Sixteen clients get between five and fifteen times the throughput of one, at a median latency of about 5 ms.
- **Reads are an order of magnitude cheaper than writes.** A get writes nothing: the leader confirms with a majority that it still leads and answers from memory.
- **Five nodes are slower than three here because they share a disk.** With separate disks the followers would sync in parallel.
- **The tail is long under heavy load.** At 64 clients the 99th percentile reaches hundreds of milliseconds, and in one pass on five nodes two seconds. A client that waits a second gives up on the node and asks again, and a two-second tail means that happened. The likely cause is a node stuck in a slow sync for longer than an election timeout, so that the others elected a new leader. That is a guess; it was not traced.

## What group commit changed

The first version of the server wrote and synced once per request. The benchmark showed it at once: throughput did not grow with the number of clients.

| nodes | operation | clients | ops/s before | ops/s after |
| --- | --- | --- | --- | --- |
| 3 | put | 1 | 165 | 167 to 554 |
| 3 | put | 16 | 170 | 2,508 to 2,679 |
| 3 | put | 64 | 787 | 3,277 to 4,258 |
| 5 | put | 1 | 529 | 493 to 502 |
| 5 | put | 16 | 554 | 1,840 to 1,933 |
| 5 | put | 64 | 546 | 974 to 1,818 |

The change is small. The node's thread now takes everything waiting in its inbox at once, lets the core work through all of it, and writes whatever the core asked for in one batch with one sync before telling the core. While that sync runs, the next batch collects. See [ADR 0008](adr/0008-group-commit-at-the-node.md).

The "before" column is a single pass. Given how much the passes differ, the factor is somewhere around fifteen for sixteen clients on three nodes and around three on five, not a number to quote to two digits.

## Failover

The leader of a three-node cluster is killed, and a client that retries every 100 ms keeps trying to write. The time is from the kill to the first acknowledged write.

| pass | kills | fastest | median | slowest |
| --- | --- | --- | --- | --- |
| before group commit | 7 | 525 ms | 542 ms | 1,001 ms |
| after, pass 1 | 7 | 503 ms | 616 ms | 836 ms |
| after, pass 2 | 7 | 516 ms | 692 ms | 968 ms |

A tick is 50 ms. A follower gives up on a leader after 10 to 19 ticks without hearing from it, so between half a second and a second, and then needs a pre-vote round and a vote round. The measurements sit where that puts them. A shorter tick makes failover faster and needless elections more likely; 50 ms is a default, not a tuned value.

## The simulator

The test suite runs 10,000 seeds, each 1,500 ticks of faults on a cluster of five or seven nodes, plus the time it takes each cluster to settle afterwards. On this machine that takes between 28 and 63 seconds across 16 threads; the run timed for this page took 59.

At 50 ms a tick, 1,500 ticks are 75 seconds of cluster time, and 10,000 seeds are 208 hours of it. In 59 seconds that is about 12,000 times faster than the clusters would run for real, with a fault rate no real cluster comes near.

## Not measured

- Anything across machines.
- Anything against another system. A comparison with etcd on the same machine would be the honest next step and was not done.
- Large values, large stores, or snapshots under load.
- Recovery time of a node that restarts with a long log.
