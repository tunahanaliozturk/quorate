# 0001: A Raft core without I/O, and a simulator written for it

Status: accepted, 2026-11-17

## Context

The project exists to show that a consensus implementation can be tested to the point where its safety claims mean something. That needs faults that are rare in real clusters to happen thousands of times per test run, and it needs every failure to be reproducible.

There were three ways to get there:

1. Use an existing Raft library such as openraft and build the store around it.
2. Write Raft against async traits for network and storage, and run it under a simulation runtime such as turmoil or madsim.
3. Write Raft as a pure state machine that returns the I/O it wants done, and write a small simulator that drives it.

## Decision

Option 3. The core exposes `step(input) -> outputs` and nothing else. It holds no sockets, files, timers or threads, and takes its randomness from a seeded generator. A driver performs the outputs: the real driver uses tokio and katman, the simulator uses in-process queues and katman's `SimFs`.

Raft is implemented in this repository, not taken from a library.

## Consequences

- The simulator is an ordinary single-threaded loop. It needs no patched runtime and no special build flags, and it runs the same on Linux, macOS and Windows.
- One seed fixes the whole run. There is no scheduler outside the simulator's control.
- The core is easy to unit test: feed inputs, compare outputs.
- The driver has to respect ordering rules the core cannot enforce by itself, mainly "persist before send". The core makes this explicit: a message that depends on durable state is only emitted after the driver reports that state durable. The ack-before-durable variant in the test suite checks that the simulator would notice a violation.
- Writing Raft from scratch costs time and will produce bugs a mature library has already fixed. That is accepted; finding them with the simulator is the point.
- tokio is confined to the transport and the server binary, so the real network path is tested less thoroughly than the core. M7 adds real-process kill tests to cover it.
