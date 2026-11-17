# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows [Semantic Versioning](https://semver.org/).

## Unreleased

- Repository scaffold, design document and roadmap.
- Raft core as a pure state machine: leader election, log replication, a no-op entry on election and a conflict hint for fast log back-up. State is in memory only.
- Durability: the core issues write requests and holds messages until they are reported durable. `Storage` keeps the term, vote and log in a katman database.
- Simulator faults: slow disks and power cuts with restart from storage.
- Deterministic simulator with message drop, duplication, delay and partitions, checking election safety, log matching, leader completeness, state machine safety and recovery after faults.
