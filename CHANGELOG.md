# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows [Semantic Versioning](https://semver.org/).

## Unreleased

- Repository scaffold, design document and roadmap.
- Raft core as a pure state machine: leader election, log replication, a no-op entry on election and a conflict hint for fast log back-up. State is in memory only.
- Deterministic simulator with message drop, duplication, delay and partitions, checking election safety, log matching, leader completeness, state machine safety and recovery after faults.
