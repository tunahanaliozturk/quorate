//! A deterministic cluster simulator.
//!
//! One thread owns every node, the network between them, their disks and a virtual clock. A seed
//! decides the cluster size, the fault rates and every random choice after that, so a failing
//! seed replays the same run step for step.
//!
//! Faults:
//!
//! - Messages are dropped, duplicated and delayed, which also reorders them.
//! - The network is partitioned in several shapes.
//! - Disks are slow: a write completes some ticks after it was requested.
//! - Nodes lose power. A node that goes down forgets everything in memory and every write that
//!   had not completed. Its storage is a katman database on katman's simulated filesystem, which
//!   drops unsynced bytes on a power cut. It restarts later from what that database holds.
//!
//! Checked after every step: election safety, state machine safety, leader completeness, and that
//! no node applies an entry before its own disk has it.
//! Checked every few ticks and at the end: log matching, and that committed entries never change.
//! At the end every node is restarted, the network heals and the cluster has to converge on one
//! log that holds every entry that was ever committed, and then commit something new.

use std::collections::{BTreeMap, HashMap, VecDeque};

use katman::{Options, SimFs, Torn};

use crate::Config;
use crate::raft::{Bug, Raft, Role};
use crate::rng::Rng;
use crate::storage::{Recovered, Storage};
use crate::types::{Entry, Index, Input, Message, NodeId, Output, Persist, Term};

/// A broken promise. Nodes are named by their position in the cluster, starting at 0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Violation {
    /// Two leaders in one term.
    ElectionSafety {
        term: Term,
        first: usize,
        second: usize,
    },
    /// Two logs agree on an entry's index and term but differ somewhere before it.
    LogMatching { a: usize, b: usize, index: usize },
    /// A node became leader without an entry that was already committed.
    LeaderCompleteness { leader: usize, index: usize },
    /// A node applied an entry at an index where another node applied a different one.
    StateMachineSafety { node: usize, index: Index },
    /// A node applied an entry while the write that stores it was still in flight.
    AppliedBeforeDurable { node: usize, index: Index },
    /// A node's log no longer holds an entry it had committed.
    CommittedEntryChanged { node: usize, index: usize },
    /// The healed cluster did not make progress.
    Liveness { reason: &'static str },
}

impl Violation {
    pub(crate) const fn is_safety(&self) -> bool {
        !matches!(self, Self::Liveness { .. })
    }
}

struct Envelope {
    from: usize,
    to: usize,
    message: Message,
}

/// The parts of a node that a power cut destroys.
struct Live {
    raft: Raft,
    storage: Storage,
    /// Writes the disk has not finished, with the tick at which each completes.
    disk: VecDeque<(u64, Persist)>,
}

struct Node {
    config: Config,
    /// The node's disk. It outlives crashes.
    fs: SimFs,
    /// `None` while the node is down.
    live: Option<Live>,
    restart_at: u64,
}

pub(crate) struct Sim {
    rng: Rng,
    bug: Option<Bug>,
    nodes: Vec<Node>,
    now: u64,
    sequence: u64,
    /// Messages in flight, ordered by delivery time and then by send order.
    queue: BTreeMap<(u64, u64), Envelope>,
    /// `blocked[from][to]`: messages on this link are lost.
    blocked: Vec<Vec<bool>>,
    drop_percent: u64,
    duplicate_percent: u64,
    max_delay: u64,
    max_disk_delay: u64,
    crash_per_mille: u64,
    /// The first leader seen in each term.
    leaders: HashMap<Term, usize>,
    /// Every entry any node has applied, by position.
    committed: Vec<Entry>,
    next_command: u64,
}

fn to_usize(value: u64) -> usize {
    usize::try_from(value).expect("simulator values fit in usize")
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("simulator values fit in u64")
}

fn node_id(position: usize) -> NodeId {
    NodeId::new(to_u64(position) + 1)
}

fn position_of(id: NodeId) -> usize {
    to_usize(id.get() - 1)
}

impl Sim {
    pub(crate) fn new(seed: u64, bug: Option<Bug>) -> Self {
        let mut rng = Rng::new(seed);
        let size = if rng.below(2) == 0 { 3 } else { 5 };
        // A batch of one makes followers catch up an entry at a time, which is where most of the
        // interesting partial states come from.
        let max_batch = [1, 2, 64][to_usize(rng.below(3))];
        let nodes = (0..size)
            .map(|position| {
                let peers = (0..size).filter(|&other| other != position).map(node_id);
                Node {
                    config: Config::new(node_id(position), peers, 0).max_batch(max_batch),
                    fs: SimFs::new(),
                    live: None,
                    restart_at: 0,
                }
            })
            .collect();
        let mut sim = Self {
            bug,
            nodes,
            now: 0,
            sequence: 0,
            queue: BTreeMap::new(),
            blocked: vec![vec![false; size]; size],
            drop_percent: rng.below(21),
            duplicate_percent: rng.below(11),
            max_delay: 1 + rng.below(8),
            max_disk_delay: [0, 0, 3, 8][to_usize(rng.below(4))],
            crash_per_mille: [0, 4, 12][to_usize(rng.below(3))],
            leaders: HashMap::new(),
            committed: Vec::new(),
            next_command: 0,
            rng,
        };
        for node in 0..size {
            sim.boot(node);
        }
        sim
    }

    /// Runs `ticks` of faults, then heals everything and checks that the cluster recovers.
    /// Returns how many entries were committed.
    pub(crate) fn run(mut self, ticks: u64) -> Result<usize, Violation> {
        for _ in 0..ticks {
            if self.chance(3) {
                self.change_partition();
            }
            if self.rng.below(1000) < self.crash_per_mille {
                let node = self.pick(self.nodes.len());
                self.crash(node);
            }
            self.advance()?;
            if self.chance(30) {
                self.propose()?;
            }
            if self.now.is_multiple_of(32) {
                self.check_logs()?;
            }
        }
        self.check_logs()?;
        self.settle()?;
        Ok(self.committed.len())
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.rng.below(100) < percent
    }

    fn pick(&mut self, len: usize) -> usize {
        to_usize(self.rng.below(to_u64(len)))
    }

    fn raft(&self, node: usize) -> Option<&Raft> {
        self.nodes[node].live.as_ref().map(|live| &live.raft)
    }

    /// The nodes that are up, with their positions.
    fn up(&self) -> impl Iterator<Item = (usize, &Raft)> {
        (0..self.nodes.len()).filter_map(|node| self.raft(node).map(|raft| (node, raft)))
    }

    fn leaders(&self) -> Vec<usize> {
        self.up()
            .filter(|(_, raft)| raft.role() == Role::Leader)
            .map(|(node, _)| node)
            .collect()
    }

    /// Starts a node from whatever its disk holds.
    fn boot(&mut self, node: usize) {
        let slot = &mut self.nodes[node];
        let options = Options::default().with_sim_fs(slot.fs.clone());
        let Recovered {
            storage,
            hard_state,
            log,
        } = Storage::open("node", options).expect("storage opens after any crash");
        // A new seed each time, or a node would repeat the election timeouts of its last life.
        slot.config.seed = self.rng.next_u64();
        let mut raft = Raft::recover(slot.config.clone(), hard_state, log)
            .expect("the simulator builds valid configurations");
        if let Some(bug) = self.bug {
            raft.inject(bug);
        }
        slot.live = Some(Live {
            raft,
            storage,
            disk: VecDeque::new(),
        });
    }

    /// Cuts the power to a node. Memory and unfinished writes are gone; the disk keeps what was
    /// synced, plus a random part of anything katman had written without syncing.
    fn crash(&mut self, node: usize) {
        let slot = &mut self.nodes[node];
        let Some(live) = slot.live.take() else {
            return;
        };
        slot.fs = slot.fs.power_loss(Torn::RandomPrefix(self.rng.next_u64()));
        drop(live);
        slot.restart_at = self.now + 1 + self.rng.below(60);
    }

    /// Moves the clock one tick: restarts nodes, finishes disk writes, delivers messages, then
    /// ticks every node.
    fn advance(&mut self) -> Result<(), Violation> {
        self.now += 1;
        for node in 0..self.nodes.len() {
            if self.nodes[node].live.is_none() && self.nodes[node].restart_at <= self.now {
                self.boot(node);
            }
            while let Some(persist) = self.next_finished_write(node) {
                self.finish_write(node, &persist)?;
            }
        }
        while let Some(entry) = self.queue.first_entry() {
            if entry.key().0 > self.now {
                break;
            }
            let envelope = entry.remove();
            if self.blocked[envelope.from][envelope.to] {
                continue;
            }
            let input = Input::Message {
                from: node_id(envelope.from),
                message: envelope.message,
            };
            self.step(envelope.to, input)?;
        }
        for node in 0..self.nodes.len() {
            self.step(node, Input::Tick)?;
        }
        Ok(())
    }

    fn next_finished_write(&mut self, node: usize) -> Option<Persist> {
        let disk = &mut self.nodes[node].live.as_mut()?.disk;
        if disk.front()?.0 > self.now {
            return None;
        }
        disk.pop_front().map(|(_, persist)| persist)
    }

    /// The disk has the write: store it for real, then tell the node.
    fn finish_write(&mut self, node: usize, persist: &Persist) -> Result<(), Violation> {
        if let Some(live) = self.nodes[node].live.as_mut() {
            live.storage
                .persist(persist)
                .expect("the simulated disk does not fail");
        }
        self.step(node, Input::Persisted { seq: persist.seq() })
    }

    /// Hands a write to the node's disk. It finishes after a random delay, in request order.
    fn start_write(&mut self, node: usize, persist: Persist) -> Result<(), Violation> {
        let delay = self.rng.below(self.max_disk_delay + 1);
        let Some(live) = self.nodes[node].live.as_mut() else {
            return Ok(());
        };
        if delay == 0 && live.disk.is_empty() {
            return self.finish_write(node, &persist);
        }
        let after_earlier_writes = live.disk.back().map_or(0, |(at, _)| *at);
        live.disk
            .push_back(((self.now + delay).max(after_earlier_writes), persist));
        Ok(())
    }

    /// Feeds one input to a node and carries out what it asks for. A node that is down ignores it.
    fn step(&mut self, node: usize, input: Input) -> Result<(), Violation> {
        let Some(live) = self.nodes[node].live.as_mut() else {
            return Ok(());
        };
        for output in live.raft.step(input) {
            match output {
                Output::Persist(persist) => self.start_write(node, persist)?,
                Output::Send { to, message } => self.send(node, position_of(to), &message),
                Output::Apply { index, entry } => self.record(node, index, entry)?,
                Output::NotLeader { .. } => {}
            }
        }
        self.check_leader(node)
    }

    fn send(&mut self, from: usize, to: usize, message: &Message) {
        if self.chance(self.drop_percent) {
            return;
        }
        let copies = if self.chance(self.duplicate_percent) {
            2
        } else {
            1
        };
        for _ in 0..copies {
            let at = self.now + 1 + self.rng.below(self.max_delay);
            self.sequence += 1;
            self.queue.insert(
                (at, self.sequence),
                Envelope {
                    from,
                    to,
                    message: message.clone(),
                },
            );
        }
    }

    /// Proposes a fresh command to a node that believes it is the leader. That includes leaders
    /// cut off from the majority, whose entries can never commit.
    fn propose(&mut self) -> Result<(), Violation> {
        let leaders = self.leaders();
        if leaders.is_empty() {
            return Ok(());
        }
        let node = leaders[self.pick(leaders.len())];
        self.next_command += 1;
        self.step(
            node,
            Input::Propose(self.next_command.to_be_bytes().to_vec()),
        )
    }

    fn change_partition(&mut self) {
        let size = self.nodes.len();
        match self.rng.below(5) {
            // Heal.
            0 | 1 => self.blocked = vec![vec![false; size]; size],
            // Two sides that cannot talk to each other.
            2 => {
                let left: Vec<bool> = (0..size).map(|_| self.rng.below(2) == 0).collect();
                for from in 0..size {
                    for to in 0..size {
                        self.blocked[from][to] = left[from] != left[to];
                    }
                }
            }
            // A leader cut off in both directions. It keeps believing it leads.
            3 => {
                let isolated = self
                    .leaders()
                    .first()
                    .copied()
                    .unwrap_or_else(|| self.pick(size));
                for other in (0..size).filter(|&other| other != isolated) {
                    self.blocked[isolated][other] = true;
                    self.blocked[other][isolated] = true;
                }
            }
            // One direction of one link.
            _ => {
                let from = self.pick(size);
                let to = self.pick(size);
                if from != to {
                    self.blocked[from][to] = true;
                }
            }
        }
    }

    /// A node must have stored an entry durably before it applies it. Then state machine safety:
    /// every node applies the same entry at each index. A node that
    /// restarted applies its log again from the start, and has to match what it applied before.
    fn record(&mut self, node: usize, index: Index, entry: Entry) -> Result<(), Violation> {
        let in_flight = self.nodes[node].live.as_ref().is_some_and(|live| {
            let touches = |persist: &Persist| {
                persist
                    .log()
                    .is_some_and(|write| write.first_index() <= index)
            };
            live.disk.iter().any(|(_, persist)| touches(persist))
        });
        if in_flight {
            return Err(Violation::AppliedBeforeDurable { node, index });
        }
        let position = to_usize(index.get().saturating_sub(1));
        match self.committed.get(position) {
            Some(known) if *known == entry => Ok(()),
            None if position == self.committed.len() && index != Index::ZERO => {
                self.committed.push(entry);
                Ok(())
            }
            _ => Err(Violation::StateMachineSafety { node, index }),
        }
    }

    /// Election safety, and leader completeness the moment a node first leads a term.
    fn check_leader(&mut self, node: usize) -> Result<(), Violation> {
        let Some(raft) = self.raft(node) else {
            return Ok(());
        };
        if raft.role() != Role::Leader {
            return Ok(());
        }
        let term = raft.term();
        match self.leaders.get(&term) {
            Some(&first) if first == node => Ok(()),
            Some(&first) => Err(Violation::ElectionSafety {
                term,
                first,
                second: node,
            }),
            None => {
                let log = raft.log();
                let missing =
                    (0..self.committed.len()).find(|&i| log.get(i) != self.committed.get(i));
                self.leaders.insert(term, node);
                match missing {
                    Some(index) => Err(Violation::LeaderCompleteness {
                        leader: node,
                        index,
                    }),
                    None => Ok(()),
                }
            }
        }
    }

    /// Log matching between every pair of running nodes, and committed prefixes against the
    /// record.
    fn check_logs(&self) -> Result<(), Violation> {
        for (a, first) in self.up() {
            let log = first.log();
            let committed = to_usize(first.commit_index().get());
            if let Some(index) = (0..committed).find(|&i| log.get(i) != self.committed.get(i)) {
                return Err(Violation::CommittedEntryChanged { node: a, index });
            }
            for (b, second) in self.up().filter(|&(b, _)| b > a) {
                let other = second.log();
                let shared = log.len().min(other.len());
                // The highest position where both hold an entry of the same term. Everything up
                // to it has to be identical.
                let Some(agreed) = (0..shared)
                    .rev()
                    .find(|&i| log[i].term() == other[i].term())
                else {
                    continue;
                };
                if let Some(index) = (0..=agreed).find(|&i| log[i] != other[i]) {
                    return Err(Violation::LogMatching { a, b, index });
                }
            }
        }
        Ok(())
    }

    /// Whether every node is up and holds the whole committed log and nothing else.
    fn converged(&self) -> bool {
        let length = self.committed.len();
        self.up().count() == self.nodes.len()
            && self.up().all(|(_, raft)| {
                raft.log().len() == length && to_usize(raft.commit_index().get()) == length
            })
    }

    /// Advances until the nodes converge, for at most `budget` ticks.
    fn advance_until_converged(&mut self, budget: u64) -> Result<bool, Violation> {
        for _ in 0..budget {
            if self.converged() {
                return Ok(true);
            }
            self.advance()?;
        }
        Ok(self.converged())
    }

    /// Stops every fault and expects the cluster to recover: first every node ends up with the
    /// same fully committed log, then a new write commits everywhere within a fixed time.
    ///
    /// Converging on the record of applied entries is also the durability check. Every entry a
    /// node ever applied is in that record, so an entry lost in a crash shows up here as a log
    /// that cannot match it.
    fn settle(&mut self) -> Result<(), Violation> {
        let size = self.nodes.len();
        self.blocked = vec![vec![false; size]; size];
        self.drop_percent = 0;
        self.duplicate_percent = 0;
        self.max_delay = 1;
        self.max_disk_delay = 0;
        self.crash_per_mille = 0;
        for node in &mut self.nodes {
            node.restart_at = 0;
        }

        // A follower that fell behind catches up one batch per round trip, and the smallest batch
        // is one entry, so the time allowed grows with the longest log.
        let longest = self
            .up()
            .map(|(_, raft)| raft.log().len())
            .max()
            .unwrap_or(0);
        let budget = 300 + 4 * to_u64(longest.max(self.committed.len()));
        if !self.advance_until_converged(budget)? {
            return Err(Violation::Liveness {
                reason: "the nodes did not converge after the faults stopped",
            });
        }

        let before = self.committed.len();
        self.propose()?;
        if !self.advance_until_converged(50)? || self.committed.len() <= before {
            return Err(Violation::Liveness {
                reason: "a write after recovery did not commit everywhere",
            });
        }
        self.check_logs()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{Sim, Violation};
    use crate::raft::Bug;

    /// Ticks of faults per seed, before everything heals.
    const TICKS: u64 = 1_500;

    fn env(name: &str) -> Option<u64> {
        std::env::var(name)
            .ok()
            .map(|value| value.parse().expect("the variable holds a number"))
    }

    /// The seeds to run: the one in `QUORATE_SEED`, or the first `QUORATE_SEEDS` (10,000 if unset).
    fn seeds() -> std::ops::Range<u64> {
        match env("QUORATE_SEED") {
            Some(seed) => seed..seed + 1,
            None => 0..env("QUORATE_SEEDS").unwrap_or(10_000),
        }
    }

    /// The first seed below `limit` on which the simulator reports a safety violation.
    fn first_safety_violation(bug: Bug, limit: u64) -> Option<(u64, Violation)> {
        (0..limit).find_map(|seed| match Sim::new(seed, Some(bug)).run(TICKS) {
            Err(violation) if violation.is_safety() => Some((seed, violation)),
            _ => None,
        })
    }

    #[test]
    fn every_seed_keeps_every_invariant_and_recovers() {
        let range = seeds();
        let next = AtomicU64::new(range.start);
        let failures = Mutex::new(Vec::new());
        let committed = AtomicU64::new(0);
        let workers = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);

        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| {
                    loop {
                        let seed = next.fetch_add(1, Ordering::Relaxed);
                        if seed >= range.end {
                            break;
                        }
                        match Sim::new(seed, None).run(TICKS) {
                            Ok(entries) => {
                                committed.fetch_add(super::to_u64(entries), Ordering::Relaxed);
                            }
                            Err(violation) => failures
                                .lock()
                                .expect("no worker panics")
                                .push((seed, violation)),
                        }
                    }
                });
            }
        });

        let mut failures = failures.into_inner().expect("no worker panics");
        failures.sort_by_key(|(seed, _)| *seed);
        assert!(
            failures.is_empty(),
            "failing seeds (replay with QUORATE_SEED=n): {failures:?}"
        );
        // A run that commits nothing would pass every safety check without testing anything.
        assert!(committed.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn committing_an_earlier_term_by_counting_replicas_is_caught() {
        let found = first_safety_violation(Bug::CommitEarlierTerm, 1_000);
        assert!(
            found.is_some(),
            "no seed below 1,000 exposed the figure 8 commit bug"
        );
    }

    #[test]
    fn voting_without_the_log_check_is_caught() {
        let found = first_safety_violation(Bug::VoteWithoutLogCheck, 1_000);
        assert!(
            found.is_some(),
            "no seed below 1,000 exposed the missing vote check"
        );
    }

    #[test]
    fn sending_before_the_write_is_durable_is_caught() {
        let found = first_safety_violation(Bug::SendBeforeDurable, 1_000);
        assert!(
            found.is_some(),
            "no seed below 1,000 exposed acknowledging before durable"
        );
    }

    #[test]
    fn a_seed_replays_to_the_same_result() {
        let run = || Sim::new(42, Some(Bug::VoteWithoutLogCheck)).run(TICKS);
        assert_eq!(run(), run());
    }
}
