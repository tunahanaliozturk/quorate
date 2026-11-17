//! A deterministic cluster simulator.
//!
//! One thread owns every node, the network between them and a virtual clock. A seed decides the
//! cluster size, the fault rates and every random choice after that, so a failing seed replays the
//! same run step for step.
//!
//! Faults: messages are dropped, duplicated and delayed (which reorders them), and the network is
//! partitioned in several shapes. Nodes do not crash yet; that needs durable storage.
//!
//! Checked after every step: election safety, state machine safety and leader completeness.
//! Checked every few ticks and at the end: log matching, and that committed entries never change.
//! At the end the network heals and the cluster has to converge and commit something new.

use std::collections::{BTreeMap, HashMap};

use crate::Config;
use crate::raft::{Bug, Raft, Role};
use crate::rng::Rng;
use crate::types::{Entry, Index, Input, Message, NodeId, Output, Term};

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

pub(crate) struct Sim {
    rng: Rng,
    nodes: Vec<Raft>,
    now: u64,
    sequence: u64,
    /// Messages in flight, ordered by delivery time and then by send order.
    queue: BTreeMap<(u64, u64), Envelope>,
    /// `blocked[from][to]`: messages on this link are lost.
    blocked: Vec<Vec<bool>>,
    drop_percent: u64,
    duplicate_percent: u64,
    max_delay: u64,
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
                let config =
                    Config::new(node_id(position), peers, rng.next_u64()).max_batch(max_batch);
                let mut node =
                    Raft::new(config).expect("the simulator builds valid configurations");
                if let Some(bug) = bug {
                    node.inject(bug);
                }
                node
            })
            .collect();
        Self {
            nodes,
            now: 0,
            sequence: 0,
            queue: BTreeMap::new(),
            blocked: vec![vec![false; size]; size],
            drop_percent: rng.below(21),
            duplicate_percent: rng.below(11),
            max_delay: 1 + rng.below(8),
            leaders: HashMap::new(),
            committed: Vec::new(),
            next_command: 0,
            rng,
        }
    }

    /// Runs `ticks` of faults, then heals the network and checks that the cluster recovers.
    /// Returns how many entries were committed.
    pub(crate) fn run(mut self, ticks: u64) -> Result<usize, Violation> {
        for _ in 0..ticks {
            if self.chance(3) {
                self.change_partition();
            }
            self.advance()?;
            if self.chance(30) {
                self.propose()?;
            }
            if self.now % 32 == 0 {
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

    /// Moves the clock one tick: delivers what is due, then ticks every node.
    fn advance(&mut self) -> Result<(), Violation> {
        self.now += 1;
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

    fn step(&mut self, node: usize, input: Input) -> Result<(), Violation> {
        for output in self.nodes[node].step(input) {
            match output {
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
        let leaders: Vec<usize> = (0..self.nodes.len())
            .filter(|&node| self.nodes[node].role() == Role::Leader)
            .collect();
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
                let leader = (0..size).find(|&node| self.nodes[node].role() == Role::Leader);
                let isolated = leader.unwrap_or_else(|| self.pick(size));
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

    /// State machine safety: every node applies the same entry at each index.
    fn record(&mut self, node: usize, index: Index, entry: Entry) -> Result<(), Violation> {
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
        if self.nodes[node].role() != Role::Leader {
            return Ok(());
        }
        let term = self.nodes[node].term();
        match self.leaders.get(&term) {
            Some(&first) if first == node => Ok(()),
            Some(&first) => Err(Violation::ElectionSafety {
                term,
                first,
                second: node,
            }),
            None => {
                self.leaders.insert(term, node);
                let log = self.nodes[node].log();
                match (0..self.committed.len()).find(|&i| log.get(i) != self.committed.get(i)) {
                    Some(index) => Err(Violation::LeaderCompleteness {
                        leader: node,
                        index,
                    }),
                    None => Ok(()),
                }
            }
        }
    }

    /// Log matching between every pair, and committed prefixes against the record.
    fn check_logs(&self) -> Result<(), Violation> {
        for (a, first) in self.nodes.iter().enumerate() {
            let log = first.log();
            let committed = to_usize(first.commit_index().get());
            if let Some(index) = (0..committed).find(|&i| log.get(i) != self.committed.get(i)) {
                return Err(Violation::CommittedEntryChanged { node: a, index });
            }
            for (b, second) in self.nodes.iter().enumerate().skip(a + 1) {
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

    /// Whether every node holds the whole committed log and nothing else.
    fn converged(&self) -> bool {
        let length = self.committed.len();
        self.nodes
            .iter()
            .all(|node| node.log().len() == length && to_usize(node.commit_index().get()) == length)
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

    /// Heals the network and expects the cluster to recover: first every node ends up with the
    /// same fully committed log, then a new write commits everywhere within a fixed time.
    fn settle(&mut self) -> Result<(), Violation> {
        let size = self.nodes.len();
        self.blocked = vec![vec![false; size]; size];
        self.drop_percent = 0;
        self.duplicate_percent = 0;
        self.max_delay = 1;

        // A follower that fell behind catches up one batch per round trip, and the smallest batch
        // is one entry, so the time allowed grows with the longest log.
        let longest = self
            .nodes
            .iter()
            .map(|node| node.log().len())
            .max()
            .unwrap_or(0);
        if !self.advance_until_converged(300 + 4 * to_u64(longest))? {
            return Err(Violation::Liveness {
                reason: "the nodes did not converge after the network healed",
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

    /// Ticks of faults per seed, before the network heals.
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
    fn a_seed_replays_to_the_same_result() {
        let run = || Sim::new(42, Some(Bug::VoteWithoutLogCheck)).run(TICKS);
        assert_eq!(run(), run());
    }
}
