//! A deterministic cluster simulator.
//!
//! One thread owns every node, the network between them, their disks, the clients and a virtual
//! clock. A seed decides the cluster size, the fault rates and every random choice after that, so
//! a failing seed replays the same run step for step.
//!
//! Faults:
//!
//! - Messages are dropped, duplicated and delayed, which also reorders them.
//! - The network is partitioned in several shapes.
//! - Disks are slow: a write completes some ticks after it was requested.
//! - Clocks drift: now and then a node starts ticking only half or a fifth of the time, and a
//!   leader that gets cut off often slows to a tenth.
//! - Nodes lose power. A node that goes down forgets everything in memory and every write that
//!   had not completed. Its log and its state machine are katman databases on katman's simulated
//!   filesystem, which drops unsynced bytes on a power cut. It restarts later from what they hold.
//! - Logs are compacted: every few applied entries a node forces its state machine to disk and
//!   drops the log before it. A follower that falls behind the leader's log gets a snapshot.
//! - The membership changes: two spare nodes run from the start, and at random moments one
//!   joins as a learner, a learner is promoted or dropped, or a voter is removed.
//! - Clients lose requests and replies, give up waiting and send the same request to another
//!   node, and now and then send an old request again. Most gets are served by the leader
//!   without a log entry; the rest go through the log.
//!
//! Checked after every step: election safety, state machine safety, leader completeness, and that
//! no node applies an entry before its own disk has it.
//! Checked every few ticks and at the end: log matching, that committed entries never change, and
//! that no log holds more applied entries than the compaction policy allows.
//! At the end every node is restarted, the network heals and the cluster has to converge on one
//! log that holds every entry that was ever committed, answer every client, and commit something
//! new. Then the history of every key is checked for linearizability, and the state machines of
//! all nodes are compared.

use std::collections::{BTreeMap, HashMap, VecDeque};

use katman::{Options, SimFs, Torn};

use crate::Config;
use crate::kv::{Op, Reply, Request, SessionId};
use crate::linearizability::{Call, Operation, Return, linearizable};
use crate::membership::{Change, Membership};
use crate::raft::{Bug, Raft, Role};
use crate::rng::Rng;
use crate::state::{Outcome, Snapshot, StateMachine};
use crate::storage::{Recovered, Storage};
use crate::types::{Entry, Index, Input, Message, NodeId, Output, Payload, Persist, ReadId, Term};

/// Keys the clients work on. Few, so that their operations collide.
const KEYS: usize = 2;
/// Values the clients write. Few, so that a compare-and-set often finds what it expects.
const VALUES: u64 = 3;
const CLIENTS: u64 = 3;
/// How long a client waits for an answer before it sends its request again.
const CLIENT_TIMEOUT: u64 = 20;

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
    /// A node keeps more applied entries in its log than the compaction policy allows.
    LogNotCompacted { node: usize },
    /// A node came back with a log that starts after what its state machine holds. The entries in
    /// between are gone for good.
    StateLost { node: usize },
    /// What the clients saw on this key cannot be explained by any order of their operations.
    NotLinearizable { key: usize },
    /// A node's state machine differs from node 0's although both applied the same log.
    StateDiverged { node: usize },
    /// The healed cluster did not make progress.
    Liveness { reason: &'static str },
}

impl Violation {
    pub(crate) const fn is_safety(&self) -> bool {
        !matches!(self, Self::Liveness { .. })
    }
}

/// What travels between two nodes.
#[derive(Clone)]
enum Parcel {
    Message(Message),
    /// A copy of the sender's state machine, with the term and round the core asked to pass on.
    Snapshot {
        term: Term,
        round: u64,
        snapshot: Snapshot,
    },
}

struct Envelope {
    from: usize,
    to: usize,
    parcel: Parcel,
}

/// What a run achieved, to check that the tests are not passing on an idle cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Totals {
    pub(crate) committed: usize,
    pub(crate) operations: usize,
    /// Snapshots that replaced a follower's state machine.
    pub(crate) snapshots: usize,
    /// Membership changes that committed.
    pub(crate) changes: usize,
}

/// The parts of a node that a power cut destroys.
struct Live {
    raft: Raft,
    storage: Storage,
    state: StateMachine,
    /// Writes the disk has not finished, with the tick at which each completes.
    disk: VecDeque<(u64, Persist)>,
    /// Reads the node may serve once its state machine has applied the given index.
    parked: Vec<(Index, ReadId)>,
}

struct Node {
    config: Config,
    /// The node's disk. It outlives crashes.
    fs: SimFs,
    /// `None` while the node is down.
    live: Option<Live>,
    restart_at: u64,
    /// How fast the node's clock runs: the chance, in percent, that it ticks when real time
    /// does. Timeouts are the only thing a node uses its clock for, so a slow clock makes it
    /// slow to notice that it has been cut off.
    clock_percent: u64,
    /// How many times the node has started. A client that was waiting on an earlier life of the
    /// node gets no answer from a later one.
    life: u64,
}

/// The request a client is waiting on.
struct Waiting {
    request: Vec<u8>,
    key: usize,
    /// Position of the operation in the key's history.
    slot: usize,
    /// The node the request went to, and which life of it.
    target: Option<(usize, u64)>,
    sent_at: u64,
    /// For a get that does not go through the log: the id of the read last sent.
    read: Option<ReadId>,
}

struct Client {
    session: SessionId,
    seq: u64,
    waiting: Option<Waiting>,
    /// The last request that was answered, to send again as a stray copy.
    answered: Option<Vec<u8>>,
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
    /// A node compacts its log once this many applied entries have piled up in it.
    compact_every: u64,
    /// Applied entries a compaction leaves in the log, for followers that are only a little
    /// behind.
    retain: u64,
    snapshots: usize,
    /// How many nodes the cluster is founded with. The nodes after them start outside it.
    founders: usize,
    /// Whether the run changes the membership at random.
    reshuffle: bool,
    /// The first leader seen in each term.
    leaders: HashMap<Term, usize>,
    /// Every entry any node has applied, by position.
    committed: Vec<Entry>,
    next_command: u64,
    next_read: u64,
    clients: Vec<Client>,
    /// Whether clients start new operations and lose requests and replies.
    clients_busy: bool,
    /// What the clients did and saw, per key.
    histories: Vec<Vec<Operation>>,
    /// The clock of the histories: one tick per invocation or return.
    events: u64,
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

fn key_bytes(key: usize) -> Vec<u8> {
    vec![b'k', u8::try_from(key).expect("there are few keys")]
}

fn value_bytes(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn value_of(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(
        bytes
            .try_into()
            .expect("clients only write eight-byte values"),
    )
}

fn returned(reply: &Reply) -> Return {
    match reply {
        Reply::Done => Return::Done,
        Reply::Value(value) => Return::Value(value.as_deref().map(value_of)),
        Reply::Swapped(swapped) => Return::Swapped(*swapped),
    }
}

impl Sim {
    pub(crate) fn new(seed: u64, bug: Option<Bug>) -> Self {
        Self::build(seed, bug, Some(2), None)
    }

    /// A cluster with one spare node for every founder, no partitions and no power cuts, for
    /// the run that replaces every node.
    pub(crate) fn for_replacement(seed: u64) -> Self {
        // Full batches. With one entry per message a leader sends an entry, waits for the
        // answer and only then sends the next, and under clients that never pause its log grows
        // faster than it commits. A membership change then waits behind a backlog that never
        // clears. The random runs are short enough not to mind; this one is not.
        let mut sim = Self::build(seed, None, None, Some(64));
        sim.crash_per_mille = 0;
        sim.reshuffle = false;
        sim
    }

    /// `spares` nodes run from the start without being members, to be added later. `None` means
    /// as many as there are founders.
    fn build(seed: u64, bug: Option<Bug>, spares: Option<usize>, batch: Option<usize>) -> Self {
        let mut rng = Rng::new(seed);
        let founders = if rng.below(2) == 0 { 3 } else { 5 };
        let size = founders + spares.unwrap_or(founders);
        // A batch of one makes followers catch up an entry at a time, which is where most of the
        // interesting partial states come from.
        let max_batch = [1, 2, 64][to_usize(rng.below(3))];
        let max_batch = batch.unwrap_or(max_batch);
        let nodes = (0..size)
            .map(|position| {
                let config = if position < founders {
                    let peers = (0..founders)
                        .filter(|&other| other != position)
                        .map(node_id);
                    Config::new(node_id(position), peers, 0)
                } else {
                    Config::joining(node_id(position), 0)
                };
                Node {
                    config: config.max_batch(max_batch),
                    fs: SimFs::new(),
                    live: None,
                    restart_at: 0,
                    clock_percent: 100,
                    life: 0,
                }
            })
            .collect();
        let clients = (0..CLIENTS)
            .map(|client| Client {
                session: SessionId::new(1000 + client),
                seq: 0,
                waiting: None,
                answered: None,
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
            compact_every: [8, 32, 128][to_usize(rng.below(3))],
            retain: [0, 4][to_usize(rng.below(2))],
            snapshots: 0,
            founders,
            reshuffle: true,
            leaders: HashMap::new(),
            committed: Vec::new(),
            next_command: 0,
            next_read: 0,
            clients,
            clients_busy: true,
            histories: vec![Vec::new(); KEYS],
            events: 0,
            rng,
        };
        for node in 0..size {
            sim.boot(node)
                .expect("an empty disk holds nothing that could be lost");
        }
        sim
    }

    /// Runs `ticks` of faults, then heals everything and checks that the cluster recovers.
    /// Returns how many entries were committed and how many client operations completed.
    pub(crate) fn run(mut self, ticks: u64) -> Result<Totals, Violation> {
        for _ in 0..ticks {
            if self.chance(3) {
                self.change_partition();
            }
            if self.chance(1) {
                let node = self.pick(self.nodes.len());
                self.nodes[node].clock_percent = [100, 100, 50, 20][to_usize(self.rng.below(4))];
            }
            if self.rng.below(1000) < self.crash_per_mille {
                // Half the time the victim is a leader. A leader that dies soon after it was
                // elected leaves entries behind that only it holds, which is where the hard
                // cases for the commit rule start.
                let leader = self.leaders().first().copied();
                let node = match leader {
                    Some(leader) if self.chance(50) => leader,
                    _ => self.pick(self.nodes.len()),
                };
                self.crash(node);
            }
            self.advance()?;
            if self.chance(10) {
                self.propose_junk()?;
            }
            if self.reshuffle && self.chance(2) {
                self.change_membership_at_random()?;
            }
            if self.now.is_multiple_of(32) {
                self.check_logs()?;
            }
        }
        self.check_logs()?;
        self.settle()?;
        Ok(self.totals())
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
    fn boot(&mut self, node: usize) -> Result<(), Violation> {
        let slot = &mut self.nodes[node];
        let options = || Options::default().with_sim_fs(slot.fs.clone());
        let Recovered {
            storage,
            hard_state,
            base,
            log,
        } = Storage::open("log", options()).expect("storage opens after any crash");
        let mut state = StateMachine::open("state", options())
            .expect("the state machine opens after any crash");
        // A new seed each time, or a node would repeat the election timeouts of its last life.
        slot.config.seed = self.rng.next_u64();
        state
            .bootstrap(&slot.config.founding_membership())
            .expect("the simulated disk does not fail");
        let membership = state.membership().cloned();
        let recovered = Raft::recover(
            slot.config.clone(),
            hard_state,
            base,
            log,
            state.applied(),
            membership,
        );
        let Ok(mut raft) = recovered else {
            return Err(Violation::StateLost { node });
        };
        if let Some(bug) = self.bug {
            raft.inject(bug);
        }
        if self.bug == Some(Bug::ApplyRetriesAgain) {
            state.inject_apply_retries_again();
        }
        slot.life += 1;
        slot.live = Some(Live {
            raft,
            storage,
            state,
            disk: VecDeque::new(),
            parked: Vec::new(),
        });
        // The write that moved the start of the log may have been lost with the power, while the
        // state machine it was based on survived. The policy applies from the first moment.
        self.maybe_compact(node)
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

    /// Moves the clock one tick: restarts nodes, finishes disk writes, delivers messages, ticks
    /// every node, then lets the clients act.
    fn advance(&mut self) -> Result<(), Violation> {
        self.now += 1;
        for node in 0..self.nodes.len() {
            if self.nodes[node].live.is_none() && self.nodes[node].restart_at <= self.now {
                self.boot(node)?;
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
            self.deliver(envelope)?;
        }
        for node in 0..self.nodes.len() {
            if self.chance(self.nodes[node].clock_percent) {
                self.step(node, Input::Tick)?;
            }
        }
        for client in 0..self.clients.len() {
            self.act(client)?;
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
    /// A write that finishes at once is handed back, for the caller to complete.
    fn start_write(&mut self, node: usize, persist: Persist) -> Option<Persist> {
        let delay = self.rng.below(self.max_disk_delay + 1);
        let live = self.nodes[node].live.as_mut()?;
        if delay == 0 && live.disk.is_empty() {
            return Some(persist);
        }
        let after_earlier_writes = live.disk.back().map_or(0, |(at, _)| *at);
        live.disk
            .push_back(((self.now + delay).max(after_earlier_writes), persist));
        None
    }

    /// Feeds one input to a node and carries out what it asks for. A node that is down ignores it.
    fn step(&mut self, node: usize, input: Input) -> Result<(), Violation> {
        let Some(live) = self.nodes[node].live.as_mut() else {
            return Ok(());
        };
        let mut finished = None;
        for output in live.raft.step(input) {
            match output {
                Output::Persist(persist) => finished = self.start_write(node, persist),
                Output::Send { to, message } => {
                    self.send(node, position_of(to), &Parcel::Message(message));
                }
                Output::SendSnapshot { to, term, round } => {
                    let state = self.nodes[node].live.as_ref().map(|live| &live.state);
                    let snapshot = state
                        .map(|state| state.snapshot().expect("the simulated disk does not fail"));
                    if let Some(snapshot) = snapshot {
                        self.send(
                            node,
                            position_of(to),
                            &Parcel::Snapshot {
                                term,
                                round,
                                snapshot,
                            },
                        );
                    }
                }
                Output::Apply { index, entry } => self.apply(node, index, entry)?,
                Output::ReadReady { id, index } => {
                    if let Some(live) = self.nodes[node].live.as_mut() {
                        live.parked.push((index, id));
                    }
                }
                Output::ReadRejected { id, .. } => self.read_rejected(id),
                Output::NotLeader { .. } | Output::ChangeRejected { .. } => {}
            }
        }
        self.serve_reads(node);
        self.check_leader(node)?;
        // Only now, after everything this step asked for is done. Telling the node earlier would
        // run a second step inside the first and apply later entries before earlier ones.
        if let Some(persist) = finished {
            self.finish_write(node, &persist)?;
        }
        self.maybe_compact(node)
    }

    /// The compaction policy a real node would run: once enough applied entries have piled up in
    /// the log, force the state machine to disk and tell the node that the log before that point
    /// can go.
    fn maybe_compact(&mut self, node: usize) -> Result<(), Violation> {
        let Some(live) = self.nodes[node].live.as_mut() else {
            return Ok(());
        };
        // The node's own count, not the state machine's. The state machine can be ahead: a
        // snapshot from a leader the node no longer follows is installed all the same, because
        // it is committed state, but the node does not move its log for it.
        let applied = live.raft.applied_index().get();
        if applied < live.raft.log_base().index().get() + self.compact_every {
            return Ok(());
        }
        // The order is the point. With the bug the log is cut on the word of a state machine
        // whose latest writes a power cut can still take away.
        let durable = if self.bug == Some(Bug::CompactBeforeCheckpoint) {
            applied
        } else {
            live.state
                .checkpoint()
                .expect("the simulated disk does not fail")
                .index()
                .get()
        };
        self.step(
            node,
            Input::Compact {
                index: Index::new(durable.saturating_sub(self.retain)),
            },
        )
    }

    fn send(&mut self, from: usize, to: usize, parcel: &Parcel) {
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
                    parcel: parcel.clone(),
                },
            );
        }
    }

    /// Hands a parcel to the node it was sent to.
    fn deliver(&mut self, envelope: Envelope) -> Result<(), Violation> {
        let from = node_id(envelope.from);
        let (term, round, snapshot) = match envelope.parcel {
            Parcel::Message(message) => {
                return self.step(envelope.to, Input::Message { from, message });
            }
            Parcel::Snapshot {
                term,
                round,
                snapshot,
            } => (term, round, snapshot),
        };
        let Some(live) = self.nodes[envelope.to].live.as_mut() else {
            return Ok(());
        };
        // The state machine first, and durably. Only then is the node told, because it drops its
        // log on the strength of it.
        let installed = live
            .state
            .install(&snapshot)
            .expect("the simulated disk does not fail");
        self.snapshots += usize::from(installed);
        self.step(
            envelope.to,
            Input::Snapshot {
                from,
                term,
                round,
                snapshot: snapshot.meta(),
                membership: snapshot.membership().cloned().unwrap_or_default(),
            },
        )?;
        // The worst moment for the power to go: the state machine holds the snapshot, and the
        // write that makes the log match it may still be in flight. The node must come back
        // with a log that agrees with its state machine either way.
        if installed && self.crash_per_mille > 0 && self.chance(30) {
            self.crash(envelope.to);
        }
        Ok(())
    }

    /// A committed entry reaches a node's state machine, and its answer may reach a client.
    fn apply(&mut self, node: usize, index: Index, entry: Entry) -> Result<(), Violation> {
        let outcome = match self.nodes[node].live.as_mut() {
            Some(live) => live
                .state
                .apply(index, &entry)
                .expect("entries arrive in order on a disk that works"),
            None => None,
        };
        self.record(node, index, entry)?;
        if let Some(outcome) = outcome {
            self.answer(node, &outcome);
        }
        Ok(())
    }

    /// Proposes bytes that are not a request to a node that believes it is the leader. That
    /// includes leaders cut off from the majority, whose entries can never commit. The state
    /// machines have to agree that the entry changes nothing.
    fn propose_junk(&mut self) -> Result<(), Violation> {
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

    // ---- Membership. ----

    /// The leader of the highest term, by position.
    fn leader(&self) -> Option<usize> {
        let leaders = self.up().filter(|(_, raft)| raft.role() == Role::Leader);
        leaders
            .max_by_key(|(_, raft)| raft.term())
            .map(|(node, _)| node)
    }

    /// The members according to that leader, by position.
    fn members(&self) -> Option<Vec<usize>> {
        let raft = self.raft(self.leader()?)?;
        Some(raft.membership().members().map(position_of).collect())
    }

    /// Asks some node that believes it leads for a random change that fits its membership: a
    /// spare joins as a learner, a learner is promoted or dropped, a voter is removed. The
    /// cluster never goes below three voters.
    fn change_membership_at_random(&mut self) -> Result<(), Violation> {
        let leaders = self.leaders();
        if leaders.is_empty() {
            return Ok(());
        }
        let leader = leaders[self.pick(leaders.len())];
        let Some(membership) = self.raft(leader).map(|raft| raft.membership().clone()) else {
            return Ok(());
        };
        let outsiders: Vec<NodeId> = (0..self.nodes.len())
            .map(node_id)
            .filter(|&id| !membership.contains(id))
            .collect();
        let mut changes = Vec::new();
        if !outsiders.is_empty() {
            changes.push(Change::AddLearner(outsiders[self.pick(outsiders.len())]));
        }
        if !membership.learners().is_empty() {
            let learner = membership.learners()[self.pick(membership.learners().len())];
            changes.extend([
                Change::Promote(learner),
                Change::Promote(learner),
                Change::Remove(learner),
            ]);
        }
        if membership.voters().len() > 3 {
            changes.push(Change::Remove(
                membership.voters()[self.pick(membership.voters().len())],
            ));
        }
        if changes.is_empty() {
            return Ok(());
        }
        let change = changes[self.pick(changes.len())];
        self.step(leader, Input::ChangeMembership(change))
    }

    /// Replaces every founding node with a spare, one step at a time, while the clients keep
    /// working: the spare joins as a learner, is promoted, and the founder is removed. At the end
    /// the cluster consists of nodes that were not there at the start, and the usual checks say
    /// whether anything was lost on the way.
    pub(crate) fn run_replacement(mut self) -> Result<Totals, Violation> {
        for old in 0..self.founders {
            let (old, new) = (node_id(old), node_id(self.founders + old));
            self.change_until(Change::AddLearner(new), |members| members.contains(new))?;
            self.change_until(Change::Promote(new), |members| members.is_voter(new))?;
            self.change_until(Change::Remove(old), |members| !members.contains(old))?;
        }
        self.check_logs()?;
        self.settle()?;
        let expected: Vec<usize> = (self.founders..2 * self.founders).collect();
        if self.members() != Some(expected) {
            return Err(Violation::Liveness {
                reason: "the cluster did not end up with the new nodes",
            });
        }
        Ok(self.totals())
    }

    /// Keeps asking the leader for `change` until its state machine shows it. The leader refuses
    /// while an earlier change is uncommitted or a learner is behind, and leaders come and go, so
    /// this takes a few tries.
    fn change_until(
        &mut self,
        change: Change,
        done: impl Fn(&Membership) -> bool,
    ) -> Result<(), Violation> {
        for tick in 0..5_000_u64 {
            self.advance()?;
            if self.now.is_multiple_of(32) {
                self.check_logs()?;
            }
            if !tick.is_multiple_of(10) {
                continue;
            }
            let Some(leader) = self.leader() else {
                continue;
            };
            // Applied, not merely in the leader's log: an entry that has not committed can still
            // be overwritten when the leader changes, and the change would be gone again.
            let applied = self.nodes[leader]
                .live
                .as_ref()
                .and_then(|live| live.state.membership());
            if applied.is_some_and(&done) {
                return Ok(());
            }
            self.step(leader, Input::ChangeMembership(change))?;
        }
        Err(Violation::Liveness {
            reason: "a membership change did not go through",
        })
    }

    fn totals(&self) -> Totals {
        Totals {
            committed: self.committed.len(),
            operations: self.histories.iter().map(Vec::len).sum(),
            snapshots: self.snapshots,
            changes: self
                .committed
                .iter()
                .filter(|entry| matches!(entry.payload(), Payload::Config(_)))
                .count(),
        }
    }

    // ---- Clients. ----

    /// One client's turn: maybe start an operation, send or resend what it is waiting on, and
    /// now and then send a stray copy of an old request.
    fn act(&mut self, client: usize) -> Result<(), Violation> {
        if self.clients[client].waiting.is_none() && self.clients_busy && self.chance(40) {
            self.invoke(client);
        }
        let overdue = self.clients[client]
            .waiting
            .as_ref()
            .is_some_and(|waiting| {
                waiting.target.is_none() || self.now - waiting.sent_at > CLIENT_TIMEOUT
            });
        if overdue {
            self.send_request(client)?;
        }
        if self.clients_busy
            && self.chance(2)
            && let Some(old) = self.clients[client].answered.clone()
        {
            let node = self.pick(self.nodes.len());
            self.step(node, Input::Propose(old))?;
        }
        Ok(())
    }

    /// Starts a new operation and writes its invocation into the history.
    fn invoke(&mut self, client: usize) {
        let key = self.pick(KEYS);
        let value = self.rng.below(VALUES);
        let call = match self.rng.below(4) {
            0 => Call::Get,
            1 => Call::Put(value),
            2 => Call::Delete,
            _ => {
                let expected = match self.rng.below(VALUES + 1) {
                    0 => None,
                    other => Some(other - 1),
                };
                Call::CompareAndSet { expected, value }
            }
        };
        let op = match call {
            Call::Get => Op::Get {
                key: key_bytes(key),
            },
            Call::Put(value) => Op::Put {
                key: key_bytes(key),
                value: value_bytes(value),
            },
            Call::Delete => Op::Delete {
                key: key_bytes(key),
            },
            Call::CompareAndSet { expected, value } => Op::CompareAndSet {
                key: key_bytes(key),
                expected: expected.map(value_bytes),
                value: value_bytes(value),
            },
        };
        self.events += 1;
        self.histories[key].push(Operation {
            call,
            invoked: self.events,
            returned: None,
        });

        let slot = self.histories[key].len() - 1;
        // Most gets take the read path; the rest go through the log like any other operation.
        let read = (call == Call::Get && self.chance(80)).then_some(ReadId::new(0));
        let client = &mut self.clients[client];
        client.seq += 1;
        let request = Request::new(client.session, client.seq, op).encode();
        client.waiting = Some(Waiting {
            request,
            key,
            slot,
            target: None,
            sent_at: self.now,
            read,
        });
    }

    /// Sends the request the client is waiting on to a node: usually one that claims to lead,
    /// sometimes any. The request may be lost on the way.
    fn send_request(&mut self, client: usize) -> Result<(), Violation> {
        let leaders = self.leaders();
        let node = if !leaders.is_empty() && self.chance(80) {
            leaders[self.pick(leaders.len())]
        } else {
            self.pick(self.nodes.len())
        };
        let lost = self.clients_busy && self.chance(10);
        let life = self.nodes[node].life;
        self.next_read += 1;
        let id = ReadId::new(self.next_read);
        let Some(waiting) = self.clients[client].waiting.as_mut() else {
            return Ok(());
        };
        waiting.target = Some((node, life));
        waiting.sent_at = self.now;
        let input = if waiting.read.is_some() {
            // A fresh id each time, so an answer to an earlier attempt is not taken for this one.
            waiting.read = Some(id);
            Input::Read { id }
        } else {
            Input::Propose(waiting.request.clone())
        };
        if lost {
            return Ok(());
        }
        self.step(node, input)
    }

    /// Serves the reads a node has been cleared to answer and whose index it has applied.
    fn serve_reads(&mut self, node: usize) {
        let Some(live) = self.nodes[node].live.as_mut() else {
            return;
        };
        let applied = live.state.applied_index();
        let (due, parked) = core::mem::take(&mut live.parked)
            .into_iter()
            .partition(|(index, _)| *index <= applied);
        live.parked = parked;
        let due: Vec<(Index, ReadId)> = due;
        for (_, id) in due {
            self.answer_read(node, id);
        }
    }

    /// A node answers a read from its state machine, if the client is still waiting on this node
    /// for this read. The answer may be lost on the way.
    fn answer_read(&mut self, node: usize, id: ReadId) {
        let lost = self.clients_busy && self.chance(10);
        let life = self.nodes[node].life;
        let Some(running) = self.nodes[node].live.as_ref() else {
            return;
        };
        let expected =
            |waiting: &Waiting| waiting.read == Some(id) && waiting.target == Some((node, life));
        let Some(client) = self
            .clients
            .iter_mut()
            .find(|client| client.waiting.as_ref().is_some_and(expected))
        else {
            return;
        };
        if lost {
            return;
        }
        if let Some(waiting) = client.waiting.take() {
            let value = running
                .state
                .get(&key_bytes(waiting.key))
                .expect("the simulated disk does not fail");
            self.events += 1;
            let result = Return::Value(value.as_deref().map(value_of));
            self.histories[waiting.key][waiting.slot].returned = Some((self.events, result));
        }
    }

    /// A node refused a read. The client tries again on its next turn, likely elsewhere.
    fn read_rejected(&mut self, id: ReadId) {
        let waiting = self
            .clients
            .iter_mut()
            .filter_map(|client| client.waiting.as_mut());
        if let Some(waiting) = waiting.into_iter().find(|waiting| waiting.read == Some(id)) {
            waiting.target = None;
        }
    }

    /// A node applied a request and answers the client, if the client is still waiting on this
    /// node for this request. The answer may be lost on the way.
    fn answer(&mut self, node: usize, outcome: &Outcome) {
        let lost = self.clients_busy && self.chance(10);
        let life = self.nodes[node].life;
        let Some(client) = self
            .clients
            .iter_mut()
            .find(|client| client.session == outcome.session)
        else {
            return;
        };
        let expected = client.seq == outcome.seq
            && client
                .waiting
                .as_ref()
                .is_some_and(|waiting| waiting.target == Some((node, life)));
        if !expected || lost {
            return;
        }
        if let Some(waiting) = client.waiting.take() {
            self.events += 1;
            self.histories[waiting.key][waiting.slot].returned =
                Some((self.events, returned(&outcome.reply)));
            client.answered = Some(waiting.request);
        }
    }

    // ---- Faults. ----

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
                // Often its clock is slow as well, so it takes long to notice. This is the node
                // that would serve stale reads if it trusted itself.
                if self.chance(50) {
                    self.nodes[isolated].clock_percent = 10;
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

    // ---- Checks. ----

    /// A node must have stored an entry durably before it applies it. Then state machine safety:
    /// every node applies the same entry at each index.
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
                // Entries before the base are in the leader's snapshot. Those it applied itself,
                // and state machine safety covers them.
                let base = to_usize(raft.log_base().index().get());
                let log = raft.log();
                let missing = (base..self.committed.len())
                    .find(|&i| log.get(i - base) != self.committed.get(i));
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
            let base = to_usize(first.log_base().index().get());
            let log = first.log();
            let committed = to_usize(first.commit_index().get());
            if let Some(index) =
                (base..committed).find(|&i| log.get(i - base) != self.committed.get(i))
            {
                return Err(Violation::CommittedEntryChanged { node: a, index });
            }
            // The log stays bounded: applied entries do not pile up past the compaction policy.
            if first.applied_index().get() >= first.log_base().index().get() + self.compact_every {
                return Err(Violation::LogNotCompacted { node: a });
            }
            for (b, second) in self.up().filter(|&(b, _)| b > a) {
                let other_base = to_usize(second.log_base().index().get());
                let other = second.log();
                // The positions both logs hold, counted from the start of history.
                let shared = base.max(other_base)..(base + log.len()).min(other_base + other.len());
                // The highest of them where both hold an entry of the same term. Everything they
                // both hold up to it has to be identical.
                let same_term = |i: &usize| log[*i - base].term() == other[*i - other_base].term();
                let Some(agreed) = shared.clone().rev().find(same_term) else {
                    continue;
                };
                if let Some(index) =
                    (shared.start..=agreed).find(|&i| log[i - base] != other[i - other_base])
                {
                    return Err(Violation::LogMatching { a, b, index });
                }
            }
        }
        Ok(())
    }

    /// Whether every member holds the whole committed log and nothing else, and no client is
    /// still waiting.
    fn quiet(&self) -> bool {
        let length = to_u64(self.committed.len());
        let Some(members) = self.members() else {
            return false;
        };
        // Applied as well as committed: a node whose disk is still writing knows that an entry
        // is committed before it may apply it.
        let settled = |node: usize| {
            self.raft(node).is_some_and(|raft| {
                [
                    raft.last_log_index(),
                    raft.commit_index(),
                    raft.applied_index(),
                ]
                .iter()
                .all(|index| index.get() == length)
            })
        };
        self.clients.iter().all(|client| client.waiting.is_none())
            && members.into_iter().all(settled)
    }

    /// Advances until the cluster is quiet, for at most `budget` ticks.
    fn advance_until_quiet(&mut self, budget: u64) -> Result<bool, Violation> {
        for _ in 0..budget {
            if self.quiet() {
                return Ok(true);
            }
            self.advance()?;
        }
        Ok(self.quiet())
    }

    /// Stops every fault and expects the cluster to recover: every node ends up with the same
    /// fully committed log, every client gets its answer, and a new write commits everywhere
    /// within a fixed time. Then the histories and the state machines are checked.
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
        self.clients_busy = false;
        for node in &mut self.nodes {
            node.restart_at = 0;
            node.clock_percent = 100;
        }

        // A follower that fell behind catches up one batch per round trip, and the smallest batch
        // is one entry, so the time allowed grows with the longest log.
        let longest = self
            .up()
            .map(|(_, raft)| to_usize(raft.last_log_index().get()))
            .max()
            .unwrap_or(0);
        let budget = 300 + 4 * to_u64(longest.max(self.committed.len()));
        if !self.advance_until_quiet(budget)? {
            return Err(Violation::Liveness {
                reason: "the cluster did not settle after the faults stopped",
            });
        }

        let before = self.committed.len();
        self.propose_junk()?;
        if !self.advance_until_quiet(50)? || self.committed.len() <= before {
            return Err(Violation::Liveness {
                reason: "a write after recovery did not commit everywhere",
            });
        }
        self.check_logs()?;

        if let Some(key) = (0..KEYS).find(|&key| !linearizable(&self.histories[key])) {
            return Err(Violation::NotLinearizable { key });
        }
        // Only the members are compared. A node that was removed, or never added, stopped
        // receiving the log at some point and is not expected to keep up.
        let members = self.members().unwrap_or_default();
        let dump = |node: usize| self.nodes[node].live.as_ref().map(|live| live.state.dump());
        let reference = members.first().and_then(|&node| dump(node));
        match members.iter().find(|&&node| dump(node) != reference) {
            Some(&node) => Err(Violation::StateDiverged { node }),
            None => Ok(()),
        }
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
        let operations = AtomicU64::new(0);
        let snapshots = AtomicU64::new(0);
        let changes = AtomicU64::new(0);
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
                            Ok(totals) => {
                                operations
                                    .fetch_add(super::to_u64(totals.operations), Ordering::Relaxed);
                                snapshots
                                    .fetch_add(super::to_u64(totals.snapshots), Ordering::Relaxed);
                                changes.fetch_add(super::to_u64(totals.changes), Ordering::Relaxed);
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
        // A run in which no client got an answer would pass every check without testing anything.
        assert!(operations.load(Ordering::Relaxed) > 0);
        // Nor would snapshots or membership changes be tested if none ever happened. A single seed
        // may well not; a hundred do.
        if range.end - range.start >= 100 {
            assert!(snapshots.load(Ordering::Relaxed) > 0);
            assert!(changes.load(Ordering::Relaxed) > 0);
        }
    }

    #[test]
    fn committing_an_earlier_term_by_counting_replicas_is_caught() {
        // This bug needs a long chain of events and has become rare since pre-vote and
        // check-quorum cut down on needless elections. The chain itself is reproduced step by
        // step in `raft::tests`; here the random search gets a larger budget.
        let found = first_safety_violation(Bug::CommitEarlierTerm, 3_000);
        assert!(
            found.is_some(),
            "no seed below 3,000 exposed the figure 8 commit bug"
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
    fn carrying_out_a_retried_request_twice_is_caught() {
        let found = first_safety_violation(Bug::ApplyRetriesAgain, 1_000);
        assert!(
            matches!(found, Some((_, Violation::NotLinearizable { .. }))),
            "expected a history that is not linearizable, got {found:?}"
        );
    }

    #[test]
    fn answering_a_read_without_confirming_leadership_is_caught() {
        let found = first_safety_violation(Bug::ReadWithoutQuorum, 1_000);
        assert!(
            matches!(found, Some((_, Violation::NotLinearizable { .. }))),
            "expected a history that is not linearizable, got {found:?}"
        );
    }

    #[test]
    fn compacting_the_log_before_the_state_machine_is_durable_is_caught() {
        let found = first_safety_violation(Bug::CompactBeforeCheckpoint, 1_000);
        assert!(
            matches!(found, Some((_, Violation::StateLost { .. }))),
            "expected a node that lost its state, got {found:?}"
        );
    }

    #[test]
    fn accepting_a_membership_change_while_another_is_in_flight_is_caught() {
        let found = first_safety_violation(Bug::ChangeWhileInProgress, 1_000);
        assert!(
            found.is_some(),
            "no seed below 1,000 exposed two membership changes in flight"
        );
    }

    #[test]
    fn every_node_can_be_replaced_one_by_one_and_nothing_is_lost() {
        let mut operations = 0;
        for seed in 0..env("QUORATE_REPLACEMENTS").unwrap_or(1_000) {
            match Sim::for_replacement(seed).run_replacement() {
                Ok(totals) => operations += totals.operations,
                Err(violation) => panic!("seed {seed}: {violation:?}"),
            }
        }
        assert!(operations > 0);
    }

    #[test]
    fn a_seed_replays_to_the_same_result() {
        let run = || Sim::new(42, Some(Bug::VoteWithoutLogCheck)).run(TICKS);
        assert_eq!(run(), run());
    }
}
