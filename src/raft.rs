//! The Raft state machine: leader election and log replication.
//!
//! Nothing in here does I/O, reads a clock or starts a thread. Time arrives as [`Input::Tick`],
//! the network as [`Input::Message`], and everything the node wants done leaves as [`Output`].
//! Randomness comes from the seed in [`Config`]. Two nodes built from the same configuration and
//! fed the same inputs produce the same outputs.
//!
//! # Durability
//!
//! The term, the vote and the log must survive a crash. When a step changes any of them the node
//! emits one [`Output::Persist`] and keeps going; it does not wait. What it does do is hold back
//! every message until the state that message depends on is reported durable with
//! [`Input::Persisted`]. A vote is not sent before it is on disk, and an append is not
//! acknowledged before the entries are. A leader counts its own log towards a quorum only up to
//! its durable index, and no node applies an entry it has not stored durably.
//!
//! # Staying out of the way
//!
//! Two rules keep a node with a bad connection from disturbing a healthy cluster. Before an
//! election a node asks whether it could win (pre-vote); asking changes nothing, so a node that
//! was cut off returns with the term it left with. And a leader that hears from fewer than a
//! quorum for a whole election timeout steps down (check-quorum).

use std::collections::VecDeque;

use crate::config::{Config, ConfigError};
use crate::log::Log;
use crate::membership::{Change, ChangeError, Membership};
use crate::rng::Rng;
use crate::types::{
    AppendOutcome, Entry, HardState, Index, Input, LogWrite, Message, NodeId, Output, Payload,
    Persist, PersistSeq, ReadId, SnapshotMeta, Term,
};

/// What a node currently is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Role {
    /// Accepts entries from a leader.
    Follower,
    /// Asking whether an election could be won, without starting one.
    PreCandidate,
    /// Asking for votes.
    Candidate,
    /// Replicating its log to the others.
    Leader,
}

/// A rule the simulator tests switch off, to prove they would notice it missing.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Bug {
    /// Not a rule of the core: the state machine carries out a retried request a second time.
    ApplyRetriesAgain,
    /// Commit an entry from an earlier term by counting replicas (Raft paper, figure 8).
    CommitEarlierTerm,
    /// Grant a vote without checking that the candidate's log is up to date.
    VoteWithoutLogCheck,
    /// Send messages without waiting for the state they depend on to be durable.
    SendBeforeDurable,
    /// Not a rule of the core: the driver has the log compacted before the state machine is on
    /// disk.
    CompactBeforeCheckpoint,
    /// Accept a membership change while an earlier one is not committed.
    ChangeWhileInProgress,
    /// Answer a read without asking a quorum whether this node still leads.
    ReadWithoutQuorum,
    /// Start an election without asking first. Not a safety bug; used to show what pre-vote
    /// prevents.
    SkipPreVote,
}

/// What the leader knows about one follower.
#[derive(Debug)]
struct Progress {
    peer: NodeId,
    /// The next entry to send.
    next: Index,
    /// The last entry known to be stored on the follower.
    matched: Index,
    /// The highest read round the follower has answered.
    acked_round: u64,
    /// Whether the follower has answered anything since the last quorum check.
    active: bool,
    /// Ticks since a snapshot was asked for, while the follower needs one. `None` otherwise.
    snapshot: Option<u32>,
}

/// A read waiting for a quorum to confirm that this node still leads.
#[derive(Debug)]
struct PendingRead {
    id: ReadId,
    /// The commit index when the read arrived. Any state that includes it may answer the read.
    index: Index,
    /// Followers that answer a message of this round or a later one confirm the read.
    round: u64,
}

#[derive(Debug)]
enum State {
    Follower {
        leader: Option<NodeId>,
    },
    PreCandidate {
        votes: Vec<NodeId>,
    },
    Candidate {
        votes: Vec<NodeId>,
    },
    Leader {
        progress: Vec<Progress>,
        since_heartbeat: u32,
        since_quorum_check: u32,
        /// Counts the reads accepted in this term. Every `AppendEntries` carries it.
        round: u64,
        reads: VecDeque<PendingRead>,
    },
}

/// One Raft node.
#[derive(Debug)]
pub struct Raft {
    id: NodeId,
    /// The membership in force: the last configuration entry in the log, committed or not.
    membership: Membership,
    /// The index of that entry, or `floor_index` if the log holds none.
    config_index: Index,
    /// A committed position and the membership as of it, for when the log holds no
    /// configuration entry: the start of the log, or the state machine's position at restart.
    floor_index: Index,
    floor_membership: Membership,
    election_ticks: u32,
    heartbeat_ticks: u32,
    max_batch: usize,
    rng: Rng,
    term: Term,
    voted_for: Option<NodeId>,
    log: Log,
    commit: Index,
    applied: Index,
    state: State,
    /// Ticks since the election timer was last reset.
    elapsed: u32,
    /// The current, randomised election timeout.
    timeout: u32,

    /// The term or the vote changed in this step.
    hard_state_dirty: bool,
    /// The lowest log index that changed in this step.
    log_dirty_from: Option<Index>,
    /// The start of the log moved in this step.
    base_dirty: bool,
    /// The number of the last persist request issued.
    persist_seq: u64,
    /// The highest persist request reported durable.
    durable_seq: u64,
    /// The prefix of the in-memory log that is known to be on disk.
    durable_index: Index,
    /// For each request still in flight, the last log index it makes durable.
    in_flight: VecDeque<(u64, Index)>,
    /// Outputs waiting for a persist request, tagged with the request they wait for.
    held: VecDeque<(u64, Output)>,

    #[cfg(test)]
    bug: Option<Bug>,
}

impl Raft {
    /// Creates a node that has never run: a follower in term 0 with an empty log.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] when the configuration cannot work: the node lists itself or a
    /// peer twice, the heartbeat is not shorter than the election timeout, or the batch size is
    /// zero.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        let start = SnapshotMeta::default();
        Self::recover(config, HardState::default(), start, Vec::new(), start, None)
    }

    /// Restarts a node from what its storage held. It comes back as a follower.
    ///
    /// `base` and `entries` are the durable log: the point it starts after, and the entries from
    /// there on. `applied` is how far the state machine got. Those entries were committed, so the
    /// node starts with its commit index there and emits [`Output::Apply`] from the entry after
    /// it. `membership` is the membership the state machine recorded for that position; without
    /// one the node starts from [`Config::founding_membership`].
    ///
    /// The log may not agree with the state machine. That happens when a snapshot was installed
    /// and the node went down before its log was rewritten to match: the state machine is at the
    /// snapshot, and the log is still the old one, too short or gone another way. The test is the
    /// term at the state machine's position. If the log holds a different one, or none, it starts
    /// over after that position; nothing the old log held is needed any more.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] for the same reasons as [`Raft::new`], and
    /// [`ConfigError::StateBehindLog`] when the log starts after `applied`.
    pub fn recover(
        config: Config,
        hard_state: HardState,
        base: SnapshotMeta,
        entries: Vec<Entry>,
        applied: SnapshotMeta,
        membership: Option<Membership>,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        if applied.index() < base.index() {
            return Err(ConfigError::StateBehindLog);
        }
        let mut log = Log::from_parts(base, entries);
        let start_over = log.term_at(applied.index()) != Some(applied.term());
        if start_over {
            log.reset(applied);
        }
        let floor_membership = match membership {
            Some(membership) => membership,
            None if config.joining => Membership::default(),
            None => Membership::new(config.peers.into_iter().chain([config.id]), []),
        };
        let mut node = Self {
            id: config.id,
            membership: Membership::default(),
            config_index: Index::ZERO,
            floor_index: applied.index(),
            floor_membership,
            election_ticks: config.election_ticks,
            heartbeat_ticks: config.heartbeat_ticks,
            max_batch: config.max_batch,
            rng: Rng::new(config.seed),
            term: hard_state.term(),
            voted_for: hard_state.voted_for(),
            durable_index: log.last_index(),
            log,
            commit: applied.index(),
            applied: applied.index(),
            state: State::Follower { leader: None },
            elapsed: 0,
            timeout: 0,
            hard_state_dirty: false,
            // A log that started over is written out with the first step.
            log_dirty_from: start_over.then(|| applied.index().next()),
            base_dirty: start_over,
            persist_seq: 0,
            durable_seq: 0,
            in_flight: VecDeque::new(),
            held: VecDeque::new(),
            #[cfg(test)]
            bug: None,
        };
        node.reload_membership();
        node.reset_timer();
        Ok(node)
    }

    /// Feeds one input to the node and returns what the driver must do, in order.
    pub fn step(&mut self, input: Input) -> Vec<Output> {
        let mut out = Vec::new();
        let mut produced = Vec::new();
        match input {
            Input::Tick => self.tick(&mut produced),
            Input::Message { from, message } => self.receive(from, message, &mut produced),
            Input::Propose(command) => self.propose(command, &mut produced),
            Input::Read { id } => self.read(id, &mut produced),
            Input::Compact { index } => self.compact(index),
            Input::Snapshot {
                from,
                term,
                round,
                snapshot,
                membership,
            } => {
                if term > self.term {
                    self.become_follower(term, None, &mut produced);
                }
                self.on_snapshot(from, term, round, snapshot, membership, &mut produced);
            }
            Input::ChangeMembership(change) => self.change_membership(change, &mut produced),
            Input::Persisted { seq } => self.on_persisted(seq, &mut out),
        }
        self.step_down_if_removed(&mut produced);
        if let Some(persist) = self.take_persist() {
            out.push(Output::Persist(persist));
        }
        self.release_or_hold(produced, &mut out);
        self.apply_committed(&mut out);
        out
    }

    /// This node's id.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// The current term.
    #[must_use]
    pub const fn term(&self) -> Term {
        self.term
    }

    /// The current role.
    #[must_use]
    pub const fn role(&self) -> Role {
        match self.state {
            State::Follower { .. } => Role::Follower,
            State::PreCandidate { .. } => Role::PreCandidate,
            State::Candidate { .. } => Role::Candidate,
            State::Leader { .. } => Role::Leader,
        }
    }

    /// The leader of the current term, as far as this node knows.
    #[must_use]
    pub const fn leader(&self) -> Option<NodeId> {
        match self.state {
            State::Follower { leader } => leader,
            State::PreCandidate { .. } | State::Candidate { .. } => None,
            State::Leader { .. } => Some(self.id),
        }
    }

    /// The highest index known to be committed.
    #[must_use]
    pub const fn commit_index(&self) -> Index {
        self.commit
    }

    /// The membership in force on this node: the one in the last configuration entry of its log,
    /// whether that entry is committed or not.
    #[must_use]
    pub const fn membership(&self) -> &Membership {
        &self.membership
    }

    /// The last index the node has handed out with [`Output::Apply`], or taken over from a
    /// snapshot. The log can be compacted up to here and no further.
    #[must_use]
    pub const fn applied_index(&self) -> Index {
        self.applied
    }

    /// The entries the node holds in memory, starting right after [`Raft::log_base`]. The tail
    /// may not be durable yet.
    #[must_use]
    pub fn log(&self) -> &[Entry] {
        self.log.entries()
    }

    /// The point the log starts after. Everything up to it is in a snapshot.
    #[must_use]
    pub const fn log_base(&self) -> SnapshotMeta {
        self.log.base()
    }

    /// The index of the last entry in the log.
    #[must_use]
    pub fn last_log_index(&self) -> Index {
        self.log.last_index()
    }

    #[cfg(test)]
    pub(crate) const fn inject(&mut self, bug: Bug) {
        self.bug = Some(bug);
    }

    /// Votes needed to win an election or commit an entry.
    fn quorum(&self) -> usize {
        self.membership.quorum()
    }

    /// Whether this node's vote counts. A learner, a node that was removed and a node that has
    /// not been added yet do not start elections.
    fn is_voter(&self) -> bool {
        self.membership.is_voter(self.id)
    }

    /// Takes a membership into force. A leader starts replicating to members it did not know and
    /// stops for those that are gone.
    fn adopt(&mut self, membership: Membership) {
        self.membership = membership;
        let next = self.log.last_index().next();
        let State::Leader { progress, .. } = &mut self.state else {
            return;
        };
        progress.retain(|follower| self.membership.contains(follower.peer));
        for member in self.membership.members() {
            if member != self.id && !progress.iter().any(|follower| follower.peer == member) {
                progress.push(Progress {
                    peer: member,
                    next,
                    matched: Index::ZERO,
                    acked_round: 0,
                    active: false,
                    snapshot: None,
                });
            }
        }
    }

    /// Works out the membership in force from the log: its last configuration entry, or the
    /// floor if it holds none. Needed whenever entries leave the log, at either end.
    fn reload_membership(&mut self) {
        let base = self.log.base().index().get();
        let last = self
            .log
            .entries()
            .iter()
            .enumerate()
            .rev()
            .find_map(|(position, entry)| match entry.payload() {
                Payload::Config(membership) => Some((position, membership.clone())),
                Payload::Noop | Payload::Command(_) => None,
            });
        let (index, membership) = match last {
            Some((position, membership)) => {
                let offset = u64::try_from(position)
                    .unwrap_or(u64::MAX)
                    .saturating_add(1);
                (Index::new(base.saturating_add(offset)), membership)
            }
            None => (self.floor_index, self.floor_membership.clone()),
        };
        self.config_index = index;
        self.adopt(membership);
    }

    /// Restarts the election timer with a fresh timeout in `election_ticks..2 * election_ticks`.
    /// The spread is what stops two candidates from splitting the vote for ever.
    fn reset_timer(&mut self) {
        let spread = self.rng.below(u64::from(self.election_ticks));
        self.elapsed = 0;
        self.timeout = self
            .election_ticks
            .saturating_add(u32::try_from(spread).unwrap_or(0));
    }

    // ---- Persistent state. Every change goes through these four, so none is forgotten. ----

    fn set_term(&mut self, term: Term) {
        self.term = term;
        self.voted_for = None;
        self.hard_state_dirty = true;
    }

    fn set_vote(&mut self, candidate: NodeId) {
        self.voted_for = Some(candidate);
        self.hard_state_dirty = true;
    }

    fn append(&mut self, entry: Entry) {
        let membership = match entry.payload() {
            Payload::Config(membership) => Some(membership.clone()),
            Payload::Noop | Payload::Command(_) => None,
        };
        self.log.append(entry);
        let index = self.log.last_index();
        self.log_dirty_from = Some(self.log_dirty_from.map_or(index, |from| from.min(index)));
        if let Some(membership) = membership {
            // In force from the moment it is in the log, not from the moment it commits (Raft
            // thesis 4.1). A node cannot tell when an entry commits elsewhere, so a rule based
            // on commitment would have different nodes switch at different times anyway.
            self.config_index = index;
            self.adopt(membership);
        }
    }

    /// Drops every entry after `index`, including from what is counted as durable: the entries
    /// are still on disk until the next persist request replaces them, but they are no longer
    /// part of this node's log.
    fn truncate_after(&mut self, index: Index) {
        self.log.truncate_after(index);
        let first_gone = index.next();
        self.log_dirty_from = Some(
            self.log_dirty_from
                .map_or(first_gone, |from| from.min(first_gone)),
        );
        self.durable_index = self.durable_index.min(index);
        for (_, last) in &mut self.in_flight {
            *last = (*last).min(index);
        }
        if self.config_index > index {
            // The membership entry in force was one of the entries that went.
            self.reload_membership();
        }
    }

    /// The persist request for what this step changed, if it changed anything.
    fn take_persist(&mut self) -> Option<Persist> {
        let dirty_from = self.log_dirty_from.take();
        let base_moved = core::mem::take(&mut self.base_dirty);
        if !self.hard_state_dirty && !base_moved && dirty_from.is_none() {
            return None;
        }
        self.hard_state_dirty = false;
        self.persist_seq += 1;
        self.in_flight
            .push_back((self.persist_seq, self.log.last_index()));
        Some(Persist::new(
            PersistSeq::new(self.persist_seq),
            HardState::new(self.term, self.voted_for),
            base_moved.then(|| self.log.base()),
            dirty_from.map(|first| LogWrite::new(first, self.log.slice_from(first, usize::MAX))),
        ))
    }

    fn on_persisted(&mut self, seq: PersistSeq, out: &mut Vec<Output>) {
        let seq = seq.get().min(self.persist_seq);
        if seq <= self.durable_seq {
            return;
        }
        self.durable_seq = seq;
        while let Some(&(request, last)) = self.in_flight.front() {
            if request > seq {
                break;
            }
            self.durable_index = last;
            self.in_flight.pop_front();
        }
        while self
            .held
            .front()
            .is_some_and(|(request, _)| *request <= seq)
        {
            if let Some((_, output)) = self.held.pop_front() {
                out.push(output);
            }
        }
        // The leader's own log now counts for more.
        self.advance_commit();
    }

    /// Emits what this step produced, or holds it until the state it depends on is durable.
    ///
    /// Everything waits for the latest request, not only what strictly needs it. That keeps
    /// messages in order and makes the rule easy to check.
    // ponytail: a leader could send entries to followers while its own write is in flight
    // (Raft thesis, 10.2.1). Do it when the benchmarks say the disk is the bottleneck.
    fn release_or_hold(&mut self, produced: Vec<Output>, out: &mut Vec<Output>) {
        let waiting = self.persist_seq > self.durable_seq;
        #[cfg(test)]
        let waiting = waiting && self.bug != Some(Bug::SendBeforeDurable);

        if waiting {
            let request = self.persist_seq;
            self.held
                .extend(produced.into_iter().map(|output| (request, output)));
        } else {
            out.extend(produced);
        }
    }

    // ---- Time and clients. ----

    fn tick(&mut self, out: &mut Vec<Output>) {
        if matches!(self.state, State::Leader { .. }) {
            self.leader_tick(out);
            return;
        }
        self.elapsed = self.elapsed.saturating_add(1);
        if self.elapsed < self.timeout {
            return;
        }
        if self.is_voter() {
            self.pre_campaign(out);
        } else {
            self.reset_timer();
        }
    }

    fn leader_tick(&mut self, out: &mut Vec<Output>) {
        let quorum = self.quorum();
        let own = usize::from(self.is_voter());
        let patience = self.election_ticks.saturating_mul(2);
        let State::Leader {
            progress,
            since_heartbeat,
            since_quorum_check,
            ..
        } = &mut self.state
        else {
            return;
        };
        // A snapshot that has not been acknowledged for two election timeouts was probably lost.
        // Forget that it was asked for, so the next heartbeat asks again.
        for follower in progress.iter_mut() {
            follower.snapshot = follower
                .snapshot
                .map(|waited| waited + 1)
                .filter(|&waited| waited < patience);
        }
        // Check-quorum: a leader that has heard from fewer than a quorum of voters for a whole
        // election timeout is cut off. Its followers have given up on it or soon will, so it
        // stops accepting work it could never commit.
        *since_quorum_check += 1;
        if *since_quorum_check >= self.election_ticks {
            *since_quorum_check = 0;
            let voters = progress
                .iter()
                .filter(|follower| self.membership.is_voter(follower.peer));
            let heard_from = own + voters.filter(|follower| follower.active).count();
            for follower in progress.iter_mut() {
                follower.active = false;
            }
            if heard_from < quorum {
                self.become_follower(self.term, None, out);
                return;
            }
        }
        *since_heartbeat += 1;
        if *since_heartbeat >= self.heartbeat_ticks {
            *since_heartbeat = 0;
            self.broadcast_append(out);
        }
    }

    fn propose(&mut self, command: Vec<u8>, out: &mut Vec<Output>) {
        if !matches!(self.state, State::Leader { .. }) {
            out.push(Output::NotLeader {
                leader: self.leader(),
            });
            return;
        }
        self.append(Entry::new(self.term, Payload::Command(command)));
        self.broadcast_append(out);
    }

    /// A linearizable read without a log entry (`ReadIndex`, Raft thesis 6.4).
    ///
    /// The read may be served from any state that includes the commit index of this moment,
    /// provided this node really is the leader at this moment. It cannot know that by itself: a
    /// newer leader may exist that it has not heard of. So it asks. Followers that answer a
    /// message sent after the read arrived confirm that no newer term had reached them by then,
    /// and a quorum of them leaves no room for a newer leader.
    fn read(&mut self, id: ReadId, out: &mut Vec<Output>) {
        let index = self.commit;
        // Until an entry of its own term is committed, a new leader does not know how far the
        // previous leaders got, so its commit index may be behind.
        let knows_commit = self.log.term_at(index) == Some(self.term);
        let leader = self.leader();
        let alone = self.is_voter() && self.quorum() == 1;
        let State::Leader { round, reads, .. } = &mut self.state else {
            out.push(Output::ReadRejected { id, leader });
            return;
        };
        if !knows_commit {
            out.push(Output::ReadRejected { id, leader });
            return;
        }
        #[cfg(test)]
        let alone = alone || self.bug == Some(Bug::ReadWithoutQuorum);

        if alone {
            out.push(Output::ReadReady { id, index });
            return;
        }
        *round += 1;
        reads.push_back(PendingRead {
            id,
            index,
            round: *round,
        });
        self.broadcast_append(out);
    }

    /// Releases the reads that a quorum has confirmed.
    fn confirm_reads(&mut self, out: &mut Vec<Output>) {
        let quorum = self.quorum();
        let own = usize::from(self.is_voter());
        let State::Leader {
            progress, reads, ..
        } = &mut self.state
        else {
            return;
        };
        while let Some(read) = reads.front() {
            let voters = progress
                .iter()
                .filter(|follower| self.membership.is_voter(follower.peer));
            let confirmed = own
                + voters
                    .filter(|follower| follower.acked_round >= read.round)
                    .count();
            if confirmed < quorum {
                break;
            }
            out.push(Output::ReadReady {
                id: read.id,
                index: read.index,
            });
            reads.pop_front();
        }
    }

    // ---- Elections. ----

    /// Asks whether an election could be won, without disturbing anyone (pre-vote, Raft thesis
    /// 9.6). Nothing changes on this node or on the ones it asks. A node cut off from the
    /// cluster therefore keeps its term, and when it comes back it does not force the leader
    /// out with a term nobody else has seen.
    fn pre_campaign(&mut self, out: &mut Vec<Output>) {
        self.reset_timer();
        // The only voter needs nobody's opinion.
        let direct = self.quorum() == 1;
        #[cfg(test)]
        let direct = direct || self.bug == Some(Bug::SkipPreVote);

        if direct {
            self.campaign(out);
            return;
        }
        self.state = State::PreCandidate {
            votes: vec![self.id],
        };
        let message = Message::PreVote {
            term: self.term.next(),
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        self.send_to_other_voters(&message, out);
    }

    fn send_to_other_voters(&self, message: &Message, out: &mut Vec<Output>) {
        for &voter in self.membership.voters() {
            if voter != self.id {
                out.push(Output::Send {
                    to: voter,
                    message: message.clone(),
                });
            }
        }
    }

    /// Starts an election for the next term.
    fn campaign(&mut self, out: &mut Vec<Output>) {
        self.set_term(self.term.next());
        self.set_vote(self.id);
        self.state = State::Candidate {
            votes: vec![self.id],
        };
        self.reset_timer();
        if self.quorum() == 1 {
            self.become_leader(out);
            return;
        }
        let message = Message::RequestVote {
            term: self.term,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        self.send_to_other_voters(&message, out);
    }

    fn become_leader(&mut self, out: &mut Vec<Output>) {
        self.state = State::Leader {
            progress: Vec::new(),
            since_heartbeat: 0,
            since_quorum_check: 0,
            round: 0,
            reads: VecDeque::new(),
        };
        // Fills in the followers, each expected to continue right after this log.
        self.adopt(self.membership.clone());
        // A leader may only commit by counting replicas of an entry from its own term. Without
        // this entry, earlier entries would wait for the next client command.
        self.append(Entry::new(self.term, Payload::Noop));
        self.broadcast_append(out);
    }

    /// Steps down, adopting `term` if it is newer. Reads a leader was still confirming are
    /// refused: it can no longer vouch for them.
    fn become_follower(&mut self, term: Term, leader: Option<NodeId>, out: &mut Vec<Output>) {
        if term > self.term {
            self.set_term(term);
        }
        match core::mem::replace(&mut self.state, State::Follower { leader }) {
            State::Follower { .. } => {}
            State::Leader { reads, .. } => {
                out.extend(reads.into_iter().map(|read| Output::ReadRejected {
                    id: read.id,
                    leader,
                }));
                // A leader has no running election timer.
                self.reset_timer();
            }
            // The timer was running for this node's own election.
            State::PreCandidate { .. } | State::Candidate { .. } => self.reset_timer(),
        }
    }

    fn receive(&mut self, from: NodeId, message: Message, out: &mut Vec<Output>) {
        // A pre-vote asks about a term nobody is in yet, and a granted pre-vote repeats that
        // term back. Neither is evidence that the term exists.
        let names_a_real_term = match message {
            Message::PreVote { .. } => false,
            Message::PreVoteResponse { granted, .. } => !granted,
            _ => true,
        };
        if names_a_real_term && message.term() > self.term {
            self.become_follower(message.term(), None, out);
        }
        match message {
            Message::PreVote {
                term,
                last_log_index,
                last_log_term,
            } => {
                self.on_pre_vote(from, term, last_log_index, last_log_term, out);
            }
            Message::PreVoteResponse { term, granted } => {
                self.on_pre_vote_response(from, term, granted, out);
            }
            Message::RequestVote {
                term,
                last_log_index,
                last_log_term,
            } => {
                self.on_request_vote(from, term, last_log_index, last_log_term, out);
            }
            Message::RequestVoteResponse { term, granted } => {
                self.on_vote(from, term, granted, out);
            }
            Message::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                round,
            } => {
                let outcome = self.on_append(
                    from,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    out,
                );
                let message = Message::AppendEntriesResponse {
                    term: self.term,
                    outcome,
                    round,
                };
                out.push(Output::Send { to: from, message });
            }
            Message::AppendEntriesResponse {
                term,
                outcome,
                round,
            } => {
                self.on_append_response(from, term, outcome, round, out);
            }
        }
    }

    /// Raft §5.4.1: only vote for a candidate whose log is at least as complete as ours. This is
    /// what guarantees that a new leader holds every committed entry.
    fn log_is_up_to_date(&self, last_log_index: Index, last_log_term: Term) -> bool {
        let up_to_date =
            (last_log_term, last_log_index) >= (self.log.last_term(), self.log.last_index());
        #[cfg(test)]
        let up_to_date = up_to_date || self.bug == Some(Bug::VoteWithoutLogCheck);

        up_to_date
    }

    fn on_pre_vote(
        &mut self,
        from: NodeId,
        term: Term,
        last_log_index: Index,
        last_log_term: Term,
        out: &mut Vec<Output>,
    ) {
        // A node that still hears from a leader says no. Otherwise a node that can send but not
        // receive would collect pre-votes and then depose a healthy leader.
        let has_a_leader = match self.state {
            State::Leader { .. } => true,
            State::Follower { leader: Some(_) } => self.elapsed < self.election_ticks,
            State::Follower { leader: None }
            | State::PreCandidate { .. }
            | State::Candidate { .. } => false,
        };
        let granted = term > self.term
            && !has_a_leader
            && self.log_is_up_to_date(last_log_index, last_log_term);
        // A grant repeats the term that was asked about, so the asker can match it to its
        // question. A refusal carries this node's own term, in case the asker is behind.
        let term = if granted { term } else { self.term };
        out.push(Output::Send {
            to: from,
            message: Message::PreVoteResponse { term, granted },
        });
    }

    fn on_pre_vote_response(
        &mut self,
        from: NodeId,
        term: Term,
        granted: bool,
        out: &mut Vec<Output>,
    ) {
        if !granted || term != self.term.next() || !self.membership.is_voter(from) {
            return;
        }
        let quorum = self.quorum();
        let State::PreCandidate { votes } = &mut self.state else {
            return;
        };
        if !votes.contains(&from) {
            votes.push(from);
        }
        if votes.len() >= quorum {
            self.campaign(out);
        }
    }

    fn on_request_vote(
        &mut self,
        from: NodeId,
        term: Term,
        last_log_index: Index,
        last_log_term: Term,
        out: &mut Vec<Output>,
    ) {
        let free_to_vote = self.voted_for.is_none_or(|candidate| candidate == from);
        let granted = term == self.term
            && free_to_vote
            && self.log_is_up_to_date(last_log_index, last_log_term);
        if granted {
            self.set_vote(from);
            self.reset_timer();
        }
        let message = Message::RequestVoteResponse {
            term: self.term,
            granted,
        };
        out.push(Output::Send { to: from, message });
    }

    fn on_vote(&mut self, from: NodeId, term: Term, granted: bool, out: &mut Vec<Output>) {
        // Only the voters of this node's membership elect it, whoever else may answer.
        if term != self.term || !granted || !self.membership.is_voter(from) {
            return;
        }
        let quorum = self.quorum();
        let State::Candidate { votes } = &mut self.state else {
            return;
        };
        if !votes.contains(&from) {
            votes.push(from);
        }
        if votes.len() >= quorum {
            self.become_leader(out);
        }
    }

    // ---- Replication. ----

    /// The answer to an append that does not fit this node's log, with a hint for the leader.
    fn refusal(&self, prev_log_index: Index) -> AppendOutcome {
        AppendOutcome::Rejected {
            prev_log_index,
            retry_from: if prev_log_index > self.log.last_index() {
                self.log.last_index().next()
            } else {
                self.log.first_index_of_term(prev_log_index)
            },
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the fields of one message, taken apart by the caller"
    )]
    fn on_append(
        &mut self,
        from: NodeId,
        term: Term,
        mut prev_log_index: Index,
        mut prev_log_term: Term,
        mut entries: Vec<Entry>,
        leader_commit: Index,
        out: &mut Vec<Output>,
    ) -> AppendOutcome {
        if term < self.term {
            // From a deposed leader. The term in our answer makes it step down.
            return self.refusal(prev_log_index);
        }
        self.become_follower(term, Some(from), out);
        self.reset_timer();

        // A late message can start inside what this node has already folded into a snapshot.
        // Those entries are committed, so they are the leader's entries by definition: skip them
        // and start at the base.
        let base = self.log.base();
        if prev_log_index < base.index() {
            let covered = base.index().get() - prev_log_index.get();
            let covered = usize::try_from(covered).unwrap_or(usize::MAX);
            if covered >= entries.len() {
                let length = u64::try_from(entries.len()).unwrap_or(u64::MAX);
                return AppendOutcome::Accepted {
                    match_index: Index::new(prev_log_index.get().saturating_add(length)),
                };
            }
            entries.drain(..covered);
            prev_log_index = base.index();
            prev_log_term = base.term();
        }
        if self.log.term_at(prev_log_index) != Some(prev_log_term) {
            return self.refusal(prev_log_index);
        }

        let mut index = prev_log_index;
        for entry in entries {
            index = index.next();
            match self.log.term_at(index) {
                // Already stored. A duplicate or reordered message must not truncate what a later
                // message appended.
                Some(existing) if existing == entry.term() => {}
                Some(_) => {
                    self.truncate_after(index.prev());
                    self.append(entry);
                }
                None => self.append(entry),
            }
        }

        // `index` is the last entry this message vouches for. Entries after it may be leftovers
        // from an old leader, so the commit index must not run past it.
        let commit = leader_commit.min(index);
        if commit > self.commit {
            self.commit = commit;
        }
        AppendOutcome::Accepted { match_index: index }
    }

    fn on_append_response(
        &mut self,
        from: NodeId,
        term: Term,
        outcome: AppendOutcome,
        round: u64,
        out: &mut Vec<Output>,
    ) {
        if term != self.term {
            return;
        }
        let last = self.log.last_index();
        let State::Leader { progress, .. } = &mut self.state else {
            return;
        };
        let Some(follower) = progress.iter_mut().find(|progress| progress.peer == from) else {
            return;
        };
        // Any answer in this term, late or duplicated or a refusal, shows that the follower was
        // still following when it answered the message of that round.
        follower.active = true;
        follower.acked_round = follower.acked_round.max(round);

        let send_more = match outcome {
            // A late or duplicated answer brings nothing new.
            AppendOutcome::Accepted { match_index } if match_index <= follower.matched => false,
            AppendOutcome::Accepted { match_index } => {
                follower.matched = match_index;
                follower.next = follower.next.max(match_index.next());
                // Whatever it was waiting for, entries or a snapshot, has arrived.
                follower.snapshot = None;
                follower.next <= last
            }
            // An answer to a probe we have already moved on from.
            AppendOutcome::Rejected { prev_log_index, .. }
                if follower.next.prev() != prev_log_index =>
            {
                false
            }
            AppendOutcome::Rejected {
                prev_log_index,
                retry_from,
            } => {
                let retry = prev_log_index.min(retry_from).max(follower.matched.next());
                // Probe again at once only if this moved us back. A follower that refuses an
                // index it has acknowledged has lost data, and answering each refusal with the
                // same request would multiply with every duplicated message. The heartbeat
                // still retries.
                let moved_back = retry < follower.next;
                follower.next = retry;
                moved_back
            }
        };
        self.advance_commit();
        self.confirm_reads(out);
        if send_more {
            self.replicate(Some(from), false, out);
        }
    }

    /// The `AppendEntries` message that continues the log at `next`, which the log must hold.
    fn append_from(&self, next: Index) -> Message {
        let prev_log_index = next.prev();
        let round = match self.state {
            State::Leader { round, .. } => round,
            _ => 0,
        };
        Message::AppendEntries {
            term: self.term,
            prev_log_index,
            prev_log_term: self.log.term_at(prev_log_index).unwrap_or_default(),
            entries: self.log.slice_from(next, self.max_batch),
            leader_commit: self.commit,
            round,
        }
    }

    fn broadcast_append(&mut self, out: &mut Vec<Output>) {
        self.replicate(None, true, out);
    }

    // ponytail: stop and wait. A follower is sent the batch at `next` again and again until it
    // answers; the batch after it goes out only then. Throughput is one batch per round trip,
    // which with a batch of one entry is less than a busy client produces. Sending ahead
    // (advancing `next` when a batch goes out, falling back on a refusal) removes the ceiling.
    /// Sends one follower, or all of them, what they need next.
    ///
    /// A follower whose next entry is still in the log gets entries. One that needs entries the
    /// log no longer holds gets a snapshot, asked for once and then waited on. On a heartbeat
    /// such a follower is also sent an append it will refuse: the refusal still tells the leader
    /// that the follower is there, and the message keeps the follower from starting an election
    /// while the snapshot is on its way.
    fn replicate(&mut self, only: Option<NodeId>, heartbeat: bool, out: &mut Vec<Output>) {
        let base = self.log.base().index();
        let term = self.term;
        let State::Leader {
            progress, round, ..
        } = &mut self.state
        else {
            return;
        };
        let round = *round;
        let mut plan = Vec::with_capacity(progress.len());
        for follower in progress
            .iter_mut()
            .filter(|follower| only.is_none_or(|peer| peer == follower.peer))
        {
            let behind = follower.next <= base;
            let ask = behind && follower.snapshot.is_none();
            if ask {
                follower.snapshot = Some(0);
            }
            plan.push((follower.peer, follower.next, behind, ask));
        }
        for (peer, next, behind, ask) in plan {
            if !behind {
                out.push(Output::Send {
                    to: peer,
                    message: self.append_from(next),
                });
            } else if heartbeat {
                out.push(Output::Send {
                    to: peer,
                    message: self.append_from(base.next()),
                });
            }
            if ask {
                out.push(Output::SendSnapshot {
                    to: peer,
                    term,
                    round,
                });
            }
        }
    }

    // ---- Snapshots. ----

    /// Drops the log up to `index`. The driver calls this once its state machine holds everything
    /// up to `index` durably, so the entries are no longer needed to rebuild it.
    fn compact(&mut self, index: Index) {
        // Never past what was applied: later entries have not reached the state machine.
        let index = index.min(self.applied);
        if index <= self.log.base().index() {
            return;
        }
        if index >= self.floor_index {
            // The entry that set the membership may be among those that go. Remember what it
            // said.
            let count = index.get() - self.log.base().index().get();
            let count = usize::try_from(count).unwrap_or(usize::MAX);
            let covered = self
                .log
                .entries()
                .iter()
                .take(count)
                .rev()
                .find_map(|entry| match entry.payload() {
                    Payload::Config(membership) => Some(membership.clone()),
                    Payload::Noop | Payload::Command(_) => None,
                });
            if let Some(membership) = covered {
                self.floor_membership = membership;
            }
            self.floor_index = index;
        }
        self.log.compact_to(index);
        self.base_dirty = true;
        self.reload_membership();
    }

    /// A snapshot arrived from the leader and the driver has put it into the state machine, if it
    /// was newer than what the state machine held.
    fn on_snapshot(
        &mut self,
        from: NodeId,
        term: Term,
        round: u64,
        snapshot: SnapshotMeta,
        membership: Membership,
        out: &mut Vec<Output>,
    ) {
        let outcome = if term < self.term {
            self.refusal(snapshot.index())
        } else {
            self.become_follower(term, Some(from), out);
            self.reset_timer();
            if snapshot.index() > self.applied {
                if self.log.term_at(snapshot.index()) == Some(snapshot.term()) {
                    // The log already holds the entry the snapshot ends with, so it agrees with
                    // the leader up to there. Only the part the snapshot covers goes; what comes
                    // after stays, and stays as durable as it was.
                    self.log.compact_to(snapshot.index());
                } else {
                    // The log is too short or has gone another way. It starts over after the
                    // snapshot.
                    self.log.reset(snapshot);
                    let first = snapshot.index().next();
                    self.log_dirty_from =
                        Some(self.log_dirty_from.map_or(first, |from| from.min(first)));
                    // The new start is not on disk until the write below completes, and writes
                    // still in flight must not be counted for entries that are gone.
                    self.durable_index = self.durable_index.min(snapshot.index());
                    for (_, last) in &mut self.in_flight {
                        *last = (*last).min(snapshot.index());
                    }
                }
                self.base_dirty = true;
                self.commit = self.commit.max(snapshot.index());
                self.applied = snapshot.index();
                // The snapshot says who was in the cluster at its last entry. Entries the log
                // kept after it may say something newer.
                if !membership.voters().is_empty() {
                    self.floor_index = snapshot.index();
                    self.floor_membership = membership;
                }
                self.reload_membership();
            }
            // Everything up to the snapshot is committed, so it matches the leader's log.
            AppendOutcome::Accepted {
                match_index: snapshot.index(),
            }
        };
        let message = Message::AppendEntriesResponse {
            term: self.term,
            outcome,
            round,
        };
        out.push(Output::Send { to: from, message });
    }

    /// Moves the commit index to the highest entry durably stored on a quorum.
    fn advance_commit(&mut self) {
        let State::Leader { progress, .. } = &self.state else {
            return;
        };
        let voters = progress
            .iter()
            .filter(|follower| self.membership.is_voter(follower.peer));
        let mut stored: Vec<Index> = voters.map(|follower| follower.matched).collect();
        // The leader's own copy counts only once it is on disk, like everyone else's, and only
        // while the leader is a voter. A leader that has removed itself still runs the cluster
        // until that change commits, without counting towards the majority that commits it.
        if self.is_voter() {
            stored.push(self.durable_index);
        }
        stored.sort_unstable_by(|a, b| b.cmp(a));
        let Some(&candidate) = stored.get(self.quorum() - 1) else {
            return;
        };
        if candidate <= self.commit {
            return;
        }
        // Raft §5.4.2: an entry from an earlier term is not committed by counting replicas. It
        // could still be overwritten by a leader that never saw it. Committing an entry of the
        // current term commits everything before it.
        let from_this_term = self.log.term_at(candidate) == Some(self.term);
        #[cfg(test)]
        let from_this_term = from_this_term || self.bug == Some(Bug::CommitEarlierTerm);

        if from_this_term {
            self.commit = candidate;
        }
    }

    // ---- Membership. ----

    /// Starts a membership change, if this node leads and the change is allowed right now.
    fn change_membership(&mut self, change: Change, out: &mut Vec<Output>) {
        let State::Leader { progress, .. } = &self.state else {
            out.push(Output::NotLeader {
                leader: self.leader(),
            });
            return;
        };
        let caught_up = |peer: NodeId| {
            progress
                .iter()
                .any(|follower| follower.peer == peer && follower.matched >= self.commit)
        };
        let verdict = if self.config_index > self.commit && !self.has_bug_change_in_progress() {
            // One change at a time. Each single step is safe against the membership before it;
            // two unfinished steps are not safe against each other.
            Err(ChangeError::InProgress)
        } else if self.log.term_at(self.commit) != Some(self.term) {
            // A new leader may hold an uncommitted change from its predecessor without being
            // able to tell. An entry of its own term has to commit first.
            Err(ChangeError::NotReady)
        } else if matches!(change, Change::Promote(peer) if !caught_up(peer) && self.membership.learners().contains(&peer))
        {
            Err(ChangeError::NotCaughtUp)
        } else {
            self.membership.apply(change)
        };
        match verdict {
            Ok(membership) => {
                self.append(Entry::new(self.term, Payload::Config(membership)));
                self.broadcast_append(out);
            }
            Err(reason) => out.push(Output::ChangeRejected { change, reason }),
        }
    }

    #[cfg(test)]
    fn has_bug_change_in_progress(&self) -> bool {
        self.bug == Some(Bug::ChangeWhileInProgress)
    }

    #[cfg(not(test))]
    #[expect(clippy::unused_self, reason = "the test build reads a field here")]
    const fn has_bug_change_in_progress(&self) -> bool {
        false
    }

    /// A leader that is no longer a voter hands over once the change that removed it commits.
    /// Until then it has to stay: someone must replicate that very entry.
    fn step_down_if_removed(&mut self, out: &mut Vec<Output>) {
        let leading = matches!(self.state, State::Leader { .. });
        if leading && !self.is_voter() && self.config_index <= self.commit {
            self.become_follower(self.term, None, out);
        }
    }

    /// Emits committed entries, but never one this node has not stored durably: after a crash it
    /// could not find the entry it had already acted on.
    fn apply_committed(&mut self, out: &mut Vec<Output>) {
        let limit = self.commit.min(self.durable_index);
        while self.applied < limit {
            let index = self.applied.next();
            let Some(entry) = self.log.get(index) else {
                return;
            };
            out.push(Output::Apply {
                index,
                entry: entry.clone(),
            });
            self.applied = index;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::{Bug, Raft, Role};
    use crate::membership::{Change, ChangeError, Membership};
    use crate::types::{
        AppendOutcome, Entry, HardState, Index, Input, Message, NodeId, Output, Payload, ReadId,
        SnapshotMeta, Term,
    };
    use crate::{Config, ConfigError};

    #[test]
    fn a_single_node_commits_nothing_until_its_own_write_is_durable() {
        let mut node =
            Raft::new(Config::new(NodeId::new(1), [], 1)).expect("a valid configuration");
        let mut requests = Vec::new();
        for _ in 0..20 {
            for output in node.step(Input::Tick) {
                match output {
                    Output::Persist(request) => requests.push(request),
                    other => panic!("nothing but a write may leave the node yet, got {other:?}"),
                }
            }
        }
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(node.commit_index(), Index::ZERO);

        let seq = requests
            .last()
            .expect("winning the election changed the hard state")
            .seq();
        let outputs = node.step(Input::Persisted { seq });
        assert!(
            matches!(outputs.as_slice(), [Output::Apply { .. }]),
            "got {outputs:?}"
        );
        assert_eq!(node.commit_index(), Index::new(1));
    }

    /// With several nodes this rule hides behind another one: a leader does not send an entry
    /// before its own write is durable, so no follower can acknowledge it earlier. A single node
    /// has no such cover, and the rule has to hold by itself.
    #[test]
    fn a_leader_counts_its_own_log_only_up_to_what_is_durable() {
        let mut node =
            Raft::new(Config::new(NodeId::new(1), [], 1)).expect("a valid configuration");
        let mut writes = Vec::new();
        let mut feed = |node: &mut Raft, input: Input| {
            for output in node.step(input) {
                if let Output::Persist(request) = output {
                    writes.push(request.seq());
                }
            }
        };
        for _ in 0..20 {
            feed(&mut node, Input::Tick);
        }
        feed(&mut node, Input::Propose(b"a".to_vec()));
        feed(&mut node, Input::Propose(b"b".to_vec()));
        // Three writes are in flight: the election with its no-op, then one per command.
        let [election, first, _second] = writes.as_slice() else {
            panic!("expected three writes, got {writes:?}");
        };

        node.step(Input::Persisted { seq: *election });
        assert_eq!(node.commit_index(), Index::new(1));
        node.step(Input::Persisted { seq: *first });
        assert_eq!(node.commit_index(), Index::new(2));
    }

    #[test]
    fn a_vote_is_not_sent_before_it_is_durable() {
        let peers = [NodeId::new(2), NodeId::new(3)];
        let mut node =
            Raft::new(Config::new(NodeId::new(1), peers, 1)).expect("a valid configuration");
        let request = Message::RequestVote {
            term: Term::new(1),
            last_log_index: Index::ZERO,
            last_log_term: Term::new(0),
        };

        let outputs = node.step(Input::Message {
            from: NodeId::new(2),
            message: request,
        });
        let [Output::Persist(write)] = outputs.as_slice() else {
            panic!("only the write may leave the node, got {outputs:?}");
        };
        assert_eq!(write.hard_state().voted_for(), Some(NodeId::new(2)));

        let outputs = node.step(Input::Persisted { seq: write.seq() });
        let granted = Message::RequestVoteResponse {
            term: Term::new(1),
            granted: true,
        };
        assert_eq!(
            outputs,
            [Output::Send {
                to: NodeId::new(2),
                message: granted
            }]
        );
    }

    /// A cluster with an instant network and instant disks, for tests that are about one rule
    /// and need no faults. A node that is cut off neither sends nor receives.
    struct Cluster {
        nodes: Vec<Raft>,
        cut_off: Vec<bool>,
        /// Links that lose every message, as (from, to).
        broken: Vec<(usize, usize)>,
        ready: Vec<ReadId>,
        rejected: Vec<ReadId>,
        /// Snapshots a leader asked its driver to send, as (leader, follower).
        asked: Vec<(usize, NodeId)>,
        /// Membership changes a leader refused.
        refused: Vec<(Change, ChangeError)>,
        /// With a manual network, messages wait in the mailbox until the test delivers them.
        manual: bool,
        mailbox: Vec<(usize, usize, Message)>,
    }

    impl Cluster {
        /// A cluster whose messages go nowhere until the test delivers them, and whose leaders
        /// send one entry per message, so that a test can stop between two entries.
        fn manual(size: u64, bug: Option<Bug>) -> Self {
            let mut cluster = Self::with_batch(size, bug, 1);
            cluster.manual = true;
            cluster
        }

        fn new(size: u64, bug: Option<Bug>) -> Self {
            Self::with_batch(size, bug, 64)
        }

        fn with_batch(size: u64, bug: Option<Bug>, max_batch: usize) -> Self {
            let nodes: Vec<Raft> = (1..=size)
                .map(|id| {
                    let peers = (1..=size).filter(|&peer| peer != id).map(NodeId::new);
                    let config = Config::new(NodeId::new(id), peers, id).max_batch(max_batch);
                    let mut node = Raft::new(config).expect("a valid configuration");
                    if let Some(bug) = bug {
                        node.inject(bug);
                    }
                    node
                })
                .collect();
            let cut_off = vec![false; nodes.len()];
            Self {
                nodes,
                cut_off,
                broken: Vec::new(),
                ready: Vec::new(),
                rejected: Vec::new(),
                asked: Vec::new(),
                refused: Vec::new(),
                manual: false,
                mailbox: Vec::new(),
            }
        }

        /// Starts a node that is not a member of anything yet, and returns its position.
        fn add_joiner(&mut self) -> usize {
            let id = u64::try_from(self.nodes.len()).expect("few nodes") + 1;
            let node =
                Raft::new(Config::joining(NodeId::new(id), id)).expect("a valid configuration");
            self.nodes.push(node);
            self.cut_off.push(false);
            self.nodes.len() - 1
        }

        fn position(&self, id: NodeId) -> usize {
            self.nodes
                .iter()
                .position(|node| node.id() == id)
                .expect("a node of this cluster")
        }

        /// Feeds one input and everything that follows from it, until the cluster is still.
        fn feed(&mut self, node: usize, input: Input) {
            let mut queue = VecDeque::from([(node, input)]);
            while let Some((node, input)) = queue.pop_front() {
                let from = self.nodes[node].id();
                for output in self.nodes[node].step(input) {
                    match output {
                        Output::Persist(request) => {
                            queue.push_front((node, Input::Persisted { seq: request.seq() }));
                        }
                        Output::Send { to, message } => {
                            let to = self.position(to);
                            if self.manual {
                                self.mailbox.push((node, to, message));
                                continue;
                            }
                            let broken = self.broken.contains(&(node, to));
                            if !self.cut_off[node] && !self.cut_off[to] && !broken {
                                queue.push_back((to, Input::Message { from, message }));
                            }
                        }
                        Output::ReadReady { id, .. } => self.ready.push(id),
                        Output::ReadRejected { id, .. } => self.rejected.push(id),
                        Output::SendSnapshot { to, .. } => self.asked.push((node, to)),
                        Output::ChangeRejected { change, reason } => {
                            self.refused.push((change, reason));
                        }
                        Output::Apply { .. } | Output::NotLeader { .. } => {}
                    }
                }
            }
        }

        /// Ticks one node only.
        fn run_one(&mut self, node: usize, ticks: u32) {
            for _ in 0..ticks {
                self.feed(node, Input::Tick);
            }
        }

        /// Delivers up to `limit` of the waiting messages that `wanted` picks, and the messages
        /// those cause, in order. Whatever is left afterwards is lost.
        fn deliver(&mut self, limit: usize, wanted: impl Fn(usize, usize, &Message) -> bool) {
            let mut delivered = 0;
            while delivered < limit {
                let Some(position) = self
                    .mailbox
                    .iter()
                    .position(|(from, to, message)| wanted(*from, *to, message))
                else {
                    break;
                };
                let (from, to, message) = self.mailbox.remove(position);
                let from = self.nodes[from].id();
                self.feed(to, Input::Message { from, message });
                delivered += 1;
            }
            self.mailbox.clear();
        }

        fn run(&mut self, ticks: u32) {
            for _ in 0..ticks {
                for node in 0..self.nodes.len() {
                    self.feed(node, Input::Tick);
                }
            }
        }

        /// The leader with the highest term.
        fn leader(&self) -> usize {
            (0..self.nodes.len())
                .filter(|&node| self.nodes[node].role() == Role::Leader)
                .max_by_key(|&node| self.nodes[node].term())
                .expect("the cluster has a leader")
        }
    }

    #[test]
    fn a_node_that_was_cut_off_does_not_force_an_election_when_it_returns() {
        // The second run switches pre-vote off, to show that the first one passes because of it.
        for (bug, disturbed) in [(None, false), (Some(Bug::SkipPreVote), true)] {
            let mut cluster = Cluster::new(3, bug);
            cluster.run(60);
            let leader = cluster.leader();
            let term = cluster.nodes[leader].term();

            let outsider = (leader + 1) % 3;
            cluster.cut_off[outsider] = true;
            cluster.run(200);
            cluster.cut_off[outsider] = false;
            cluster.run(60);

            let term_now = cluster.nodes[cluster.leader()].term();
            assert_eq!(
                term_now != term,
                disturbed,
                "with {bug:?} the term went from {term:?} to {term_now:?}"
            );
            if !disturbed {
                assert_eq!(cluster.leader(), leader);
                assert_eq!(cluster.nodes[outsider].term(), term);
                assert_eq!(cluster.nodes[outsider].role(), Role::Follower);
            }
        }
    }

    /// The node hears everyone but the leader. It runs out of patience and asks for pre-votes,
    /// and the other follower, which hears the leader fine, has to say no. If it said yes the
    /// node would win an election the cluster did not need.
    #[test]
    fn a_node_that_cannot_hear_the_leader_does_not_depose_it() {
        let mut cluster = Cluster::new(3, None);
        cluster.run(60);
        let leader = cluster.leader();
        let term = cluster.nodes[leader].term();

        cluster.broken.push((leader, (leader + 1) % 3));
        cluster.run(200);

        assert_eq!(cluster.leader(), leader);
        assert_eq!(cluster.nodes[leader].term(), term);
    }

    #[test]
    fn a_leader_that_loses_its_quorum_steps_down() {
        let mut cluster = Cluster::new(3, None);
        cluster.run(60);
        let old = cluster.leader();
        let term = cluster.nodes[old].term();

        cluster.cut_off[old] = true;
        // Two election timeouts at most: one check may still see answers from before the cut.
        cluster.run(25);
        assert_ne!(cluster.nodes[old].role(), Role::Leader);

        cluster.run(60);
        let new = cluster.leader();
        assert_ne!(new, old);
        assert!(cluster.nodes[new].term() > term);
    }

    #[test]
    fn a_read_is_answered_only_once_a_quorum_confirms_the_leader() {
        let mut cluster = Cluster::new(3, None);
        cluster.run(60);
        let leader = cluster.leader();

        cluster.feed(leader, Input::Read { id: ReadId::new(1) });
        assert_eq!(cluster.ready, [ReadId::new(1)]);

        // A follower does not serve reads.
        cluster.feed((leader + 1) % 3, Input::Read { id: ReadId::new(2) });
        assert_eq!(cluster.rejected, [ReadId::new(2)]);

        // Cut off, the leader cannot confirm anything. The read waits, and is refused when the
        // leader gives up.
        cluster.cut_off[leader] = true;
        cluster.feed(leader, Input::Read { id: ReadId::new(3) });
        assert_eq!(cluster.ready, [ReadId::new(1)]);
        cluster.run(25);
        assert_eq!(cluster.ready, [ReadId::new(1)]);
        assert_eq!(cluster.rejected, [ReadId::new(2), ReadId::new(3)]);
    }

    /// What the rule above prevents: a leader that is cut off answers from state that the rest
    /// of the cluster may already have moved past.
    #[test]
    fn without_the_quorum_round_a_cut_off_leader_answers_reads() {
        let mut cluster = Cluster::new(3, Some(Bug::ReadWithoutQuorum));
        cluster.run(60);
        let leader = cluster.leader();
        cluster.cut_off[leader] = true;
        cluster.feed(leader, Input::Read { id: ReadId::new(1) });
        assert_eq!(cluster.ready, [ReadId::new(1)]);
    }

    /// A new leader holds every committed entry, but its commit index can be behind: the old
    /// leader may have committed entries without having told it yet. Until an entry of its own
    /// term commits, it does not know where the log really ends, and a read answered from its
    /// commit index could miss a write that was already acknowledged.
    #[test]
    fn a_new_leader_refuses_reads_until_it_has_committed_in_its_own_term() {
        let peer = NodeId::new(2);
        let config = Config::new(NodeId::new(1), [peer, NodeId::new(3)], 1);
        let mut node = Raft::new(config).expect("a valid configuration");
        let mut outputs = Vec::new();
        // Feeds an input, and reports every write as durable at once.
        let mut feed = |node: &mut Raft, input: Input| {
            let mut inputs = VecDeque::from([input]);
            while let Some(input) = inputs.pop_front() {
                for output in node.step(input) {
                    if let Output::Persist(request) = &output {
                        inputs.push_back(Input::Persisted { seq: request.seq() });
                    }
                    outputs.push(output);
                }
            }
        };

        while node.role() == Role::Follower {
            feed(&mut node, Input::Tick);
        }
        let granted = Message::PreVoteResponse {
            term: Term::new(1),
            granted: true,
        };
        feed(
            &mut node,
            Input::Message {
                from: peer,
                message: granted,
            },
        );
        let granted = Message::RequestVoteResponse {
            term: Term::new(1),
            granted: true,
        };
        feed(
            &mut node,
            Input::Message {
                from: peer,
                message: granted,
            },
        );
        assert_eq!(node.role(), Role::Leader);
        // The no-op is in the log, and nobody has acknowledged it.
        assert_eq!(node.commit_index(), Index::ZERO);

        feed(&mut node, Input::Read { id: ReadId::new(1) });
        let answers: Vec<&Output> = outputs
            .iter()
            .filter(|output| {
                matches!(
                    output,
                    Output::ReadReady { .. } | Output::ReadRejected { .. }
                )
            })
            .collect();
        let refused = Output::ReadRejected {
            id: ReadId::new(1),
            leader: Some(NodeId::new(1)),
        };
        assert_eq!(answers, [&refused]);
    }

    fn point(index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta::new(Index::new(index), Term::new(term))
    }

    fn noops(term: u64, count: usize) -> Vec<Entry> {
        vec![Entry::new(Term::new(term), Payload::Noop); count]
    }

    /// A follower in a cluster of three, restarted from the given storage.
    fn follower(base: SnapshotMeta, entries: Vec<Entry>, applied: SnapshotMeta) -> Raft {
        let config = Config::new(NodeId::new(1), [NodeId::new(2), NodeId::new(3)], 1);
        let hard_state = HardState::new(Term::new(1), None);
        Raft::recover(config, hard_state, base, entries, applied, None).expect("consistent storage")
    }

    /// Feeds an input and reports every write as durable at once. Returns everything the node
    /// asked for.
    fn settle(node: &mut Raft, input: Input) -> Vec<Output> {
        let mut outputs = Vec::new();
        let mut inputs = VecDeque::from([input]);
        while let Some(input) = inputs.pop_front() {
            for output in node.step(input) {
                if let Output::Persist(request) = &output {
                    inputs.push_back(Input::Persisted { seq: request.seq() });
                }
                outputs.push(output);
            }
        }
        outputs
    }

    fn append(prev: u64, entries: Vec<Entry>, leader_commit: u64) -> Input {
        let message = Message::AppendEntries {
            term: Term::new(1),
            prev_log_index: Index::new(prev),
            prev_log_term: Term::new(1),
            entries,
            leader_commit: Index::new(leader_commit),
            round: 0,
        };
        Input::Message {
            from: NodeId::new(2),
            message,
        }
    }

    /// The outcome in the one answer the node sent.
    fn answer(outputs: &[Output]) -> AppendOutcome {
        let outcomes: Vec<AppendOutcome> = outputs
            .iter()
            .filter_map(|output| match output {
                Output::Send {
                    message: Message::AppendEntriesResponse { outcome, .. },
                    ..
                } => Some(*outcome),
                _ => None,
            })
            .collect();
        let [outcome] = outcomes.as_slice() else {
            panic!("expected one answer, got {outputs:?}");
        };
        *outcome
    }

    #[test]
    fn a_follower_that_fell_behind_the_compacted_log_is_sent_a_snapshot() {
        let mut cluster = Cluster::new(3, None);
        cluster.run(60);
        let leader = cluster.leader();
        let behind = (leader + 1) % 3;

        cluster.cut_off[behind] = true;
        for command in 0..5_u8 {
            cluster.feed(leader, Input::Propose(vec![command]));
        }
        let committed = cluster.nodes[leader].commit_index();
        assert_eq!(committed, Index::new(6), "the no-op and five commands");
        cluster.feed(leader, Input::Compact { index: committed });
        assert_eq!(cluster.nodes[leader].log_base().index(), committed);
        assert_eq!(cluster.nodes[leader].log(), []);

        // Back in touch, the follower needs entries the leader no longer has.
        cluster.cut_off[behind] = false;
        cluster.run(3);
        let target = cluster.nodes[behind].id();
        assert_eq!(
            cluster.asked,
            [(leader, target)],
            "one snapshot, asked for once"
        );
        assert_eq!(cluster.nodes[behind].last_log_index(), Index::new(1));

        // The driver's part: the snapshot reaches the follower's state machine, and the node is
        // told where it now stands.
        let snapshot = cluster.nodes[leader].log_base();
        let term = cluster.nodes[leader].term();
        let from = cluster.nodes[leader].id();
        cluster.feed(
            behind,
            Input::Snapshot {
                from,
                term,
                round: 0,
                snapshot,
                membership: Membership::default(),
            },
        );
        assert_eq!(cluster.nodes[behind].log_base(), snapshot);
        assert_eq!(cluster.nodes[behind].commit_index(), committed);

        // From there it follows the log again.
        cluster.feed(leader, Input::Propose(b"after".to_vec()));
        cluster.run(3);
        assert_eq!(cluster.nodes[behind].last_log_index(), Index::new(7));
        assert_eq!(cluster.nodes[behind].commit_index(), Index::new(7));
        assert_eq!(cluster.asked.len(), 1);
    }

    #[test]
    fn the_log_is_never_compacted_past_what_was_applied() {
        let mut node =
            Raft::new(Config::new(NodeId::new(1), [], 1)).expect("a valid configuration");
        for _ in 0..20 {
            settle(&mut node, Input::Tick);
        }
        assert_eq!(node.commit_index(), Index::new(1));
        // Proposed, and its write still in flight: in the log, not applied.
        node.step(Input::Propose(b"pending".to_vec()));
        assert_eq!(node.last_log_index(), Index::new(2));

        node.step(Input::Compact {
            index: Index::new(2),
        });
        assert_eq!(node.log_base().index(), Index::new(1));
        assert_eq!(node.log().len(), 1);
    }

    #[test]
    fn a_restart_with_the_state_machine_ahead_of_the_log_starts_the_log_over() {
        // A snapshot up to index 5 was installed, and the power went before the log caught up.
        let mut node = follower(SnapshotMeta::default(), noops(1, 2), point(5, 1));
        assert_eq!(node.log_base(), point(5, 1));
        assert_eq!(node.log(), []);
        assert_eq!(node.commit_index(), Index::new(5));

        // The first thing the node does is put the new start of its log on disk.
        let outputs = node.step(Input::Tick);
        let [Output::Persist(write)] = outputs.as_slice() else {
            panic!("expected one write, got {outputs:?}");
        };
        assert_eq!(write.base(), Some(point(5, 1)));
        let log = write.log().expect("the old entries are replaced");
        assert_eq!((log.first_index(), log.entries().len()), (Index::new(6), 0));
    }

    #[test]
    fn a_restart_with_the_log_ahead_of_the_state_machine_is_refused() {
        let config = Config::new(NodeId::new(1), [], 1);
        let result = Raft::recover(
            config,
            HardState::default(),
            point(5, 1),
            Vec::new(),
            point(3, 1),
            None,
        );
        assert_eq!(result.err(), Some(ConfigError::StateBehindLog));
    }

    #[test]
    fn a_late_append_that_starts_inside_the_snapshot_is_matched_from_the_base() {
        let mut node = follower(point(5, 1), Vec::new(), point(5, 1));

        // Entirely inside the snapshot: nothing to store, and it all matches.
        let outputs = settle(&mut node, append(2, noops(1, 3), 5));
        assert_eq!(
            answer(&outputs),
            AppendOutcome::Accepted {
                match_index: Index::new(5)
            }
        );
        assert_eq!(node.last_log_index(), Index::new(5));

        // Straddling the base: the part after it is stored.
        let outputs = settle(&mut node, append(3, noops(1, 4), 5));
        assert_eq!(
            answer(&outputs),
            AppendOutcome::Accepted {
                match_index: Index::new(7)
            }
        );
        assert_eq!(node.log().len(), 2);
    }

    #[test]
    fn a_snapshot_that_is_not_ahead_changes_nothing_and_is_still_acknowledged() {
        let mut node = follower(SnapshotMeta::default(), noops(1, 6), point(6, 1));
        let input = Input::Snapshot {
            from: NodeId::new(2),
            term: Term::new(1),
            round: 4,
            snapshot: point(4, 1),
            membership: Membership::default(),
        };
        let outputs = settle(&mut node, input);

        assert_eq!(
            answer(&outputs),
            AppendOutcome::Accepted {
                match_index: Index::new(4)
            }
        );
        assert_eq!(node.log_base(), SnapshotMeta::default());
        assert_eq!(node.log().len(), 6);
    }

    fn is_vote(message: &Message) -> bool {
        matches!(
            message,
            Message::PreVote { .. }
                | Message::PreVoteResponse { .. }
                | Message::RequestVote { .. }
                | Message::RequestVoteResponse { .. }
        )
    }

    /// Runs an election by hand in a cluster with a manual network: `candidate` can reach only
    /// `voters`, and only election messages get through. Everything else is lost, including the
    /// first entries of the new leader.
    fn elect(cluster: &mut Cluster, candidate: usize, voters: &[usize]) {
        // The voters have to be out of patience with whoever they followed, or they refuse.
        for &voter in voters {
            for _ in 0..10 {
                cluster.feed(voter, Input::Tick);
            }
        }
        cluster.mailbox.clear();
        let reachable = |from: usize, to: usize, message: &Message| {
            let pair = (from == candidate && voters.contains(&to))
                || (to == candidate && voters.contains(&from));
            pair && is_vote(message)
        };
        // A node that still believes it leads an old term first has to notice that it does not,
        // so being leader is not enough: it has to be leader of a new term.
        let before = cluster.nodes[candidate].term();
        let won = |cluster: &Cluster| {
            let node = &cluster.nodes[candidate];
            node.role() == Role::Leader && node.term() > before
        };
        for _ in 0..300 {
            if won(cluster) && cluster.mailbox.is_empty() {
                break;
            }
            cluster.feed(candidate, Input::Tick);
            cluster.deliver(usize::MAX, reachable);
        }
        assert!(won(cluster), "the election did not finish");
    }

    /// Figure 8 of the Raft paper, step by step, on five nodes A to E.
    ///
    /// An entry from an old term reaches a majority while its leader is in a newer term. Counting
    /// replicas says it is committed. It is not: a node that never saw it can still be elected
    /// and overwrite it. Raft therefore commits only entries of the leader's own term by
    /// counting, and with them everything before.
    ///
    /// The second run removes that rule and shows the entry being acknowledged and then lost.
    #[test]
    fn an_entry_of_an_earlier_term_is_not_committed_by_counting_replicas() {
        const A: usize = 0;
        const B: usize = 1;
        const C: usize = 2;
        const D: usize = 3;
        const E: usize = 4;
        let only = |x: usize, y: usize| {
            move |from: usize, to: usize, _: &Message| (from, to) == (x, y) || (from, to) == (y, x)
        };

        for (bug, lost) in [(None, false), (Some(Bug::CommitEarlierTerm), true)] {
            let mut cluster = Cluster::manual(5, bug);

            // (a) A leads and gets its entry onto B, no further.
            elect(&mut cluster, A, &[B, C]);
            let first_term = cluster.nodes[A].term();
            cluster.run_one(A, 2);
            cluster.deliver(usize::MAX, only(A, B));
            assert_eq!(cluster.nodes[B].last_log_index(), Index::new(1));
            assert_eq!(cluster.nodes[A].commit_index(), Index::ZERO);

            // (b) A is cut off. E wins with C and D, and its own entry at index 1 stays on E.
            elect(&mut cluster, E, &[C, D]);
            assert!(cluster.nodes[E].term() > first_term);

            // (c) E is cut off. A wins again with B and C, and brings C up to date one entry at a
            // time. C takes A's old entry, and the exchange stops before the entry of A's new
            // term reaches it: a refusal, the old entry, its acknowledgement.
            elect(&mut cluster, A, &[B, C]);
            cluster.run_one(A, 2);
            cluster.deliver(4, only(A, C));
            assert_eq!(cluster.nodes[C].last_log_index(), Index::new(1));
            assert_eq!(cluster.nodes[C].log()[0].term(), first_term);
            // B acknowledges the new entry. Now A, B and C hold the old entry: a majority. Only A
            // and B hold an entry of A's current term: not a majority.
            cluster.run_one(A, 2);
            cluster.deliver(usize::MAX, only(A, B));
            assert_eq!(cluster.nodes[B].last_log_index(), Index::new(2));

            let acknowledged = cluster.nodes[A].commit_index() == Index::new(1);
            assert_eq!(
                acknowledged,
                lost,
                "with {bug:?}, A's commit index is {:?}",
                cluster.nodes[A].commit_index()
            );

            // (d) A is cut off. E can win with C and D: its log ends in a newer term than theirs.
            // As leader it replaces C's copy of the old entry with its own.
            elect(&mut cluster, E, &[C, D]);
            cluster.run_one(E, 2);
            cluster.deliver(usize::MAX, only(E, C));
            assert_ne!(
                cluster.nodes[C].log()[0].term(),
                first_term,
                "E overwrote the old entry on C"
            );
            // Without the rule, an entry A reported as committed is now gone from a majority,
            // and the leader does not have it. With the rule nothing was promised.
        }
    }

    #[test]
    fn a_restart_with_a_log_that_went_another_way_starts_the_log_over() {
        // The state machine holds a snapshot up to index 4, whose last entry was of term 2. The
        // log on disk is longer than that, and holds an entry of term 1 at index 4: it is the log
        // from before the snapshot, and the write that would have replaced it was lost.
        let node = follower(SnapshotMeta::default(), noops(1, 6), point(4, 2));
        assert_eq!(node.log_base(), point(4, 2));
        assert_eq!(node.log(), []);
    }

    #[test]
    fn a_snapshot_that_ends_inside_a_matching_log_keeps_the_entries_after_it() {
        let mut node = follower(SnapshotMeta::default(), noops(1, 6), point(2, 1));
        let input = Input::Snapshot {
            from: NodeId::new(2),
            term: Term::new(1),
            round: 0,
            snapshot: point(4, 1),
            membership: Membership::default(),
        };
        let outputs = settle(&mut node, input);

        assert_eq!(
            answer(&outputs),
            AppendOutcome::Accepted {
                match_index: Index::new(4)
            }
        );
        assert_eq!(node.log_base(), point(4, 1));
        assert_eq!(node.log().len(), 2);
        assert_eq!(node.applied_index(), Index::new(4));
    }

    #[test]
    fn a_snapshot_the_log_disagrees_with_replaces_the_whole_log() {
        let mut node = follower(SnapshotMeta::default(), noops(1, 6), point(2, 1));
        // Index 4 in the snapshot is of term 3; the log has term 1 there.
        let input = Input::Snapshot {
            from: NodeId::new(2),
            term: Term::new(3),
            round: 0,
            snapshot: point(4, 3),
            membership: Membership::default(),
        };
        settle(&mut node, input);

        assert_eq!(node.log_base(), point(4, 3));
        assert_eq!(node.log(), []);
        assert_eq!(node.commit_index(), Index::new(4));
    }

    fn voters(cluster: &Cluster, node: usize) -> Vec<u64> {
        cluster.nodes[node]
            .membership()
            .voters()
            .iter()
            .map(|id| id.get())
            .collect()
    }

    fn learners(cluster: &Cluster, node: usize) -> Vec<u64> {
        cluster.nodes[node]
            .membership()
            .learners()
            .iter()
            .map(|id| id.get())
            .collect()
    }

    #[test]
    fn a_node_that_has_not_been_added_never_starts_an_election() {
        let mut cluster = Cluster::new(3, None);
        let outsider = cluster.add_joiner();
        cluster.run(300);
        assert_eq!(cluster.nodes[outsider].role(), Role::Follower);
        assert_eq!(cluster.nodes[outsider].term(), Term::new(0));
        assert_eq!(cluster.nodes[outsider].last_log_index(), Index::ZERO);
    }

    #[test]
    fn a_learner_gets_the_log_and_does_not_count_towards_the_majority() {
        let mut cluster = Cluster::new(3, None);
        cluster.run(60);
        let leader = cluster.leader();
        let learner = cluster.add_joiner();
        let id = cluster.nodes[learner].id();

        cluster.feed(leader, Input::ChangeMembership(Change::AddLearner(id)));
        cluster.run(4);
        assert_eq!(learners(&cluster, leader), [4]);
        // The learner has caught up, and learned the membership from the log.
        assert_eq!(
            cluster.nodes[learner].last_log_index(),
            cluster.nodes[leader].last_log_index()
        );
        assert_eq!(
            cluster.nodes[learner].membership(),
            cluster.nodes[leader].membership()
        );

        // With both voters gone, the leader and the learner are two of four, and no majority of
        // three. Nothing commits.
        for node in 0..3 {
            cluster.cut_off[node] = node != leader;
        }
        let committed = cluster.nodes[leader].commit_index();
        cluster.feed(leader, Input::Propose(b"stuck".to_vec()));
        cluster.run(3);
        assert_eq!(
            cluster.nodes[learner].last_log_index(),
            cluster.nodes[leader].last_log_index()
        );
        assert_eq!(cluster.nodes[leader].commit_index(), committed);
    }

    #[test]
    fn a_learner_is_promoted_only_once_it_has_caught_up() {
        let mut cluster = Cluster::new(3, None);
        cluster.run(60);
        let leader = cluster.leader();
        let learner = cluster.add_joiner();
        let id = cluster.nodes[learner].id();

        // Added while unreachable: it is a learner, and it has nothing.
        cluster.cut_off[learner] = true;
        cluster.feed(leader, Input::ChangeMembership(Change::AddLearner(id)));
        cluster.feed(leader, Input::ChangeMembership(Change::Promote(id)));
        assert_eq!(
            cluster.refused,
            [(Change::Promote(id), ChangeError::NotCaughtUp)]
        );

        cluster.cut_off[learner] = false;
        cluster.run(4);
        cluster.feed(leader, Input::ChangeMembership(Change::Promote(id)));
        cluster.run(2);
        assert_eq!(voters(&cluster, leader), [1, 2, 3, 4]);
        assert_eq!(voters(&cluster, learner), [1, 2, 3, 4]);
        assert_eq!(cluster.refused.len(), 1);
    }

    #[test]
    fn a_second_change_waits_until_the_first_has_committed() {
        let mut cluster = Cluster::new(3, None);
        cluster.run(60);
        let leader = cluster.leader();
        let first = cluster.add_joiner();
        let second = cluster.add_joiner();
        let (first, second) = (cluster.nodes[first].id(), cluster.nodes[second].id());

        // Nobody can acknowledge, so the first change stays uncommitted.
        for node in 0..3 {
            cluster.cut_off[node] = node != leader;
        }
        cluster.feed(leader, Input::ChangeMembership(Change::AddLearner(first)));
        assert_eq!(
            learners(&cluster, leader),
            [first.get()],
            "in force as soon as it is in the log"
        );
        cluster.feed(leader, Input::ChangeMembership(Change::AddLearner(second)));
        assert_eq!(
            cluster.refused,
            [(Change::AddLearner(second), ChangeError::InProgress)]
        );

        cluster.cut_off = vec![false; cluster.nodes.len()];
        cluster.run(3);
        cluster.feed(leader, Input::ChangeMembership(Change::AddLearner(second)));
        assert_eq!(learners(&cluster, leader), [first.get(), second.get()]);
    }

    #[test]
    fn a_leader_that_removes_itself_hands_over_once_the_change_commits() {
        let mut cluster = Cluster::new(3, None);
        cluster.run(60);
        let old = cluster.leader();
        let id = cluster.nodes[old].id();
        let remaining: Vec<u64> = (1..=3).filter(|&voter| voter != id.get()).collect();

        cluster.feed(old, Input::ChangeMembership(Change::Remove(id)));
        // The instant network has committed the change already, with the votes of the other two.
        assert_ne!(cluster.nodes[old].role(), Role::Leader);
        assert_eq!(voters(&cluster, old), remaining);

        cluster.run(60);
        let new = cluster.leader();
        assert_ne!(new, old);
        assert_eq!(voters(&cluster, new), remaining);
        // The removed node hears nothing any more, and does not try to come back.
        let term = cluster.nodes[new].term();
        cluster.run(200);
        assert_eq!(cluster.leader(), new);
        assert_eq!(cluster.nodes[new].term(), term);
    }

    #[test]
    fn a_membership_entry_that_is_overwritten_takes_its_membership_with_it() {
        let mut node = follower(SnapshotMeta::default(), Vec::new(), SnapshotMeta::default());
        let founding = node.membership().clone();
        let grown = founding
            .apply(Change::AddLearner(NodeId::new(9)))
            .expect("a new server");

        // The leader of term 1 adds a learner. The entry is in force here before it commits.
        let change = Entry::new(Term::new(1), Payload::Config(grown.clone()));
        let message = Message::AppendEntries {
            term: Term::new(1),
            prev_log_index: Index::ZERO,
            prev_log_term: Term::new(0),
            entries: vec![change],
            leader_commit: Index::ZERO,
            round: 0,
        };
        settle(
            &mut node,
            Input::Message {
                from: NodeId::new(2),
                message,
            },
        );
        assert_eq!(node.membership(), &grown);

        // It never commits. A leader of term 2 that did not have it overwrites it.
        let message = Message::AppendEntries {
            term: Term::new(2),
            prev_log_index: Index::ZERO,
            prev_log_term: Term::new(0),
            entries: noops(2, 1),
            leader_commit: Index::ZERO,
            round: 0,
        };
        settle(
            &mut node,
            Input::Message {
                from: NodeId::new(3),
                message,
            },
        );
        assert_eq!(node.membership(), &founding);
    }

    #[test]
    fn the_membership_outlives_the_entry_that_set_it() {
        let mut cluster = Cluster::new(3, None);
        cluster.run(60);
        let leader = cluster.leader();
        let learner = cluster.add_joiner();
        let id = cluster.nodes[learner].id();
        cluster.feed(leader, Input::ChangeMembership(Change::AddLearner(id)));
        cluster.run(4);
        let membership = cluster.nodes[leader].membership().clone();

        // Compaction drops the entry. The membership it set stays in force.
        let committed = cluster.nodes[leader].commit_index();
        cluster.feed(leader, Input::Compact { index: committed });
        assert_eq!(cluster.nodes[leader].log(), []);
        assert_eq!(cluster.nodes[leader].membership(), &membership);

        // After a restart the log cannot say who is in the cluster. The state machine can, and
        // without it the node would fall back to the members it was founded with.
        let config = Config::new(NodeId::new(1), [NodeId::new(2), NodeId::new(3)], 1);
        let base = cluster.nodes[leader].log_base();
        let restart = |known: Option<Membership>| {
            Raft::recover(
                config.clone(),
                HardState::default(),
                base,
                Vec::new(),
                base,
                known,
            )
            .expect("consistent storage")
        };
        assert_eq!(restart(Some(membership.clone())).membership(), &membership);
        assert_eq!(restart(None).membership(), &config.founding_membership());
    }

    /// Votes are counted against this node's own membership. An answer from anyone else, however
    /// willing, does not bring an election closer.
    #[test]
    fn only_voters_of_its_own_membership_can_elect_a_node() {
        let mut node = follower(SnapshotMeta::default(), Vec::new(), SnapshotMeta::default());
        let (member, stranger) = (NodeId::new(2), NodeId::new(9));
        while node.role() == Role::Follower {
            settle(&mut node, Input::Tick);
        }
        assert_eq!(node.role(), Role::PreCandidate);
        let asked = node.term().get() + 1;

        let pre_vote = Message::PreVoteResponse {
            term: Term::new(asked),
            granted: true,
        };
        settle(
            &mut node,
            Input::Message {
                from: stranger,
                message: pre_vote.clone(),
            },
        );
        assert_eq!(
            node.role(),
            Role::PreCandidate,
            "a stranger's pre-vote does not count"
        );
        settle(
            &mut node,
            Input::Message {
                from: member,
                message: pre_vote,
            },
        );
        assert_eq!(node.role(), Role::Candidate);

        let vote = Message::RequestVoteResponse {
            term: node.term(),
            granted: true,
        };
        settle(
            &mut node,
            Input::Message {
                from: stranger,
                message: vote.clone(),
            },
        );
        assert_eq!(
            node.role(),
            Role::Candidate,
            "a stranger's vote does not count"
        );
        settle(
            &mut node,
            Input::Message {
                from: member,
                message: vote,
            },
        );
        assert_eq!(node.role(), Role::Leader);
    }
}
