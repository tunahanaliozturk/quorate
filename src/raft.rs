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

use std::collections::VecDeque;

use crate::config::{Config, ConfigError};
use crate::log::Log;
use crate::rng::Rng;
use crate::types::{
    AppendOutcome, Entry, HardState, Index, Input, LogWrite, Message, NodeId, Output, Payload,
    Persist, PersistSeq, Term,
};

/// What a node currently is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Role {
    /// Accepts entries from a leader.
    Follower,
    /// Asking for votes.
    Candidate,
    /// Replicating its log to the others.
    Leader,
}

/// A rule the simulator tests switch off, to prove they would notice it missing.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Bug {
    /// Commit an entry from an earlier term by counting replicas (Raft paper, figure 8).
    CommitEarlierTerm,
    /// Grant a vote without checking that the candidate's log is up to date.
    VoteWithoutLogCheck,
    /// Send messages without waiting for the state they depend on to be durable.
    SendBeforeDurable,
}

/// What the leader knows about one follower.
#[derive(Debug)]
struct Progress {
    peer: NodeId,
    /// The next entry to send.
    next: Index,
    /// The last entry known to be stored on the follower.
    matched: Index,
}

#[derive(Debug)]
enum State {
    Follower {
        leader: Option<NodeId>,
    },
    Candidate {
        votes: Vec<NodeId>,
    },
    Leader {
        progress: Vec<Progress>,
        since_heartbeat: u32,
    },
}

/// One Raft node.
#[derive(Debug)]
pub struct Raft {
    id: NodeId,
    peers: Vec<NodeId>,
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
        Self::recover(config, HardState::default(), Vec::new())
    }

    /// Restarts a node from what its storage held. It comes back as a follower.
    ///
    /// `log` must be exactly the durable log, with the entry for index 1 first. The commit index
    /// is not stored; the node learns it again from the leader, and emits [`Output::Apply`] from
    /// index 1 again.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] for the same reasons as [`Raft::new`].
    pub fn recover(
        config: Config,
        hard_state: HardState,
        log: Vec<Entry>,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        let log = Log::from_entries(log);
        let mut node = Self {
            id: config.id,
            peers: config.peers,
            election_ticks: config.election_ticks,
            heartbeat_ticks: config.heartbeat_ticks,
            max_batch: config.max_batch,
            rng: Rng::new(config.seed),
            term: hard_state.term(),
            voted_for: hard_state.voted_for(),
            durable_index: log.last_index(),
            log,
            commit: Index::ZERO,
            applied: Index::ZERO,
            state: State::Follower { leader: None },
            elapsed: 0,
            timeout: 0,
            hard_state_dirty: false,
            log_dirty_from: None,
            persist_seq: 0,
            durable_seq: 0,
            in_flight: VecDeque::new(),
            held: VecDeque::new(),
            #[cfg(test)]
            bug: None,
        };
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
            Input::Persisted { seq } => self.on_persisted(seq, &mut out),
        }
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
            State::Candidate { .. } => Role::Candidate,
            State::Leader { .. } => Role::Leader,
        }
    }

    /// The leader of the current term, as far as this node knows.
    #[must_use]
    pub const fn leader(&self) -> Option<NodeId> {
        match self.state {
            State::Follower { leader } => leader,
            State::Candidate { .. } => None,
            State::Leader { .. } => Some(self.id),
        }
    }

    /// The highest index known to be committed.
    #[must_use]
    pub const fn commit_index(&self) -> Index {
        self.commit
    }

    /// The whole in-memory log. The entry at index `i` is at position `i - 1`. The tail may not
    /// be durable yet.
    #[must_use]
    pub fn log(&self) -> &[Entry] {
        self.log.entries()
    }

    #[cfg(test)]
    pub(crate) const fn inject(&mut self, bug: Bug) {
        self.bug = Some(bug);
    }

    /// Votes needed to win an election or commit an entry.
    fn quorum(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
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
        self.log.append(entry);
        let index = self.log.last_index();
        self.log_dirty_from = Some(self.log_dirty_from.map_or(index, |from| from.min(index)));
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
    }

    /// The persist request for what this step changed, if it changed anything.
    fn take_persist(&mut self) -> Option<Persist> {
        let dirty_from = self.log_dirty_from.take();
        if !self.hard_state_dirty && dirty_from.is_none() {
            return None;
        }
        self.hard_state_dirty = false;
        self.persist_seq += 1;
        self.in_flight
            .push_back((self.persist_seq, self.log.last_index()));
        let log =
            dirty_from.map(|first| LogWrite::new(first, self.log.slice_from(first, usize::MAX)));
        Some(Persist::new(
            PersistSeq::new(self.persist_seq),
            HardState::new(self.term, self.voted_for),
            log,
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
        if let State::Leader {
            since_heartbeat, ..
        } = &mut self.state
        {
            *since_heartbeat += 1;
            if *since_heartbeat >= self.heartbeat_ticks {
                *since_heartbeat = 0;
                self.broadcast_append(out);
            }
            return;
        }
        self.elapsed = self.elapsed.saturating_add(1);
        if self.elapsed >= self.timeout {
            self.campaign(out);
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

    // ---- Elections. ----

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
        for &peer in &self.peers {
            out.push(Output::Send {
                to: peer,
                message: message.clone(),
            });
        }
    }

    fn become_leader(&mut self, out: &mut Vec<Output>) {
        let next = self.log.last_index().next();
        let progress = self
            .peers
            .iter()
            .map(|&peer| Progress {
                peer,
                next,
                matched: Index::ZERO,
            })
            .collect();
        self.state = State::Leader {
            progress,
            since_heartbeat: 0,
        };
        // A leader may only commit by counting replicas of an entry from its own term. Without
        // this entry, earlier entries would wait for the next client command.
        self.append(Entry::new(self.term, Payload::Noop));
        self.broadcast_append(out);
    }

    /// Steps down, adopting `term` if it is newer.
    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.term {
            self.set_term(term);
        }
        let was_follower = matches!(self.state, State::Follower { .. });
        self.state = State::Follower { leader };
        if !was_follower {
            // A leader has no running election timer, and a candidate's was for its own election.
            self.reset_timer();
        }
    }

    fn receive(&mut self, from: NodeId, message: Message, out: &mut Vec<Output>) {
        if message.term() > self.term {
            self.become_follower(message.term(), None);
        }
        match message {
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
            } => {
                let outcome = self.on_append(
                    from,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                );
                let message = Message::AppendEntriesResponse {
                    term: self.term,
                    outcome,
                };
                out.push(Output::Send { to: from, message });
            }
            Message::AppendEntriesResponse { term, outcome } => {
                self.on_append_response(from, term, outcome, out);
            }
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
        // Raft §5.4.1: only vote for a candidate whose log is at least as complete as ours. This
        // is what guarantees that a new leader holds every committed entry.
        let up_to_date =
            (last_log_term, last_log_index) >= (self.log.last_term(), self.log.last_index());
        #[cfg(test)]
        let up_to_date = up_to_date || self.bug == Some(Bug::VoteWithoutLogCheck);

        let free_to_vote = self.voted_for.is_none_or(|candidate| candidate == from);
        let granted = term == self.term && free_to_vote && up_to_date;
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
        if term != self.term || !granted {
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

    fn on_append(
        &mut self,
        from: NodeId,
        term: Term,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    ) -> AppendOutcome {
        let rejected = |log: &Log| AppendOutcome::Rejected {
            prev_log_index,
            retry_from: if prev_log_index > log.last_index() {
                log.last_index().next()
            } else {
                log.first_index_of_term(prev_log_index)
            },
        };
        if term < self.term {
            // From a deposed leader. The term in our answer makes it step down.
            return rejected(&self.log);
        }
        self.become_follower(term, Some(from));
        self.reset_timer();
        if self.log.term_at(prev_log_index) != Some(prev_log_term) {
            return rejected(&self.log);
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
        let send_more = match outcome {
            AppendOutcome::Accepted { match_index } => {
                if match_index <= follower.matched {
                    return; // A late or duplicated answer; nothing new.
                }
                follower.matched = match_index;
                follower.next = follower.next.max(match_index.next());
                follower.next <= last
            }
            AppendOutcome::Rejected {
                prev_log_index,
                retry_from,
            } => {
                if follower.next.prev() != prev_log_index {
                    return; // An answer to a probe we have already moved on from.
                }
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
        let next = follower.next;
        self.advance_commit();
        if send_more {
            out.push(Output::Send {
                to: from,
                message: self.append_from(next),
            });
        }
    }

    /// The `AppendEntries` message for a follower whose next entry is `next`.
    fn append_from(&self, next: Index) -> Message {
        let prev_log_index = next.prev();
        Message::AppendEntries {
            term: self.term,
            prev_log_index,
            prev_log_term: self.log.term_at(prev_log_index).unwrap_or_default(),
            entries: self.log.slice_from(next, self.max_batch),
            leader_commit: self.commit,
        }
    }

    fn broadcast_append(&self, out: &mut Vec<Output>) {
        let State::Leader { progress, .. } = &self.state else {
            return;
        };
        for follower in progress {
            out.push(Output::Send {
                to: follower.peer,
                message: self.append_from(follower.next),
            });
        }
    }

    /// Moves the commit index to the highest entry durably stored on a quorum.
    fn advance_commit(&mut self) {
        let State::Leader { progress, .. } = &self.state else {
            return;
        };
        let mut stored: Vec<Index> = progress.iter().map(|follower| follower.matched).collect();
        // The leader's own copy counts only once it is on disk, like everyone else's.
        stored.push(self.durable_index);
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
    use super::{Raft, Role};
    use crate::Config;
    use crate::types::{Index, Input, Message, NodeId, Output, Term};

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
}
