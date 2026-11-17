//! The Raft state machine: leader election and log replication.
//!
//! Nothing in here does I/O, reads a clock or starts a thread. Time arrives as [`Input::Tick`],
//! the network as [`Input::Message`], and everything the node wants done leaves as [`Output`].
//! Randomness comes from the seed in [`Config`]. Two nodes built from the same configuration and
//! fed the same inputs produce the same outputs.
//!
//! State lives in memory only for now. Durable storage and crash recovery arrive with the next
//! milestone; until then a node must not be restarted.

use crate::config::{Config, ConfigError};
use crate::log::Log;
use crate::rng::Rng;
use crate::types::{AppendOutcome, Entry, Index, Input, Message, NodeId, Output, Payload, Term};

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
    #[cfg(test)]
    bug: Option<Bug>,
}

impl Raft {
    /// Creates a follower in term 0 with an empty log.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] when the configuration cannot work: the node lists itself or a
    /// peer twice, the heartbeat is not shorter than the election timeout, or the batch size is
    /// zero.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        config.validate()?;
        let mut node = Self {
            id: config.id,
            peers: config.peers,
            election_ticks: config.election_ticks,
            heartbeat_ticks: config.heartbeat_ticks,
            max_batch: config.max_batch,
            rng: Rng::new(config.seed),
            term: Term::default(),
            voted_for: None,
            log: Log::default(),
            commit: Index::ZERO,
            applied: Index::ZERO,
            state: State::Follower { leader: None },
            elapsed: 0,
            timeout: 0,
            #[cfg(test)]
            bug: None,
        };
        node.reset_timer();
        Ok(node)
    }

    /// Feeds one input to the node and returns what the driver must do, in order.
    pub fn step(&mut self, input: Input) -> Vec<Output> {
        let mut out = Vec::new();
        match input {
            Input::Tick => self.tick(&mut out),
            Input::Message { from, message } => self.receive(from, message, &mut out),
            Input::Propose(command) => self.propose(command, &mut out),
        }
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

    /// The whole log. The entry at index `i` is at position `i - 1`.
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
        self.log
            .append(Entry::new(self.term, Payload::Command(command)));
        self.broadcast_append(out);
        self.advance_commit();
    }

    /// Starts an election for the next term.
    fn campaign(&mut self, out: &mut Vec<Output>) {
        self.term = self.term.next();
        self.voted_for = Some(self.id);
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
        self.log.append(Entry::new(self.term, Payload::Noop));
        self.broadcast_append(out);
        self.advance_commit();
    }

    /// Steps down, adopting `term` if it is newer.
    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
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
            self.voted_for = Some(from);
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
                    self.log.truncate_after(index.prev());
                    self.log.append(entry);
                }
                None => self.log.append(entry),
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
                follower.next = prev_log_index.min(retry_from).max(follower.matched.next());
                true
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

    /// Moves the commit index to the highest entry stored on a quorum.
    fn advance_commit(&mut self) {
        let State::Leader { progress, .. } = &self.state else {
            return;
        };
        let mut stored: Vec<Index> = progress.iter().map(|follower| follower.matched).collect();
        stored.push(self.log.last_index());
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

    fn apply_committed(&mut self, out: &mut Vec<Output>) {
        while self.applied < self.commit {
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
