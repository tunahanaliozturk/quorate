//! Identifiers, log entries and the messages nodes exchange.

/// Identifies one node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u64);

impl NodeId {
    /// Wraps a raw identifier. Any value is valid; it only has to be unique in the cluster.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A Raft term: a period with at most one leader. Terms only grow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Term(u64);

impl Term {
    /// Wraps a raw term number.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw term number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// A position in the log. The first entry is at index 1; index 0 means "before the first entry".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Index(u64);

impl Index {
    /// The position before the first entry.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw log position.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw log position.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub(crate) const fn prev(self) -> Self {
        Self(self.0.saturating_sub(1))
    }
}

/// What a log entry carries.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Payload {
    /// Written by a new leader so that it can commit entries from earlier terms.
    Noop,
    /// A client command. The core never looks inside.
    Command(Vec<u8>),
}

/// One entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    term: Term,
    payload: Payload,
}

impl Entry {
    /// Builds an entry. Drivers need this to decode entries from disk or from the wire.
    #[must_use]
    pub const fn new(term: Term, payload: Payload) -> Self {
        Self { term, payload }
    }

    /// The term of the leader that created the entry.
    #[must_use]
    pub const fn term(&self) -> Term {
        self.term
    }

    /// What the entry carries.
    #[must_use]
    pub const fn payload(&self) -> &Payload {
        &self.payload
    }
}

/// A follower's answer to `AppendEntries`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendOutcome {
    /// The follower's log now matches the leader's up to `match_index`.
    Accepted {
        /// Last index known to match the leader's log.
        match_index: Index,
    },
    /// The follower has no entry with the leader's term at `prev_log_index`.
    Rejected {
        /// The `prev_log_index` of the request being refused, so a late answer cannot be mistaken
        /// for the answer to a newer request.
        prev_log_index: Index,
        /// Where the leader should try next: just past the follower's log if it is too short,
        /// otherwise the first index of the term the follower holds at `prev_log_index`. This
        /// skips a whole conflicting term per round trip instead of one entry.
        retry_from: Index,
    },
}

/// A message between two nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Message {
    /// A candidate asks for a vote.
    RequestVote {
        /// The candidate's term.
        term: Term,
        /// Index of the candidate's last log entry.
        last_log_index: Index,
        /// Term of the candidate's last log entry.
        last_log_term: Term,
    },
    /// The answer to `RequestVote`.
    RequestVoteResponse {
        /// The responder's term.
        term: Term,
        /// Whether the vote was given.
        granted: bool,
    },
    /// A leader replicates entries. With no entries it is a heartbeat.
    AppendEntries {
        /// The leader's term.
        term: Term,
        /// Index of the entry just before `entries`.
        prev_log_index: Index,
        /// Term of the entry at `prev_log_index`.
        prev_log_term: Term,
        /// The entries to store, in order.
        entries: Vec<Entry>,
        /// The leader's commit index.
        leader_commit: Index,
    },
    /// The answer to `AppendEntries`.
    AppendEntriesResponse {
        /// The responder's term.
        term: Term,
        /// Whether the entries were stored.
        outcome: AppendOutcome,
    },
}

impl Message {
    /// The sender's term, which every message carries.
    #[must_use]
    pub const fn term(&self) -> Term {
        match self {
            Self::RequestVote { term, .. }
            | Self::RequestVoteResponse { term, .. }
            | Self::AppendEntries { term, .. }
            | Self::AppendEntriesResponse { term, .. } => *term,
        }
    }
}

/// Something that happens to a node. The driver feeds these to [`Raft::step`](crate::Raft::step).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Input {
    /// One unit of logical time has passed.
    Tick,
    /// A message arrived from a peer.
    Message {
        /// The sender.
        from: NodeId,
        /// What it sent.
        message: Message,
    },
    /// A client wants this command replicated.
    Propose(Vec<u8>),
}

/// Something the driver has to do after a step.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Output {
    /// Send a message to a peer. Delivery may fail; the core retries by itself.
    Send {
        /// The receiver.
        to: NodeId,
        /// What to send.
        message: Message,
    },
    /// An entry is committed. Each node emits every index once, in order.
    Apply {
        /// The entry's position in the log.
        index: Index,
        /// The committed entry.
        entry: Entry,
    },
    /// A proposal was refused because this node is not the leader.
    NotLeader {
        /// The leader this node last heard from, if any.
        leader: Option<NodeId>,
    },
}
