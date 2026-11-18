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

/// What a node must remember across a restart, besides its log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HardState {
    term: Term,
    voted_for: Option<NodeId>,
}

impl HardState {
    /// Builds a hard state. Storage needs this to hand back what it read.
    #[must_use]
    pub const fn new(term: Term, voted_for: Option<NodeId>) -> Self {
        Self { term, voted_for }
    }

    /// The latest term the node has seen.
    #[must_use]
    pub const fn term(&self) -> Term {
        self.term
    }

    /// Who the node voted for in that term, if anyone.
    #[must_use]
    pub const fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }
}

/// Numbers the [`Persist`] requests of one node, in the order it issued them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PersistSeq(u64);

impl PersistSeq {
    /// Wraps a raw sequence number.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw sequence number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A point in the log that a snapshot covers: everything up to and including `index` is in the
/// snapshot, and `term` is the term of the entry at `index`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotMeta {
    index: Index,
    term: Term,
}

impl SnapshotMeta {
    /// Builds a snapshot point.
    #[must_use]
    pub const fn new(index: Index, term: Term) -> Self {
        Self { index, term }
    }

    /// The last index the snapshot covers.
    #[must_use]
    pub const fn index(&self) -> Index {
        self.index
    }

    /// The term of the entry at that index.
    #[must_use]
    pub const fn term(&self) -> Term {
        self.term
    }
}

/// Identifies one read request on one node. The driver chooses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadId(u64);

impl ReadId {
    /// Wraps a raw read id.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw read id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A change to the stored log: drop everything at or after `first_index`, then store `entries`
/// starting there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogWrite {
    first_index: Index,
    entries: Vec<Entry>,
}

impl LogWrite {
    /// Builds a log write.
    #[must_use]
    pub const fn new(first_index: Index, entries: Vec<Entry>) -> Self {
        Self {
            first_index,
            entries,
        }
    }

    /// Where the new entries start. Anything stored at or after this index is replaced.
    #[must_use]
    pub const fn first_index(&self) -> Index {
        self.first_index
    }

    /// The entries to store, in order.
    #[must_use]
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
}

/// State the driver must make durable. It has to be applied as one atomic write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Persist {
    seq: PersistSeq,
    hard_state: HardState,
    base: Option<SnapshotMeta>,
    log: Option<LogWrite>,
}

impl Persist {
    /// Builds a persist request.
    #[must_use]
    pub const fn new(
        seq: PersistSeq,
        hard_state: HardState,
        base: Option<SnapshotMeta>,
        log: Option<LogWrite>,
    ) -> Self {
        Self {
            seq,
            hard_state,
            base,
            log,
        }
    }

    /// The number to report back in [`Input::Persisted`] once the write is durable.
    #[must_use]
    pub const fn seq(&self) -> PersistSeq {
        self.seq
    }

    /// The hard state to store.
    #[must_use]
    pub const fn hard_state(&self) -> HardState {
        self.hard_state
    }

    /// The new start of the log, if it moved: every stored entry up to and including this
    /// index is to be removed. Applied before [`Persist::log`].
    #[must_use]
    pub const fn base(&self) -> Option<SnapshotMeta> {
        self.base
    }

    /// The change to the log, if the log changed.
    #[must_use]
    pub const fn log(&self) -> Option<&LogWrite> {
        self.log.as_ref()
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
    /// A node asks whether it would get a vote for `term`, without being in that term yet.
    PreVote {
        /// The term the node would start an election for: its own plus one.
        term: Term,
        /// Index of the node's last log entry.
        last_log_index: Index,
        /// Term of the node's last log entry.
        last_log_term: Term,
    },
    /// The answer to `PreVote`.
    PreVoteResponse {
        /// The term that was asked about if `granted`, otherwise the responder's own term.
        term: Term,
        /// Whether the responder would vote for the node.
        granted: bool,
    },
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
        /// The leader's read round, to be repeated in the answer. It lets the leader tell that an
        /// answer belongs to a message it sent after a given read arrived.
        round: u64,
    },
    /// The answer to `AppendEntries`.
    AppendEntriesResponse {
        /// The responder's term.
        term: Term,
        /// Whether the entries were stored.
        outcome: AppendOutcome,
        /// The round of the request being answered.
        round: u64,
    },
}

impl Message {
    /// The term the message carries. For a pre-vote and a granted answer to one, that is a term
    /// the sender is asking about, not one it is in.
    #[must_use]
    pub const fn term(&self) -> Term {
        match self {
            Self::PreVote { term, .. }
            | Self::PreVoteResponse { term, .. }
            | Self::RequestVote { term, .. }
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
    /// A client wants to read. The node answers with [`Output::ReadReady`] or
    /// [`Output::ReadRejected`] for the same id.
    Read {
        /// Chosen by the driver, to match the answer to the read.
        id: ReadId,
    },
    /// The state machine holds everything up to `index` durably, so the log before it may go.
    /// The node never drops entries it has not handed out with [`Output::Apply`].
    Compact {
        /// The last index the log no longer needs to hold.
        index: Index,
    },
    /// A snapshot arrived from the leader. The driver feeds this after it has installed the
    /// snapshot into its state machine durably, or found that the state machine was already at
    /// least as far.
    Snapshot {
        /// The leader that sent it.
        from: NodeId,
        /// The leader's term, as given in [`Output::SendSnapshot`].
        term: Term,
        /// The round given in [`Output::SendSnapshot`].
        round: u64,
        /// The point the snapshot covers.
        snapshot: SnapshotMeta,
    },
    /// The [`Persist`] request with this number, and every earlier one, is on stable storage.
    Persisted {
        /// The sequence number of the request that completed.
        seq: PersistSeq,
    },
}

/// Something the driver has to do after a step.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Output {
    /// Make this state durable, then report it with [`Input::Persisted`]. Requests must be
    /// carried out in the order they were issued. The node holds back every message that depends
    /// on the state until the report arrives.
    Persist(Persist),
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
    /// A follower needs entries the log no longer holds. The driver takes a snapshot of its
    /// state machine as it is now and sends it; the receiving driver installs it and reports
    /// with [`Input::Snapshot`]. The core never sees the data.
    SendSnapshot {
        /// The follower that needs it.
        to: NodeId,
        /// This node's term, to pass along.
        term: Term,
        /// The read round, to pass along.
        round: u64,
    },
    /// The read may be served from the state machine as soon as it has applied `index`. The
    /// result is linearizable.
    ReadReady {
        /// The read this answers.
        id: ReadId,
        /// The state machine must have applied at least this index before it answers.
        index: Index,
    },
    /// The read cannot be served here: this node is not the leader, stopped being it while the
    /// read waited, or has only just become it. The client should try again.
    ReadRejected {
        /// The read this answers.
        id: ReadId,
        /// The leader this node last heard from, if any.
        leader: Option<NodeId>,
    },
    /// A proposal was refused because this node is not the leader.
    NotLeader {
        /// The leader this node last heard from, if any.
        leader: Option<NodeId>,
    },
}
