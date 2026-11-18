//! One node of a running cluster: the Raft core, its storage, its state machine, and the
//! bookkeeping that connects clients to what the core decides.
//!
//! The node does no networking. It is handed [`Event`]s and returns [`Effect`]s, and whoever owns
//! it moves the bytes. That keeps everything here testable without a socket, and keeps the rules
//! that matter in one place:
//!
//! - Writes that come up together go to disk together, with one sync.
//! - A write is made durable, and only then is the core told. The core holds its messages until
//!   it hears, so nothing leaves the node on the strength of state that could still be lost.
//! - The state machine is forced to disk before the log is compacted, and a snapshot is installed
//!   before the core is told it arrived.
//! - A client is answered when the entry carrying its request is applied on this node, not when
//!   it is proposed.

use core::fmt;
use std::collections::{HashMap, VecDeque};
use std::path::Path;

use katman::Options;

use crate::config::{Config, ConfigError};
use crate::kv::{Op, Reply, SessionId};
use crate::membership::Change;
use crate::raft::{Raft, Role};
use crate::state::StateMachine;
use crate::storage::{Recovered, Storage, StorageError};
use crate::types::{Index, Input, NodeId, Output, Persist, ReadId};
use crate::wire::{ClientRequest, ClientResponse, PeerFrame, Status};

/// Names one client connection for as long as it lasts.
pub(crate) type ConnId = u64;

/// Something that happened, for the node to react to.
#[derive(Debug)]
pub(crate) enum Event {
    /// One unit of time passed.
    Tick,
    /// A frame arrived from another node.
    Peer { from: NodeId, frame: PeerFrame },
    /// A request arrived from a client.
    Client {
        conn: ConnId,
        request: ClientRequest,
    },
    /// A client went away. Nothing is owed to it any more.
    Disconnected(ConnId),
}

/// Something for the owner of the node to send.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Effect {
    ToPeer {
        to: NodeId,
        frame: PeerFrame,
    },
    ToClient {
        conn: ConnId,
        response: ClientResponse,
    },
}

/// Why a node could not start or had to stop. There is no recovering in place from any of
/// these: the process exits and starts again from its disk.
#[derive(Debug)]
#[non_exhaustive]
pub enum NodeError {
    /// The disk failed, or holds something this program did not write.
    Storage(StorageError),
    /// The configuration cannot work, or the stored state contradicts itself.
    Config(ConfigError),
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(f),
            Self::Config(error) => error.fmt(f),
        }
    }
}

impl core::error::Error for NodeError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Config(error) => Some(error),
        }
    }
}

impl From<StorageError> for NodeError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<ConfigError> for NodeError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

/// When a node drops the front of its log.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Compaction {
    /// Compact once this many applied entries have piled up in the log.
    pub(crate) every: u64,
    /// Applied entries left in the log afterwards, for followers that are only a little behind
    /// and would otherwise need a snapshot.
    pub(crate) retain: u64,
}

impl Default for Compaction {
    fn default() -> Self {
        Self {
            every: 4096,
            retain: 512,
        }
    }
}

/// A read the core has been asked to clear.
struct PendingRead {
    conn: ConnId,
    seq: u64,
    key: Vec<u8>,
}

pub(crate) struct Node {
    raft: Raft,
    storage: Storage,
    state: StateMachine,
    compaction: Compaction,
    /// Requests proposed on this node and not applied yet, with the connection that waits.
    proposals: HashMap<(SessionId, u64), ConnId>,
    /// Reads the core has not cleared yet.
    reads: HashMap<ReadId, PendingRead>,
    /// Reads the core has cleared, waiting for the state machine to reach their index.
    parked: Vec<(Index, ReadId)>,
    next_read: u64,
    /// Membership changes handed to the core in the events being handled.
    changes: Vec<(Change, ConnId)>,
    /// Writes the core has asked for and the disk has not seen yet.
    unwritten: Vec<Persist>,
}

impl Node {
    /// Starts a node from the directory `dir`, creating it if the node has never run. The log
    /// goes to `dir/log` and the state machine to `dir/state`.
    pub(crate) fn open(
        dir: &Path,
        config: Config,
        options: &Options,
        compaction: Compaction,
    ) -> Result<Self, NodeError> {
        let Recovered {
            storage,
            hard_state,
            base,
            log,
        } = Storage::open(dir.join("log"), options.clone())?;
        let mut state = StateMachine::open(dir.join("state"), options.clone())?;
        state.bootstrap(&config.founding_membership())?;
        let membership = state.membership().cloned();
        let raft = Raft::recover(config, hard_state, base, log, state.applied(), membership)?;
        // A compaction has to leave the log shorter than the threshold, or it would run again at
        // once and for ever.
        let every = compaction.every.max(1);
        let compaction = Compaction {
            every,
            retain: compaction.retain.min(every - 1),
        };
        Ok(Self {
            raft,
            storage,
            state,
            compaction,
            proposals: HashMap::new(),
            reads: HashMap::new(),
            parked: Vec::new(),
            next_read: 0,
            changes: Vec::new(),
            unwritten: Vec::new(),
        })
    }

    pub(crate) fn status(&self) -> Status {
        Status {
            id: self.raft.id(),
            role: self.raft.role(),
            term: self.raft.term(),
            leader: self.raft.leader(),
            commit: self.raft.commit_index(),
            membership: self.raft.membership().clone(),
        }
    }

    /// Reacts to one event and returns what has to be sent.
    #[cfg(test)]
    pub(crate) fn handle(&mut self, event: Event) -> Result<Vec<Effect>, NodeError> {
        self.handle_all([event])
    }

    /// Reacts to several events at once and returns what has to be sent.
    ///
    /// Handing over everything that has piled up is what makes the node fast under load: the
    /// writes all those events lead to reach the disk together, with one sync. While that sync
    /// runs, the next batch of events piles up. A node with one client pays one sync per request,
    /// and a node with a hundred pays hardly more.
    ///
    /// An error means the node can no longer trust its own state and must stop.
    pub(crate) fn handle_all(
        &mut self,
        events: impl IntoIterator<Item = Event>,
    ) -> Result<Vec<Effect>, NodeError> {
        let mut effects = Vec::new();
        let mut inputs = VecDeque::new();
        for event in events {
            self.take_in(event, &mut inputs, &mut effects)?;
        }
        self.work_off(&mut inputs, &mut effects)?;

        // Whatever the core did not refuse, it took.
        for (_, conn) in self.changes.drain(..) {
            effects.push(Effect::ToClient {
                conn,
                response: ClientResponse::ChangeAccepted,
            });
        }
        Ok(effects)
    }

    /// Turns an event into inputs for the core.
    fn take_in(
        &mut self,
        event: Event,
        inputs: &mut VecDeque<Input>,
        effects: &mut Vec<Effect>,
    ) -> Result<(), NodeError> {
        match event {
            Event::Tick => inputs.push_back(Input::Tick),
            Event::Peer {
                from,
                frame: PeerFrame::Message(message),
            } => {
                inputs.push_back(Input::Message { from, message });
            }
            Event::Peer {
                from,
                frame:
                    PeerFrame::Snapshot {
                        term,
                        round,
                        snapshot,
                    },
            } => {
                // The state machine first, and durably. The core drops its log on the strength
                // of this.
                self.state.install(&snapshot)?;
                inputs.push_back(Input::Snapshot {
                    from,
                    term,
                    round,
                    snapshot: snapshot.meta(),
                    membership: snapshot.membership().cloned().unwrap_or_default(),
                });
            }
            Event::Client { conn, request } => self.accept(conn, request, inputs, effects),
            Event::Disconnected(conn) => {
                self.proposals.retain(|_, waiting| *waiting != conn);
                self.reads.retain(|_, read| read.conn != conn);
            }
        }
        Ok(())
    }

    /// Runs the core until it has nothing left to do, writing to disk each time it has caught up
    /// with its inputs.
    fn work_off(
        &mut self,
        inputs: &mut VecDeque<Input>,
        effects: &mut Vec<Effect>,
    ) -> Result<(), NodeError> {
        loop {
            // One input at a time, each to the end, before the next.
            while let Some(input) = inputs.pop_front() {
                for output in self.raft.step(input) {
                    self.carry_out(output, effects)?;
                }
                self.serve_reads(effects)?;
                self.maybe_compact(inputs)?;
            }
            // The core has seen everything there is, and has asked for some writes on the way.
            // They go to disk together. Only then is it told, and that may set it going again:
            // messages it was holding back are released, entries become ready to apply.
            let Some(newest) = self.unwritten.last() else {
                return Ok(());
            };
            let seq = newest.seq();
            self.storage.persist_all(&self.unwritten)?;
            self.unwritten.clear();
            inputs.push_back(Input::Persisted { seq });
        }
    }

    /// Takes a client's request, or says straight away that this is the wrong node.
    fn accept(
        &mut self,
        conn: ConnId,
        request: ClientRequest,
        inputs: &mut VecDeque<Input>,
        effects: &mut Vec<Effect>,
    ) {
        let request = match request {
            ClientRequest::Status => {
                effects.push(Effect::ToClient {
                    conn,
                    response: ClientResponse::Status(self.status()),
                });
                return;
            }
            // Asking a follower would get an answer that names no request. Check here instead.
            _ if self.raft.role() != Role::Leader => {
                let response = ClientResponse::NotLeader {
                    leader: self.raft.leader(),
                };
                effects.push(Effect::ToClient { conn, response });
                return;
            }
            request => request,
        };
        match request {
            ClientRequest::Status => {}
            ClientRequest::Change(change) => {
                self.changes.push((change, conn));
                inputs.push_back(Input::ChangeMembership(change));
            }
            ClientRequest::Op(request) => {
                if let Op::Get { key } = request.op() {
                    // A read needs no log entry: the core confirms that this node still leads,
                    // and the state machine answers.
                    self.next_read += 1;
                    let id = ReadId::new(self.next_read);
                    self.reads.insert(
                        id,
                        PendingRead {
                            conn,
                            seq: request.seq(),
                            key: key.clone(),
                        },
                    );
                    inputs.push_back(Input::Read { id });
                } else {
                    self.proposals
                        .insert((request.session(), request.seq()), conn);
                    inputs.push_back(Input::Propose(request.encode()));
                }
            }
        }
    }

    fn carry_out(&mut self, output: Output, effects: &mut Vec<Effect>) -> Result<(), NodeError> {
        match output {
            // Not written yet. The core is not told until it is, and holds its messages till then.
            Output::Persist(persist) => self.unwritten.push(persist),
            Output::Send { to, message } => effects.push(Effect::ToPeer {
                to,
                frame: PeerFrame::Message(message),
            }),
            Output::SendSnapshot { to, term, round } => {
                let snapshot = self.state.snapshot()?;
                effects.push(Effect::ToPeer {
                    to,
                    frame: PeerFrame::Snapshot {
                        term,
                        round,
                        snapshot,
                    },
                });
            }
            Output::Apply { index, entry } => {
                let Some(outcome) = self.state.apply(index, &entry)? else {
                    return Ok(());
                };
                // Answered here only if it was proposed here. On every other node the same entry
                // is applied and nobody is waiting.
                if let Some(conn) = self.proposals.remove(&(outcome.session, outcome.seq)) {
                    let response = ClientResponse::Reply {
                        seq: outcome.seq,
                        reply: outcome.reply,
                    };
                    effects.push(Effect::ToClient { conn, response });
                }
            }
            Output::ReadReady { id, index } => self.parked.push((index, id)),
            Output::ReadRejected { id, leader } => {
                if let Some(read) = self.reads.remove(&id) {
                    effects.push(Effect::ToClient {
                        conn: read.conn,
                        response: ClientResponse::NotLeader { leader },
                    });
                }
            }
            Output::ChangeRejected { change, reason } => {
                if let Some(position) = self.changes.iter().position(|(asked, _)| *asked == change)
                {
                    let (_, conn) = self.changes.remove(position);
                    effects.push(Effect::ToClient {
                        conn,
                        response: ClientResponse::ChangeRejected(reason),
                    });
                }
            }
            // Requests are not handed to a core that does not lead, so this names nothing a
            // client is waiting for.
            Output::NotLeader { .. } => {}
        }
        Ok(())
    }

    /// Answers the reads that the core has cleared and the state machine has caught up with.
    fn serve_reads(&mut self, effects: &mut Vec<Effect>) -> Result<(), NodeError> {
        let applied = self.state.applied_index();
        let (due, parked): (Vec<_>, Vec<_>) = core::mem::take(&mut self.parked)
            .into_iter()
            .partition(|(index, _)| *index <= applied);
        self.parked = parked;
        for (_, id) in due {
            let Some(read) = self.reads.remove(&id) else {
                continue;
            };
            let reply = Reply::Value(self.state.get(&read.key)?);
            effects.push(Effect::ToClient {
                conn: read.conn,
                response: ClientResponse::Reply {
                    seq: read.seq,
                    reply,
                },
            });
        }
        Ok(())
    }

    /// Forces the state machine to disk and lets the core drop the log before it, once enough
    /// applied entries have piled up.
    fn maybe_compact(&mut self, inputs: &mut VecDeque<Input>) -> Result<(), NodeError> {
        // The core's count, not the state machine's, which can be ahead after a snapshot from a
        // leader the core no longer follows.
        let applied = self.raft.applied_index().get();
        if applied < self.raft.log_base().index().get() + self.compaction.every {
            return Ok(());
        }
        let durable = self.state.checkpoint()?.index().get();
        inputs.push_back(Input::Compact {
            index: Index::new(durable.saturating_sub(self.compaction.retain)),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::Path;

    use katman::{Options, SimFs, Torn};

    use super::{Compaction, ConnId, Effect, Event, Node};
    use crate::Config;
    use crate::kv::{Op, Reply, Request, SessionId};
    use crate::membership::{Change, ChangeError};
    use crate::raft::Role;
    use crate::types::NodeId;
    use crate::wire::{ClientRequest, ClientResponse};

    /// Three nodes with an instant network. A node that is down receives nothing.
    struct Cluster {
        nodes: Vec<Option<Node>>,
        disks: Vec<SimFs>,
        replies: Vec<(ConnId, ClientResponse)>,
        compaction: Compaction,
    }

    fn id(position: usize) -> NodeId {
        NodeId::new(u64::try_from(position).expect("few nodes") + 1)
    }

    impl Cluster {
        fn new(compaction: Compaction) -> Self {
            let mut cluster = Self {
                nodes: Vec::new(),
                disks: Vec::new(),
                replies: Vec::new(),
                compaction,
            };
            for position in 0..3 {
                cluster.disks.push(SimFs::new());
                cluster.nodes.push(None);
                cluster.start(position);
            }
            cluster
        }

        fn start(&mut self, position: usize) {
            let peers = (0..3).filter(|&other| other != position).map(id);
            let config = Config::new(
                id(position),
                peers,
                u64::try_from(position).expect("few nodes"),
            );
            let options = Options::default().with_sim_fs(self.disks[position].clone());
            let node = Node::open(Path::new("node"), config, &options, self.compaction)
                .expect("the node starts");
            self.nodes[position] = Some(node);
        }

        fn cut_power(&mut self, position: usize) {
            self.disks[position] = self.disks[position].power_loss(Torn::Nothing);
            self.nodes[position] = None;
        }

        /// Hands an event to a node and everything that follows from it to whoever it is for.
        fn deliver(&mut self, position: usize, event: Event) {
            let mut queue = VecDeque::from([(position, event)]);
            while let Some((position, event)) = queue.pop_front() {
                // A node that is down, or that only exists as an id in someone's membership.
                let Some(node) = self.nodes.get_mut(position).and_then(Option::as_mut) else {
                    continue;
                };
                for effect in node.handle(event).expect("a healthy node") {
                    match effect {
                        Effect::ToPeer { to, frame } => {
                            let to = usize::try_from(to.get() - 1).expect("few nodes");
                            queue.push_back((
                                to,
                                Event::Peer {
                                    from: id(position),
                                    frame,
                                },
                            ));
                        }
                        Effect::ToClient { conn, response } => self.replies.push((conn, response)),
                    }
                }
            }
        }

        fn run(&mut self, ticks: u32) {
            for _ in 0..ticks {
                for position in 0..3 {
                    self.deliver(position, Event::Tick);
                }
            }
        }

        fn leader(&self) -> usize {
            let leads = |node: &Option<Node>| {
                node.as_ref()
                    .is_some_and(|node| node.status().role == Role::Leader)
            };
            self.nodes
                .iter()
                .position(leads)
                .expect("the cluster has a leader")
        }

        /// Sends a request and returns what came back on its connection.
        fn ask(
            &mut self,
            position: usize,
            conn: ConnId,
            request: ClientRequest,
        ) -> Vec<ClientResponse> {
            self.replies.clear();
            self.deliver(position, Event::Client { conn, request });
            self.replies
                .iter()
                .filter(|(to, _)| *to == conn)
                .map(|(_, response)| response.clone())
                .collect()
        }

        fn op(&mut self, position: usize, seq: u64, op: Op) -> Vec<ClientResponse> {
            self.ask(
                position,
                1,
                ClientRequest::Op(Request::new(SessionId::new(7), seq, op)),
            )
        }
    }

    fn put(key: &[u8], value: &[u8]) -> Op {
        Op::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn get(key: &[u8]) -> Op {
        Op::Get { key: key.to_vec() }
    }

    fn value(seq: u64, value: Option<&[u8]>) -> Vec<ClientResponse> {
        vec![ClientResponse::Reply {
            seq,
            reply: Reply::Value(value.map(<[u8]>::to_vec)),
        }]
    }

    #[test]
    fn a_write_and_a_read_go_through_the_leader_and_a_follower_says_who_leads() {
        let mut cluster = Cluster::new(Compaction::default());
        cluster.run(60);
        let leader = cluster.leader();
        let follower = (leader + 1) % 3;

        assert_eq!(
            cluster.op(leader, 1, put(b"k", b"v")),
            [ClientResponse::Reply {
                seq: 1,
                reply: Reply::Done
            }]
        );
        assert_eq!(cluster.op(leader, 2, get(b"k")), value(2, Some(b"v")));
        assert_eq!(cluster.op(leader, 3, get(b"other")), value(3, None));

        let elsewhere = || {
            [ClientResponse::NotLeader {
                leader: Some(id(leader)),
            }]
        };
        assert_eq!(cluster.op(follower, 4, get(b"k")), elsewhere());
        assert_eq!(cluster.op(follower, 4, put(b"k", b"w")), elsewhere());
    }

    #[test]
    fn a_request_sent_twice_is_carried_out_once_and_answered_twice() {
        let mut cluster = Cluster::new(Compaction::default());
        cluster.run(60);
        let leader = cluster.leader();
        let claim = Op::CompareAndSet {
            key: b"k".to_vec(),
            expected: None,
            value: b"mine".to_vec(),
        };
        let won = [ClientResponse::Reply {
            seq: 1,
            reply: Reply::Swapped(true),
        }];

        assert_eq!(cluster.op(leader, 1, claim.clone()), won);
        // The client did not hear back and asks again. Carried out a second time, this would
        // find the key taken.
        assert_eq!(cluster.op(leader, 1, claim), won);
    }

    #[test]
    fn a_node_that_lost_power_is_caught_up_by_snapshot_and_keeps_its_data() {
        let mut cluster = Cluster::new(Compaction {
            every: 8,
            retain: 2,
        });
        cluster.run(60);
        let leader = cluster.leader();
        let victim = (leader + 1) % 3;

        cluster.cut_power(victim);
        for seq in 1..=40_u64 {
            cluster.op(leader, seq, put(b"k", &seq.to_be_bytes()));
        }
        // The leader has compacted well past anything the victim ever saw.
        cluster.start(victim);
        // The leader asked for a snapshot to be sent while the victim was away, and asks again
        // only after two election timeouts without an answer.
        cluster.run(40);

        let status = |cluster: &Cluster, position: usize| {
            cluster.nodes[position].as_ref().expect("up").status()
        };
        assert_eq!(
            status(&cluster, victim).commit,
            status(&cluster, leader).commit
        );

        // Power off again, and the data is still there afterwards: it came by snapshot and was
        // forced to disk when it was installed.
        cluster.cut_power(victim);
        cluster.start(victim);
        let node = cluster.nodes[victim].as_ref().expect("up");
        assert!(
            node.state.applied_index().get() >= 38,
            "applied {:?}",
            node.state.applied_index()
        );
        assert!(node.state.get(b"k").expect("a healthy disk").is_some());
    }

    #[test]
    fn a_membership_change_is_accepted_or_refused_with_the_reason() {
        let mut cluster = Cluster::new(Compaction::default());
        cluster.run(60);
        let leader = cluster.leader();
        let change = |change| ClientRequest::Change(change);

        let added = cluster.ask(leader, 2, change(Change::AddLearner(NodeId::new(4))));
        assert_eq!(added, [ClientResponse::ChangeAccepted]);
        let again = cluster.ask(leader, 2, change(Change::AddLearner(NodeId::new(4))));
        assert_eq!(
            again,
            [ClientResponse::ChangeRejected(ChangeError::AlreadyMember)]
        );
        // Node 4 does not exist here, so it never catches up.
        let early = cluster.ask(leader, 2, change(Change::Promote(NodeId::new(4))));
        assert_eq!(
            early,
            [ClientResponse::ChangeRejected(ChangeError::NotCaughtUp)]
        );

        let follower = (leader + 1) % 3;
        let elsewhere = cluster.ask(follower, 2, change(Change::Remove(NodeId::new(4))));
        assert_eq!(
            elsewhere,
            [ClientResponse::NotLeader {
                leader: Some(id(leader))
            }]
        );
    }
}
