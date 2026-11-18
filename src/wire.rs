//! What goes over a socket.
//!
//! A connection carries frames: four bytes of length, four bytes of CRC32C, then the payload. The
//! first frame says who is calling ([`Hello`]); after that a peer connection carries
//! [`PeerFrame`]s one way, and a client connection carries [`ClientRequest`]s one way and
//! [`ClientResponse`]s the other.
//!
//! Everything is written by hand, field by field, big-endian. Decoding never trusts a length it
//! has not checked against the bytes in hand, never panics, and accepts a buffer only if it is
//! exactly one value: no prefix of an encoding and no encoding with bytes appended is valid.

use std::io::{self, Read, Write};

use crate::kv::{Malformed, Reader, Reply, Request, put_bytes, put_optional};
use crate::membership::{Change, ChangeError, Membership};
use crate::raft::Role;
use crate::state::Snapshot;
use crate::storage::{decode_entry, encode_entry};
use crate::types::{AppendOutcome, Index, Message, NodeId, Term};

/// The first bytes of every connection, so that something else that connects by mistake is
/// turned away at once.
const MAGIC: [u8; 2] = *b"QR";
/// Bumped when the format changes in a way an older node would misread.
const VERSION: u8 = 1;
/// The largest frame one node sends another. A snapshot is one frame, so this also bounds the
/// store a snapshot can carry.
pub(crate) const PEER_FRAME: usize = 64 << 20;
/// The largest frame to or from a client: one request or one response, so one key and value.
/// Anyone can open a client connection, which is why this is much smaller.
pub(crate) const CLIENT_FRAME: usize = 8 << 20;
/// The largest first frame of a connection. It only says who is calling.
pub(crate) const HELLO_FRAME: usize = 64;

fn invalid(what: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what)
}

/// Writes one frame.
///
/// # Errors
///
/// Returns the writer's error, or `InvalidData` for a payload over the frame limit.
pub(crate) fn write_frame(writer: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    if payload.len() > PEER_FRAME {
        return Err(invalid("the frame is too large to send"));
    }
    let length =
        u32::try_from(payload.len()).map_err(|_| invalid("the frame is too large to send"))?;
    let mut header = [0; 8];
    header[..4].copy_from_slice(&length.to_be_bytes());
    header[4..].copy_from_slice(&crc32c::crc32c(payload).to_be_bytes());
    writer.write_all(&header)?;
    writer.write_all(payload)?;
    writer.flush()
}

/// Reads one frame of at most `limit` bytes and checks it.
///
/// The length in the header is a claim by whoever is on the other end. It is checked against
/// `limit` before anything else, and even then memory is taken only as the bytes arrive: a
/// connection that announces a large frame and sends nothing costs nothing.
///
/// # Errors
///
/// Returns the reader's error (`UnexpectedEof` when the other side hung up), or `InvalidData`
/// for a frame that claims to be over the limit or does not match its checksum. After
/// `InvalidData` the stream is out of step and has to be closed.
pub(crate) fn read_frame(reader: &mut impl Read, limit: usize) -> io::Result<Vec<u8>> {
    let mut header = [0; 8];
    reader.read_exact(&mut header)?;
    let (length, checksum) = header.split_at(4);
    let length = u32::from_be_bytes(length.try_into().map_err(|_| invalid("a short header"))?);
    let checksum = u32::from_be_bytes(checksum.try_into().map_err(|_| invalid("a short header"))?);
    if !usize::try_from(length).is_ok_and(|length| length <= limit) {
        return Err(invalid("the frame is too large to receive"));
    }
    let mut payload = Vec::new();
    let arrived = (&mut *reader)
        .take(u64::from(length))
        .read_to_end(&mut payload)?;
    if u64::try_from(arrived).ok() != Some(u64::from(length)) {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    if crc32c::crc32c(&payload) != checksum {
        return Err(invalid("the frame does not match its checksum"));
    }
    Ok(payload)
}

/// Who is on the other end of a new connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Hello {
    /// Another node of the cluster. It will send [`PeerFrame`]s.
    Peer(NodeId),
    /// A client. It will send [`ClientRequest`]s and read [`ClientResponse`]s.
    Client,
}

/// What one node sends another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerFrame {
    Message(Message),
    /// A copy of the sender's state machine, with the term and round its core asked to pass on.
    Snapshot {
        term: Term,
        round: u64,
        snapshot: Snapshot,
    },
}

/// What a client asks a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClientRequest {
    Op(Request),
    Change(Change),
    Status,
}

/// A node's view of itself and the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    /// The node that answered.
    pub id: NodeId,
    /// What it currently is.
    pub role: Role,
    /// Its term.
    pub term: Term,
    /// The leader it follows, if it knows one.
    pub leader: Option<NodeId>,
    /// Its commit index.
    pub commit: Index,
    /// The membership in force on it.
    pub membership: Membership,
}

/// What a node answers a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClientResponse {
    /// The result of the request with this sequence number.
    Reply {
        seq: u64,
        reply: Reply,
    },
    /// This node cannot serve the request. The client should try the leader, if one is named.
    NotLeader {
        leader: Option<NodeId>,
    },
    /// The leader has put the membership change into its log. It is not committed yet.
    ChangeAccepted,
    ChangeRejected(ChangeError),
    Status(Status),
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_node(out: &mut Vec<u8>, node: Option<NodeId>) {
    match node {
        None => out.push(0),
        Some(id) => {
            out.push(1);
            put_u64(out, id.get());
        }
    }
}

/// A node that may be absent. `Err` means the bytes are not a well-formed field.
fn node(reader: &mut Reader<'_>) -> Result<Option<NodeId>, Malformed> {
    match reader.u8() {
        Some(0) => Ok(None),
        Some(1) => reader
            .u64()
            .map(|id| Some(NodeId::new(id)))
            .ok_or(Malformed),
        _ => Err(Malformed),
    }
}

fn flag(reader: &mut Reader<'_>) -> Option<bool> {
    match reader.u8()? {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    }
}

/// Runs `decode` and accepts its result only if it used up every byte.
fn exactly<T>(bytes: &[u8], decode: impl FnOnce(&mut Reader<'_>) -> Option<T>) -> Option<T> {
    let mut reader = Reader(bytes);
    let value = decode(&mut reader)?;
    reader.0.is_empty().then_some(value)
}

impl Hello {
    pub(crate) fn encode(self) -> Vec<u8> {
        let mut out = vec![MAGIC[0], MAGIC[1], VERSION];
        match self {
            Self::Peer(id) => {
                out.push(0);
                put_u64(&mut out, id.get());
            }
            Self::Client => out.push(1),
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        exactly(bytes, |reader| {
            let greeting = [reader.u8()?, reader.u8()?, reader.u8()?];
            if greeting != [MAGIC[0], MAGIC[1], VERSION] {
                return None;
            }
            match reader.u8()? {
                0 => Some(Self::Peer(NodeId::new(reader.u64()?))),
                1 => Some(Self::Client),
                _ => None,
            }
        })
    }
}

fn put_message(out: &mut Vec<u8>, message: &Message) {
    match message {
        Message::PreVote {
            term,
            last_log_index,
            last_log_term,
        } => {
            out.push(0);
            put_u64(out, term.get());
            put_u64(out, last_log_index.get());
            put_u64(out, last_log_term.get());
        }
        Message::PreVoteResponse { term, granted } => {
            out.push(1);
            put_u64(out, term.get());
            out.push(u8::from(*granted));
        }
        Message::RequestVote {
            term,
            last_log_index,
            last_log_term,
        } => {
            out.push(2);
            put_u64(out, term.get());
            put_u64(out, last_log_index.get());
            put_u64(out, last_log_term.get());
        }
        Message::RequestVoteResponse { term, granted } => {
            out.push(3);
            put_u64(out, term.get());
            out.push(u8::from(*granted));
        }
        Message::AppendEntries {
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            round,
        } => {
            out.push(4);
            put_u64(out, term.get());
            put_u64(out, prev_log_index.get());
            put_u64(out, prev_log_term.get());
            put_u64(out, leader_commit.get());
            put_u64(out, *round);
            put_u64(out, u64::try_from(entries.len()).unwrap_or(u64::MAX));
            for entry in entries {
                put_bytes(out, &encode_entry(entry));
            }
        }
        Message::AppendEntriesResponse {
            term,
            outcome,
            round,
        } => {
            out.push(5);
            put_u64(out, term.get());
            put_u64(out, *round);
            match outcome {
                AppendOutcome::Accepted { match_index } => {
                    out.push(0);
                    put_u64(out, match_index.get());
                }
                AppendOutcome::Rejected {
                    prev_log_index,
                    retry_from,
                } => {
                    out.push(1);
                    put_u64(out, prev_log_index.get());
                    put_u64(out, retry_from.get());
                }
            }
        }
    }
}

fn message(reader: &mut Reader<'_>) -> Option<Message> {
    let tag = reader.u8()?;
    let term = Term::new(reader.u64()?);
    Some(match tag {
        0 | 2 => {
            let last_log_index = Index::new(reader.u64()?);
            let last_log_term = Term::new(reader.u64()?);
            if tag == 0 {
                Message::PreVote {
                    term,
                    last_log_index,
                    last_log_term,
                }
            } else {
                Message::RequestVote {
                    term,
                    last_log_index,
                    last_log_term,
                }
            }
        }
        1 => Message::PreVoteResponse {
            term,
            granted: flag(reader)?,
        },
        3 => Message::RequestVoteResponse {
            term,
            granted: flag(reader)?,
        },
        4 => {
            let prev_log_index = Index::new(reader.u64()?);
            let prev_log_term = Term::new(reader.u64()?);
            let leader_commit = Index::new(reader.u64()?);
            let round = reader.u64()?;
            let count = reader.u64()?;
            // Not `with_capacity(count)`: the count is a claim, and a large one would allocate
            // before a single entry has been checked.
            let mut entries = Vec::new();
            for _ in 0..count {
                entries.push(decode_entry(&reader.bytes()?).ok()?);
            }
            Message::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                round,
            }
        }
        5 => {
            let round = reader.u64()?;
            let outcome = match reader.u8()? {
                0 => AppendOutcome::Accepted {
                    match_index: Index::new(reader.u64()?),
                },
                1 => AppendOutcome::Rejected {
                    prev_log_index: Index::new(reader.u64()?),
                    retry_from: Index::new(reader.u64()?),
                },
                _ => return None,
            };
            Message::AppendEntriesResponse {
                term,
                outcome,
                round,
            }
        }
        _ => return None,
    })
}

impl PeerFrame {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::Message(message) => {
                out.push(0);
                put_message(&mut out, message);
            }
            Self::Snapshot {
                term,
                round,
                snapshot,
            } => {
                out.push(1);
                put_u64(&mut out, term.get());
                put_u64(&mut out, *round);
                snapshot.encode(&mut out);
            }
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        exactly(bytes, |reader| match reader.u8()? {
            0 => message(reader).map(Self::Message),
            1 => Some(Self::Snapshot {
                term: Term::new(reader.u64()?),
                round: reader.u64()?,
                snapshot: Snapshot::decode(reader)?,
            }),
            _ => None,
        })
    }
}

fn put_change(out: &mut Vec<u8>, change: Change) {
    let (tag, id) = match change {
        Change::AddLearner(id) => (0, id),
        Change::Promote(id) => (1, id),
        Change::Remove(id) => (2, id),
    };
    out.push(tag);
    put_u64(out, id.get());
}

fn change(reader: &mut Reader<'_>) -> Option<Change> {
    let tag = reader.u8()?;
    let id = NodeId::new(reader.u64()?);
    match tag {
        0 => Some(Change::AddLearner(id)),
        1 => Some(Change::Promote(id)),
        2 => Some(Change::Remove(id)),
        _ => None,
    }
}

const CHANGE_ERRORS: [ChangeError; 7] = [
    ChangeError::AlreadyMember,
    ChangeError::NotALearner,
    ChangeError::NotAMember,
    ChangeError::LastVoter,
    ChangeError::InProgress,
    ChangeError::NotReady,
    ChangeError::NotCaughtUp,
];

const ROLES: [Role; 4] = [
    Role::Follower,
    Role::PreCandidate,
    Role::Candidate,
    Role::Leader,
];

/// The position of `value` in `table`, as the byte that stands for it on the wire.
fn code<T: PartialEq>(table: &[T], value: &T) -> u8 {
    let position = table.iter().position(|entry| entry == value);
    position
        .and_then(|position| u8::try_from(position).ok())
        .unwrap_or(u8::MAX)
}

impl ClientRequest {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::Op(request) => {
                out.push(0);
                put_bytes(&mut out, &request.encode());
            }
            Self::Change(change) => {
                out.push(1);
                put_change(&mut out, *change);
            }
            Self::Status => out.push(2),
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        exactly(bytes, |reader| match reader.u8()? {
            0 => Request::decode(&reader.bytes()?).map(Self::Op),
            1 => change(reader).map(Self::Change),
            2 => Some(Self::Status),
            _ => None,
        })
    }
}

impl ClientResponse {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::Reply { seq, reply } => {
                out.push(0);
                put_u64(&mut out, *seq);
                let mut body = Vec::new();
                reply.encode(&mut body);
                put_bytes(&mut out, &body);
            }
            Self::NotLeader { leader } => {
                out.push(1);
                put_node(&mut out, *leader);
            }
            Self::ChangeAccepted => out.push(2),
            Self::ChangeRejected(reason) => {
                out.push(3);
                out.push(code(&CHANGE_ERRORS, reason));
            }
            Self::Status(status) => {
                out.push(4);
                put_u64(&mut out, status.id.get());
                out.push(code(&ROLES, &status.role));
                put_u64(&mut out, status.term.get());
                put_node(&mut out, status.leader);
                put_u64(&mut out, status.commit.get());
                put_bytes(&mut out, &status.membership.encode());
            }
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        exactly(bytes, |reader| match reader.u8()? {
            0 => Some(Self::Reply {
                seq: reader.u64()?,
                reply: Reply::decode(&reader.bytes()?)?,
            }),
            1 => Some(Self::NotLeader {
                leader: node(reader).ok()?,
            }),
            2 => Some(Self::ChangeAccepted),
            3 => CHANGE_ERRORS
                .get(usize::from(reader.u8()?))
                .copied()
                .map(Self::ChangeRejected),
            4 => Some(Self::Status(Status {
                id: NodeId::new(reader.u64()?),
                role: *ROLES.get(usize::from(reader.u8()?))?,
                term: Term::new(reader.u64()?),
                leader: node(reader).ok()?,
                commit: Index::new(reader.u64()?),
                membership: Membership::decode(&reader.bytes()?)?,
            })),
            _ => None,
        })
    }
}

impl Snapshot {
    /// Appends the snapshot: where it ends, the membership there, and every row.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        let (meta, membership, rows) = self.parts();
        put_u64(out, meta.index().get());
        put_u64(out, meta.term().get());
        put_optional(out, membership.map(Membership::encode).as_deref());
        put_u64(out, u64::try_from(rows.len()).unwrap_or(u64::MAX));
        for (key, value) in rows {
            put_bytes(out, key);
            put_bytes(out, value);
        }
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Option<Self> {
        let meta =
            crate::types::SnapshotMeta::new(Index::new(reader.u64()?), Term::new(reader.u64()?));
        let membership = match reader.optional().ok()? {
            None => None,
            Some(bytes) => Some(Membership::decode(&bytes)?),
        };
        let count = reader.u64()?;
        let mut rows = Vec::new();
        for _ in 0..count {
            rows.push((reader.bytes()?, reader.bytes()?));
        }
        Some(Self::from_parts(meta, membership, rows))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{
        CLIENT_FRAME, ClientRequest, ClientResponse, HELLO_FRAME, Hello, PEER_FRAME, PeerFrame,
        Status, read_frame, write_frame,
    };
    use crate::kv::{Op, Reply, Request, SessionId};
    use crate::membership::{Change, ChangeError, Membership};
    use crate::raft::Role;
    use crate::rng::Rng;
    use crate::state::Snapshot;
    use crate::types::{AppendOutcome, Entry, Index, Message, NodeId, Payload, SnapshotMeta, Term};

    fn members() -> Membership {
        Membership::new([1, 2, 3].map(NodeId::new), [NodeId::new(4)])
    }

    fn peer_frames() -> Vec<PeerFrame> {
        let (term, index) = (Term::new(7), Index::new(u64::MAX));
        let entries = vec![
            Entry::new(Term::new(6), Payload::Noop),
            Entry::new(term, Payload::Command(b"command".to_vec())),
            Entry::new(term, Payload::Command(Vec::new())),
            Entry::new(term, Payload::Config(members())),
        ];
        let rows = vec![(b"a".to_vec(), Vec::new()), (b"dk".to_vec(), vec![0; 300])];
        let meta = SnapshotMeta::new(Index::new(9), term);
        let messages = [
            Message::PreVote {
                term,
                last_log_index: index,
                last_log_term: Term::new(6),
            },
            Message::PreVoteResponse {
                term,
                granted: true,
            },
            Message::RequestVote {
                term,
                last_log_index: Index::ZERO,
                last_log_term: Term::new(0),
            },
            Message::RequestVoteResponse {
                term,
                granted: false,
            },
            Message::AppendEntries {
                term,
                prev_log_index: Index::new(3),
                prev_log_term: Term::new(6),
                entries,
                leader_commit: Index::new(2),
                round: 11,
            },
            Message::AppendEntries {
                term,
                prev_log_index: Index::ZERO,
                prev_log_term: Term::new(0),
                entries: Vec::new(),
                leader_commit: Index::ZERO,
                round: 0,
            },
            Message::AppendEntriesResponse {
                term,
                outcome: AppendOutcome::Accepted { match_index: index },
                round: 3,
            },
            Message::AppendEntriesResponse {
                term,
                outcome: AppendOutcome::Rejected {
                    prev_log_index: Index::new(5),
                    retry_from: Index::new(2),
                },
                round: 0,
            },
        ];
        let mut frames: Vec<PeerFrame> = messages.into_iter().map(PeerFrame::Message).collect();
        for membership in [None, Some(members())] {
            let snapshot = Snapshot::from_parts(meta, membership, rows.clone());
            frames.push(PeerFrame::Snapshot {
                term,
                round: 5,
                snapshot,
            });
        }
        frames.push(PeerFrame::Snapshot {
            term,
            round: 0,
            snapshot: Snapshot::from_parts(meta, None, Vec::new()),
        });
        frames
    }

    fn client_requests() -> Vec<ClientRequest> {
        let op = Op::CompareAndSet {
            key: b"k".to_vec(),
            expected: None,
            value: b"v".to_vec(),
        };
        vec![
            ClientRequest::Op(Request::new(SessionId::new(u64::MAX), 3, op)),
            ClientRequest::Op(Request::new(
                SessionId::new(1),
                1,
                Op::Get { key: Vec::new() },
            )),
            ClientRequest::Change(Change::AddLearner(NodeId::new(4))),
            ClientRequest::Change(Change::Promote(NodeId::new(4))),
            ClientRequest::Change(Change::Remove(NodeId::new(1))),
            ClientRequest::Status,
        ]
    }

    fn client_responses() -> Vec<ClientResponse> {
        let status = |role, leader| Status {
            id: NodeId::new(2),
            role,
            term: Term::new(9),
            leader,
            commit: Index::new(40),
            membership: members(),
        };
        let mut responses = vec![
            ClientResponse::Reply {
                seq: 8,
                reply: Reply::Done,
            },
            ClientResponse::Reply {
                seq: 0,
                reply: Reply::Value(None),
            },
            ClientResponse::Reply {
                seq: u64::MAX,
                reply: Reply::Value(Some(b"value".to_vec())),
            },
            ClientResponse::Reply {
                seq: 1,
                reply: Reply::Swapped(true),
            },
            ClientResponse::NotLeader { leader: None },
            ClientResponse::NotLeader {
                leader: Some(NodeId::new(3)),
            },
            ClientResponse::ChangeAccepted,
            ClientResponse::Status(status(Role::Leader, Some(NodeId::new(2)))),
            ClientResponse::Status(status(Role::PreCandidate, None)),
        ];
        let refusals = [
            ChangeError::AlreadyMember,
            ChangeError::NotALearner,
            ChangeError::NotAMember,
            ChangeError::LastVoter,
            ChangeError::InProgress,
            ChangeError::NotReady,
            ChangeError::NotCaughtUp,
        ];
        responses.extend(refusals.map(ClientResponse::ChangeRejected));
        responses
    }

    /// Decodes bytes as one kind of value and encodes the result again.
    type Decoder = fn(&[u8]) -> Option<Vec<u8>>;

    /// Every sample of every kind, encoded, with a decoder that says whether bytes are accepted
    /// as that kind and decode back to the same bytes.
    fn encodings() -> Vec<(Vec<u8>, Decoder)> {
        let mut all: Vec<(Vec<u8>, Decoder)> = Vec::new();
        for hello in [Hello::Client, Hello::Peer(NodeId::new(5))] {
            all.push((hello.encode(), |bytes| {
                Hello::decode(bytes).map(Hello::encode)
            }));
        }
        for frame in peer_frames() {
            all.push((frame.encode(), |bytes| {
                PeerFrame::decode(bytes).map(|frame| frame.encode())
            }));
        }
        for request in client_requests() {
            all.push((request.encode(), |bytes| {
                ClientRequest::decode(bytes).map(|request| request.encode())
            }));
        }
        for response in client_responses() {
            all.push((response.encode(), |bytes| {
                ClientResponse::decode(bytes).map(|response| response.encode())
            }));
        }
        all
    }

    #[test]
    fn everything_survives_encoding() {
        for frame in peer_frames() {
            assert_eq!(PeerFrame::decode(&frame.encode()), Some(frame));
        }
        for request in client_requests() {
            assert_eq!(ClientRequest::decode(&request.encode()), Some(request));
        }
        for response in client_responses() {
            assert_eq!(ClientResponse::decode(&response.encode()), Some(response));
        }
        for hello in [Hello::Client, Hello::Peer(NodeId::new(u64::MAX))] {
            assert_eq!(Hello::decode(&hello.encode()), Some(hello));
        }
    }

    #[test]
    fn no_prefix_and_no_padded_encoding_is_accepted() {
        for (bytes, decode) in encodings() {
            for cut in 0..bytes.len() {
                assert_eq!(
                    decode(&bytes[..cut]),
                    None,
                    "{cut} of {} bytes were accepted",
                    bytes.len()
                );
            }
            let mut padded = bytes.clone();
            padded.push(0);
            assert_eq!(decode(&padded), None);
        }
    }

    /// Whatever arrives, decoding returns. A changed byte gives another value or a refusal, never
    /// a panic and never an allocation sized by a number from the wire.
    #[test]
    fn damaged_and_random_bytes_never_panic() {
        let mut rng = Rng::new(1);
        for (bytes, decode) in encodings() {
            for position in 0..bytes.len() {
                for value in [0, 1, 0x7f, 0xff] {
                    let mut damaged = bytes.clone();
                    damaged[position] = value;
                    if let Some(again) = decode(&damaged) {
                        // What is accepted is a real value: it encodes back to what came in.
                        assert_eq!(again, damaged);
                    }
                }
            }
            for _ in 0..200 {
                let length = usize::try_from(rng.below(64)).expect("small");
                let noise: Vec<u8> = (0..length)
                    .map(|_| rng.next_u64().to_be_bytes()[0])
                    .collect();
                let _ = decode(&noise);
            }
        }
    }

    #[test]
    fn a_frame_survives_the_wire_and_damage_is_noticed() {
        let payload = b"some payload".to_vec();
        let mut wire = Vec::new();
        write_frame(&mut wire, &payload).expect("writing to memory");
        write_frame(&mut wire, &[]).expect("writing to memory");

        let mut reader = Cursor::new(&wire);
        assert_eq!(
            read_frame(&mut reader, PEER_FRAME).expect("a whole frame"),
            payload
        );
        assert_eq!(
            read_frame(&mut reader, PEER_FRAME).expect("an empty frame"),
            Vec::<u8>::new()
        );
        assert_eq!(
            read_frame(&mut reader, PEER_FRAME)
                .expect_err("nothing left")
                .kind(),
            std::io::ErrorKind::UnexpectedEof
        );

        // One bit of the payload flipped.
        let mut flipped = wire.clone();
        flipped[10] ^= 1;
        assert_eq!(
            read_frame(&mut Cursor::new(&flipped), PEER_FRAME)
                .expect_err("damage")
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        // A length that claims more than any frame may hold is refused before anything is read.
        let mut huge = wire;
        huge[..4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(
            read_frame(&mut Cursor::new(&huge), PEER_FRAME)
                .expect_err("too large")
                .kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    /// The header is a claim. It is held against the limit of the connection it arrives on, and
    /// a claim that is never backed by bytes ends as a short read, not as a buffer of that size.
    #[test]
    fn a_frame_is_held_to_the_limit_of_its_connection() {
        let mut wire = Vec::new();
        write_frame(&mut wire, &[7; 100]).expect("writing to memory");
        assert_eq!(
            read_frame(&mut Cursor::new(&wire), CLIENT_FRAME)
                .expect("under the limit")
                .len(),
            100
        );
        let refused = read_frame(&mut Cursor::new(&wire), HELLO_FRAME).expect_err("over the limit");
        assert_eq!(refused.kind(), std::io::ErrorKind::InvalidData);

        // The largest frame a client connection allows is announced, and three bytes follow.
        let mut promise = u32::try_from(CLIENT_FRAME)
            .expect("the limit fits a header")
            .to_be_bytes()
            .to_vec();
        promise.extend_from_slice(&[0, 0, 0, 0, 1, 2, 3]);
        let short =
            read_frame(&mut Cursor::new(&promise), CLIENT_FRAME).expect_err("the bytes never came");
        assert_eq!(short.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn a_greeting_from_something_else_is_turned_away() {
        assert_eq!(Hello::decode(b"GET / HTTP/1.1"), None);
        let mut other_version = Hello::Client.encode();
        other_version[2] += 1;
        assert_eq!(Hello::decode(&other_version), None);
    }
}
