//! A client for a quorate cluster.
//!
//! The client keeps one connection, to the node it believes leads. When that node says it does
//! not lead, or does not answer in time, the client moves on to another and asks again. It asks
//! with the same session and sequence number, so a request that did go through the first time is
//! not carried out a second time.

use core::fmt;
use core::time::Duration;
use std::hash::{BuildHasher, RandomState};
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::Instant;

use crate::kv::{Op, Reply, Request, SessionId};
use crate::membership::{Change, ChangeError};
use crate::types::NodeId;
use crate::wire::{
    CLIENT_FRAME, ClientRequest, ClientResponse, Hello, Status, read_frame, write_frame,
};

/// Why a request did not get an answer.
#[derive(Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// The client was given no servers.
    NoServers,
    /// No node answered in time. **The request may or may not have been carried out.** Sending
    /// it again through the same client is safe.
    Timeout,
    /// The node asked is not one of the client's servers.
    UnknownServer(NodeId),
    /// The leader refused a membership change.
    Rejected(ChangeError),
    /// A node answered with something that does not fit the request.
    Protocol,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoServers => f.write_str("no servers to ask"),
            Self::Timeout => {
                f.write_str("no answer in time; the request may or may not have been carried out")
            }
            Self::UnknownServer(id) => write!(f, "server {} is not known to this client", id.get()),
            Self::Rejected(reason) => write!(f, "the leader refused: {reason}"),
            Self::Protocol => f.write_str("a server gave an answer that does not fit the request"),
        }
    }
}

impl core::error::Error for ClientError {}

/// A connection to a cluster, with a session of its own.
#[derive(Debug)]
pub struct Client {
    servers: Vec<(NodeId, SocketAddr)>,
    session: SessionId,
    seq: u64,
    /// The position in `servers` of the node to ask next.
    current: usize,
    stream: Option<TcpStream>,
    attempt: Duration,
    patience: Duration,
}

impl Client {
    /// A client that knows these servers. It picks a random session id.
    pub fn new(servers: impl IntoIterator<Item = (NodeId, SocketAddr)>) -> Self {
        Self {
            servers: servers.into_iter().collect(),
            session: SessionId::new(RandomState::new().hash_one(0_u8)),
            seq: 0,
            current: 0,
            stream: None,
            attempt: Duration::from_secs(1),
            patience: Duration::from_secs(10),
        }
    }

    /// Sets how long to wait for one node before trying the next, and how long to keep trying
    /// altogether before giving up with [`ClientError::Timeout`].
    #[must_use]
    pub const fn timeouts(mut self, attempt: Duration, patience: Duration) -> Self {
        self.attempt = attempt;
        self.patience = patience;
        self
    }

    /// Reads a key. The answer is linearizable: it reflects every write that was acknowledged
    /// before this call began.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError`] when no node answers in time.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        match self.op(Op::Get { key: key.to_vec() })? {
            Reply::Value(value) => Ok(value),
            _ => Err(ClientError::Protocol),
        }
    }

    /// Sets a key, whatever it held.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError`] when no node answers in time. The write may still have happened.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ClientError> {
        match self.op(Op::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        })? {
            Reply::Done => Ok(()),
            _ => Err(ClientError::Protocol),
        }
    }

    /// Removes a key. Removing a key that is not there is not an error.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError`] when no node answers in time. The delete may still have happened.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), ClientError> {
        match self.op(Op::Delete { key: key.to_vec() })? {
            Reply::Done => Ok(()),
            _ => Err(ClientError::Protocol),
        }
    }

    /// Sets a key only if it holds `expected`; `None` means it must be absent. Returns whether
    /// the value was written.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError`] when no node answers in time. The write may still have happened.
    pub fn compare_and_set(
        &mut self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool, ClientError> {
        let op = Op::CompareAndSet {
            key: key.to_vec(),
            expected: expected.map(<[u8]>::to_vec),
            value: value.to_vec(),
        };
        match self.op(op)? {
            Reply::Swapped(swapped) => Ok(swapped),
            _ => Err(ClientError::Protocol),
        }
    }

    /// Asks the leader for one membership change. `Ok` means the leader has put the change into
    /// its log, not that it has committed.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Rejected`] with the leader's reason, or another [`ClientError`]
    /// when no node answers in time.
    pub fn change_membership(&mut self, change: Change) -> Result<(), ClientError> {
        match self.ask(&ClientRequest::Change(change), None)? {
            ClientResponse::ChangeAccepted => Ok(()),
            ClientResponse::ChangeRejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::Protocol),
        }
    }

    /// Asks one particular node how it sees itself and the cluster.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::UnknownServer`] for a node the client was not given, and
    /// [`ClientError::Timeout`] when the node does not answer.
    pub fn status(&self, server: NodeId) -> Result<Status, ClientError> {
        let address = self
            .servers
            .iter()
            .find(|(id, _)| *id == server)
            .map(|(_, address)| *address);
        let address = address.ok_or(ClientError::UnknownServer(server))?;
        let mut stream = self.connect(address).map_err(|_| ClientError::Timeout)?;
        match exchange(&mut stream, &ClientRequest::Status) {
            Ok(ClientResponse::Status(status)) => Ok(status),
            Ok(_) => Err(ClientError::Protocol),
            Err(_) => Err(ClientError::Timeout),
        }
    }

    fn op(&mut self, op: Op) -> Result<Reply, ClientError> {
        self.seq += 1;
        let seq = self.seq;
        match self.ask(
            &ClientRequest::Op(Request::new(self.session, seq, op)),
            Some(seq),
        )? {
            ClientResponse::Reply { reply, .. } => Ok(reply),
            _ => Err(ClientError::Protocol),
        }
    }

    /// Sends a request until some node answers it for good. `seq` is the sequence number a
    /// reply has to carry to count.
    fn ask(
        &mut self,
        request: &ClientRequest,
        seq: Option<u64>,
    ) -> Result<ClientResponse, ClientError> {
        if self.servers.is_empty() {
            return Err(ClientError::NoServers);
        }
        let deadline = Instant::now() + self.patience;
        while Instant::now() < deadline {
            match self.attempt(request) {
                Ok(ClientResponse::NotLeader { leader }) => {
                    let named = leader
                        .and_then(|leader| self.servers.iter().position(|(id, _)| *id == leader));
                    match named {
                        Some(position) if position != self.current => self.current = position,
                        // It does not know, or still names itself: an election is under way.
                        _ => {
                            self.current = (self.current + 1) % self.servers.len();
                            thread::sleep(Duration::from_millis(50));
                        }
                    }
                    self.stream = None;
                }
                // An answer to an earlier request on this connection. It cannot happen while
                // every timeout drops the connection, and is not trusted if it does.
                Ok(ClientResponse::Reply { seq: answered, .. }) if seq != Some(answered) => {
                    self.stream = None;
                }
                Ok(response) => return Ok(response),
                Err(_) => {
                    self.stream = None;
                    self.current = (self.current + 1) % self.servers.len();
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
        Err(ClientError::Timeout)
    }

    /// One request to the current node, over the kept connection or a new one.
    fn attempt(&mut self, request: &ClientRequest) -> io::Result<ClientResponse> {
        let stream = if let Some(stream) = self.stream.take() {
            stream
        } else {
            let address = self.servers.get(self.current).map(|(_, address)| *address);
            self.connect(address.ok_or(io::ErrorKind::NotFound)?)?
        };
        let stream = self.stream.insert(stream);
        exchange(stream, request)
    }

    fn connect(&self, address: SocketAddr) -> io::Result<TcpStream> {
        let mut stream = TcpStream::connect_timeout(&address, self.attempt)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(self.attempt))?;
        stream.set_write_timeout(Some(self.attempt))?;
        write_frame(&mut stream, &Hello::Client.encode())?;
        Ok(stream)
    }
}

fn exchange(stream: &mut TcpStream, request: &ClientRequest) -> io::Result<ClientResponse> {
    write_frame(stream, &request.encode())?;
    ClientResponse::decode(&read_frame(stream, CLIENT_FRAME)?)
        .ok_or_else(|| io::ErrorKind::InvalidData.into())
}
