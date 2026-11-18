//! A node on the network.
//!
//! One thread owns the node and is the only one that touches it. Every other thread turns bytes
//! into events for it, or its effects into bytes:
//!
//! - one thread accepts connections;
//! - one thread per incoming connection reads frames, from a peer or from a client;
//! - one thread per peer writes frames to it, connecting again whenever the link breaks.
//!
//! Blocking sockets and threads, no async runtime. A cluster has a handful of peers, and the
//! thread per client connection is the first thing to replace when there are many clients.
//!
//! Nothing here is needed for correctness. A message that is lost, late or sent twice is what
//! the core is built for, so a full queue or a broken link simply drops it.

use core::convert::Infallible;
use core::fmt;
use core::time::Duration;
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::io;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::Instant;

use katman::Options;

use crate::config::Config;
use crate::node::{Compaction, ConnId, Effect, Event, Node, NodeError};
use crate::types::NodeId;
use crate::wire::{
    CLIENT_FRAME, ClientRequest, ClientResponse, HELLO_FRAME, Hello, PEER_FRAME, PeerFrame,
    read_frame, write_frame,
};

/// Frames waiting for one peer. When the peer is slow or away, newer frames are dropped.
const PEER_QUEUE: usize = 1024;
/// How long a write to a client or a peer may block before the connection is given up.
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a new connection has to say who it is.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

/// How to run one node.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// This node's id.
    pub id: NodeId,
    /// The address to listen on, for peers and clients alike.
    pub listen: SocketAddr,
    /// Where the other nodes listen. Unless `joining` is set, these are also the voters the
    /// cluster is founded with, together with this node.
    pub peers: Vec<(NodeId, SocketAddr)>,
    /// Where the log and the state machine live.
    pub data_dir: PathBuf,
    /// The node joins a cluster that already exists. It knows of no members until a leader adds
    /// it; `peers` then only says where to reach the others.
    pub joining: bool,
    /// The length of one tick. An election timeout is 10 to 19 ticks and a heartbeat goes out
    /// every 2, so 50 milliseconds gives half a second to a second.
    pub tick: Duration,
}

/// Why a node stopped.
#[derive(Debug)]
#[non_exhaustive]
pub enum ServerError {
    /// The listening address could not be bound.
    Io(io::Error),
    /// The node could not start or could no longer trust its state.
    Node(NodeError),
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "network: {error}"),
            Self::Node(error) => error.fmt(f),
        }
    }
}

impl core::error::Error for ServerError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Node(error) => Some(error),
        }
    }
}

impl From<io::Error> for ServerError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<NodeError> for ServerError {
    fn from(error: NodeError) -> Self {
        Self::Node(error)
    }
}

/// The write halves of the client connections, by connection.
type Clients = Arc<Mutex<HashMap<ConnId, TcpStream>>>;

/// Runs a node until it fails. It does not return otherwise: a node is stopped by stopping the
/// process, and everything it has acknowledged is on disk by then.
///
/// # Errors
///
/// Returns a [`ServerError`] when the address cannot be bound, the stored state cannot be
/// opened, or the disk fails while the node runs.
pub fn serve(config: &ServerConfig) -> Result<Infallible, ServerError> {
    let listener = TcpListener::bind(config.listen)?;
    let others: Vec<(NodeId, SocketAddr)> = config
        .peers
        .iter()
        .copied()
        .filter(|(peer, _)| *peer != config.id)
        .collect();
    // Different on every start, so that two nodes started together do not time out together.
    let seed = RandomState::new().hash_one(config.id.get());
    let raft = if config.joining {
        Config::joining(config.id, seed)
    } else {
        Config::new(config.id, others.iter().map(|(peer, _)| *peer), seed)
    };
    let mut node = Node::open(
        &config.data_dir,
        raft,
        &Options::default(),
        Compaction::default(),
    )?;

    let (events, inbox) = mpsc::channel();
    let clients = Clients::default();
    let mut links = HashMap::new();
    for (peer, address) in others {
        let (frames, queue) = mpsc::sync_channel(PEER_QUEUE);
        let own = config.id;
        thread::spawn(move || write_to_peer(own, address, &queue));
        links.insert(peer, frames);
    }
    {
        let clients = Arc::clone(&clients);
        thread::spawn(move || accept(&listener, &events, &clients));
    }
    run(&mut node, &inbox, &links, &clients, config.tick)
}

/// The node's own thread: one event at a time, a tick whenever one is due.
fn run(
    node: &mut Node,
    inbox: &Receiver<Event>,
    links: &HashMap<NodeId, SyncSender<Vec<u8>>>,
    clients: &Clients,
    tick: Duration,
) -> Result<Infallible, ServerError> {
    let mut next_tick = Instant::now() + tick;
    loop {
        let now = Instant::now();
        let event = if now >= next_tick {
            // A node that was busy for a while ticks once, not once for every tick it missed.
            next_tick = (next_tick + tick).max(now);
            Event::Tick
        } else {
            match inbox.recv_timeout(next_tick - now) {
                Ok(event) => event,
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => continue,
            }
        };
        for effect in node.handle(event)? {
            match effect {
                Effect::ToPeer { to, frame } => {
                    if let Some(link) = links.get(&to) {
                        // A full queue means the peer is slow or away. The core sends again.
                        let _ = link.try_send(frame.encode());
                    }
                }
                Effect::ToClient { conn, response } => answer(clients, conn, &response),
            }
        }
    }
}

// ponytail: the node's thread writes to the client itself, and waits up to WRITE_TIMEOUT for a
// client that does not read. A writer thread per client, as for peers, takes that off the node.
fn answer(clients: &Clients, conn: ConnId, response: &ClientResponse) {
    let mut clients = clients.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(mut stream) = clients.get(&conn) else {
        return;
    };
    if write_frame(&mut stream, &response.encode()).is_err() {
        // Closing both halves wakes the thread that reads from this client, which tidies up.
        let _ = stream.shutdown(Shutdown::Both);
        clients.remove(&conn);
    }
}

fn accept(listener: &TcpListener, events: &Sender<Event>, clients: &Clients) {
    let next_conn = Arc::new(AtomicU64::new(1));
    for stream in listener.incoming() {
        let Ok(stream) = stream else {
            continue;
        };
        let events = events.clone();
        let clients = Arc::clone(clients);
        let conn = next_conn.fetch_add(1, Ordering::Relaxed);
        thread::spawn(move || {
            // Any error ends the connection, and that is all it needs to do.
            let _ = read_connection(stream, conn, &events, &clients);
        });
    }
}

/// Reads one incoming connection to its end.
fn read_connection(
    mut stream: TcpStream,
    conn: ConnId,
    events: &Sender<Event>,
    clients: &Clients,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(HELLO_TIMEOUT))?;
    let hello = Hello::decode(&read_frame(&mut stream, HELLO_FRAME)?);
    stream.set_read_timeout(None)?;
    match hello {
        None => Ok(()),
        Some(Hello::Peer(from)) => loop {
            let Some(frame) = PeerFrame::decode(&read_frame(&mut stream, PEER_FRAME)?) else {
                return Ok(());
            };
            if events.send(Event::Peer { from, frame }).is_err() {
                return Ok(());
            }
        },
        Some(Hello::Client) => {
            let writer = stream.try_clone()?;
            writer.set_write_timeout(Some(WRITE_TIMEOUT))?;
            clients
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(conn, writer);
            let outcome = read_client(&mut stream, conn, events);
            clients
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&conn);
            let _ = events.send(Event::Disconnected(conn));
            outcome
        }
    }
}

fn read_client(stream: &mut TcpStream, conn: ConnId, events: &Sender<Event>) -> io::Result<()> {
    loop {
        let Some(request) = ClientRequest::decode(&read_frame(stream, CLIENT_FRAME)?) else {
            return Ok(());
        };
        if events.send(Event::Client { conn, request }).is_err() {
            return Ok(());
        }
    }
}

/// Sends frames to one peer for as long as the node runs. The connection is opened when there
/// is something to send and opened again when it breaks. A frame that cannot be sent is dropped.
fn write_to_peer(own: NodeId, address: SocketAddr, queue: &Receiver<Vec<u8>>) {
    let mut link: Option<TcpStream> = None;
    while let Ok(frame) = queue.recv() {
        if link.is_none() {
            link = connect(own, address).ok();
        }
        let Some(stream) = link.as_mut() else {
            // The peer is away. Do not spin on the frames that keep arriving for it.
            thread::sleep(Duration::from_millis(100));
            continue;
        };
        if write_frame(stream, &frame).is_err() {
            link = None;
        }
    }
}

fn connect(own: NodeId, address: SocketAddr) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(500))?;
    stream.set_nodelay(true)?;
    stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
    write_frame(&mut stream, &Hello::Peer(own).encode())?;
    Ok(stream)
}
