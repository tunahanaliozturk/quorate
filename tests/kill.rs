//! Real processes, real sockets and real disks.
//!
//! Three `quorate serve` processes form a cluster. Clients read and write through the public
//! client while the test keeps finding the leader and killing it, the hard way, then starting it
//! again. Afterwards everything the clients saw has to be linearizable: no acknowledged write
//! lost, no stale read, no request carried out twice.
//!
//! The simulator covers far more interleavings than this ever will. What it cannot cover is the
//! code it does not run: the wire format, the sockets, the threads, the real filesystem and the
//! process actually dying.

use std::hash::{BuildHasher, RandomState};
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use quorate::linearizability::{Call, Operation, Return, linearizable};
use quorate::{Client, NodeId, Role};

const NODES: u64 = 3;
const CLIENTS: u64 = 4;
const KEYS: u8 = 2;
/// Few values, so that a compare-and-set often finds what it expects.
const VALUES: u64 = 3;
const KILLS: usize = 4;

/// A new directory under the system's temporary directory, for this run alone.
///
/// The name cannot be guessed, the directory must not exist yet, and on Unix only its owner can
/// enter it. A fixed name in a shared temporary directory could be prepared by someone else:
/// as a link to somewhere this process would then write, or as a directory they can read.
fn fresh_dir(prefix: &str) -> io::Result<PathBuf> {
    let unique = RandomState::new().hash_one(std::process::id());
    let dir = std::env::temp_dir().join(format!("{prefix}-{unique:016x}"));
    let builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    let builder = {
        let mut builder = builder;
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder
    };
    // Not recursive, so it fails if anything is already there.
    builder.create(&dir)?;
    Ok(dir)
}

struct Cluster {
    dir: PathBuf,
    addresses: Vec<(NodeId, SocketAddr)>,
    processes: Vec<Option<Child>>,
}

impl Cluster {
    fn start() -> io::Result<Self> {
        // Ports the system considers free right now. Another process could take one before the
        // node binds it; the test then fails at startup and says so.
        let mut addresses = Vec::new();
        let mut listeners = Vec::new();
        for id in 1..=NODES {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            addresses.push((NodeId::new(id), listener.local_addr()?));
            listeners.push(listener);
        }
        drop(listeners);

        let dir = fresh_dir("quorate-kill")?;

        let mut cluster = Self {
            dir,
            addresses,
            processes: Vec::new(),
        };
        for position in 0..cluster.addresses.len() {
            cluster.processes.push(None);
            cluster.spawn(position)?;
        }
        Ok(cluster)
    }

    fn spawn(&mut self, position: usize) -> io::Result<()> {
        let (id, address) = self.addresses[position];
        let mut command = Command::new(env!("CARGO_BIN_EXE_quorate"));
        command.arg("serve");
        command.args(["--id", &id.get().to_string()]);
        command.args(["--listen", &address.to_string()]);
        command
            .arg("--data")
            .arg(self.dir.join(format!("node-{}", id.get())));
        for (peer, address) in &self.addresses {
            command.args(["--peer", &format!("{}={address}", peer.get())]);
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        self.processes[position] = Some(command.spawn()?);
        Ok(())
    }

    /// Ends a node the way a crash would: no warning, no chance to finish anything.
    fn kill(&mut self, position: usize) -> io::Result<()> {
        if let Some(mut process) = self.processes[position].take() {
            process.kill()?;
            process.wait()?;
        }
        Ok(())
    }

    /// The position of a running node that says it leads.
    fn leader(&self) -> Option<usize> {
        let quick = Duration::from_millis(300);
        let client = Client::new(self.addresses.iter().copied()).timeouts(quick, quick);
        let leads = |id: NodeId| {
            client
                .status(id)
                .is_ok_and(|status| status.role == Role::Leader)
        };
        let position = self.addresses.iter().position(|(id, _)| leads(*id))?;
        self.processes[position].is_some().then_some(position)
    }

    fn wait_for_leader(&self) -> io::Result<usize> {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Some(leader) = self.leader() {
                return Ok(leader);
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err(io::Error::other("no leader within 30 seconds"))
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for position in 0..self.processes.len() {
            // Whatever is still running goes; there is nothing to do about one that will not.
            let _ = self.kill(position);
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A small generator. Different clients do different things; nothing depends on more than that.
struct Random(u64);

impl Random {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

fn value_bytes(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// The number a stored value stands for. Anything that is not eight bytes was not written by
/// this test, and becomes a number no client ever writes, which the checker then rejects.
fn value_of(bytes: &[u8]) -> u64 {
    bytes.try_into().map_or(u64::MAX, u64::from_be_bytes)
}

/// What the clients did and saw, per key, on one shared clock.
struct Recorder {
    clock: AtomicU64,
    histories: Vec<Mutex<Vec<Operation>>>,
    stop: AtomicBool,
}

/// One client: random operations until told to stop, each written into the history of its key
/// with the time it was sent and the time it was answered.
fn work(servers: &[(NodeId, SocketAddr)], seed: u64, recorder: &Recorder) {
    // Every other client is impatient: it gives up on a node after a few milliseconds, which is
    // about as long as a write takes, and asks again elsewhere. Many of its requests therefore
    // reach the log twice. Without that, a request is only ever repeated when a leader dies, and
    // a cluster that carried out repeats twice would get through this test most of the time.
    let attempt = if seed.is_multiple_of(2) {
        Duration::from_millis(500)
    } else {
        Duration::from_millis(4)
    };
    let mut client = Client::new(servers.iter().copied()).timeouts(attempt, Duration::from_secs(8));
    let mut random = Random(0x9E37_79B9_7F4A_7C15 ^ seed);
    while !recorder.stop.load(Ordering::Relaxed) {
        let key = u8::try_from(random.below(u64::from(KEYS))).unwrap_or(0);
        let name = [b'k', key];
        let value = random.below(VALUES);
        let call = match random.below(4) {
            0 => Call::Get,
            1 => Call::Put(value),
            2 => Call::Delete,
            _ => {
                let expected = match random.below(VALUES + 1) {
                    0 => None,
                    other => Some(other - 1),
                };
                Call::CompareAndSet { expected, value }
            }
        };

        let invoked = recorder.clock.fetch_add(1, Ordering::SeqCst);
        let result = match call {
            Call::Get => client
                .get(&name)
                .map(|found| Return::Value(found.as_deref().map(value_of))),
            Call::Put(value) => client
                .put(&name, &value_bytes(value))
                .map(|()| Return::Done),
            Call::Delete => client.delete(&name).map(|()| Return::Done),
            Call::CompareAndSet { expected, value } => {
                let expected = expected.map(value_bytes);
                let expected = expected.as_ref().map(<[u8; 8]>::as_slice);
                client
                    .compare_and_set(&name, expected, &value_bytes(value))
                    .map(Return::Swapped)
            }
        };
        // Taken after the answer arrived. An operation that got no answer may have taken effect
        // at any later time, or never; the checker allows for both.
        let returned = result
            .ok()
            .map(|result| (recorder.clock.fetch_add(1, Ordering::SeqCst), result));
        let mut history = recorder.histories[usize::from(key)]
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        history.push(Operation {
            call,
            invoked,
            returned,
        });
        drop(history);
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn killing_the_leader_under_load_loses_nothing_and_shows_nothing_stale() -> io::Result<()> {
    let mut cluster = Cluster::start()?;
    cluster.wait_for_leader()?;

    let recorder = Recorder {
        clock: AtomicU64::new(1),
        histories: (0..KEYS).map(|_| Mutex::new(Vec::new())).collect(),
        stop: AtomicBool::new(false),
    };
    let servers = cluster.addresses.clone();

    let killed = thread::scope(|scope| {
        for seed in 0..CLIENTS {
            let (servers, recorder) = (&servers, &recorder);
            scope.spawn(move || work(servers, seed, recorder));
        }
        // The clients stop whatever happens here, or the scope would wait for them for ever.
        let outcome = (|| {
            for _ in 0..KILLS {
                thread::sleep(Duration::from_millis(1500));
                let leader = cluster.wait_for_leader()?;
                cluster.kill(leader)?;
                // The other two elect a new leader and carry on while this one is away.
                thread::sleep(Duration::from_millis(1500));
                cluster.spawn(leader)?;
            }
            thread::sleep(Duration::from_millis(1500));
            io::Result::Ok(KILLS)
        })();
        recorder.stop.store(true, Ordering::Relaxed);
        outcome
    })?;

    let mut answered = 0;
    let mut unanswered = 0;
    for (key, history) in recorder.histories.iter().enumerate() {
        let mut history = history
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        history.sort_by_key(|operation| operation.invoked);
        answered += history
            .iter()
            .filter(|operation| operation.returned.is_some())
            .count();
        unanswered += history
            .iter()
            .filter(|operation| operation.returned.is_none())
            .count();
        assert!(
            linearizable(&history),
            "the history of key {key} is not linearizable: {history:?}"
        );
    }
    assert_eq!(killed, KILLS);
    // A cluster that answered almost nothing would pass the check without being tested. The bar
    // is low on purpose: a laptop answers about 9,000 operations in a run, a busy CI machine with
    // a slow disk a few hundred.
    assert!(
        answered > 100,
        "only {answered} operations were answered ({unanswered} were not)"
    );
    Ok(())
}
