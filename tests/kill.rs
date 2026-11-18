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

#![expect(
    clippy::expect_used,
    clippy::panic,
    reason = "a test: when the setup fails, the test fails"
)]

use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use quorate::linearizability::{Call, Operation, Return, linearizable};
use quorate::{Client, NodeId, Role};

const NODES: u64 = 3;
const CLIENTS: u64 = 4;
const KEYS: usize = 2;
/// Few values, so that a compare-and-set often finds what it expects.
const VALUES: u64 = 3;
const KILLS: usize = 4;

struct Cluster {
    dir: PathBuf,
    addresses: Vec<(NodeId, SocketAddr)>,
    processes: Vec<Option<Child>>,
}

impl Cluster {
    fn start() -> Self {
        // Ports the system considers free right now. Another process could take one before the
        // node binds it; the test then fails at startup and says so.
        let listeners: Vec<TcpListener> = (0..NODES)
            .map(|_| TcpListener::bind("127.0.0.1:0").expect("a free port"))
            .collect();
        let addresses = listeners
            .iter()
            .zip(1..)
            .map(|(listener, id)| {
                (
                    NodeId::new(id),
                    listener.local_addr().expect("a bound address"),
                )
            })
            .collect();
        drop(listeners);

        let unique = std::process::id();
        let dir = std::env::temp_dir().join(format!("quorate-kill-{unique}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("a temporary directory");

        let mut cluster = Self {
            dir,
            addresses,
            processes: Vec::new(),
        };
        for position in 0..cluster.addresses.len() {
            cluster.processes.push(None);
            cluster.spawn(position);
        }
        cluster
    }

    fn spawn(&mut self, position: usize) {
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
        self.processes[position] = Some(command.spawn().expect("the node starts"));
    }

    /// Ends a node the way a crash would: no warning, no chance to finish anything.
    fn kill(&mut self, position: usize) {
        if let Some(mut process) = self.processes[position].take() {
            process.kill().expect("the node can be killed");
            process.wait().expect("the node has gone");
        }
    }

    /// The position of the node that leads, according to a majority that agrees on it.
    fn leader(&self) -> Option<usize> {
        let client = Client::new(self.addresses.iter().copied())
            .timeouts(Duration::from_millis(300), Duration::from_millis(300));
        let position = self.addresses.iter().position(|(id, _)| {
            client
                .status(*id)
                .is_ok_and(|status| status.role == Role::Leader)
        })?;
        self.processes[position].is_some().then_some(position)
    }

    fn wait_for_leader(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if let Some(leader) = self.leader() {
                return leader;
            }
            thread::sleep(Duration::from_millis(100));
        }
        panic!("no leader within 20 seconds");
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for position in 0..self.processes.len() {
            self.kill(position);
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A small generator, so that a failing run can at least be described.
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

fn value_of(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(
        bytes
            .try_into()
            .expect("clients only write eight-byte values"),
    )
}

/// One client: random operations until told to stop, each written into the history of its key
/// with the time it was sent and the time it was answered.
fn work(
    servers: &[(NodeId, SocketAddr)],
    seed: u64,
    clock: &AtomicU64,
    histories: &[Mutex<Vec<Operation>>],
    stop: &AtomicBool,
) {
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
    while !stop.load(Ordering::Relaxed) {
        let key = usize::try_from(random.below(KEYS as u64)).expect("few keys");
        let name = [b'k', u8::try_from(key).expect("few keys")];
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

        let invoked = clock.fetch_add(1, Ordering::SeqCst);
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
            .map(|result| (clock.fetch_add(1, Ordering::SeqCst), result));
        histories[key]
            .lock()
            .expect("no client panics")
            .push(Operation {
                call,
                invoked,
                returned,
            });
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn killing_the_leader_under_load_loses_nothing_and_shows_nothing_stale() {
    let mut cluster = Cluster::start();
    cluster.wait_for_leader();

    let clock = AtomicU64::new(1);
    let histories: Vec<Mutex<Vec<Operation>>> = (0..KEYS).map(|_| Mutex::new(Vec::new())).collect();
    let stop = AtomicBool::new(false);
    let servers = cluster.addresses.clone();

    let mut killed = 0;
    thread::scope(|scope| {
        for seed in 0..CLIENTS {
            let (servers, clock, histories, stop) = (&servers, &clock, &histories, &stop);
            scope.spawn(move || work(servers, seed, clock, histories, stop));
        }

        for _ in 0..KILLS {
            thread::sleep(Duration::from_millis(1500));
            let leader = cluster.wait_for_leader();
            cluster.kill(leader);
            killed += 1;
            // The other two elect a new leader and carry on while this one is away.
            thread::sleep(Duration::from_millis(1500));
            cluster.spawn(leader);
        }
        thread::sleep(Duration::from_millis(1500));
        stop.store(true, Ordering::Relaxed);
    });

    let mut answered = 0;
    let mut unanswered = 0;
    for (key, history) in histories.iter().enumerate() {
        let mut history = history.lock().expect("no client panics").clone();
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
    // A cluster that answered almost nothing would pass the check without being tested.
    assert!(
        answered > 500,
        "only {answered} operations were answered ({unanswered} were not)"
    );
}
