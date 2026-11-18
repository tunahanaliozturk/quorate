//! `quorate bench`: measures a cluster of real processes on this machine.
//!
//! The nodes are separate `quorate serve` processes talking over loopback, each with its own
//! directory, all on the same disk. That is the cheapest setup that still runs everything a
//! deployment would: the wire format, the sockets, and one fsync per write on every node.
//!
//! What it does not have is a network. Round trips over loopback take microseconds, so the
//! numbers say what the disk and the code cost, and are an upper bound for anything spread over
//! machines.

use core::time::Duration;
use std::io::{self, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

use quorate::{Client, NodeId, Role};

use crate::{Args, Failure, fail};

/// Keys written before a read workload, and read back at random.
const PRELOADED: u64 = 1_000;
const VALUE: [u8; 100] = [7; 100];

struct Cluster {
    dir: PathBuf,
    addresses: Vec<(NodeId, SocketAddr)>,
    processes: Vec<Option<Child>>,
}

impl Cluster {
    fn start(nodes: u64) -> Result<Self, Failure> {
        let mut addresses = Vec::new();
        let mut listeners = Vec::new();
        for id in 1..=nodes {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            addresses.push((NodeId::new(id), listener.local_addr()?));
            listeners.push(listener);
        }
        drop(listeners);
        let dir = std::env::temp_dir().join(format!("quorate-bench-{}", std::process::id()));
        // Left over from a run that was interrupted, if at all.
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let mut cluster = Self {
            dir,
            addresses,
            processes: Vec::new(),
        };
        for position in 0..cluster.addresses.len() {
            cluster.processes.push(None);
            cluster.spawn(position)?;
        }
        cluster.leader()?;
        Ok(cluster)
    }

    fn spawn(&mut self, position: usize) -> io::Result<()> {
        let Some(&(id, address)) = self.addresses.get(position) else {
            return Ok(());
        };
        let mut command = Command::new(std::env::current_exe()?);
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
            .stderr(Stdio::null());
        if let Some(slot) = self.processes.get_mut(position) {
            *slot = Some(command.spawn()?);
        }
        Ok(())
    }

    fn kill(&mut self, position: usize) {
        if let Some(mut process) = self.processes.get_mut(position).and_then(Option::take) {
            // A process that is already gone is as good as killed.
            let _ = process.kill();
            let _ = process.wait();
        }
    }

    fn client(&self) -> Client {
        Client::new(self.addresses.iter().copied())
    }

    /// Waits for a node that says it leads, and returns its position.
    fn leader(&self) -> Result<usize, Failure> {
        let quick = Duration::from_millis(300);
        let client = self.client().timeouts(quick, quick);
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            let leads = |id: NodeId| {
                client
                    .status(id)
                    .is_ok_and(|status| status.role == Role::Leader)
            };
            if let Some(position) = self.addresses.iter().position(|(id, _)| leads(*id)) {
                return Ok(position);
            }
            thread::sleep(Duration::from_millis(50));
        }
        fail("the cluster did not elect a leader within 30 seconds")
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Workload {
    Put,
    Get,
}

/// One timed run.
struct Sample {
    per_second: f64,
    /// Latencies in microseconds, sorted.
    latencies: Vec<u64>,
    failed: usize,
}

impl Sample {
    /// The latency below which the given share of operations finished, in milliseconds.
    fn percentile(&self, percent: usize) -> f64 {
        let position =
            (self.latencies.len() * percent / 100).min(self.latencies.len().saturating_sub(1));
        let micros = self.latencies.get(position).copied().unwrap_or(0);
        // Latencies are far below the range where this conversion loses anything.
        Duration::from_micros(micros).as_secs_f64() * 1_000.0
    }
}

fn key(number: u64) -> Vec<u8> {
    format!("key-{number}").into_bytes()
}

/// Runs `clients` clients for `seconds`, each sending one operation after another.
fn measure(cluster: &Cluster, workload: Workload, clients: u64, seconds: u64) -> Sample {
    let stop = AtomicBool::new(false);
    let started = Instant::now();
    let per_client: Vec<(Vec<u64>, usize)> = thread::scope(|scope| {
        let workers: Vec<_> = (0..clients)
            .map(|worker| {
                let stop = &stop;
                scope.spawn(move || {
                    let mut client = cluster.client();
                    let mut latencies = Vec::new();
                    let mut failed = 0;
                    let mut count = worker;
                    while !stop.load(Ordering::Relaxed) {
                        // A cheap way to spread over the preloaded keys without a generator.
                        count = count
                            .wrapping_mul(6_364_136_223_846_793_005)
                            .wrapping_add(1_442_695_040_888_963_407);
                        let name = key((count >> 33) % PRELOADED);
                        let sent = Instant::now();
                        let outcome = match workload {
                            Workload::Put => client.put(&name, &VALUE).is_ok(),
                            Workload::Get => client.get(&name).is_ok(),
                        };
                        if outcome {
                            latencies.push(
                                u64::try_from(sent.elapsed().as_micros()).unwrap_or(u64::MAX),
                            );
                        } else {
                            failed += 1;
                        }
                    }
                    (latencies, failed)
                })
            })
            .collect();
        thread::sleep(Duration::from_secs(seconds));
        stop.store(true, Ordering::Relaxed);
        workers
            .into_iter()
            .filter_map(|worker| worker.join().ok())
            .collect()
    });
    let elapsed = started.elapsed().as_secs_f64();
    let mut latencies: Vec<u64> = per_client
        .iter()
        .flat_map(|(latencies, _)| latencies.iter().copied())
        .collect();
    latencies.sort_unstable();
    let failed = per_client.iter().map(|(_, failed)| failed).sum();
    let done = u32::try_from(latencies.len()).unwrap_or(u32::MAX);
    Sample {
        per_second: f64::from(done) / elapsed,
        latencies,
        failed,
    }
}

/// Runs a workload several times and keeps the run with the median throughput. One run on a
/// laptop can land on a slow moment of the disk; the median of a few is steadier than any one.
fn median_of(
    cluster: &Cluster,
    workload: Workload,
    clients: u64,
    seconds: u64,
    runs: usize,
) -> Option<Sample> {
    let mut samples: Vec<Sample> = (0..runs)
        .map(|_| measure(cluster, workload, clients, seconds))
        .collect();
    samples.sort_by(|a, b| a.per_second.total_cmp(&b.per_second));
    let middle = samples.len() / 2;
    samples.into_iter().nth(middle)
}

fn throughput(nodes: u64, seconds: u64, runs: usize, out: &mut impl Write) -> Result<(), Failure> {
    let cluster = Cluster::start(nodes)?;
    let mut client = cluster.client();
    for number in 0..PRELOADED {
        client.put(&key(number), &VALUE)?;
    }
    for workload in [Workload::Put, Workload::Get] {
        for clients in [1, 16, 64] {
            let Some(sample) = median_of(&cluster, workload, clients, seconds, runs) else {
                continue;
            };
            writeln!(
                out,
                "| {nodes} | {} | {clients} | {:.0} | {:.2} | {:.2} | {:.2} | {} |",
                if workload == Workload::Put {
                    "put"
                } else {
                    "get"
                },
                sample.per_second,
                sample.percentile(50),
                sample.percentile(99),
                sample.percentile(100),
                sample.failed,
            )?;
            out.flush()?;
        }
    }
    Ok(())
}

/// Kills the leader and times how long it takes until a write is acknowledged again.
fn failover(rounds: usize, out: &mut impl Write) -> Result<(), Failure> {
    let mut cluster = Cluster::start(3)?;
    let mut client = cluster
        .client()
        .timeouts(Duration::from_millis(100), Duration::from_secs(30));
    client.put(b"warm-up", &VALUE)?;
    let mut gaps = Vec::new();
    for round in 0..rounds {
        let leader = cluster.leader()?;
        cluster.kill(leader);
        let killed = Instant::now();
        client.put(format!("after-{round}").as_bytes(), &VALUE)?;
        gaps.push(killed.elapsed().as_secs_f64() * 1_000.0);
        cluster.spawn(leader)?;
        // Long enough for the node to be back and caught up before the next leader goes.
        thread::sleep(Duration::from_secs(2));
    }
    gaps.sort_by(f64::total_cmp);
    let pick = |position: usize| gaps.get(position).copied().unwrap_or(0.0);
    writeln!(
        out,
        "| {rounds} | {:.0} | {:.0} | {:.0} |",
        pick(0),
        pick(gaps.len() / 2),
        pick(gaps.len().saturating_sub(1))
    )?;
    Ok(())
}

pub(crate) fn run(args: &Args, out: &mut impl Write) -> Result<(), Failure> {
    let number = |name: &str, default: u64| match args.all(name).next() {
        None => Ok(default),
        Some(text) => text
            .parse::<u64>()
            .map_err(|_| Failure(format!("--{name} {text}: expected a number"))),
    };
    let seconds = number("seconds", 5)?;
    let runs = usize::try_from(number("runs", 3)?).unwrap_or(3).max(1);
    let rounds = usize::try_from(number("rounds", 7)?).unwrap_or(7).max(1);

    writeln!(
        out,
        "Throughput and latency, {seconds} s per run, median of {runs} runs. Latencies in milliseconds.\n"
    )?;
    writeln!(
        out,
        "| nodes | operation | clients | ops/s | p50 | p99 | max | failed |"
    )?;
    writeln!(out, "| --- | --- | --- | --- | --- | --- | --- | --- |")?;
    for nodes in [3, 5] {
        throughput(nodes, seconds, runs, out)?;
    }

    writeln!(
        out,
        "\nTime from killing the leader to the next acknowledged write, three nodes, in milliseconds.\n"
    )?;
    writeln!(out, "| kills | fastest | median | slowest |")?;
    writeln!(out, "| --- | --- | --- | --- |")?;
    failover(rounds, out)
}
