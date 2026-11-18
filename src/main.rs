//! The `quorate` command: run a node, or talk to a cluster.

use core::fmt::Display;
use core::time::Duration;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use quorate::{Change, Client, ClientError, NodeId, ServerConfig, serve};

mod bench;

const USAGE: &str = "\
quorate: a linearizable key-value store replicated with Raft

Run a node:
  quorate serve --id N --listen ADDR --data DIR [--peer N=ADDR]... [--join] [--tick-ms MS]

    --peer   another node of the cluster; repeat for each
    --join   this node joins a cluster that already exists; add it with `member add-learner`

Use a cluster (give every node with --server N=ADDR):
  quorate get KEY --server N=ADDR...
  quorate put KEY VALUE --server N=ADDR...
  quorate delete KEY --server N=ADDR...
  quorate cas KEY EXPECTED VALUE --server N=ADDR...     EXPECTED is - for \"must be absent\"
  quorate member add-learner|promote|remove N --server N=ADDR...
  quorate status --server N=ADDR...

Measure this machine (starts its own clusters, takes a few minutes):
  quorate bench [--seconds S] [--runs N] [--rounds N]
";

/// What went wrong, as one line for the user.
struct Failure(String);

impl<E: Display> From<E> for Failure {
    fn from(error: E) -> Self {
        Self(error.to_string())
    }
}

fn fail<T>(message: impl Into<String>) -> Result<T, Failure> {
    Err(Failure(message.into()))
}

/// The command line, split into what has a name and what does not.
struct Args {
    positional: Vec<String>,
    named: Vec<(String, String)>,
    switches: Vec<String>,
}

impl Args {
    fn parse(raw: impl Iterator<Item = String>) -> Result<Self, Failure> {
        let mut args = Self {
            positional: Vec::new(),
            named: Vec::new(),
            switches: Vec::new(),
        };
        let mut raw = raw.peekable();
        while let Some(arg) = raw.next() {
            match arg.strip_prefix("--") {
                None => args.positional.push(arg),
                Some("join") => args.switches.push("join".to_owned()),
                Some(name) => match raw.next() {
                    Some(value) => args.named.push((name.to_owned(), value)),
                    None => return fail(format!("--{name} needs a value")),
                },
            }
        }
        Ok(args)
    }

    fn all(&self, name: &str) -> impl Iterator<Item = &str> {
        self.named
            .iter()
            .filter(move |(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    fn one(&self, name: &str) -> Result<&str, Failure> {
        self.all(name)
            .next()
            .ok_or_else(|| Failure(format!("--{name} is required")))
    }

    fn positional(&self, position: usize, what: &str) -> Result<&str, Failure> {
        self.positional
            .get(position)
            .map(String::as_str)
            .ok_or_else(|| Failure(format!("{what} is missing")))
    }

    /// Every `--name N=ADDR`, as nodes and where they listen.
    fn nodes(&self, name: &str) -> Result<Vec<(NodeId, SocketAddr)>, Failure> {
        self.all(name)
            .map(|pair| {
                let Some((id, address)) = pair.split_once('=') else {
                    return fail(format!("--{name} {pair}: expected N=ADDR"));
                };
                Ok((
                    node_id(id)?,
                    address
                        .parse()
                        .map_err(|_| Failure(format!("{address} is not an address")))?,
                ))
            })
            .collect()
    }
}

fn node_id(text: &str) -> Result<NodeId, Failure> {
    match text.parse() {
        Ok(id) => Ok(NodeId::new(id)),
        Err(_) => fail(format!("{text} is not a node id")),
    }
}

fn run(args: &Args, out: &mut impl Write) -> Result<(), Failure> {
    let command = args.positional(0, "a command")?;
    if command == "serve" {
        let tick = args.all("tick-ms").next().unwrap_or("50");
        let config = ServerConfig {
            id: node_id(args.one("id")?)?,
            listen: args.one("listen")?.parse()?,
            peers: args.nodes("peer")?,
            data_dir: PathBuf::from(args.one("data")?),
            joining: args.switches.iter().any(|switch| switch == "join"),
            tick: Duration::from_millis(tick.parse()?),
        };
        match serve(&config)? {}
    }

    if command == "bench" {
        return bench::run(args, out);
    }

    let servers = args.nodes("server")?;
    let mut client = Client::new(servers.iter().copied());
    let key = |position| args.positional(position, "the key").map(str::as_bytes);
    match command {
        "get" => match client.get(key(1)?)? {
            Some(value) => writeln!(out, "{}", String::from_utf8_lossy(&value))?,
            None => return fail("not found"),
        },
        "put" => client.put(key(1)?, args.positional(2, "the value")?.as_bytes())?,
        "delete" => client.delete(key(1)?)?,
        "cas" => {
            let expected = args.positional(2, "the expected value")?;
            let expected = (expected != "-").then_some(expected.as_bytes());
            if !client.compare_and_set(
                key(1)?,
                expected,
                args.positional(3, "the new value")?.as_bytes(),
            )? {
                return fail("the key did not hold the expected value");
            }
        }
        "member" => {
            let id = node_id(args.positional(2, "the node id")?)?;
            let change = match args.positional(1, "what to do with the member")? {
                "add-learner" => Change::AddLearner(id),
                "promote" => Change::Promote(id),
                "remove" => Change::Remove(id),
                other => return fail(format!("unknown member command: {other}")),
            };
            client.change_membership(change)?;
        }
        "status" => {
            for (id, _) in &servers {
                match client.status(*id) {
                    Ok(status) => writeln!(
                        out,
                        "node {}: {:?} in term {}, leader {}, commit {}, voters {:?}, learners {:?}",
                        id.get(),
                        status.role,
                        status.term.get(),
                        status.leader.map_or_else(
                            || "unknown".to_owned(),
                            |leader| leader.get().to_string()
                        ),
                        status.commit.get(),
                        status
                            .membership
                            .voters()
                            .iter()
                            .map(|voter| voter.get())
                            .collect::<Vec<_>>(),
                        status
                            .membership
                            .learners()
                            .iter()
                            .map(|learner| learner.get())
                            .collect::<Vec<_>>(),
                    )?,
                    Err(ClientError::Timeout) => writeln!(out, "node {}: no answer", id.get())?,
                    Err(error) => return Err(error.into()),
                }
            }
        }
        other => return fail(format!("unknown command: {other}\n\n{USAGE}")),
    }
    Ok(())
}

fn main() -> ExitCode {
    let mut out = io::stdout().lock();
    let mut err = io::stderr().lock();
    let args = std::env::args().skip(1);
    let outcome = if std::env::args().len() < 2 {
        fail(USAGE)
    } else {
        Args::parse(args).and_then(|args| run(&args, &mut out))
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure(message)) => {
            // Nothing sensible is left to do if even this cannot be written.
            let _ = writeln!(err, "{message}");
            ExitCode::FAILURE
        }
    }
}
