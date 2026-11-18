//! quorate: a linearizable key-value store replicated with Raft.
//!
//! The crate has two faces. As a program it is a server and a client: [`serve`] runs a node, and
//! [`Client`] talks to a cluster. As a library it is the parts those are made of, each usable by
//! itself: [`Raft`] is the core, leader election and log replication as a pure state machine;
//! [`Storage`] keeps a node's term, vote and log in a katman database; [`StateMachine`] is the
//! key-value store that committed entries are applied to.
//!
//! The core does no I/O. Feed it [`Input`]s with [`Raft::step`] and carry out the [`Output`]s it
//! returns:
//!
//! ```
//! use std::collections::VecDeque;
//!
//! use quorate::{Config, Input, NodeId, Output, Raft};
//!
//! // A cluster of one. This driver has no disk, so it reports every write as durable at once.
//! let mut node = Raft::new(Config::new(NodeId::new(1), [], 7))?;
//! let mut inputs: VecDeque<Input> = std::iter::repeat_n(Input::Tick, 20).collect();
//! inputs.push_back(Input::Propose(b"hello".to_vec()));
//!
//! let mut applied = 0;
//! while let Some(input) = inputs.pop_front() {
//!     for output in node.step(input) {
//!         match output {
//!             Output::Persist(request) => inputs.push_front(Input::Persisted { seq: request.seq() }),
//!             Output::Apply { .. } => applied += 1,
//!             _ => {}
//!         }
//!     }
//! }
//! // The no-op the new leader wrote, then the command.
//! assert_eq!(applied, 2);
//! # Ok::<(), quorate::ConfigError>(())
//! ```

mod client;
mod config;
mod kv;
#[cfg(any(test, feature = "testkit"))]
pub mod linearizability;
mod log;
mod membership;
mod node;
mod raft;
mod rng;
mod server;
#[cfg(test)]
mod sim;
mod state;
mod storage;
mod types;
mod wire;

pub use client::{Client, ClientError};
pub use config::{Config, ConfigError};
pub use kv::{Op, Reply, Request, SessionId};
pub use membership::{Change, ChangeError, Membership};
pub use node::NodeError;
pub use raft::{Raft, Role};
pub use server::{ServerConfig, ServerError, serve};
pub use state::{Outcome, Snapshot, StateMachine};
pub use storage::{Recovered, Storage, StorageError};
pub use types::{
    AppendOutcome, Entry, HardState, Index, Input, LogWrite, Message, NodeId, Output, Payload,
    Persist, PersistSeq, ReadId, SnapshotMeta, Term,
};
pub use wire::Status;
