//! quorate: a linearizable key-value store replicated with Raft.
//!
//! So far the crate holds the Raft core: leader election and log replication as a pure state
//! machine. Feed a [`Raft`] node [`Input`]s with [`Raft::step`] and carry out the [`Output`]s it
//! returns. Storage, the key-value layer and the network come in later milestones; see
//! `docs/roadmap.md`.
//!
//! ```
//! use quorate::{Config, Input, NodeId, Output, Raft, Role};
//!
//! // A cluster of one elects itself and commits what it is given.
//! let mut node = Raft::new(Config::new(NodeId::new(1), [], 7))?;
//! while node.role() != Role::Leader {
//!     node.step(Input::Tick);
//! }
//! let outputs = node.step(Input::Propose(b"hello".to_vec()));
//! assert!(matches!(outputs.as_slice(), [Output::Apply { .. }]));
//! # Ok::<(), quorate::ConfigError>(())
//! ```

mod config;
mod log;
mod raft;
mod rng;
#[cfg(test)]
mod sim;
mod types;

pub use config::{Config, ConfigError};
pub use raft::{Raft, Role};
pub use types::{AppendOutcome, Entry, Index, Input, Message, NodeId, Output, Payload, Term};
