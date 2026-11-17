//! Node configuration and its validation.

use core::fmt;

use crate::types::NodeId;

/// How one node is set up. Membership is fixed for the life of the node.
#[derive(Debug, Clone)]
pub struct Config {
    pub(crate) id: NodeId,
    pub(crate) peers: Vec<NodeId>,
    pub(crate) seed: u64,
    pub(crate) election_ticks: u32,
    pub(crate) heartbeat_ticks: u32,
    pub(crate) max_batch: usize,
}

impl Config {
    /// A configuration with default timing: an election timeout of 10 to 19 ticks, a heartbeat
    /// every 2 ticks and at most 64 entries per message.
    ///
    /// `peers` are the other nodes. `seed` drives the randomised election timeout; give every node
    /// a different one.
    pub fn new(id: NodeId, peers: impl IntoIterator<Item = NodeId>, seed: u64) -> Self {
        Self {
            id,
            peers: peers.into_iter().collect(),
            seed,
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_batch: 64,
        }
    }

    /// Sets the base election timeout. The actual timeout is drawn from `ticks..2 * ticks` each
    /// time it is reset.
    #[must_use]
    pub const fn election_ticks(mut self, ticks: u32) -> Self {
        self.election_ticks = ticks;
        self
    }

    /// Sets how many ticks a leader waits between heartbeats.
    #[must_use]
    pub const fn heartbeat_ticks(mut self, ticks: u32) -> Self {
        self.heartbeat_ticks = ticks;
        self
    }

    /// Sets the largest number of entries in one `AppendEntries` message.
    #[must_use]
    pub const fn max_batch(mut self, entries: usize) -> Self {
        self.max_batch = entries;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.peers.contains(&self.id) {
            return Err(ConfigError::SelfInPeers);
        }
        let mut seen = self.peers.clone();
        seen.sort_unstable();
        let duplicate = seen.windows(2).find_map(|pair| match pair {
            [a, b] if a == b => Some(*a),
            _ => None,
        });
        if let Some(peer) = duplicate {
            return Err(ConfigError::DuplicatePeer(peer));
        }
        if self.heartbeat_ticks == 0 || self.heartbeat_ticks >= self.election_ticks {
            return Err(ConfigError::HeartbeatNotBelowElection);
        }
        if self.max_batch == 0 {
            return Err(ConfigError::ZeroBatch);
        }
        Ok(())
    }
}

/// Why a [`Config`] was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// The node's own id is in its peer list.
    SelfInPeers,
    /// A peer is listed twice.
    DuplicatePeer(NodeId),
    /// The heartbeat interval is zero, or not shorter than the election timeout. A leader that
    /// cannot send a heartbeat before its followers time out can never stay leader.
    HeartbeatNotBelowElection,
    /// The batch size is zero, so no entry could ever be sent.
    ZeroBatch,
    /// The state machine has applied more entries than the log holds. The node's storage is
    /// damaged and it must not start.
    AppliedPastLog,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SelfInPeers => f.write_str("the node's own id is in its peer list"),
            Self::DuplicatePeer(peer) => write!(f, "peer {} is listed twice", peer.get()),
            Self::HeartbeatNotBelowElection => f.write_str(
                "the heartbeat interval must be at least 1 and below the election timeout",
            ),
            Self::ZeroBatch => f.write_str("the batch size must be at least 1"),
            Self::AppliedPastLog => f.write_str("the state machine is ahead of the log"),
        }
    }
}

impl core::error::Error for ConfigError {}
