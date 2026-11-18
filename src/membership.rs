//! Who is in the cluster, and the one-step changes that are allowed.
//!
//! A cluster changes one server at a time (Raft thesis, chapter 4). Adding or removing a single
//! voter keeps every majority of the old set overlapping every majority of the new one, so the
//! two can never elect different leaders, and no in-between configuration is needed.
//!
//! A new server joins as a learner first: it receives the log and does not vote. It becomes a
//! voter in a second step, once it has caught up. Otherwise a new voter with an empty log would
//! count towards the quorum while being unable to help reach it.

use core::fmt;

use crate::types::NodeId;

/// The members of a cluster.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Membership {
    voters: Vec<NodeId>,
    learners: Vec<NodeId>,
}

/// One step from one membership to the next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Change {
    /// Start sending the log to a new server. It does not vote.
    AddLearner(NodeId),
    /// Turn a learner into a voter.
    Promote(NodeId),
    /// Remove a voter or a learner.
    Remove(NodeId),
}

/// Why a change was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChangeError {
    /// The server to add is already a voter or a learner.
    AlreadyMember,
    /// The server to promote is not a learner.
    NotALearner,
    /// The server to remove is not a member.
    NotAMember,
    /// Removing the server would leave no voter.
    LastVoter,
    /// An earlier change is not committed yet. Two changes in flight could each be safe against
    /// the membership before them and unsafe against each other.
    InProgress,
    /// The leader has not committed an entry of its own term yet, so it cannot be sure which
    /// membership is the committed one.
    NotReady,
    /// The learner to promote has not caught up with the log.
    NotCaughtUp,
}

impl fmt::Display for ChangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AlreadyMember => "the server is already a member",
            Self::NotALearner => "the server is not a learner",
            Self::NotAMember => "the server is not a member",
            Self::LastVoter => "the last voter cannot be removed",
            Self::InProgress => "an earlier membership change is not committed yet",
            Self::NotReady => "the leader has not committed an entry of its own term yet",
            Self::NotCaughtUp => "the learner has not caught up with the log",
        })
    }
}

impl core::error::Error for ChangeError {}

impl Membership {
    /// Builds a membership. A server listed as both is a voter.
    pub fn new(
        voters: impl IntoIterator<Item = NodeId>,
        learners: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        let mut voters: Vec<NodeId> = voters.into_iter().collect();
        voters.sort_unstable();
        voters.dedup();
        let mut learners: Vec<NodeId> = learners
            .into_iter()
            .filter(|id| !voters.contains(id))
            .collect();
        learners.sort_unstable();
        learners.dedup();
        Self { voters, learners }
    }

    /// The servers whose votes and acknowledgements count, in order of id.
    #[must_use]
    pub fn voters(&self) -> &[NodeId] {
        &self.voters
    }

    /// The servers that receive the log without voting, in order of id.
    #[must_use]
    pub fn learners(&self) -> &[NodeId] {
        &self.learners
    }

    /// Every member, voters first.
    pub fn members(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.voters.iter().chain(&self.learners).copied()
    }

    /// Whether `id` is a voter.
    #[must_use]
    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.contains(&id)
    }

    /// Whether `id` is a voter or a learner.
    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.is_voter(id) || self.learners.contains(&id)
    }

    /// How many voters make a majority.
    #[must_use]
    pub const fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// The membership after `change`.
    ///
    /// # Errors
    ///
    /// Returns a [`ChangeError`] when the change does not apply to this membership: the server
    /// is in the wrong state for it, or it would remove the last voter.
    pub fn apply(&self, change: Change) -> Result<Self, ChangeError> {
        let mut next = self.clone();
        match change {
            Change::AddLearner(id) => {
                if self.contains(id) {
                    return Err(ChangeError::AlreadyMember);
                }
                next.learners.push(id);
                next.learners.sort_unstable();
            }
            Change::Promote(id) => {
                if !self.learners.contains(&id) {
                    return Err(ChangeError::NotALearner);
                }
                next.learners.retain(|&learner| learner != id);
                next.voters.push(id);
                next.voters.sort_unstable();
            }
            Change::Remove(id) => {
                if !self.contains(id) {
                    return Err(ChangeError::NotAMember);
                }
                if self.voters == [id] {
                    return Err(ChangeError::LastVoter);
                }
                next.voters.retain(|&voter| voter != id);
                next.learners.retain(|&learner| learner != id);
            }
        }
        Ok(next)
    }

    /// The bytes storage keeps: the number of voters, the number of learners, then their ids.
    /// Both counts are there so that no prefix of an encoding is itself a valid one.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let count = |ids: &[NodeId]| u64::try_from(ids.len()).unwrap_or(u64::MAX).to_be_bytes();
        let mut bytes = count(&self.voters).to_vec();
        bytes.extend_from_slice(&count(&self.learners));
        for id in self.members() {
            bytes.extend_from_slice(&id.get().to_be_bytes());
        }
        bytes
    }

    /// Reads a membership back. `None` for bytes that are not one.
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        let (voters, rest) = bytes.split_first_chunk::<8>()?;
        let (learners, rest) = rest.split_first_chunk::<8>()?;
        let (ids, leftover) = rest.as_chunks::<8>();
        let voters = usize::try_from(u64::from_be_bytes(*voters)).ok()?;
        let learners = usize::try_from(u64::from_be_bytes(*learners)).ok()?;
        if !leftover.is_empty() || voters.checked_add(learners)? != ids.len() {
            return None;
        }
        let ids: Vec<NodeId> = ids
            .iter()
            .map(|id| NodeId::new(u64::from_be_bytes(*id)))
            .collect();
        let (voters, learners) = ids.split_at(voters);
        let decoded = Self::new(voters.iter().copied(), learners.iter().copied());
        // Refuse anything `encode` would not have written: unsorted ids, or the same id twice.
        (decoded.voters == voters && decoded.learners == learners).then_some(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::{Change, ChangeError, Membership};
    use crate::types::NodeId;

    fn ids(raw: &[u64]) -> Vec<NodeId> {
        raw.iter().copied().map(NodeId::new).collect()
    }

    fn membership(voters: &[u64], learners: &[u64]) -> Membership {
        Membership::new(ids(voters), ids(learners))
    }

    #[test]
    fn a_server_joins_as_a_learner_and_is_promoted_in_a_second_step() {
        let start = membership(&[1, 2, 3], &[]);
        let joined = start
            .apply(Change::AddLearner(NodeId::new(4)))
            .expect("a new server");
        assert_eq!(joined, membership(&[1, 2, 3], &[4]));
        assert_eq!(joined.quorum(), 2, "a learner does not change the majority");

        let promoted = joined
            .apply(Change::Promote(NodeId::new(4)))
            .expect("a learner");
        assert_eq!(promoted, membership(&[1, 2, 3, 4], &[]));
        assert_eq!(promoted.quorum(), 3);
    }

    #[test]
    fn changes_that_do_not_fit_are_refused() {
        let start = membership(&[1, 2], &[3]);
        let refused = |change| start.apply(change).expect_err("a change that does not fit");
        assert_eq!(
            refused(Change::AddLearner(NodeId::new(1))),
            ChangeError::AlreadyMember
        );
        assert_eq!(
            refused(Change::AddLearner(NodeId::new(3))),
            ChangeError::AlreadyMember
        );
        assert_eq!(
            refused(Change::Promote(NodeId::new(1))),
            ChangeError::NotALearner
        );
        assert_eq!(
            refused(Change::Promote(NodeId::new(9))),
            ChangeError::NotALearner
        );
        assert_eq!(
            refused(Change::Remove(NodeId::new(9))),
            ChangeError::NotAMember
        );

        let alone = membership(&[1], &[2]);
        assert_eq!(
            alone.apply(Change::Remove(NodeId::new(1))),
            Err(ChangeError::LastVoter)
        );
        assert_eq!(
            alone.apply(Change::Remove(NodeId::new(2))),
            Ok(membership(&[1], &[]))
        );
    }

    #[test]
    fn a_membership_survives_encoding() {
        for members in [
            membership(&[], &[]),
            membership(&[7], &[]),
            membership(&[3, 1, 2], &[9, 4]),
        ] {
            assert_eq!(Membership::decode(&members.encode()), Some(members));
        }
    }

    #[test]
    fn damaged_memberships_are_refused() {
        let bytes = membership(&[1, 2], &[3]).encode();
        for cut in 0..bytes.len() {
            assert_eq!(
                Membership::decode(&bytes[..cut]),
                None,
                "a prefix of {cut} bytes was accepted"
            );
        }
        // More voters claimed than there are ids.
        let mut too_many = bytes.clone();
        too_many[7] = 9;
        assert_eq!(Membership::decode(&too_many), None);
        // The same server twice.
        let mut twice = bytes;
        twice[31] = 1;
        assert_eq!(Membership::decode(&twice), None);
    }
}
