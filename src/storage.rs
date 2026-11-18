//! Durable storage for one node's hard state and log, in a katman database.
//!
//! Layout: the hard state is under the key `h`, the point the log starts after under `b`, and log
//! entry `i` under `l` followed by `i` as eight big-endian bytes, so a scan of the `l` prefix
//! returns the log in order. Every [`Persist`] request becomes one atomic, synced katman batch.

use core::fmt;
use std::path::Path;

use katman::{Db, Options, SyncMode, WriteBatch};

use crate::membership::Membership;
use crate::types::{Entry, HardState, Index, NodeId, Payload, Persist, SnapshotMeta, Term};

const HARD_STATE_KEY: &[u8] = b"h";
const BASE_KEY: &[u8] = b"b";
const LOG_PREFIX: u8 = b'l';
const KIND_NOOP: u8 = 0;
const KIND_COMMAND: u8 = 1;
const KIND_CONFIG: u8 = 2;

/// Why storage could not be read or written.
#[derive(Debug)]
#[non_exhaustive]
pub enum StorageError {
    /// The database reported an error.
    Db(katman::Error),
    /// The database holds something this module did not write. The node must not start.
    Corrupt(&'static str),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Db(error) => write!(f, "storage: {error}"),
            Self::Corrupt(what) => write!(f, "storage is corrupt: {what}"),
        }
    }
}

impl core::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Db(error) => Some(error),
            Self::Corrupt(_) => None,
        }
    }
}

impl From<katman::Error> for StorageError {
    fn from(error: katman::Error) -> Self {
        Self::Db(error)
    }
}

/// What a node's storage held when it was opened.
#[derive(Debug)]
pub struct Recovered {
    /// The storage, ready for new writes.
    pub storage: Storage,
    /// The stored term and vote; the default for a node that has never run.
    pub hard_state: HardState,
    /// The point the stored log starts after; the default for a log that was never compacted.
    pub base: SnapshotMeta,
    /// The stored log, starting with the entry right after `base`.
    pub log: Vec<Entry>,
}

/// One node's durable state.
#[derive(Debug)]
pub struct Storage {
    db: Db,
    /// The index the stored log starts after.
    base: Index,
    last_index: Index,
}

impl Storage {
    /// Opens the database in `dir` and reads back what it holds.
    ///
    /// Writes are always synced, whatever `options.sync` says: a Raft node that acknowledges
    /// something its disk can still lose breaks the algorithm.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Db`] when katman cannot open or read the directory, and
    /// [`StorageError::Corrupt`] when the contents do not decode or the log has a gap.
    pub fn open(dir: impl AsRef<Path>, mut options: Options) -> Result<Recovered, StorageError> {
        options.sync = SyncMode::Always;
        let db = Db::open(dir, options)?;

        let hard_state = match db.get(HARD_STATE_KEY)? {
            Some(bytes) => decode_hard_state(&bytes)?,
            None => HardState::default(),
        };
        let base = match db.get(BASE_KEY)? {
            Some(bytes) => decode_base(&bytes)?,
            None => SnapshotMeta::default(),
        };

        let mut log = Vec::new();
        let mut last_index = base.index();
        for item in db.scan_prefix([LOG_PREFIX]) {
            let (key, value) = item?;
            last_index = last_index.next();
            if key != log_key(last_index) {
                return Err(StorageError::Corrupt(
                    "the log has a gap or a malformed key",
                ));
            }
            log.push(decode_entry(&value)?);
        }

        let storage = Self {
            db,
            base: base.index(),
            last_index,
        };
        Ok(Recovered {
            storage,
            hard_state,
            base,
            log,
        })
    }

    /// Makes one persist request durable. When this returns `Ok` the state survives a power cut.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Db`] when the write fails. The node must stop: its in-memory state
    /// is ahead of its disk and it cannot know by how much.
    pub fn persist(&mut self, persist: &Persist) -> Result<(), StorageError> {
        let mut batch = WriteBatch::new();
        batch.put(HARD_STATE_KEY, encode_hard_state(persist.hard_state()));

        let mut base = self.base;
        let mut last_index = self.last_index;
        if let Some(new_base) = persist.base() {
            batch.put(BASE_KEY, encode_base(new_base));
            // ponytail: one delete per entry, in the batch that moves the base. The batch grows
            // with the distance between two compactions. A range delete in katman would make it
            // constant; worth it once compactions are far apart.
            let mut index = base.next();
            while index <= new_base.index().min(last_index) {
                batch.delete(log_key(index));
                index = index.next();
            }
            base = new_base.index();
            last_index = last_index.max(base);
        }
        if let Some(write) = persist.log() {
            let mut index = write.first_index();
            for entry in write.entries() {
                batch.put(log_key(index), encode_entry(entry));
                index = index.next();
            }
            // `index` is now one past the new end. Whatever was stored from there on belonged to
            // a log this node has given up.
            let new_last = index.prev();
            while index <= last_index {
                batch.delete(log_key(index));
                index = index.next();
            }
            last_index = new_last;
        }

        self.db.write(batch)?;
        self.base = base;
        self.last_index = last_index;
        Ok(())
    }
}

fn log_key(index: Index) -> [u8; 9] {
    let mut key = [LOG_PREFIX; 9];
    key[1..].copy_from_slice(&index.get().to_be_bytes());
    key
}

fn encode_hard_state(hard_state: HardState) -> [u8; 17] {
    let mut bytes = [0; 17];
    bytes[..8].copy_from_slice(&hard_state.term().get().to_be_bytes());
    if let Some(candidate) = hard_state.voted_for() {
        bytes[8] = 1;
        bytes[9..].copy_from_slice(&candidate.get().to_be_bytes());
    }
    bytes
}

fn decode_hard_state(bytes: &[u8]) -> Result<HardState, StorageError> {
    let corrupt = StorageError::Corrupt("the hard state has the wrong shape");
    let Some((term, rest)) = bytes.split_first_chunk::<8>() else {
        return Err(corrupt);
    };
    let Some((&voted, rest)) = rest.split_first() else {
        return Err(corrupt);
    };
    let Ok(candidate) = <[u8; 8]>::try_from(rest) else {
        return Err(corrupt);
    };
    let voted_for = match voted {
        0 => None,
        1 => Some(NodeId::new(u64::from_be_bytes(candidate))),
        _ => return Err(corrupt),
    };
    Ok(HardState::new(
        Term::new(u64::from_be_bytes(*term)),
        voted_for,
    ))
}

/// A snapshot point as sixteen bytes: index, then term. The state machine stores its applied
/// position the same way.
pub(crate) fn encode_base(base: SnapshotMeta) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&base.index().get().to_be_bytes());
    bytes[8..].copy_from_slice(&base.term().get().to_be_bytes());
    bytes
}

pub(crate) fn decode_base(bytes: &[u8]) -> Result<SnapshotMeta, StorageError> {
    let corrupt = StorageError::Corrupt("a log position has the wrong shape");
    let Some((index, term)) = bytes.split_first_chunk::<8>() else {
        return Err(corrupt);
    };
    let Ok(term) = <[u8; 8]>::try_from(term) else {
        return Err(corrupt);
    };
    Ok(SnapshotMeta::new(
        Index::new(u64::from_be_bytes(*index)),
        Term::new(u64::from_be_bytes(term)),
    ))
}

pub(crate) fn encode_entry(entry: &Entry) -> Vec<u8> {
    let membership;
    let (kind, body): (u8, &[u8]) = match entry.payload() {
        Payload::Noop => (KIND_NOOP, &[]),
        Payload::Command(command) => (KIND_COMMAND, command),
        Payload::Config(members) => {
            membership = members.encode();
            (KIND_CONFIG, &membership)
        }
    };
    let mut bytes = Vec::with_capacity(9 + body.len());
    bytes.extend_from_slice(&entry.term().get().to_be_bytes());
    bytes.push(kind);
    bytes.extend_from_slice(body);
    bytes
}

pub(crate) fn decode_entry(bytes: &[u8]) -> Result<Entry, StorageError> {
    let corrupt = StorageError::Corrupt("a log entry has the wrong shape");
    let Some((term, rest)) = bytes.split_first_chunk::<8>() else {
        return Err(corrupt);
    };
    let Some((&kind, command)) = rest.split_first() else {
        return Err(corrupt);
    };
    let payload = match kind {
        KIND_NOOP if command.is_empty() => Payload::Noop,
        KIND_COMMAND => Payload::Command(command.to_vec()),
        KIND_CONFIG => match Membership::decode(command) {
            Some(members) => Payload::Config(members),
            None => return Err(corrupt),
        },
        _ => return Err(corrupt),
    };
    Ok(Entry::new(Term::new(u64::from_be_bytes(*term)), payload))
}

#[cfg(test)]
mod tests {
    use katman::{Options, SimFs, Torn};

    use super::{Recovered, Storage};
    use crate::membership::Membership;
    use crate::types::{
        Entry, HardState, Index, LogWrite, NodeId, Payload, Persist, PersistSeq, SnapshotMeta, Term,
    };

    fn entry(term: u64, command: &[u8]) -> Entry {
        Entry::new(Term::new(term), Payload::Command(command.to_vec()))
    }

    fn open(fs: &SimFs) -> Recovered {
        Storage::open("node", Options::default().with_sim_fs(fs.clone())).expect("storage opens")
    }

    /// Opens the storage as it would be after a power cut.
    fn reopen(fs: &mut SimFs, storage: Storage) -> Recovered {
        *fs = fs.power_loss(Torn::Nothing);
        drop(storage);
        open(fs)
    }

    fn base(index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta::new(Index::new(index), Term::new(term))
    }

    fn request(
        term: u64,
        vote: Option<u64>,
        base: Option<SnapshotMeta>,
        log: Option<(u64, Vec<Entry>)>,
    ) -> Persist {
        Persist::new(
            PersistSeq::new(1),
            HardState::new(Term::new(term), vote.map(NodeId::new)),
            base,
            log.map(|(first, entries)| LogWrite::new(Index::new(first), entries)),
        )
    }

    #[test]
    fn a_node_that_never_ran_recovers_as_empty() {
        let recovered = open(&SimFs::new());
        assert_eq!(recovered.hard_state, HardState::default());
        assert_eq!(recovered.base, SnapshotMeta::default());
        assert_eq!(recovered.log, Vec::new());
    }

    #[test]
    fn what_was_persisted_survives_a_power_cut() {
        let mut fs = SimFs::new();
        let mut storage = open(&fs).storage;
        let log = vec![
            entry(1, b"a"),
            Entry::new(Term::new(2), Payload::Noop),
            entry(2, b""),
        ];
        storage
            .persist(&request(2, Some(3), None, Some((1, log.clone()))))
            .expect("write succeeds");

        let recovered = reopen(&mut fs, storage);
        assert_eq!(
            recovered.hard_state,
            HardState::new(Term::new(2), Some(NodeId::new(3)))
        );
        assert_eq!(recovered.log, log);
    }

    #[test]
    fn a_write_in_the_middle_replaces_the_old_tail() {
        let mut fs = SimFs::new();
        let mut storage = open(&fs).storage;
        let old = vec![
            entry(1, b"a"),
            entry(1, b"b"),
            entry(1, b"c"),
            entry(1, b"d"),
        ];
        storage
            .persist(&request(1, None, None, Some((1, old))))
            .expect("write succeeds");
        // A new leader overwrites from index 2 with a shorter tail.
        storage
            .persist(&request(2, None, None, Some((2, vec![entry(2, b"x")]))))
            .expect("write succeeds");
        // A later append lands right after the new end.
        storage
            .persist(&request(2, None, None, Some((3, vec![entry(2, b"y")]))))
            .expect("write succeeds");

        assert_eq!(
            reopen(&mut fs, storage).log,
            vec![entry(1, b"a"), entry(2, b"x"), entry(2, b"y")]
        );
    }

    #[test]
    fn a_membership_entry_survives_a_power_cut() {
        let mut fs = SimFs::new();
        let mut storage = open(&fs).storage;
        let voters = [1, 2, 3].map(NodeId::new);
        let members = Membership::new(voters, [NodeId::new(4)]);
        let log = vec![Entry::new(Term::new(1), Payload::Config(members))];
        storage
            .persist(&request(1, None, None, Some((1, log.clone()))))
            .expect("write succeeds");
        assert_eq!(reopen(&mut fs, storage).log, log);
    }

    #[test]
    fn a_hard_state_only_write_leaves_the_log_alone() {
        let fs = SimFs::new();
        let mut storage = open(&fs).storage;
        storage
            .persist(&request(1, None, None, Some((1, vec![entry(1, b"a")]))))
            .expect("write succeeds");
        storage
            .persist(&request(5, Some(2), None, None))
            .expect("write succeeds");

        let fs = fs.process_crash();
        drop(storage);
        let recovered = open(&fs);
        assert_eq!(
            recovered.hard_state,
            HardState::new(Term::new(5), Some(NodeId::new(2)))
        );
        assert_eq!(recovered.log, vec![entry(1, b"a")]);
    }

    #[test]
    fn moving_the_base_removes_the_entries_before_it_and_keeps_the_rest() {
        let mut fs = SimFs::new();
        let mut storage = open(&fs).storage;
        let log = vec![
            entry(1, b"a"),
            entry(1, b"b"),
            entry(2, b"c"),
            entry(2, b"d"),
        ];
        storage
            .persist(&request(2, None, None, Some((1, log))))
            .expect("write succeeds");
        storage
            .persist(&request(2, None, Some(base(2, 1)), None))
            .expect("write succeeds");
        // The log goes on after a compaction as if nothing happened.
        storage
            .persist(&request(2, None, None, Some((5, vec![entry(2, b"e")]))))
            .expect("write succeeds");

        let recovered = reopen(&mut fs, storage);
        assert_eq!(recovered.base, base(2, 1));
        assert_eq!(
            recovered.log,
            vec![entry(2, b"c"), entry(2, b"d"), entry(2, b"e")]
        );
    }

    #[test]
    fn a_base_past_the_end_leaves_an_empty_log_that_continues_from_there() {
        let mut fs = SimFs::new();
        let mut storage = open(&fs).storage;
        storage
            .persist(&request(
                1,
                None,
                None,
                Some((1, vec![entry(1, b"a"), entry(1, b"b")])),
            ))
            .expect("write succeeds");
        // A snapshot was installed: the log starts over after index 9, and what was stored goes.
        storage
            .persist(&request(3, None, Some(base(9, 3)), Some((10, Vec::new()))))
            .expect("write succeeds");

        let recovered = reopen(&mut fs, storage);
        assert_eq!(recovered.base, base(9, 3));
        assert_eq!(recovered.log, Vec::new());

        let mut storage = recovered.storage;
        storage
            .persist(&request(3, None, None, Some((10, vec![entry(3, b"j")]))))
            .expect("write succeeds");
        let recovered = reopen(&mut fs, storage);
        assert_eq!(recovered.base, base(9, 3));
        assert_eq!(recovered.log, vec![entry(3, b"j")]);
    }
}
