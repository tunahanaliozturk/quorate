//! The key-value state machine: what committed log entries are applied to.
//!
//! One katman database holds three things: the data, one row per client session, and the index
//! of the last entry applied. Applying an entry writes all three in one atomic batch, so after
//! any crash the database is exactly the result of applying some prefix of the log.
//!
//! That is also why this database is not synced. A power cut can lose its most recent batches,
//! but never an earlier one, and the lost entries are still in the log, which is synced. The
//! node applies them again when it restarts. See `docs/adr/0003`.

use std::path::Path;

use katman::{Db, Options, SyncMode, WriteBatch};

use crate::kv::{Op, Reply, Request, SessionId};
use crate::storage::StorageError;
use crate::types::{Entry, Index, Payload};

const APPLIED_KEY: &[u8] = b"a";
const DATA_PREFIX: u8 = b'd';
const SESSION_PREFIX: u8 = b's';

/// The answer to a request, with the session and request number it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// The session that sent the request.
    pub session: SessionId,
    /// The request's number within the session.
    pub seq: u64,
    /// The result to hand to the client.
    pub reply: Reply,
}

/// One node's copy of the key-value store.
#[derive(Debug)]
pub struct StateMachine {
    db: Db,
    applied: Index,
    #[cfg(test)]
    apply_retries_again: bool,
}

impl StateMachine {
    /// Opens the database in `dir`.
    ///
    /// Writes are never synced, whatever `options.sync` says; the module documentation explains
    /// why that is safe.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Db`] when katman cannot open or read the directory, and
    /// [`StorageError::Corrupt`] when the applied index does not decode.
    pub fn open(dir: impl AsRef<Path>, mut options: Options) -> Result<Self, StorageError> {
        options.sync = SyncMode::Never;
        let db = Db::open(dir, options)?;
        let applied = match db.get(APPLIED_KEY)? {
            None => Index::ZERO,
            Some(bytes) => {
                let Ok(raw) = <[u8; 8]>::try_from(bytes.as_slice()) else {
                    return Err(StorageError::Corrupt(
                        "the applied index has the wrong shape",
                    ));
                };
                Index::new(u64::from_be_bytes(raw))
            }
        };
        Ok(Self {
            db,
            applied,
            #[cfg(test)]
            apply_retries_again: false,
        })
    }

    /// The index of the last entry applied. A restarting node hands this to
    /// [`Raft::recover`](crate::Raft::recover).
    #[must_use]
    pub const fn applied_index(&self) -> Index {
        self.applied
    }

    /// Reads a key directly, without going through the log. The result reflects whatever this
    /// node has applied so far, which may be behind the cluster.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Db`] when the read fails.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.db.get(data_key(key))?)
    }

    /// Applies the committed entry at `index`. Entries must arrive in order, without gaps.
    ///
    /// Returns the answer for the client, or `None` when there is nobody to answer: the entry is
    /// a no-op, is not a request, was applied before a restart, or is a late copy of a request
    /// the session has already moved past.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Corrupt`] when `index` skips ahead, or when a session row does not
    /// decode, and [`StorageError::Db`] when a read or the write fails. After an error the node
    /// must stop.
    pub fn apply(&mut self, index: Index, entry: &Entry) -> Result<Option<Outcome>, StorageError> {
        if index <= self.applied {
            return Ok(None);
        }
        if index != self.applied.next() {
            return Err(StorageError::Corrupt("an entry was applied out of order"));
        }
        let mut batch = WriteBatch::new();
        batch.put(APPLIED_KEY, index.get().to_be_bytes());
        // Bytes that are not a request change nothing. Every node decides that the same way, so
        // the stores stay identical.
        let request = match entry.payload() {
            Payload::Command(command) => Request::decode(command),
            Payload::Noop => None,
        };
        let outcome = match request {
            Some(request) => self.execute(&request, &mut batch)?,
            None => None,
        };
        self.db.write(batch)?;
        self.applied = index;
        Ok(outcome)
    }

    fn execute(
        &self,
        request: &Request,
        batch: &mut WriteBatch,
    ) -> Result<Option<Outcome>, StorageError> {
        let session = request.session();
        let seq = request.seq();
        let session_key = session_key(session);

        if let Some(row) = self.db.get(session_key)? {
            let (last_seq, last_reply) = decode_session(&row)?;
            if seq < last_seq {
                return Ok(None);
            }
            let repeat = seq == last_seq;
            #[cfg(test)]
            let repeat = repeat && !self.apply_retries_again;

            if repeat {
                // The client sent this request more than once. It was carried out the first
                // time; all the copies get that first answer.
                return Ok(Some(Outcome {
                    session,
                    seq,
                    reply: last_reply,
                }));
            }
        }

        let key = data_key(request.op().key());
        let current = self.db.get(&key)?;
        let reply = match request.op() {
            Op::Get { .. } => Reply::Value(current),
            Op::Put { value, .. } => {
                batch.put(&key, value);
                Reply::Done
            }
            Op::Delete { .. } => {
                batch.delete(&key);
                Reply::Done
            }
            Op::CompareAndSet {
                expected, value, ..
            } => {
                let matches = current.as_deref() == expected.as_deref();
                if matches {
                    batch.put(&key, value);
                }
                Reply::Swapped(matches)
            }
        };

        let mut row = seq.to_be_bytes().to_vec();
        reply.encode(&mut row);
        batch.put(session_key, row);
        Ok(Some(Outcome {
            session,
            seq,
            reply,
        }))
    }

    #[cfg(test)]
    pub(crate) const fn inject_apply_retries_again(&mut self) {
        self.apply_retries_again = true;
    }

    /// Everything in the database, for comparing two nodes.
    #[cfg(test)]
    pub(crate) fn dump(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.db
            .iter()
            .map(|item| item.expect("the simulated disk does not fail"))
            .collect()
    }
}

fn data_key(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + key.len());
    out.push(DATA_PREFIX);
    out.extend_from_slice(key);
    out
}

fn session_key(session: SessionId) -> [u8; 9] {
    let mut key = [SESSION_PREFIX; 9];
    key[1..].copy_from_slice(&session.get().to_be_bytes());
    key
}

fn decode_session(row: &[u8]) -> Result<(u64, Reply), StorageError> {
    let corrupt = StorageError::Corrupt("a session row has the wrong shape");
    let Some((seq, reply)) = row.split_first_chunk::<8>() else {
        return Err(corrupt);
    };
    match Reply::decode(reply) {
        Some(reply) => Ok((u64::from_be_bytes(*seq), reply)),
        None => Err(corrupt),
    }
}

#[cfg(test)]
mod tests {
    use katman::{Options, SimFs, Torn};

    use super::StateMachine;
    use crate::kv::{Op, Reply, Request, SessionId};
    use crate::types::{Entry, Index, Payload, Term};

    fn open(fs: &SimFs) -> StateMachine {
        StateMachine::open("state", Options::default().with_sim_fs(fs.clone()))
            .expect("the state machine opens")
    }

    fn entry(session: u64, seq: u64, op: Op) -> Entry {
        let request = Request::new(SessionId::new(session), seq, op);
        Entry::new(Term::new(1), Payload::Command(request.encode()))
    }

    fn put(value: &[u8]) -> Op {
        Op::Put {
            key: b"k".to_vec(),
            value: value.to_vec(),
        }
    }

    fn get() -> Op {
        Op::Get { key: b"k".to_vec() }
    }

    fn swap(expected: Option<&[u8]>, value: &[u8]) -> Op {
        Op::CompareAndSet {
            key: b"k".to_vec(),
            expected: expected.map(<[u8]>::to_vec),
            value: value.to_vec(),
        }
    }

    /// Applies entries from index 1 and returns the replies.
    fn run(state: &mut StateMachine, entries: &[Entry]) -> Vec<Option<Reply>> {
        entries
            .iter()
            .zip(1..)
            .map(|(entry, index)| {
                let outcome = state
                    .apply(Index::new(index), entry)
                    .expect("apply succeeds");
                outcome.map(|outcome| outcome.reply)
            })
            .collect()
    }

    #[test]
    fn operations_do_what_they_say() {
        let mut state = open(&SimFs::new());
        let entries = [
            entry(1, 1, get()),
            entry(1, 2, swap(None, b"a")),
            entry(1, 3, swap(None, b"b")),
            entry(1, 4, swap(Some(b"a"), b"c")),
            entry(1, 5, get()),
            entry(1, 6, Op::Delete { key: b"k".to_vec() }),
            entry(1, 7, get()),
        ];
        let replies = run(&mut state, &entries);
        let expected = [
            Reply::Value(None),
            Reply::Swapped(true),
            Reply::Swapped(false),
            Reply::Swapped(true),
            Reply::Value(Some(b"c".to_vec())),
            Reply::Done,
            Reply::Value(None),
        ];
        assert_eq!(replies, expected.map(Some));
    }

    #[test]
    fn a_retried_request_is_carried_out_once_and_answered_the_same() {
        let mut state = open(&SimFs::new());
        let entries = [
            entry(1, 1, swap(None, b"a")),
            entry(2, 1, put(b"b")),
            // Session 1 did not hear back and sent its request again. Carried out a second time
            // it would fail, because the key now holds "b".
            entry(1, 1, swap(None, b"a")),
        ];
        let replies = run(&mut state, &entries);
        assert_eq!(
            replies,
            [Reply::Swapped(true), Reply::Done, Reply::Swapped(true)].map(Some)
        );
        assert_eq!(state.get(b"k").expect("read succeeds"), Some(b"b".to_vec()));
    }

    #[test]
    fn a_late_copy_of_an_older_request_changes_nothing() {
        let mut state = open(&SimFs::new());
        let entries = [
            entry(1, 1, put(b"a")),
            entry(1, 2, put(b"b")),
            entry(1, 1, put(b"a")),
        ];
        let replies = run(&mut state, &entries);
        assert_eq!(replies, [Some(Reply::Done), Some(Reply::Done), None]);
        assert_eq!(state.get(b"k").expect("read succeeds"), Some(b"b".to_vec()));
    }

    #[test]
    fn bytes_that_are_not_a_request_and_no_ops_only_advance_the_index() {
        let mut state = open(&SimFs::new());
        let entries = [
            Entry::new(Term::new(1), Payload::Noop),
            Entry::new(Term::new(1), Payload::Command(b"not a request".to_vec())),
        ];
        assert_eq!(run(&mut state, &entries), [None, None]);
        assert_eq!(state.applied_index(), Index::new(2));
        assert_eq!(state.dump().len(), 1);
    }

    #[test]
    fn an_entry_out_of_order_is_refused() {
        let mut state = open(&SimFs::new());
        assert!(state.apply(Index::new(2), &entry(1, 1, put(b"a"))).is_err());
        assert_eq!(state.applied_index(), Index::ZERO);
    }

    /// The database is not synced, so a power cut may lose recent batches. What is left must be
    /// the result of a prefix of the log, and applying the log again must lead to the same store.
    #[test]
    fn after_a_power_cut_the_log_can_be_applied_again() {
        let entries: Vec<Entry> = (1..=40)
            .map(|seq| {
                if seq % 3 == 0 {
                    entry(1, seq, get())
                } else {
                    entry(1, seq, put(&seq.to_be_bytes()))
                }
            })
            .collect();

        let reference = {
            let mut state = open(&SimFs::new());
            run(&mut state, &entries);
            state.dump()
        };

        for seed in 0..50 {
            let fs = SimFs::new();
            let mut state = open(&fs);
            run(&mut state, &entries);
            let fs = fs.power_loss(Torn::RandomPrefix(seed));
            drop(state);

            let mut state = open(&fs);
            let survived = state.applied_index().get();
            assert!(survived <= 40);
            // Entries the store already holds are skipped; the rest are applied for real.
            run(&mut state, &entries);
            assert_eq!(
                state.dump(),
                reference,
                "seed {seed}, {survived} entries survived the cut"
            );
        }
    }
}
