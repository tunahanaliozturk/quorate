//! The key-value commands that travel through the log, and their replies.
//!
//! A [`Request`] is what a client asks for. Encoded, it is the command of a log entry. It names a
//! session and carries a sequence number, so that a request the client had to send twice is
//! carried out once.

/// Identifies a client session. The client picks it; it only has to be unique, so a random
/// 64-bit number does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(u64);

impl SessionId {
    /// Wraps a raw session id.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw session id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// One operation on the store.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Op {
    /// Read a key.
    Get {
        /// The key to read.
        key: Vec<u8>,
    },
    /// Set a key, whatever it held.
    Put {
        /// The key to set.
        key: Vec<u8>,
        /// Its new value.
        value: Vec<u8>,
    },
    /// Remove a key. Removing a key that is not there is not an error.
    Delete {
        /// The key to remove.
        key: Vec<u8>,
    },
    /// Set a key only if it currently holds `expected`.
    CompareAndSet {
        /// The key to set.
        key: Vec<u8>,
        /// What the key must hold now; `None` means it must be absent.
        expected: Option<Vec<u8>>,
        /// Its new value if the comparison holds.
        value: Vec<u8>,
    },
}

impl Op {
    /// The key the operation touches.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Get { key }
            | Self::Put { key, .. }
            | Self::Delete { key }
            | Self::CompareAndSet { key, .. } => key,
        }
    }
}

/// The result of an [`Op`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reply {
    /// A put or a delete was carried out.
    Done,
    /// What a get found; `None` if the key was absent.
    Value(Option<Vec<u8>>),
    /// Whether a compare-and-set found what it expected and wrote the new value.
    Swapped(bool),
}

/// An operation with the session it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    session: SessionId,
    seq: u64,
    op: Op,
}

const KIND_GET: u8 = 0;
const KIND_PUT: u8 = 1;
const KIND_DELETE: u8 = 2;
const KIND_COMPARE_AND_SET: u8 = 3;

const REPLY_DONE: u8 = 0;
const REPLY_VALUE: u8 = 1;
const REPLY_SWAPPED: u8 = 2;

impl Request {
    /// Builds a request.
    ///
    /// A session sends one request at a time and gives each a higher `seq` than the one before.
    /// A retry of the same request keeps its `seq`.
    #[must_use]
    pub const fn new(session: SessionId, seq: u64, op: Op) -> Self {
        Self { session, seq, op }
    }

    /// The session the request belongs to.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }

    /// The request's number within its session.
    #[must_use]
    pub const fn seq(&self) -> u64 {
        self.seq
    }

    /// The operation.
    #[must_use]
    pub const fn op(&self) -> &Op {
        &self.op
    }

    /// The bytes to propose to the cluster.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.session.0.to_be_bytes());
        out.extend_from_slice(&self.seq.to_be_bytes());
        match &self.op {
            Op::Get { key } => {
                out.push(KIND_GET);
                put_bytes(&mut out, key);
            }
            Op::Put { key, value } => {
                out.push(KIND_PUT);
                put_bytes(&mut out, key);
                put_bytes(&mut out, value);
            }
            Op::Delete { key } => {
                out.push(KIND_DELETE);
                put_bytes(&mut out, key);
            }
            Op::CompareAndSet {
                key,
                expected,
                value,
            } => {
                out.push(KIND_COMPARE_AND_SET);
                put_bytes(&mut out, key);
                put_optional(&mut out, expected.as_deref());
                put_bytes(&mut out, value);
            }
        }
        out
    }

    /// Reads a request back. Returns `None` for bytes that are not exactly one request.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut reader = Reader(bytes);
        let session = SessionId(reader.u64()?);
        let seq = reader.u64()?;
        let op = match reader.u8()? {
            KIND_GET => Op::Get {
                key: reader.bytes()?,
            },
            KIND_PUT => Op::Put {
                key: reader.bytes()?,
                value: reader.bytes()?,
            },
            KIND_DELETE => Op::Delete {
                key: reader.bytes()?,
            },
            KIND_COMPARE_AND_SET => Op::CompareAndSet {
                key: reader.bytes()?,
                expected: reader.optional().ok()?,
                value: reader.bytes()?,
            },
            _ => return None,
        };
        reader.0.is_empty().then_some(Self { session, seq, op })
    }
}

impl Reply {
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Done => out.push(REPLY_DONE),
            Self::Value(value) => {
                out.push(REPLY_VALUE);
                put_optional(out, value.as_deref());
            }
            Self::Swapped(swapped) => {
                out.push(REPLY_SWAPPED);
                out.push(u8::from(*swapped));
            }
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        let mut reader = Reader(bytes);
        let reply = match reader.u8()? {
            REPLY_DONE => Self::Done,
            REPLY_VALUE => Self::Value(reader.optional().ok()?),
            REPLY_SWAPPED => Self::Swapped(match reader.u8()? {
                0 => false,
                1 => true,
                _ => return None,
            }),
            _ => return None,
        };
        reader.0.is_empty().then_some(reply)
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    // A slice is never longer than `u64::MAX` bytes on any platform Rust supports.
    out.extend_from_slice(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn put_optional(out: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        None => out.push(0),
        Some(bytes) => {
            out.push(1);
            put_bytes(out, bytes);
        }
    }
}

struct Malformed;

/// Reads fields off the front of a byte slice. Every method returns `None` instead of reading
/// past the end.
struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn u8(&mut self) -> Option<u8> {
        let (&first, rest) = self.0.split_first()?;
        self.0 = rest;
        Some(first)
    }

    fn u64(&mut self) -> Option<u64> {
        let (head, rest) = self.0.split_first_chunk::<8>()?;
        self.0 = rest;
        Some(u64::from_be_bytes(*head))
    }

    fn bytes(&mut self) -> Option<Vec<u8>> {
        let len = usize::try_from(self.u64()?).ok()?;
        let (head, rest) = self.0.split_at_checked(len)?;
        self.0 = rest;
        Some(head.to_vec())
    }

    /// A value that may be absent. `Err` means the bytes are not a well-formed field.
    fn optional(&mut self) -> Result<Option<Vec<u8>>, Malformed> {
        match self.u8() {
            Some(0) => Ok(None),
            Some(1) => self.bytes().map(Some).ok_or(Malformed),
            _ => Err(Malformed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Op, Reply, Request, SessionId};

    fn requests() -> Vec<Request> {
        let key = b"k".to_vec();
        [
            Op::Get { key: key.clone() },
            Op::Put {
                key: key.clone(),
                value: Vec::new(),
            },
            Op::Delete { key: Vec::new() },
            Op::CompareAndSet {
                key: key.clone(),
                expected: None,
                value: b"new".to_vec(),
            },
            Op::CompareAndSet {
                key,
                expected: Some(b"old".to_vec()),
                value: b"new".to_vec(),
            },
        ]
        .into_iter()
        .zip(1..)
        .map(|(op, seq)| Request::new(SessionId::new(u64::MAX - seq), seq, op))
        .collect()
    }

    #[test]
    fn a_request_survives_encoding() {
        for request in requests() {
            assert_eq!(Request::decode(&request.encode()), Some(request));
        }
    }

    #[test]
    fn a_reply_survives_encoding() {
        let replies = [
            Reply::Done,
            Reply::Value(None),
            Reply::Value(Some(Vec::new())),
            Reply::Value(Some(b"v".to_vec())),
            Reply::Swapped(false),
            Reply::Swapped(true),
        ];
        for reply in replies {
            let mut bytes = Vec::new();
            reply.encode(&mut bytes);
            assert_eq!(Reply::decode(&bytes), Some(reply));
        }
    }

    /// The log can hold anything a client sent. Every node must reach the same verdict on it, so
    /// decoding must never panic and never accept a prefix or a padded request.
    #[test]
    fn damaged_requests_are_refused_not_misread() {
        for request in requests() {
            let bytes = request.encode();
            for cut in 0..bytes.len() {
                assert_eq!(
                    Request::decode(&bytes[..cut]),
                    None,
                    "a prefix of {cut} bytes was accepted"
                );
            }
            let mut padded = bytes.clone();
            padded.push(0);
            assert_eq!(Request::decode(&padded), None);
        }
        // A length that claims more bytes than there are, including one that overflows `usize`.
        let mut huge = requests()[0].encode();
        huge[17..25].copy_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(Request::decode(&huge), None);
    }
}
