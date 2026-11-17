//! A linearizability checker for the history of one key.
//!
//! A history is linearizable if every operation can be placed at a single instant between its
//! invocation and its return, such that the operations in that order behave like a plain
//! single-threaded store. Operations on different keys do not interact, so each key is checked
//! by itself.
//!
//! The search is the one by Wing and Gong, with the memoisation Lowe added: pick an operation
//! that could have happened first, apply it to the model, and continue with the rest; back up
//! when an operation's result does not fit. A set of (operations placed, value of the key)
//! already seen to fail is kept, which is what makes it fast enough.

use std::collections::HashSet;

/// What a client asked for. Values are small numbers; the simulator maps them to bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Call {
    Get,
    Put(u64),
    Delete,
    CompareAndSet { expected: Option<u64>, value: u64 },
}

/// What the client was told.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Return {
    Value(Option<u64>),
    Done,
    Swapped(bool),
}

/// One operation of a history. Times come from one counter shared by all clients, so no two
/// events have the same time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Operation {
    pub(crate) call: Call,
    pub(crate) invoked: u64,
    /// When the client got its answer, and the answer. `None` if it never did: such an
    /// operation may have taken effect at any time after it was invoked, or not at all.
    pub(crate) returned: Option<(u64, Return)>,
}

/// The store would not have answered that.
struct Mismatch;

/// The value of the key after `call`, if `result` is what a correct store would have answered.
/// An operation without a result fits whatever the store would have answered.
fn step(value: Option<u64>, call: Call, result: Option<Return>) -> Result<Option<u64>, Mismatch> {
    let (next, expected) = match call {
        Call::Get => (value, Return::Value(value)),
        Call::Put(new) => (Some(new), Return::Done),
        Call::Delete => (None, Return::Done),
        Call::CompareAndSet {
            expected,
            value: new,
        } => {
            if value == expected {
                (Some(new), Return::Swapped(true))
            } else {
                (value, Return::Swapped(false))
            }
        }
    };
    if result.is_none_or(|result| result == expected) {
        Ok(next)
    } else {
        Err(Mismatch)
    }
}

struct Search<'a> {
    operations: &'a [Operation],
    placed: Vec<bool>,
    /// Operations that returned and are not placed yet. The search succeeds when this is zero.
    unplaced_returned: usize,
    failed: HashSet<(Vec<bool>, Option<u64>)>,
}

impl Search<'_> {
    fn run(&mut self, value: Option<u64>) -> bool {
        if self.unplaced_returned == 0 {
            return true;
        }
        if self.failed.contains(&(self.placed.clone(), value)) {
            return false;
        }
        // An operation can come next only if it was invoked before every unplaced operation
        // returned. Otherwise some operation finished before it started, and has to go first.
        let horizon = self
            .operations
            .iter()
            .zip(&self.placed)
            .filter(|(_, placed)| !**placed)
            .filter_map(|(operation, _)| operation.returned.map(|(at, _)| at))
            .min()
            .unwrap_or(u64::MAX);

        for index in 0..self.operations.len() {
            let operation = self.operations[index];
            if self.placed[index] || operation.invoked > horizon {
                continue;
            }
            let result = operation.returned.map(|(_, result)| result);
            let Ok(next) = step(value, operation.call, result) else {
                continue;
            };
            self.placed[index] = true;
            self.unplaced_returned -= usize::from(operation.returned.is_some());
            if self.run(next) {
                return true;
            }
            self.unplaced_returned += usize::from(operation.returned.is_some());
            self.placed[index] = false;
        }
        self.failed.insert((self.placed.clone(), value));
        false
    }
}

/// Whether the operations on one key, which starts out absent, are linearizable.
pub(crate) fn linearizable(operations: &[Operation]) -> bool {
    let mut search = Search {
        operations,
        placed: vec![false; operations.len()],
        unplaced_returned: operations
            .iter()
            .filter(|operation| operation.returned.is_some())
            .count(),
        failed: HashSet::new(),
    };
    search.run(None)
}

#[cfg(test)]
mod tests {
    use super::{Call, Operation, Return, linearizable};

    fn done(call: Call, invoked: u64, returned: u64, result: Return) -> Operation {
        Operation {
            call,
            invoked,
            returned: Some((returned, result)),
        }
    }

    fn lost(call: Call, invoked: u64) -> Operation {
        Operation {
            call,
            invoked,
            returned: None,
        }
    }

    #[test]
    fn an_empty_history_is_linearizable() {
        assert!(linearizable(&[]));
    }

    #[test]
    fn operations_one_after_another_must_agree_with_the_model() {
        let good = [
            done(Call::Get, 1, 2, Return::Value(None)),
            done(Call::Put(1), 3, 4, Return::Done),
            done(Call::Get, 5, 6, Return::Value(Some(1))),
            done(Call::Delete, 7, 8, Return::Done),
            done(Call::Get, 9, 10, Return::Value(None)),
        ];
        assert!(linearizable(&good));

        let mut bad = good;
        bad[2] = done(Call::Get, 5, 6, Return::Value(None));
        assert!(!linearizable(&bad));
    }

    #[test]
    fn a_read_that_overlaps_a_write_may_see_either_value() {
        for seen in [None, Some(1)] {
            let history = [
                done(Call::Put(1), 1, 4, Return::Done),
                done(Call::Get, 2, 3, Return::Value(seen)),
            ];
            assert!(linearizable(&history), "the read saw {seen:?}");
        }
    }

    #[test]
    fn a_read_after_a_write_returned_must_see_it() {
        let stale = [
            done(Call::Put(1), 1, 2, Return::Done),
            done(Call::Get, 3, 4, Return::Value(None)),
        ];
        assert!(!linearizable(&stale));
    }

    #[test]
    fn two_reads_cannot_see_a_value_come_back() {
        // The first read proves the second write took effect; the later read sees the first
        // write again, which no order of the two writes explains.
        let history = [
            done(Call::Put(1), 1, 2, Return::Done),
            done(Call::Put(2), 3, 4, Return::Done),
            done(Call::Get, 5, 6, Return::Value(Some(2))),
            done(Call::Get, 7, 8, Return::Value(Some(1))),
        ];
        assert!(!linearizable(&history));
    }

    #[test]
    fn only_one_of_two_racing_swaps_can_win() {
        let swap = Call::CompareAndSet {
            expected: None,
            value: 1,
        };
        let other = Call::CompareAndSet {
            expected: None,
            value: 2,
        };
        let one_wins = [
            done(swap, 1, 4, Return::Swapped(true)),
            done(other, 2, 3, Return::Swapped(false)),
        ];
        assert!(linearizable(&one_wins));
        let both_win = [
            done(swap, 1, 4, Return::Swapped(true)),
            done(other, 2, 3, Return::Swapped(true)),
        ];
        assert!(!linearizable(&both_win));
    }

    #[test]
    fn a_request_that_never_returned_may_or_may_not_have_happened() {
        let happened = [
            lost(Call::Put(1), 1),
            done(Call::Get, 2, 3, Return::Value(Some(1))),
        ];
        assert!(linearizable(&happened));
        let did_not = [
            lost(Call::Put(1), 1),
            done(Call::Get, 2, 3, Return::Value(None)),
        ];
        assert!(linearizable(&did_not));
        // It cannot have happened before it was sent.
        let too_early = [
            done(Call::Get, 1, 2, Return::Value(Some(1))),
            lost(Call::Put(1), 3),
        ];
        assert!(!linearizable(&too_early));
    }

    /// A request carried out twice: the second copy lands after another client's write, and a
    /// later read sees the first client's value again.
    #[test]
    fn a_write_applied_twice_is_not_linearizable() {
        let history = [
            done(Call::Put(1), 1, 2, Return::Done),
            done(Call::Put(2), 3, 4, Return::Done),
            done(Call::Get, 5, 6, Return::Value(Some(1))),
        ];
        assert!(!linearizable(&history));
    }
}
