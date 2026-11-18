//! The in-memory log.
//!
//! The log does not have to start at index 1. Everything up to and including the base is in a
//! snapshot; the log holds what comes after. Of the base itself only the term is kept, because
//! that is all replication ever asks about it.

use crate::types::{Entry, Index, SnapshotMeta, Term};

#[derive(Debug, Default)]
pub(crate) struct Log {
    base: SnapshotMeta,
    entries: Vec<Entry>,
}

impl Log {
    pub(crate) const fn from_parts(base: SnapshotMeta, entries: Vec<Entry>) -> Self {
        Self { base, entries }
    }

    /// Where the entry at `index` sits in the vector, if the log still holds it.
    fn position(&self, index: Index) -> Option<usize> {
        let after_base = index.get().checked_sub(self.base.index().get())?;
        usize::try_from(after_base.checked_sub(1)?).ok()
    }

    /// The point the log starts after.
    pub(crate) const fn base(&self) -> SnapshotMeta {
        self.base
    }

    /// The entries after the base.
    pub(crate) fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub(crate) fn last_index(&self) -> Index {
        let held = u64::try_from(self.entries.len()).unwrap_or(u64::MAX);
        Index::new(self.base.index().get().saturating_add(held))
    }

    pub(crate) fn last_term(&self) -> Term {
        self.entries.last().map_or(self.base.term(), Entry::term)
    }

    pub(crate) fn get(&self, index: Index) -> Option<&Entry> {
        self.entries.get(self.position(index)?)
    }

    /// The term at `index`: known for the base and for every entry held, unknown before the base
    /// and past the end. A new log has base 0 with term 0, so "the entry before the first" always
    /// matches.
    pub(crate) fn term_at(&self, index: Index) -> Option<Term> {
        if index == self.base.index() {
            Some(self.base.term())
        } else {
            self.get(index).map(Entry::term)
        }
    }

    /// The first index of the run of entries that share the term at `index`, as far back as the
    /// log goes.
    pub(crate) fn first_index_of_term(&self, index: Index) -> Index {
        let term = self.term_at(index);
        let mut first = index;
        while first > self.base.index().next() && self.term_at(first.prev()) == term {
            first = first.prev();
        }
        first
    }

    /// Up to `max` entries starting at `from`. Empty if `from` is at or before the base.
    pub(crate) fn slice_from(&self, from: Index, max: usize) -> Vec<Entry> {
        let Some(start) = self.position(from) else {
            return Vec::new();
        };
        self.entries.iter().skip(start).take(max).cloned().collect()
    }

    pub(crate) fn append(&mut self, entry: Entry) {
        self.entries.push(entry);
    }

    /// Drops every entry after `index`.
    pub(crate) fn truncate_after(&mut self, index: Index) {
        let keep = index.get().saturating_sub(self.base.index().get());
        self.entries
            .truncate(usize::try_from(keep).unwrap_or(usize::MAX));
    }

    /// Moves the base up to `index`, dropping the entries it covers. Does nothing if `index` is
    /// not after the base or is past the end.
    pub(crate) fn compact_to(&mut self, index: Index) {
        let (Some(term), Some(position)) = (self.term_at(index), self.position(index)) else {
            return;
        };
        self.entries.drain(..=position);
        self.base = SnapshotMeta::new(index, term);
    }

    /// Replaces the whole log with an empty one that starts after `base`.
    pub(crate) fn reset(&mut self, base: SnapshotMeta) {
        self.entries.clear();
        self.base = base;
    }
}

#[cfg(test)]
mod tests {
    use super::Log;
    use crate::types::{Entry, Index, Payload, SnapshotMeta, Term};

    fn log_with_terms(terms: &[u64]) -> Log {
        let entries = terms
            .iter()
            .map(|&term| Entry::new(Term::new(term), Payload::Noop))
            .collect();
        Log::from_parts(SnapshotMeta::default(), entries)
    }

    #[test]
    fn compacting_moves_the_base_and_keeps_every_later_index_where_it_was() {
        let mut log = log_with_terms(&[1, 1, 2, 2, 3]);
        log.compact_to(Index::new(3));

        assert_eq!(log.base(), SnapshotMeta::new(Index::new(3), Term::new(2)));
        assert_eq!(log.last_index(), Index::new(5));
        assert_eq!(log.entries().len(), 2);
        assert_eq!(log.term_at(Index::new(2)), None);
        assert_eq!(log.term_at(Index::new(3)), Some(Term::new(2)));
        assert_eq!(log.term_at(Index::new(4)), Some(Term::new(2)));
        assert_eq!(log.term_at(Index::new(5)), Some(Term::new(3)));
        assert_eq!(log.term_at(Index::new(6)), None);
        assert_eq!(log.slice_from(Index::new(5), 10).len(), 1);
        assert_eq!(log.slice_from(Index::new(3), 10), Vec::new());
    }

    #[test]
    fn compacting_everything_leaves_an_empty_log_that_remembers_its_last_term() {
        let mut log = log_with_terms(&[1, 4]);
        log.compact_to(Index::new(2));
        assert_eq!(log.entries(), []);
        assert_eq!(log.last_index(), Index::new(2));
        assert_eq!(log.last_term(), Term::new(4));
    }

    #[test]
    fn compacting_backwards_or_past_the_end_changes_nothing() {
        let mut log = log_with_terms(&[1, 1, 2]);
        log.compact_to(Index::new(2));
        log.compact_to(Index::new(1));
        log.compact_to(Index::new(9));
        assert_eq!(log.base().index(), Index::new(2));
        assert_eq!(log.entries().len(), 1);
    }

    #[test]
    fn truncating_and_term_runs_respect_the_base() {
        let mut log = log_with_terms(&[1, 2, 2, 2, 3]);
        log.compact_to(Index::new(2));
        // The run of term 2 reaches back into the snapshot; the answer stops at what is held.
        assert_eq!(log.first_index_of_term(Index::new(4)), Index::new(3));
        log.truncate_after(Index::new(3));
        assert_eq!(log.last_index(), Index::new(3));
        // Truncating into the snapshot empties the log and leaves the base alone.
        log.truncate_after(Index::new(1));
        assert_eq!(log.last_index(), Index::new(2));
    }
}
