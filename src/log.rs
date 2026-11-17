//! The in-memory log. Indices start at 1.

use crate::types::{Entry, Index, Term};

#[derive(Debug, Default)]
pub(crate) struct Log {
    entries: Vec<Entry>,
}

/// Where the entry at `index` sits in the vector, if `index` is a real position.
fn position(index: Index) -> Option<usize> {
    usize::try_from(index.get().checked_sub(1)?).ok()
}

impl Log {
    pub(crate) fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub(crate) fn last_index(&self) -> Index {
        Index::new(u64::try_from(self.entries.len()).unwrap_or(u64::MAX))
    }

    pub(crate) fn last_term(&self) -> Term {
        self.entries.last().map_or_else(Term::default, Entry::term)
    }

    pub(crate) fn get(&self, index: Index) -> Option<&Entry> {
        self.entries.get(position(index)?)
    }

    /// The term at `index`. Index 0 has term 0, so "the entry before the first" always matches.
    pub(crate) fn term_at(&self, index: Index) -> Option<Term> {
        if index == Index::ZERO {
            Some(Term::default())
        } else {
            self.get(index).map(Entry::term)
        }
    }

    /// The first index of the run of entries that share the term at `index`.
    pub(crate) fn first_index_of_term(&self, index: Index) -> Index {
        let term = self.term_at(index);
        let mut first = index;
        while first > Index::new(1) && self.term_at(first.prev()) == term {
            first = first.prev();
        }
        first
    }

    /// Up to `max` entries starting at `from`.
    pub(crate) fn slice_from(&self, from: Index, max: usize) -> Vec<Entry> {
        let Some(start) = position(from) else {
            return Vec::new();
        };
        self.entries.iter().skip(start).take(max).cloned().collect()
    }

    pub(crate) fn append(&mut self, entry: Entry) {
        self.entries.push(entry);
    }

    /// Drops every entry after `index`.
    pub(crate) fn truncate_after(&mut self, index: Index) {
        self.entries
            .truncate(usize::try_from(index.get()).unwrap_or(usize::MAX));
    }
}
