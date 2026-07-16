use crate::types::{LogIndex, Term};

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub data: Vec<u8>,
}

/// Append-only log with 1-based indexing.
/// entries[0] in the backing vec corresponds to LogIndex 1.
#[derive(Debug, Default)]
pub struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        Log { entries: Vec::new() }
    }

    /// Index of the last entry, or 0 if the log is empty.
    pub fn last_index(&self) -> LogIndex {
        self.entries.len() as LogIndex
    }

    /// Term of the last entry, or 0 if the log is empty.
    pub fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        self.get(index).map(|e| e.term)
    }

    /// Appends a new entry, returning its assigned index.
    pub fn append(&mut self, term: Term, data: Vec<u8>) -> LogIndex {
        let index = self.last_index() + 1;
        self.entries.push(LogEntry { term, index, data });
        index
    }

    /// Truncates the log to drop everything from `from_index` onward.
    /// Used when a conflicting entry is detected during AppendEntries.
    pub fn truncate_from(&mut self, from_index: LogIndex) {
        if from_index == 0 {
            self.entries.clear();
            return;
        }
        self.entries.truncate((from_index - 1) as usize);
    }

    pub fn entries_from(&self, index: LogIndex) -> &[LogEntry] {
        if index == 0 || index > self.last_index() {
            return &[];
        }
        &self.entries[(index - 1) as usize..]
    }
}
