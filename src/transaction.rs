use std::default::Default;

use fog_pack::{
    document::{Document, NewDocument},
    entry::{Entry, EntryRef, NewEntry}, types::Timestamp,
};

pub(crate) struct TtlEntry {
    pub(crate) entry: EntryRef,
    pub(crate) ttl: Option<i64>,
}

pub struct Transaction {
    pub(crate) new_doc: Vec<NewDocument>,
    pub(crate) doc: Vec<Document>,
    pub(crate) new_entry: Vec<NewEntry>,
    pub(crate) entry: Vec<Entry>,
    pub(crate) ttl: Vec<TtlEntry>,
    pub(crate) del_entry: Vec<EntryRef>,
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            new_doc: Vec::new(),
            doc: Vec::new(),
            new_entry: Vec::new(),
            entry: Vec::new(),
            ttl: Vec::new(),
            del_entry: Vec::new(),
        }
    }

    pub fn add_new_doc(mut self, doc: NewDocument) -> Self {
        self.new_doc.push(doc);
        self
    }

    pub fn add_doc(mut self, doc: Document) -> Self {
        self.doc.push(doc);
        self
    }

    pub fn add_new_entry(mut self, entry: NewEntry) -> Self {
        self.new_entry.push(entry);
        self
    }

    pub fn add_entry(mut self, entry: Entry) -> Self {
        self.entry.push(entry);
        self
    }

    /// Sets time at which an entry should be deleted, given as a fog-pack time. This sets the same 
    /// timer same timer used by [`set_delete_time`]. The exact deletion time may vary, depending 
    /// on how busy the database is, whether the database is currently running, etc.
    pub fn set_delete_time(mut self, entry: &EntryRef, time: Option<Timestamp>) -> Self {
        self.ttl.push(TtlEntry {
            entry: entry.clone(),
            ttl: time.map(|t| t.timestamp_utc()),
        });
        self
    }

    /// Sets time-to-live for an entry, timed in seconds from when this is called. This sets the 
    /// same timer used by [`set_delete_time`]. The exact deletion time may vary, depending on how 
    /// busy the database is, whether the database is currently running, etc.
    pub fn set_ttl(self, entry: &EntryRef, ttl: Option<u64>) -> Self {
        let time = ttl.map(|offset| { 
            Timestamp::now().expect("Time is before UNIX epoch or has very wrong nanosecond count")
                + (offset as i64) 
        });
        self.set_delete_time(entry, time)
    }

    pub fn del_entry(mut self, entry: &EntryRef) -> Self {
        self.del_entry.push(entry.clone());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn transact() {
        let doc = NewDocument::new(None, vec![0, 1, 2, 3]).unwrap();

        let transaction = Transaction::new().add_new_doc(doc);
    }
}
