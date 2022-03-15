use super::*;


/// A Node ID and User ID that together indicate the source of a retrieved Document or Entry.
struct IdPair {
    /// The Identity of a specific node. Stable for the lifetime of the running process/session. 
    /// Think of it as an IP address + port number: unique, identifies a specific process, but 
    /// shouldn't be used as a secure identifier of who we're talking to.
    nodeId: Identity,
    /// The Identity of the node's user. This identifies who, specifically, is being talked to, 
    /// regardless of which process/device is being used. This is equivalent to a long-term signing 
    /// key.
    userId: Option<Identity>, 
}

impl IdPair {

    pub fn new(nodeId: Identity, userId: Option<Identity>) -> Self {
        Self { nodeId, userId }
    }

    pub fn node(&self) -> &Identity {
        &self.nodeId
    }

    pub fn user(&self) -> &Option<Identity> {
        &self.userId
    }

}

/// A Node ID + User ID pair that, when matched, allows access to an Entry this is attached to. 
/// Allowed combinations are: just a Node ID, just a User ID, or both a Node ID and User ID.
pub struct AccessPair {
    nodeId: Option<Identity>,
    userId: Option<Identity>,
}

impl AccessPair {

    pub fn new_pair(nodeId: Identity, userId: Identity) -> Self {
        Self { nodeId: Some(nodeId), userId: Some(userId) }
    }

    pub fn new_node(nodeId: Identity) -> Self {
        Self { nodeId: Some(nodeId), userId: None }
    }

    pub fn new_user(userId: Identity) -> Self {
        Self { nodeId: None, userId: Some(userId) }
    }

    pub fn node(&self) -> &Option<Identity> {
        &self.nodeId
    }

    pub fn user(&self) -> &Option<Identity> {
        &self.userId
    }

}

struct EntryRecord {
    entry: Entry,
    perm: Permission,
    ttl: Option<u32>,
    priority: Option<u64>,
}

pub struct Transaction {
    add_docs: Vec<Document>,
    add_entries: Vec<EntryRecord>,
    del_entries: Vec<(Hash, Hash)>,
    perm_changes: Vec<(Hash, Hash, Option<Permission>)>,
    ttl_entries: Vec<(Hash, Hash, Option<u32>)>,
    add_names: Vec<(String, Hash)>,
    drop_names: Vec<String>,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            add_docs: Vec::new(),
            add_entries: Vec::new(),
            del_entries: Vec::new(),
            perm_changes: Vec::new(),
            ttl_entries: Vec::new(),
            add_names: Vec::new(),
            drop_names: Vec::new(),
        }
    }

    /// Add a Document into the database. Failure occurs if the Document's schema isn't in the 
    /// database or transaction (`SchemaNotFound`), or if the Document fails validation (`Failed`).
    pub fn add_doc(&mut self, doc: Document) {
        self.add_docs.push(doc);
    }

    /// Add an Entry into the database. Failure occurs if the Entry's parent isn't in the database 
    /// (`DocNotFound`) or if the Entry fails validation (`Failed`).
    pub fn add_entry(&mut self, entry: Entry, perm: Option<Permission>) {
        self.add_entries.push((entry, perm, None));
    }

    /// Add an Entry into the database with a time-to-live, in seconds. If `ttl` is zero, the 
    /// entry will not be added to the database, but will be routed to any active queries it 
    /// matches. Failure occurs if the Entry's parent isn't in the database (`DocNotFound`) or if 
    /// the Entry fails validation (`Failed`).
    pub fn add_entry_ttl(&mut self, entry: Entry, perm: Option<Permission>, ttl: u32) {
        self.add_entries.push((entry, perm, Some(ttl)));
    }

    /// Drop an Entry from the database, using its hash and the parent document hash. If the entry 
    /// is not in the database, nothing happens and the transaction succeeds.
    pub fn drop_entry(&mut self, doc: Hash, entry: Hash) {
        self.del_entries.push((doc, entry));
    }
    /// Change the permissions on a Document. If the document is not in the database, nothing 
    /// happens and the transaction succeeds.
    pub fn set_permission(&mut self, doc: Hash, p: Permission) {
        self.perm_changes.push((doc, p));
    }
    /// Set the time-to-live (TTL) for an Entry, in seconds. If set to None, TTL is removed and the 
    /// entry will persist indefinitely. If there is no matching entry, nothing happens and the 
    /// transaction succeeds.
    pub fn set_ttl(&mut self, doc: Hash, entry: Hash, ttl: Option<u32>) {
        self.ttl_entries.push((doc, entry, ttl));
    }
    /// Add a named Document to the database. If the name already exists, the name will point to 
    /// the new Document hash.
    pub fn add_name(&mut self, name: String, doc: Hash) {
        self.add_names.push((name, doc));
    }
    /// Remove a name from the database. If the name does not exist, nothing happens and the 
    /// transaction succeeds.
    pub fn drop_name(&mut self, name: String) {
        self.drop_names.push(name);
    }
}

// Why didn't the transaction work? Do I care, or just need to know it failed???
pub enum TransactionResult {
    /// It worked
    Ok,
    /// It failed
    Failed,
    /// At least one Document referenced was missing. This only occurs if an Entry is being added 
    /// to a document that is neither in the databse nor in the transaction.
    DocNotFound,
    /// One of the documents in the transaction used a schema that isn't in the database
    SchemaNotFound,
}


pub trait TransactionPort<O: Future<Output = TransactionResult>> {

    /// Submit a Transaction to the database, returning a TransactionResult.
    fn submit_transaction(&mut self, t: Transaction) -> O;

    /// Add a Document into the database. Failure occurs if the Document's schema isn't in the 
    /// database or transaction (`SchemaNotFound`), or if the Document fails validation (`Failed`).
    fn add_doc(&mut self, doc: Document, p: Permission) -> O {
        let mut t = Transaction::new();
        t.add_doc(doc, p);
        self.submit_transaction(t)
    }

    /// Add an Entry into the database. Failure occurs if the Entry's parent isn't in the database 
    /// (`DocNotFound`) or if the Entry fails validation (`Failed`).
    fn add_entry(&mut self, entry: Entry) -> O {
        let mut t = Transaction::new();
        t.add_entry(entry);
        self.submit_transaction(t)
    }

    /// Add an Entry into the database with a time-to-live, in seconds. If `ttl` is zero, the 
    /// entry will not be added to the database, but will be routed to any active queries it 
    /// matches. Failure occurs if the Entry's parent isn't in the database (`DocNotFound`) or if 
    /// the Entry fails validation (`Failed`).
    fn add_entry_ttl(&mut self, entry: Entry, ttl: u32) -> O {
        let mut t = Transaction::new();
        t.add_entry_ttl(entry, ttl);
        self.submit_transaction(t)
    }

    /// Drop an Entry from the database, using its hash and the parent document hash. If the entry 
    /// is not in the database, nothing happens and the transaction succeeds.
    fn drop_entry(&mut self, doc: Hash, entry: Hash) -> O {
        let mut t = Transaction::new();
        t.drop_entry(doc, entry);
        self.submit_transaction(t)
    }

    /// Change the permissions on a Document. If the document is not in the database, nothing 
    /// happens and the transaction succeeds.
    fn set_permission(&mut self, doc: Hash, p: Permission) -> O {
        let mut t = Transaction::new();
        t.set_permission(doc, p);
        self.submit_transaction(t)
    }

    /// Set the time-to-live (TTL) for an Entry, in seconds. If set to None, TTL is removed and the 
    /// entry will persist indefinitely. If there is no matching entry, nothing happens and the 
    /// transaction succeeds.
    fn set_ttl(&mut self, doc: Hash, entry: Hash, ttl: Option<u32>) -> O {
        let mut t = Transaction::new();
        t.set_ttl(doc, entry, ttl);
        self.submit_transaction(t)
    }

    /// Add a named Document to the database. If the name already exists, the name will point to 
    /// the new Document hash.
    fn add_name(&mut self, name: String, doc: Hash) -> O {
        let mut t = Transaction::new();
        t.add_name(name, doc);
        self.submit_transaction(t)
    }

    /// Remove a name from the database. If the name does not exist, nothing happens and the 
    /// transaction succeeds.
    fn drop_name(&mut self, name: String) -> O {
        let mut t = Transaction::new();
        t.drop_name(name);
        self.submit_transaction(t)
    }

}

