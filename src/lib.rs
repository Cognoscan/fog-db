extern crate fog_pack;

use fog_pack::{fogpack, Document, Entry, Identity, Hash, Value, ValueRef};
use futures_sink::*;
use std::future::Future;

/// Returns the Schema Document used for certificates.
pub fn certificate_schema() -> Document {
    Document::new(fogpack!({
        "req": {
            "subject": { "type": "Ident", "query": true },
            "key": { "type": "Str", "max_len": 100, "query": true },
            "value": { "type": "Str", "max_len": 100 },
            "start": { "type": "Time", "query": true, "ord": true },
            "end": { "type": "Time" },
        }
    })).unwrap()
}

pub fn permission_schema() -> Document {
    Document::new(fogpack!({
        "req": {
            "nets": {
                "type": "Array",
                "extra_items": { "type": "Str" }
            },
            "certs": {
                "type": "Array",
                "extra_items": {
                    "type": "Obj",
                    "req": {
                        "roots": {
                            "type": "Array",
                            "extra_items": { "type": "Ident" }
                        },
                        "chain": {
                            "type": "Array",
                            "extra_items": {
                                "type": "Obj",
                                "req": {
                                    "key": { "type": "Str" },
                                    "val": { "type": "Str" },
                                    "min_issuers": {
                                        "type": "Int",
                                        "min": 0,
                                        "max": 255,
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })).unwrap()
}

/// The priority of a query or document request.
///
/// Priorities are used to help a database or network connection prioritize requests. They do not 
/// imply strict behavior, but rather expect the request servicer to make a best effort at 
/// prioritizing based on the expected use-cases for each priority level. With the exception of 
/// `Immediate`, none of these should take absolute precedence over the others.
pub enum Priority {
    /// Should supercede all other priorities. Reserved for requests that must take absolute 
    /// priority, like emergency service information, or systems-critical communications. It is 
    /// expected that a servicer will always handle pending Immediate priorities first, and so this 
    /// priority level should be used very sparingly, if used at all.
    Immediate,
    /// Used for Real-time requests. This is for when a request is made in service to an immediate, 
    /// real-time application, like voice & video conferencing, online gaming, or other low-latency 
    /// interactive applications.
    RealTime,
    /// Used for active requests. This is meant for semi-interactive applications, like video 
    /// streaming or text chat. The application should be in active use by the user.
    High,
    /// Used for all normal requests. Anything that doesn't require low-latency feedback, or where 
    /// some laoding time is acceptable, should use this.
    Normal,
    /// Used for background requests, where it is okay if other requests usually take priority. 
    Idle
}

pub enum StrOrIndex {
    Str(String),
    Index(usize),
}

/// A Certificate Chain.
///
/// Certificate chains are used to specify requirements an Identity must meet. They consist of a 
/// set of root Identities and an optional sequence of chain links, with each link specifying a 
/// required value for a key and how many issuers must have certified that key-value pair.
///
/// If no chain links are provided, the Identity must instead be amongst the list of root 
/// Identities.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CertChain {
    roots: Vec<Identity>,
    chain: Vec<CertChainLink>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CertChainLink {
    key: String,
    val: String,
    min_issuers: u8
}

impl CertChainLink {
    /// Key of the chain link.
    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    /// Value of the chain link.
    pub fn val(&self) -> &str {
        self.val.as_str()
    }

    /// Minimum number of issuers that signed this key-value pair for the subject.
    pub fn min_issuers(&self) -> u8 {
        self.min_issuers
    }

    fn as_value(&self) -> Value {
        fogpack!({
            "key": self.key.clone(),
            "val": self.val.clone(),
            "min_issuers": self.min_issuers,
        })
    }

    fn from_value(v: &ValueRef) -> Option<Self> {
        let v = v.as_obj()?;
        let key = v.get("key")?.as_str()?.to_string();
        let val = v.get("val")?.as_str()?.to_string();
        let min_issuers = v.get("min_issuers")?.as_u64()? as u8;
        Some(Self {
            key,
            val,
            min_issuers
        })
    }
}

impl CertChain {
    /// Create a new Certificate Chain with one root signer.
    pub fn with_root(root: Identity) -> Self {
        CertChain {
            roots: vec![root],
            chain: Vec::new(),
        }
    }

    /// Create a new Certificate Chain with multiple root signers.
    pub fn with_roots(roots: Vec<Identity>) -> Self {
        CertChain {
            roots,
            chain: Vec::new(),
        }
    }

    /// Add a new root signer to the list.
    pub fn add_root(&mut self, root: Identity) {
        self.roots.push(root);
    }

    /// Add a link to the chain. Certificate chains are built up from the roots (the issuers) 
    /// towards the final endpoint (the subject). They are evaluted in reverse order, going from 
    /// a single identity back towards the root.
    pub fn add_link(&mut self, key: String, val: String, min_issuers: u8) {
        self.chain.push(CertChainLink { key, val, min_issuers });
    }

    pub fn roots(&self) -> std::slice::Iter<Identity> {
        self.roots.iter()
    }

    pub fn chain_len(&self) -> usize {
        self.chain.len()
    }

    /// Provide the links in the chain, in reverse order (going from subject up to issuer, 
    /// eventually to the root).
    pub fn links_to_root(&self) -> std::slice::Iter<CertChainLink> {
        self.chain.iter()
    }

    fn as_value(&self) -> Value {
        let chain: Vec<Value> = self.chain.iter().map(|x| x.as_value()).collect();
        let roots: Vec<Value> = self.roots.iter().map(|x| Value::from(x.clone())).collect();
        fogpack!({
            "roots": roots,
            "chain": chain
        })
    }

    fn from_value(v: &ValueRef) -> Option<Self> {
        let v = v.as_obj()?;
        let roots = v
            .get("roots")?
            .as_array()?
            .iter()
            .map(|x| x.as_id().and_then(|x: &Identity| Some(x.clone())))
            .collect::<Option<Vec<Identity>>>()?;
        let chain = v
            .get("chain")?
            .as_array()?
            .iter()
            .map(|x| CertChainLink::from_value(x))
            .collect::<Option<Vec<CertChainLink>>>()?;
        Some(Self {
            roots,
            chain,
        })
    }
}

/// Permissions for how a document or query may be shared.
///
/// Permimssions specify the allowed network classes (besides `self`) that are allowed, as well as 
/// optionally specifying certificate chains.
///
/// A minimum certificate chain lists the allowed identities that may see a given document/query. 
/// Optionally, these identies may be used as the "root" identities in a full certificate chain. 
/// Certificate chains consist of a sequence of key-value pairs, where each pair has a minimum 
/// number of issuers and a maximum chain depth.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Permission {
    nets: Vec<String>,
    certs: Vec<CertChain>,
}

impl Permission {
    pub fn new() -> Self {
        Permission {
            nets: Vec::new(),
            certs: Vec::new(),
        }
    }

    pub fn add_net(&mut self, net: String) {
        self.nets.push(net);
    }

    pub fn add_cert(&mut self, cert: CertChain) {
        self.certs.push(cert);
    }

    pub fn nets(&self) -> std::slice::Iter<String> {
        self.nets.iter()
    }

    pub fn certs(&self) -> std::slice::Iter<CertChain> {
        self.certs.iter()
    }

    /// Convert a Permission to a Document
    pub fn as_document(&self) -> Document {
        let nets: Vec<Value> = self.nets.iter().map(|x| Value::from(x.clone())).collect();
        let certs: Vec<Value> = self.certs.iter().map(|x| x.as_value()).collect();
        Document::new(fogpack!({
            "nets": nets,
            "certs": certs,
        }))
        .expect("Somehow a Permission was larger than the maximum Document size")
    }

    /// Try to read a Document as a Permission.
    pub fn from_document(doc: Document) -> Option<Self> {
        let v = doc.value();
        let v = v.as_obj()?;
        let nets = v
            .get("nets")?
            .as_array()?
            .iter()
            .map(|x| x.as_str().and_then(|x: &str| Some(x.to_string())))
            .collect::<Option<Vec<String>>>()?;
        let certs = v
            .get("certs")?
            .as_array()?
            .iter()
            .map(|x| CertChain::from_value(x))
            .collect::<Option<Vec<CertChain>>>()?;
        Some(Self {
            nets,
            certs
        })
    }
}

impl std::convert::TryFrom<Document> for Permission {
    type Error = &'static str;

    fn try_from(value: Document) -> Result<Self, Self::Error> {
        Permission::from_document(value).ok_or("Not a Permission")
    }
}

pub struct DbQuery {
    //query: Entry,
    //priority: Priority,
    //order_min_first: Option<bool>,
    //order: Option<Vec<StrOrIndex>>,
    //permission: Permission,
    //origin: Origin,
}

/// The origin of a query, document request, document response, or query response.
///
/// Contains, the network class string the query comes from, and an optional Identity associated 
/// with the origin.
pub struct Origin {
    net: String,
    id: Option<Identity>,
}

impl Origin {
    pub fn new(net: String) -> Self {
        Self {
            net,
            id: None,
        }
    }

    pub fn with_id(net: String, id: Identity) -> Self {
        Self {
            net,
            id: Some(id),
        }
    }

    pub fn get_id(&self) -> &Option<Identity> {
        &self.id
    }

    pub fn get_net(&self) -> &str {
        &self.net
    }
}

pub struct Transaction {
    add_docs: Vec<(Document, Permission)>,
    add_entries: Vec<(Entry, Option<u32>)>,
    del_entries: Vec<(Hash, Hash)>,
    perm_changes: Vec<(Hash, Permission)>,
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
    pub fn add_doc(&mut self, doc: Document, p: Permission) {
        self.add_docs.push((doc, p));
    }
    /// Add an Entry into the database. Failure occurs if the Entry's parent isn't in the database 
    /// (`DocNotFound`) or if the Entry fails validation (`Failed`).
    pub fn add_entry(&mut self, entry: Entry) {
        self.add_entries.push((entry, None));
    }
    /// Add an Entry into the database with a time-to-live, in seconds. If `ttl` is zero, the 
    /// entry will not be added to the database, but will be routed to any active queries it 
    /// matches. Failure occurs if the Entry's parent isn't in the database (`DocNotFound`) or if 
    /// the Entry fails validation (`Failed`).
    pub fn add_entry_ttl(&mut self, entry: Entry, ttl: u32) {
        self.add_entries.push((entry, Some(ttl)));
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

pub enum QueryResult {
    /// The Query didn't go through. Something was invalid.
    BadQuery,
    /// Queries will return a sequence of matching Entries.
    Entry(Entry),
    /// Queries can return documents associated with the Entry, if requested. They should be 
    /// transmitted immediately after the Entry that needs them, and the complete Document set 
    /// should be provided before the next Entry.
    Doc(Document),
    /// Database has exhausted its set of entries to run through. More may show up in the future.
    None,
}

pub enum CertResult {
    /// Document wasn't a certificate document
    NotCert,
    /// Document was a certificate document, but the signature was bad
    BadCert,
    /// Certificate would have been added, but an existing certificate has a newer start time.
    Outdated,
    /// Certificate was added successfully
    Ok,
}

pub trait QueryPort<O: Sink<QueryResult>> {
    fn submit_query(&mut self, q: DbQuery) -> O;
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

    
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_docs() {
        let perm = Permission::new();
        let perm_doc = perm.as_document();
        let perm_decoded = Permission::from_document(perm_doc).unwrap();

        assert!(perm == perm_decoded);
    }
}
