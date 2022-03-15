use fog_crypto::identity::{Identity, IdentityKey};
use fog_pack::{document::Document, entry::Entry, query::{NewQuery, Query}, types::Hash};
use futures_core::Stream;
use std::{pin::Pin, task::{Context, Poll}};

use crate::cert::*;


pub struct Transaction;

pub struct Cursor;

impl Cursor {

    /// Move to a document referenced by the current one.
    pub async fn forward(&mut self, hash: Hash) -> Result<Document, ()> {
        todo!()
    }

    /// Fork the cursor, creating a new one starting at a document referenced by the current one.
    pub async fn fork(&mut self, hash: Hash) -> Result<Document, ()> {
        todo!()
    }

    /// Return to previous document. Errors if there is no previous document.
    pub async fn back(&mut self) -> Result<(), ()> {
        todo!()
    }

    /// Initiate a query on the document under the cursor. Fails if the query is invalid.
    pub async fn query(&mut self, query: NewQuery) -> Result<CursorQuery, fog_pack::error::Error> {
        todo!()
    }

    /// Initiate a streaming query on the document under the cursor. Fails if the query is invalid.
    pub async fn stream_query(&mut self, query: NewQuery, buf_size: u16) -> Result<CursorStreamQuery, fog_pack::error::Error> {
        todo!()
    }
}

/// A stream of query results.
pub struct CursorQuery;

/// A limited-depth stream of query results. The depth of the result buffer is set on initiation.
pub struct CursorStreamQuery;

pub struct NodeAddr {
    /// Long-term Identity, notionally tied to the user of the node
    pub perm_id: Option<Identity>,
    /// Ephemeral Identity, notionally tied to the node itself
    pub eph_id: Option<Identity>,
}

pub struct QueryResult {
    pub entry: Entry,
    pub source: NodeAddr,
    pub index: Option<u64>,
}

impl Stream for CursorQuery {
    type Item = QueryResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

impl Stream for CursorStreamQuery {
    type Item = QueryResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub struct FogDb;

impl FogDb {

    /// Submit a transaction to the database.
    pub async fn submit_transaction(t: Transaction) -> Result<(), ()> {
        todo!()
    }

    /// Open a new network group using this database.
    pub async fn open_group(group: GroupBuilder) -> Group {
        todo!()
    }

}


/// A Group of connections to other nodes. A Group can be used to find specific nodes 
pub struct Group;

impl Group {
    /// Open a subgroup, specified using a new CertRule. Subgroups use the same connections but 
    /// further limit them to nodes that satisfy the additional certificate rule.
    pub async fn open_subgroup(rule: CertRule) -> Group {
        todo!()
    }

    /// Change the certificate rule used by this group. This may result in node connections being 
    /// abruptly closed. When this function completes, any remaining connections are guaranteeed to 
    /// meet the new rule.
    pub async fn update_rule(rule: Option<CertRule>) {
        todo!()
    }

    /// Open a new gate, allowing anyone in the Group to open a cursor at the given Hash.
    pub async fn open_gate(gate: &Hash) -> bool {
        todo!()
    }

    /// Close a gate. If there is no gate open for the given hash, nothing happens.
    pub async fn close_gate(gate: &Hash) {
        todo!()
    }

    /// List all open gates for this group.
    pub async fn get_gates() -> Vec<Hash> {
        todo!()
    }

    /// Open up a new cursor for this group.
    pub async fn new_cursor(start: &Hash) -> Cursor {
        todo!()
    }
}

/// Configuration settings for a new network group. To set up a group, create a new GroupBuilder 
/// and set the desired limitations. The default GroupBuilder will create a group that connects to 
/// no networks, accepts any node, provides no authentication ID, and will advertise completely in 
/// the open.
pub struct GroupBuilder {
    self_id: Option<IdentityKey>,
    nets: Vec<String>,
    rule: Option<CertRule>,
    hide_origin: bool,
}

impl Default for GroupBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupBuilder {

    /// Start building a new group.
    pub fn new() -> Self {
        Self {
            self_id: None,
            nets: Vec::new(),
            rule: None,
            hide_origin: false,
        }
    }

    /// Set the Identity to use for all connections in this group.
    pub fn id(mut self, id: IdentityKey) -> Self {
        self.self_id = Some(id);
        self
    }

    /// Add a named network type to the list of networks.
    pub fn add_net(mut self, net: impl Into<String>) -> Self {
        self.nets.push(net.into());
        self
    }

    /// Specify the certificate rule all connected nodes must satisfy
    pub fn cert_rule(mut self, rule: CertRule) -> Self {
        self.rule = Some(rule);
        self
    }

    /// Set whether or not the originating node (us) should be hidden. If hidden, initial 
    /// handshaking is done through a proxy or mixnet in order to ensure our location on the 
    /// network is not revealed except to members of the group. This slows down initial 
    /// connections.
    pub fn hide_origin(mut self, hide_origin: bool) -> Self {
        self.hide_origin = hide_origin;
        self
    }
}
