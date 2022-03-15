use crate::db::{Database, LookForDocResult};
use fog_pack::types::{Hash, Identity};
use fog_pack::document::Document;
use fog_pack::query::NewQuery;
use fog_pack::entry::Entry;
use tokio::sync::oneshot;
use crate::NetType;
use std::sync::Arc;
use crate::error::{DbError, CursorError};

// Implement a cursor for navigating a tree
// Cursors track through a document tree, fetching documents as they go
// Unlimited cursors can be spawned locally, limited by local resource usage
//
// Queries return entries, sometimes with attached documents
// A query result can be used to spawn a new cursor
//
//
// T

pub enum Indexing {
    Map(String),
    Array(u32),
}

pub struct Cursor {
    db: Database,
    root: Arc<Document>,
    stack: Vec<Arc<Document>>,
    current_refs: Vec<Hash>,
}

impl Cursor {

    /// Check the references before moving forward
    fn check_refs(&self, hash: &Hash) -> Result<(),CursorError> {
        if !self.current_refs.iter().any(|h| h == hash) {
            Err(CursorError::InvalidForward)
        }
        else { Ok(()) }
    }

    /// Internally used to move forward once we have the document to move with.
    fn forward_with_doc(&mut self, doc: Arc<Document>) -> Result<(), CursorError> {
        match crate::db::find_doc_hashes(doc.as_ref()) {
            Ok(refs) => {
                self.stack.push(doc);
                self.current_refs = refs;
                Ok(())
            },
            Err(err) => Err(CursorError::FogPack {
                info: format!("Document ({}) couldn't be deserialized for finding hashes", doc.hash()),
                err,
            }),
        }
    }

    pub(crate) fn new_local(db: Database, root: &Hash) -> Result<Option<Self>, DbError> {
        let root = if let Some(d) = db.get_doc(root)? { d } else { return Ok(None); };
        Self::from_doc(db, root).map(Some)
    }

    pub(crate) fn from_doc(db: Database, root: Arc<Document>) -> Result<Self, DbError> {
        let current_refs = crate::db::find_doc_hashes(root.as_ref())
            .map_err(|err| DbError::FogPack {
                info: format!("Root Document ({}) couldn't be deserialized for finding hashes",
                    root.hash()
                ),
                err,
        })?;
        Ok(Self {
            db,
            root,
            stack: Vec::new(),
            current_refs,
        })
    }

    /// Move back to the previous document. Errors if we cannot go back further.
    pub async fn back(&mut self) -> Result<(), CursorError> {
        if self.stack.pop().is_some() { Ok(()) } else {
            Err(CursorError::BackAtTopLevel)
        }
    }

    /// Move to a document referenced by the current one. Succeeds if the document is in the 
    /// database and is referenced, otherwise fails and doesn't move. 
    pub async fn forward_db(&mut self, hash: &Hash) -> Result<Arc<Document>, CursorError> {
        self.check_refs(hash)?;
        if let Some(doc) = self.db.get_doc(hash)? {
            self.forward_with_doc(doc.clone())?;
            Ok(doc)
        }
        else {
            Err(CursorError::NotInDatabase)
        }
    }

    /// Move to a document referenced by the current one. This may stall indefinitely if the 
    /// document isn't in the database, as it will keep looking for the document from any available 
    /// sources until it is found. Dropping this cursor will free any resources dedicated to 
    /// finding the document.
    pub async fn forward(&mut self, hash: &Hash) -> Result<Arc<Document>, CursorError> {
        self.check_refs(hash)?;
        match self.db.look_for_doc(hash).await? {
            LookForDocResult::Doc(doc) => {
                self.forward_with_doc(doc.clone())?;
                Ok(doc)
            },
            LookForDocResult::Subscriber(sub) => {
                let doc = sub.complete().await?;
                self.forward_with_doc(doc.clone())?;
                Ok(doc)
            },
        }
    }

    pub async fn fork_db(&mut self, hash: &Hash) -> Result<(Cursor, Arc<Document>), CursorError> {
        self.check_refs(hash)?;
        let doc = if let Some(d) = self.db.get_doc(hash)? { d } else {
            return Err(CursorError::NotInDatabase);
        };
        let cursor = Self::from_doc(self.db.clone(), doc.clone())?;
        Ok((cursor, doc))
    }

    /// Fork the cursor, creating a new one starting at a document referenced by the current one. 
    /// The new cursor cannot navigate back before the fork point.
    pub fn fork(&mut self, hash: &Hash) -> ForkFuture {
        if self.check_refs(hash).is_err() {
            let (tx,rx) = oneshot::channel();
            let _ = tx.send(Err(CursorError::InvalidForward));
            rx
        }
        else {
            let (mut tx, rx) = oneshot::channel();
            let db = self.db.clone();
            let hash = hash.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = tx.closed() => { },
                    result = async move {
                        match db.look_for_doc(&hash).await? {
                            LookForDocResult::Doc(doc) => Ok((Cursor::from_doc(db, doc.clone())?, doc)),
                            LookForDocResult::Subscriber(sub) => {
                                let doc = sub.complete().await?;
                                Ok((Cursor::from_doc(db, doc.clone())?, doc))
                            },
                        }
                    } => {
                        let _ = tx.send(result);
                    },
                }
            });
            rx
        }
    }

    /// Get the document the cursor is on.
    fn get_doc(&self) -> Arc<Document> {
        self.stack.last().cloned().unwrap_or_else(|| self.root.clone())
    }

    /// Initiate a query on the document under the cursor. Fails if the query is invalid.
    pub async fn query(&mut self,
        _query: NewQuery,
        _ordering: Option<Vec<Indexing>>,
    ) -> Result<CursorQuery, CursorError> {
        todo!()
    }
}

type ForkFuture = oneshot::Receiver<Result<(Cursor, Arc<Document>), CursorError>>;

/// A stream of query results.
pub struct CursorQuery;

pub struct NodeAddr {
    /// The network type this was returned on
    pub net: NetType,
    /// Long-term Identity, notionally tied to the user of the node
    pub perm_id: Option<Identity>,
    /// Ephemeral Identity, notionally tied to the node itself
    pub eph_id: Option<Identity>,
}

pub enum Usefulness {
    /// The received entry is correct and useful to the query maker.
    Useful,
    /// The received entry is correct and relevant to the query, but a newer entry was received 
    /// that makes this one useless.
    Stale,
    /// The received entry is correct, but the content was irrelevant. This is slightly worse than 
    /// stale: the query maker can't figure out why it would've received this data, even if it is 
    /// *technically* correct. Think search results that don't contain any of the search terms, a 
    /// query for cat pictures but the returned picture isn't of a cat, that sort of thing.
    Irrelevant,
    /// The received entry was incorrect - despite conforming to the schema, it violated 
    /// expectations. Example: a 2D image format where the data length doesn't match up with the 
    /// width & height values included in the format.
    Incorrect,
}

/// An entry returned from a query.
pub struct QueryResult {
    /// The entry itself.
    pub entry: Entry,
    /// Any associated documents needed to verify the entry
    pub docs: Vec<Arc<Document>>,
    /// The source node this result came from
    pub source: NodeAddr,
    /// Optional return to indicate how useful this result was to the query maker. Completing this 
    /// can be helpful
    pub useful: oneshot::Sender<Usefulness>,
}

pub enum QueryUpdate {
    /// The query has found a matching entry
    Result(QueryResult),
    /// The query has found a new node to run the query on
    NewConnection(NodeAddr),
    /// A node the query was being run on became disconnected
    LostConnection(NodeAddr),
}
