use std::collections::{HashMap, HashSet, BTreeMap};
use std::convert::TryFrom;
use std::convert::TryInto;
use std::io::Read;
use std::sync::Arc;

use crate::error::*;
use crate::Transaction;
use crate::cursor;
use dynamic_lru_cache::DynamicCache;
use fog_pack::{
    document::Document,
    entry::{Entry, EntryRef},
    schema::{NoSchema, Schema},
    types::Hash,
};
use sled::Transactional;
use tokio::sync::{mpsc, oneshot};

const BACKREF_NAMES: u8 = 0u8;
const BACKREF_IS_SCHEMA: u8 = 1u8;
const BACKREF_SCHEMA_COUNT: u8 = 2u8;
const BACKREF_ALL_COUNT: u8 = 3u8;

/// Delve through a document and find any hashes it references.
pub(crate) fn find_doc_hashes(doc: &Document) -> Result<Vec<Hash>, fog_pack::error::Error> {
    use fog_pack::types::ValueRef;
    let doc: ValueRef = doc.deserialize()?;
    let mut v = Vec::new();

    // We do the simple recursive thing here, knowing full well that no fog-pack value nests deeper
    // than fog_pack::MAX_DEPTH.
    fn add_hashes(v: &mut Vec<Hash>, val: &ValueRef) {
        match val {
            ValueRef::Hash(h) => v.push(h.to_owned()),
            ValueRef::Array(array) => array.iter().for_each(|i| add_hashes(v, i)),
            ValueRef::Map(map) => map.iter().for_each(|(_, i)| add_hashes(v, i)),
            _ => (),
        }
    }

    add_hashes(&mut v, &doc);
    Ok(v)
}

pub(crate) fn find_entry_hashes(entry: &Entry) -> Result<Vec<Hash>, fog_pack::error::Error> {
    use fog_pack::types::ValueRef;
    let entry: ValueRef = entry.deserialize()?;
    let mut v = Vec::new();

    // We do the simple recursive thing here, knowing full well that no fog-pack value nests deeper
    // than fog_pack::MAX_DEPTH.
    fn add_hashes(v: &mut Vec<Hash>, val: &ValueRef) {
        match val {
            ValueRef::Hash(h) => v.push(h.to_owned()),
            ValueRef::Array(array) => array.iter().for_each(|i| add_hashes(v, i)),
            ValueRef::Map(map) => map.iter().for_each(|(_, i)| add_hashes(v, i)),
            _ => (),
        }
    }

    add_hashes(&mut v, &entry);
    Ok(v)
}

type DocSenders = Arc<parking_lot::RwLock<HashMap<Hash, Vec<(usize, oneshot::Sender<Result<Arc<Document>, DbError>>)>>>>;

struct DocSubscriberInner {
    hash: Hash,
    id: usize,
    home: DocSenders,
    need_to_free: bool,
}

pub(crate) struct DocSubscriber {
    inner: DocSubscriberInner,
    rx: oneshot::Receiver<Result<Arc<Document>, DbError>>,
}

impl DocSubscriber {

    pub(crate) async fn complete(mut self) -> Result<Arc<Document>, DbError> {
        let res = self.rx.await.map_err(|_| DbError::WriterDied);
        self.inner.need_to_free = false;
        res?
    }
}

impl std::ops::Drop for DocSubscriberInner {
    fn drop(&mut self) {
        if self.need_to_free {
            let mut map = self.home.write();
            if let Some(v) = map.get_mut(&self.hash) {
                if let Some(pos) = v.iter().position(|(id,_)| id == &self.id) {
                    v.swap_remove(pos);
                }
                if v.is_empty() { map.remove(&self.hash); }
            }
        }
    }
}

struct EncodedEntry {
    id: EntryRef,
    data: Vec<u8>,
    refs: Vec<Hash>,
    required: Vec<Hash>,
}

struct EncodedDoc {
    hash: Hash,
    schema: Option<Hash>,
    data: Vec<u8>,
    refs: Vec<Hash>,
}

#[derive(Default)]
struct EncodedTransaction {
    doc: Vec<EncodedDoc>,
    entry: Vec<EncodedEntry>,
    ttl: Vec<crate::transaction::TtlEntry>,
    del_entry: Vec<EntryRef>,
}

impl EncodedTransaction {
    fn encode(db: &Database, mut t: Transaction) -> Result<Self, TransactionError> {
        // Get a schema and map failures into transaction failures
        fn get_schema(
            db: &Database,
            schema_hash: &Hash,
            doc: &Hash,
            entry: Option<(&str, &Hash)>,
        ) -> Result<Arc<Schema>, TransactionError> {
            match db.get_schema(schema_hash) {
                Ok(Some(schema)) => Ok(schema),
                Ok(None) => match entry {
                    None => Err(TransactionError::MissingDocSchema {
                        doc: doc.clone(),
                        schema: schema_hash.clone(),
                    }),
                    Some((key, hash)) => Err(TransactionError::MissingEntrySchema {
                        entry: EntryRef {
                            parent: doc.clone(),
                            key: key.to_string(),
                            hash: hash.clone(),
                        },
                        schema: schema_hash.clone(),
                    }),
                },
                Err(DbError::Invalid(_)) => Err(TransactionError::InvalidSchema {
                    doc: doc.clone(),
                    schema: schema_hash.clone(),
                }),
                Err(e) => Err(e.into()),
            }
        }

        // Steps to prepare a transaction:
        // - Check the new documents
        // - Check the new entries
        // - Encode the documents
        // - Encode the entries
        // - Move the ttl & del_entry vecs over to the encoded transaction
        //
        // Steps to complete the transaction (require a write lock on the database)
        // - Create backref table updates - docs & entries can be added to their batch updates too
        // - Create delete_check updates
        // - Create ttl_by_time & ttl_by_hash updates
        // - Turn all update lists into transaction batches
        // - Merge all batches into a single transaction

        // Validate each new document being added in the transaction
        for doc in t.new_doc.drain(..) {
            match doc.schema_hash() {
                None => {
                    let doc_hash = doc.hash().clone();
                    let doc = NoSchema::validate_new_doc(doc).map_err(|e| {
                        TransactionError::DocValidationFailed {
                            doc: doc_hash,
                            err: e,
                        }
                    })?;
                    t.doc.push(doc);
                }
                Some(schema_hash) => {
                    let schema = get_schema(db, schema_hash, doc.hash(), None)?;
                    let doc_hash = doc.hash().clone();
                    let doc = schema.validate_new_doc(doc).map_err(|e| {
                        TransactionError::DocValidationFailed {
                            doc: doc_hash,
                            err: e,
                        }
                    })?;
                    t.doc.push(doc);
                }
            }
        }

        // Validate each new entry being added in the transaction
        for entry in t.new_entry.drain(..) {
            let entry_ref = entry.reference().clone();
            // Gather some data first
            let schema = get_schema(
                db,
                entry.schema_hash(),
                &entry_ref.parent,
                Some((&entry_ref.key, &entry_ref.hash)),
            )?;

            // Validate the entry itself and get our checklist of additional documents to validate
            let mut checklist = schema.validate_new_entry(entry).map_err(|e| {
                TransactionError::EntryValidationFailed {
                    entry: entry_ref.clone(),
                    err: e,
                }
            })?;

            // Validate each document the entry references and has validation requirements on
            'entry_loop: for (link_hash, item) in checklist.iter() {
                // Check the local transaction set first. It's expected that often a referenced
                // document will be attached in the same transaction.
                for doc in t.doc.iter() {
                    if doc.hash() == &link_hash {
                        item.check(doc)
                            .map_err(|e| TransactionError::EntryValidationFailed {
                                entry: entry_ref.clone(),
                                err: e,
                            })?;
                        continue 'entry_loop;
                    }
                }
                // If it's not in the local set, check the database
                match db.get_doc(&link_hash) {
                    Ok(Some(doc)) => item.check(doc.as_ref()).map_err(|e| {
                        TransactionError::EntryValidationFailed {
                            entry: entry_ref.clone(),
                            err: e,
                        }
                    })?,
                    Ok(None) => {
                        return Err(TransactionError::EntryMissingRequiredDoc {
                            entry: entry_ref.clone(),
                            required: link_hash,
                        })
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            let entry =
                checklist
                    .complete()
                    .map_err(|e| TransactionError::EntryValidationFailed {
                        entry: entry_ref.clone(),
                        err: e,
                    })?;
            t.entry.push(entry);
        }

        // Encode all the documents
        let mut enc_docs: Vec<EncodedDoc> = Vec::with_capacity(t.doc.len());
        for doc in t.doc.drain(..) {
            // Skip encoding if it's already in the list of encoded documents
            if enc_docs.iter().any(|d| &d.hash == doc.hash()) {
                continue;
            }
            // Get the hashes this document links to
            let refs = find_doc_hashes(&doc).map_err(|err| TransactionError::FogPack {
                info: format!(
                    "Document ({}) couldn't be deserialized for finding hashes",
                    doc.hash()
                ),
                err,
            })?;
            // Encode using the appropriate schema
            match doc.schema_hash() {
                None => {
                    // No schema, encode directly
                    let doc_hash = doc.hash().clone();
                    let (hash, data) =
                        NoSchema::encode_doc(doc).map_err(|err| TransactionError::FogPack {
                            info: format!("Couldn't encode document ({})", doc_hash),
                            err,
                        })?;
                    enc_docs.push(EncodedDoc {
                        hash,
                        schema: None,
                        data,
                        refs,
                    });
                }
                Some(schema_hash) => {
                    // Encode with the schema
                    let schema = get_schema(db, schema_hash, doc.hash(), None)?;
                    let doc_hash = doc.hash().clone();
                    let (hash, data) =
                        schema
                            .encode_doc(doc)
                            .map_err(|err| TransactionError::FogPack {
                                info: format!("Couldn't encode document ({})", doc_hash),
                                err,
                            })?;
                    enc_docs.push(EncodedDoc {
                        hash,
                        schema: Some(schema.hash().clone()),
                        data,
                        refs,
                    });
                }
            }
        }

        // Encode all entries
        let mut enc_entries: Vec<EncodedEntry> = Vec::with_capacity(t.entry.len());
        for entry in t.entry.drain(..) {
            // Skip encoding the entry if it's already in the list
            if enc_entries.iter().any(|e| &e.id == entry.reference()) {
                continue;
            }
            // Get the hashes this entry refers to
            let refs = find_entry_hashes(&entry).map_err(|err| TransactionError::FogPack {
                info: format!(
                    "Entry {} couldn't be deserialized for finding hashes",
                    entry.reference()
                ),
                err,
            })?;
            // Encode the entry using the appropriate schema
            let entry_ref = entry.reference().clone();
            let schema = get_schema(
                db,
                entry.schema_hash(),
                &entry_ref.parent,
                Some((&entry_ref.key, &entry_ref.hash)),
            )?;
            let (id, data, required) =
                schema
                    .encode_entry(entry)
                    .map_err(|err| TransactionError::FogPack {
                        info: format!("Couldn't encode entry {}", entry_ref),
                        err,
                    })?;
            enc_entries.push(EncodedEntry {
                id,
                data,
                refs,
                required,
            });
        }

        // Make sure the deletions are all unique
        t.del_entry.sort_unstable();
        t.del_entry.dedup();

        Ok(EncodedTransaction {
            doc: enc_docs,
            entry: enc_entries,
            ttl: t.ttl,
            del_entry: t.del_entry,
        })
    }

    /// Commit the transaction. Should only be run from the a single transaction writer thread.
    fn commit(mut self, db: &Database) -> Result<(), TransactionError> {
        let mut docs_batch = sled::Batch::default();
        let mut entries_batch = sled::Batch::default();
        let mut backrefs_batch = sled::Batch::default();
        let mut delete_check_batch = sled::Batch::default();
        let mut ttl_by_time_batch = sled::Batch::default();
        let mut ttl_by_hash_batch = sled::Batch::default();

        let mut schema_add: HashMap<Hash, u64> = HashMap::new();
        let mut backrefs: HashMap<(Hash, Hash), i64> = HashMap::new();
        let mut delete_check: HashSet<Hash> = HashSet::new();
        let mut ttl_by_time = BTreeMap::new();
        let mut ttl_by_hash = BTreeMap::new();

        // Prepare the entries for addition to the database.
        for entry in self.entry.drain(..) {

            // Skip if it's loaded in
            let entry_key = Database::construct_entry_key(&entry.id);
            if db.entries.contains_key(&entry_key)? {
                continue;
            }
            // Error if we're missing an essential referenced document
            for required in entry.required.iter() {
                if self.doc.iter().any(|doc| doc.hash == *required) {
                    continue;
                }
                if db.docs.contains_key(&required)? {
                    continue;
                }
                return Err(TransactionError::EntryMissingRequiredDoc {
                    entry: entry.id,
                    required: required.clone(),
                });
            }

            // Skip if we would delete it immediately after this
            // This should come after the reference document check
            if let Some(del_index) = self.del_entry.iter().position(|d| d == &entry.id) {
                // Delete the deletion command
                self.del_entry.swap_remove(del_index);
                continue;
            }

            // Add to the entry batch and update our planned backrefs
            entries_batch.insert(&entry_key[..], entry.data);
            for link in entry.refs {
                *backrefs
                    .entry((link, entry.id.parent.clone()))
                    .or_insert(0) += 1;
            }
        }

        // Prepare the documents for addition to the database. Skip if it's already loaded in
        for doc in self.doc.drain(..) {
            if db.docs.contains_key(&doc.hash)? {
                continue;
            }

            // Add to the doc batch and update our planned backrefs
            docs_batch.insert(doc.hash.as_ref(), doc.data);
            if let Some(schema) = doc.schema {
                if !db.is_schema(&schema)? {
                    // We would've caught an InvalidSchema error earlier, during encoding of the 
                    // document, so this must be that we don't have the schema entered in the 
                    // database anymore - either because it's gone, or it is no longer marked as a 
                    // schema.
                    return Err(TransactionError::MissingDocSchema { doc: doc.hash, schema });
                }
                *schema_add.entry(schema).or_insert(0) += 1;
            }
            for link in doc.refs {
                *backrefs.entry((link, doc.hash.clone())).or_insert(0) += 1;
            }
            delete_check.insert(doc.hash.clone());
        }

        // Handle all entry deletions. They should be in the database, otherwise this transaction 
        // should fail.
        for del in self.del_entry.drain(..) {
            if let Some(entry) = db.get_entry(&del)? {
                // Remove the entry from the database
                let entry_key = Database::construct_entry_key(&del);
                entries_batch.remove(entry_key);

                // Update the backrefs table
                let entry_hashes = find_entry_hashes(&entry)
                    .map_err(|e| DbError::Corruption(format!("Entry {} in database is corrupted (found on decoding to get hashes). fog-pack error: {}", del, e)))?;
                for link in entry_hashes {
                    *backrefs
                        .entry((link.clone(), entry.parent().clone()))
                        .or_insert(0) -= 1;
                    delete_check.insert(link);
                }
            }
            else {
                return Err(TransactionError::EntryAlreadyDeleted(del));
            }
        }

        // Add/Update each TTL
        for ttl in self.ttl.drain(..) {
            use std::collections::btree_map;
            let entry_key = Database::construct_entry_key(&ttl.entry);

            // Figure out the current time setting and get a mutable reference to it
            let time_setting = match ttl_by_hash.entry(entry_key.clone()) {
                btree_map::Entry::Occupied(o) => o.into_mut(),
                btree_map::Entry::Vacant(v) => {
                    if let Some(time) = db.ttl_by_hash.get(&entry_key)? {
                        let time: [u8; 8] = time.as_ref().try_into()
                            .map_err(|_| DbError::Corruption(format!(
                                "TTL entry {} in database is corrupted, not enough bytes for i64 timestamp", &ttl.entry
                            )))?;
                        v.insert(Some(i64::from_be_bytes(time)))
                    }
                    else {
                        v.insert(None)
                    }
                },
            };

            // If there is a current time setting, look up the time list and drop our entry from it
            if let Some(time_setting) = *time_setting {
                let list = match ttl_by_time.entry(time_setting) {
                    btree_map::Entry::Occupied(o) => o.into_mut(),
                    btree_map::Entry::Vacant(v) => v.insert(db.get_ttl_entries(time_setting)?),
                };
                if let Some(idx) = list.iter().position(|e| e == &ttl.entry) { list.remove(idx); }
            }

            // Update the time setting
            *time_setting = ttl.ttl.map(|x| x.max(0));

            // If a time is now set, add it to the right time list
            if let Some(time_setting) = *time_setting {
                let list = match ttl_by_time.entry(time_setting) {
                    btree_map::Entry::Occupied(o) => o.into_mut(),
                    btree_map::Entry::Vacant(v) => v.insert(db.get_ttl_entries(time_setting)?),
                };
                list.push(ttl.entry);
            }

        }

        // Convert ttl_by_hash to batch updates
        for (key, val) in ttl_by_hash {
            match val {
                None => ttl_by_hash_batch.remove(key),
                Some(v) => ttl_by_hash_batch.insert(key, &v.to_be_bytes()),
            }
        }

        // Convert ttl_by_time to batch updates
        for (key, val) in ttl_by_time {
            match val.is_empty() {
                true => ttl_by_time_batch.remove(&key.to_be_bytes()),
                false => ttl_by_time_batch.insert(&key.to_be_bytes(), Database::construct_ttl_entries(&val)),
            }
        }

        // Update our delete check
        for hash in delete_check {
            delete_check_batch.insert(hash.as_ref(), &[]);
        }

        // Convert schema backref updates into a batch
        let mut backref_idx = Vec::new();
        for (key, add) in schema_add {
            backref_idx.clear();
            backref_idx.extend_from_slice(key.as_ref());
            backref_idx.push(BACKREF_SCHEMA_COUNT);
            let new_val = if let Some(raw) = db.backrefs.get(&backref_idx)? {
                let prev_val: [u8; 8] = raw.as_ref().try_into().map_err(|_| DbError::Corruption(format!(
                            "Schema count for schema {} is corrupted, couldn't get value.", key
                )))?;
                u64::from_le_bytes(prev_val) + add
            }
            else {
                add
            };
            backrefs_batch.insert(backref_idx.clone(), &new_val.to_le_bytes());
        }

        // Convert other backref updates into a batch
        for ((hash, referencer), diff) in backrefs {
            backref_idx.clear();
            backref_idx.extend_from_slice(hash.as_ref());
            backref_idx.extend_from_slice(referencer.as_ref());
            let (exists, new_val) = if let Some(raw) = db.backrefs.get(&backref_idx)? {
                let prev_val: [u8; 8] = raw.as_ref().try_into().map_err(|_| DbError::Corruption(format!(
                            "Backref count for document {}, referencer {} is corrupted, couldn't get value.", hash, referencer
                )))?;
                (true, i64::from_le_bytes(prev_val) + diff)
            }
            else {
                (false, diff)
            };
            if new_val < 0 {
                return Err(DbError::Corruption(format!(
                        "Transaction attempted to update backref count for document {}, referencer {}, to be negative.", hash, referencer
                )).into());
            }
            match (exists, new_val) {
                (true, 0) => backrefs_batch.remove(backref_idx.clone()),
                (false, 0) => (),
                (_, val) => backrefs_batch.insert(backref_idx.clone(), &val.to_le_bytes()),
            }
        }

        (
            &db.docs,
            &db.entries,
            &db.backrefs,
            &db.delete_check,
            &db.ttl_by_time,
            &db.ttl_by_hash,
        )
            .transaction(
                |(docs, entries, backrefs, delete_check, ttl_by_time, ttl_by_hash)| {
                    docs.apply_batch(&docs_batch)?;
                    entries.apply_batch(&entries_batch)?;
                    backrefs.apply_batch(&backrefs_batch)?;
                    delete_check.apply_batch(&delete_check_batch)?;
                    ttl_by_time.apply_batch(&ttl_by_time_batch)?;
                    ttl_by_hash.apply_batch(&ttl_by_hash_batch)?;
                    Ok(())
                },
            )?;
        Ok(())
    }
}

pub(crate) enum LookForDocResult {
    Doc(Arc<Document>),
    Subscriber(DocSubscriber),
}

enum WriteCmd {
    CheckTtl,
    LookForDoc {
        hash: Hash,
        result: oneshot::Sender<Result<LookForDocResult, DbError>>,
    },
    Transaction {
        transaction: EncodedTransaction,
        result: oneshot::Sender<Result<(), TransactionError>>,
    },
    AddSchema {
        schema: Box<Document>,
        result: oneshot::Sender<Result<(), DbError>>,
    },
    DelSchema {
        schema: Hash,
        result: oneshot::Sender<Result<(), DbError>>,
    },
    AddName {
        name: String,
        doc: Hash,
        result: oneshot::Sender<Result<Option<Hash>, DbError>>,
    },
    DelName {
        name: String,
        result: oneshot::Sender<Result<Option<Hash>, DbError>>,
    },
    Quit(oneshot::Sender<Result<(), DbError>>),
}

fn get_backref_count(db: &Database, idx: &[u8]) -> Result<u64, DbError> {
    if let Some(count) = db.backrefs.get(&idx)? {
        let count = <&[u8; 8]>::try_from(count.as_ref()).map_err(|_| {
            DbError::Corruption(format!(
                "Backref entry found that isn't a u64. entry key = {:?}",
                &idx
            ))
        })?;
        Ok(u64::from_le_bytes(*count))
    } else {
        Ok(0u64)
    }
}

fn add_schema(db: &Database, schema: Box<Document>) -> Result<(), DbError> {
    // Get the hash for indexing into the backrefs table
    let hash = schema.hash();
    let mut backref_idx: Vec<u8> = Vec::with_capacity(hash.as_ref().len() + 1);
    backref_idx.extend(hash.as_ref());

    // We have 4 scenarios:
    // 1. Exists, is a schema, and is marked as a fixed schema
    // 2. Exists, is a schema, but is not marked as a fixed schema
    // 3. Exists, isn't a schema yet.
    // 4. Doesn't exist at all, so insert it.

    // 1. Exists, is a schema, is fixed in place, we don't have do to anything
    // -----------------------------------------------------------------------
    backref_idx.push(BACKREF_IS_SCHEMA);
    if db.backrefs.contains_key(&backref_idx)? {
        return Ok(());
    }
    backref_idx.pop();

    // 2. Exists, is a schema, is not fixed in place, so fix it
    // --------------------------------------------------------
    backref_idx.push(BACKREF_SCHEMA_COUNT);
    if db.backrefs.contains_key(&backref_idx)? {
        backref_idx.pop();
        backref_idx.push(BACKREF_IS_SCHEMA);
        db.backrefs.insert(&backref_idx, &1u64.to_le_bytes())?;
        return Ok(());
    }
    backref_idx.pop();

    // Verify it's a real schema (needed for 3 & 4)
    let _ = fog_pack::schema::Schema::from_doc(&schema)
        .map_err(|e| DbError::Invalid(format!("Not a valid schema: {}", e)))?;

    // 3. Exists, but isn't a schema yet
    // ---------------------------------

    if db
        .backrefs
        .scan_prefix(&backref_idx)
        .next()
        .transpose()?
        .is_some()
    {
        let hash_list = find_doc_hashes(&schema).map_err(|e| DbError::FogPack {
            info: "Schema Document wasn't able to be decoded".into(),
            err: e,
        })?;
        if schema.schema_hash().is_some() {
            return Err(DbError::Invalid(
                "Schema that adhere to a Schema are not currently supported".into(),
            ));
        }
        let schema_hash = schema.hash();

        // Set up the batch of operations to do
        let mut backref_batch = sled::Batch::default();
        let mut delete_check_batch = sled::Batch::default();
        backref_idx.push(BACKREF_IS_SCHEMA);
        backref_batch.insert(&backref_idx[..], &1u64.to_le_bytes());

        // Excise all backrefs and schedule a delete check.
        let mut hash_set = HashSet::new();
        for hash in hash_list.iter() {
            if !hash_set.contains(hash) {
                hash_set.insert(hash);
                backref_idx.clear();
                backref_idx.extend_from_slice(hash.as_ref());
                delete_check_batch.insert(&backref_idx[..], &[]);
                backref_idx.extend_from_slice(schema_hash.as_ref());
                backref_batch.remove(&backref_idx[..]);
            }
        }

        // Update the backref and delete_check trees.
        (&db.backrefs, &db.delete_check).transaction(|(backrefs, delete_check)| {
            backrefs.apply_batch(&backref_batch)?;
            delete_check.apply_batch(&delete_check_batch)?;
            Ok(())
        })?;
        return Ok(());
    }

    // 4. Doesn't exist in the database at all
    // ---------------------------------------

    // Convert the schema to a raw document
    let (schema_hash, raw_schema) = match schema.schema_hash() {
        None => NoSchema::encode_doc(*schema).map_err(|e| DbError::FogPack {
            info: "Schema Encoding failed".into(),
            err: e,
        })?,
        Some(_) => {
            return Err(DbError::Invalid(
                "Schema that adhere to a Schema are not currently supported".into(),
            ));
        }
    };

    // Insert the schema document and pin it in the backrefs table
    backref_idx.clear();
    backref_idx.extend_from_slice(schema_hash.as_ref());
    backref_idx.push(BACKREF_IS_SCHEMA);
    (&db.docs, &db.backrefs).transaction(|(docs, backrefs)| {
        docs.insert(schema_hash.as_ref(), &raw_schema[..])?;
        backrefs.insert(&backref_idx[..], &(1u64.to_le_bytes())[..])?;
        Ok(())
    })?;

    Ok(())
}

fn del_schema(db: &Database, schema: Hash) -> Result<(), DbError> {
    // If it's pinned, we just unpin it and leave the rest to the deletion checker, which has to
    // manage detecting when the schema is actually no longer in use.
    // If it's not pinned, then we don't need to do anything.
    let mut backref_idx: Vec<u8> = Vec::with_capacity(schema.as_ref().len() + 1);
    backref_idx.extend(schema.as_ref());
    backref_idx.push(BACKREF_IS_SCHEMA);
    if db.backrefs.contains_key(&backref_idx)? {
        (&db.backrefs, &db.delete_check).transaction(|(backrefs, delete_check)| {
            backrefs.remove(&backref_idx[..])?;
            delete_check.insert(schema.as_ref(), &[])?;
            Ok(())
        })?;
    }
    Ok(())
}

fn add_name(db: &Database, name: String, doc: Hash) -> Result<Option<Hash>, DbError> {
    // Always:
    // - Replace the name in the names table
    // - Increment the name count by one in the backrefs tree for the new hash
    // If the name is already used:
    // - Reduce the name count by one in the backrefs tree for the existing hash
    // - If the existing hash's name count is reduced to zero, add to the delete_check tree
    let mut new_backref_idx = Vec::with_capacity(doc.as_ref().len() + 1);
    new_backref_idx.extend_from_slice(doc.as_ref());
    new_backref_idx.push(BACKREF_NAMES);

    let new_count = 1u64 + get_backref_count(db, &new_backref_idx)?;

    if let Some(old_hash) = db.names.get(name.as_bytes())? {
        let old_hash = Hash::try_from(old_hash.as_ref()).map_err(|_| {
            DbError::Corruption(format!("Name entry wasn't a proper hash. name = {}", name))
        })?;
        let mut old_backref_idx = Vec::with_capacity(old_hash.as_ref().len() + 1);
        old_backref_idx.extend_from_slice(old_hash.as_ref());
        old_backref_idx.push(BACKREF_NAMES);
        let old_count = get_backref_count(db, &new_backref_idx)?;

        if old_count == 0 {
            return Err(DbError::Corruption(format!(
                "Trying to decrement a backref (name type) that is already zero. doc = {}",
                &old_hash
            )));
        } else if old_count == 1 {
            (&db.names, &db.backrefs, &db.delete_check).transaction(
                |(names, backrefs, delete_check)| {
                    names.insert(name.as_bytes(), doc.as_ref())?;
                    backrefs.insert(&new_backref_idx[..], &(new_count.to_le_bytes())[..])?;
                    backrefs.remove(&old_backref_idx[..])?;
                    delete_check.insert(&old_backref_idx[..old_backref_idx.len() - 1], &[])?;
                    Ok(())
                },
            )?;
        } else {
            (&db.names, &db.backrefs).transaction(|(names, backrefs)| {
                names.insert(name.as_bytes(), doc.as_ref())?;
                backrefs.insert(&new_backref_idx[..], &(new_count.to_le_bytes())[..])?;
                backrefs.insert(&old_backref_idx[..], &((old_count - 1).to_le_bytes())[..])?;
                Ok(())
            })?;
        }
        Ok(Some(old_hash))
    } else {
        (&db.names, &db.backrefs).transaction(|(names, backrefs)| {
            names.insert(name.as_bytes(), doc.as_ref())?;
            backrefs.insert(&new_backref_idx[..], &(new_count.to_le_bytes())[..])?;
            Ok(())
        })?;
        Ok(None)
    }
}

fn del_name(db: &Database, name: String) -> Result<Option<Hash>, DbError> {
    // If the name is used:
    // - Reduce the name count by one in the backrefs tree
    // - Drop the name from the names table
    // - If the name count was reduced to zero, add to the delete_check tree
    if let Some(raw_hash) = db.names.get(name.as_bytes())? {
        let hash = Hash::try_from(raw_hash.as_ref()).map_err(|_| {
            DbError::Corruption(format!("Name entry wasn't a proper hash. name = {}", name))
        })?;
        let mut backref_idx = Vec::with_capacity(hash.as_ref().len() + 1);
        backref_idx.extend_from_slice(hash.as_ref());
        backref_idx.push(BACKREF_NAMES);
        let count = get_backref_count(db, &backref_idx)?;

        if count == 0 {
            return Err(DbError::Corruption(format!(
                "Trying to decrement a backref (name type) that is already zero. doc = {}",
                &hash
            )));
        } else if count == 1 {
            (&db.names, &db.backrefs, &db.delete_check).transaction(
                |(names, backrefs, delete_check)| {
                    names.remove(name.as_bytes())?;
                    backrefs.remove(&backref_idx[..])?;
                    delete_check.insert(&backref_idx[..backref_idx.len() - 1], &[])?;
                    Ok(())
                },
            )?;
        } else {
            (&db.names, &db.backrefs).transaction(|(names, backrefs)| {
                names.remove(name.as_bytes())?;
                backrefs.insert(&backref_idx[..], &((count - 1).to_le_bytes())[..])?;
                Ok(())
            })?;
        }
        Ok(Some(hash))
    } else {
        Ok(None)
    }
}

async fn run_write_db(db: Database, mut rx: tokio::sync::mpsc::Receiver<WriteCmd>) {
    // ok. we: attempt to get the document. failing that, we register inside the write loop, which 
    // gives us a Arc<RwLock<HashMap<usize, oneshot::Sender<Result<Arc<Document>,DbError>>>>>, 
    // along with an ID that we can drop from the HashMap. On drop, we deregister ourselves by 
    // locking and removing ourselves from the map. Ok. Because forward can stall for a while, we 
    // are in the clear with Cursor.
    let mut waiting_for_doc_id = 0;
    let waiting_for_doc: DocSenders = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let mut quit_tx = None;

    'main_loop: loop {
        // Run delete check at least once, and keep going as long as there are no pending values
        let cmd = if quit_tx.is_none() && !db.delete_check.is_empty() {
            use tokio::sync::mpsc::error::TryRecvError;
            if let Err(e) = db.run_delete_check() {
                // TODO: Report this error somewhere more visible
                // I'm not sure what to do about this yet, honestly. Print it for now.
                eprintln!("{}", e);
            }
            loop {
                match rx.try_recv() {
                    Ok(cmd) => break cmd,
                    Err(TryRecvError::Disconnected) => break 'main_loop,
                    Err(TryRecvError::Empty) => {
                        if !db.delete_check.is_empty() {
                            if let Err(e) = db.run_delete_check() {
                                // TODO: Report this error somewhere more visible
                                // I'm not sure what to do about this yet, honestly. Print it for now.
                                eprintln!("{}", e);
                            }
                        }
                        else if let Some(cmd) = rx.recv().await { break cmd } else { break 'main_loop }
                    }
                }
            }
        }
        else if let Some(cmd) = rx.recv().await { cmd } else { break };

        // Note: for any response we're going to send, we don't really care if the oneshot channel
        // has failed. Our job is to report completion, not to ensure something is done with that
        // completion message.
        match cmd {
            WriteCmd::LookForDoc { hash, result } => {
                let _ = result.send(match db.get_doc(&hash) {
                    Err(e) => Err(e),
                    Ok(Some(doc)) => Ok(LookForDocResult::Doc(doc)),
                    Ok(None) => { 
                        let (tx, rx) = oneshot::channel();
                        let sub = DocSubscriber {
                            inner: DocSubscriberInner {
                                hash: hash.clone(),
                                id: waiting_for_doc_id,
                                home: waiting_for_doc.clone(),
                                need_to_free: true,
                            },
                            rx,
                        };
                        waiting_for_doc.write().entry(hash).or_insert(Vec::new()).push(
                            (waiting_for_doc_id, tx)
                        );
                        waiting_for_doc_id += 1;
                        Ok(LookForDocResult::Subscriber(sub))
                    },
                });
            },
            WriteCmd::CheckTtl => {
                todo!();
            }
            WriteCmd::Transaction {
                transaction,
                result,
            } => {
                let _ = result.send(transaction.commit(&db));
            }
            WriteCmd::AddSchema { schema, result } => {
                let _ = result.send(add_schema(&db, schema));
            }
            WriteCmd::DelSchema { schema, result } => {
                let _ = result.send(del_schema(&db, schema));
            }
            WriteCmd::AddName { name, doc, result } => {
                let _ = result.send(add_name(&db, name, doc));
            }
            WriteCmd::DelName { name, result } => {
                let _ = result.send(del_name(&db, name));
            }
            WriteCmd::Quit(tx) => {
                // Close the channel. We'll then process any remaining commands in the queue, and
                // fire the oneshot channel.
                quit_tx = Some(tx);
                rx.close();
            }
        }
    }

    if let Some(tx) = quit_tx {
        tx.send(Ok(()))
            .expect("Couldn't send the final Quit result for the fog-db database write task");
    }
}

// # Database Internal Structure
//
//  - docs
//      - key: Hash of document
//      - val: Encoded byte vector of document
//  - entries
//      - key: (Hash of document, Hash of entry)
//      - val: Encoded byte vector of entry
//  - backrefs
//      - key: (Hash of document, Hash of backref'd document)
//      - val: u64 count of backrefs
//      - key: (Hash of document, 0u8)
//      - val: Count of name references (sets as a root document)
//      - key: (Hash of document, 1u8)
//      - val: 1 if this has been added as a schema, otherwise does not exist.
//      - key: (Hash of document, 2u8)
//      - val: Count of times this has been referred to as a schema
//  - delete_check
//      - key: Hash of document
//      - val: nothing
//  - ttl_by_time
//      - key: u64 second counter
//      - val: (Hash of document, Hash of entry)
//  - ttl_by_hash
//      - key: (Hash of document, Hash of entry)
//      - val: u64 second counter
//  - names
//      - key: name as a String
//      - val: Hash of document
//
#[derive(Clone, Debug)]
pub struct Database {
    db: sled::Db,
    pub(crate) docs: sled::Tree,
    pub(crate) entries: sled::Tree,
    pub(crate) backrefs: sled::Tree,
    pub(crate) delete_check: sled::Tree,
    pub(crate) ttl_by_time: sled::Tree,
    pub(crate) ttl_by_hash: sled::Tree,
    names: sled::Tree,
    schema_cache: DynamicCache<Hash, Schema>,
    doc_cache: DynamicCache<Hash, Document>,
    cmd_tx: mpsc::Sender<WriteCmd>,
}

impl Database {
    fn open_db(db: sled::Db) -> Result<Self, DbError> {
        // Open all the trees
        let docs = db.open_tree("docs")?;
        let entries = db.open_tree("entries")?;
        let backrefs = db.open_tree("backrefs")?;
        let delete_check = db.open_tree("delete_check")?;
        let ttl_by_time = db.open_tree("ttl_by_time")?;
        let ttl_by_hash = db.open_tree("ttl_by_hash")?;
        let names = db.open_tree("names")?;

        // Set up the caches
        let schema_cache = DynamicCache::new(64);
        let doc_cache = DynamicCache::new(128);

        // Set up the command queue for the DB write thread
        let (cmd_tx, cmd_rx) = mpsc::channel(512);
        let cmd_tx_ttl_timer = cmd_tx.clone();

        // Create our instance
        let db = Self {
            db,
            docs,
            entries,
            backrefs,
            delete_check,
            ttl_by_time,
            ttl_by_hash,
            names,
            schema_cache,
            doc_cache,
            cmd_tx,
        };

        // Set up the writer thread and a timer to make it periodically evaluate TTL entries
        tokio::spawn(run_write_db(db.clone(), cmd_rx));
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(tokio::time::Duration::from_secs(60));
            loop {
                timer.tick().await;
                if cmd_tx_ttl_timer.send(WriteCmd::CheckTtl).await.is_err() {
                    break;
                }
            }
        });
        Ok(db)
    }

    /// Open a temporary database that will be deleted on drop.
    pub fn temp() -> Result<Self, DbError> {
        let db = sled::Config::default().temporary(true).open()?;
        Self::open_db(db)
    }

    /// Open a database at the specified path, or create one if there is none yet.
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, DbError> {
        let db = sled::open(path)?;
        Self::open_db(db)
    }

    /// Submit a transaction to the database.
    pub async fn submit_transaction(&self, t: Transaction) -> Result<(), TransactionError> {
        let enc_t = EncodedTransaction::encode(self, t)?;
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(WriteCmd::Transaction {
                transaction: enc_t,
                result: tx,
            })
            .await
            .map_err(|_| DbError::WriterDied)?;
        rx.await
            .map_err(|_| DbError::WriterDied)?
    }

    /// Open a cursor that only talks to the database. Fails on database error. If the database 
    /// doesn't contain the root document, no cursor is created.
    pub fn open_cursor(&self, root: &Hash) -> Result<Option<cursor::Cursor>, DbError> {
        cursor::Cursor::new_local(self.clone(), root)
    }

    pub(crate) async fn look_for_doc(&self, hash: &Hash) -> Result<LookForDocResult, DbError> {
        match self.get_doc(hash) {
            Err(err) => Err(err),
            Ok(Some(doc)) => Ok(LookForDocResult::Doc(doc)),
            Ok(None) => {
                let (tx, rx) = oneshot::channel();
                self.cmd_tx
                    .send(WriteCmd::LookForDoc {
                        hash: hash.clone(),
                        result: tx,
                    })
                    .await
                    .map_err(|_| DbError::WriterDied)?;
                rx.await
                    .map_err(|_| DbError::WriterDied)?
            },
        }
    }

    /// Get an iterator over all named Documents in the database. The returned iterator is a
    /// snapshot of the time at which this was called, and will not include any further changes to
    /// the database.
    pub fn get_names(&self) -> impl Iterator<Item = Result<(String, Hash), DbError>> {
        self.names.iter().map(|result| {
            result.map_err(DbError::from).and_then(|ivec| {
                let name = std::str::from_utf8(ivec.0.as_ref()).map_err(|_e| {
                    DbError::Corruption("Non-UTF8 name was found inside the `names` tree".into())
                })?;
                let hash = Hash::try_from(ivec.1.as_ref()).map_err(|_e| {
                    DbError::Corruption(format!(
                        "Hash for name {} was invalid (held in the `names` tree)",
                        name
                    ))
                })?;
                Ok((String::from(name), hash))
            })
        })
    }

    /// Look up the hash of a named Document in the database. Returns None if the
    /// name is not present in the database.
    pub fn get_name(&self, name: &str) -> Result<Option<Hash>, DbError> {
        let raw_hash = if let Some(hash) = self.names.get(name.as_bytes())? { hash } else {
            return Ok(None);
        };
        let hash = Hash::try_from(raw_hash.as_ref()).map_err(|_e| {
            DbError::Corruption(format!(
                "Hash for name {} was invalid (held in the `names` tree)",
                name
            ))
        })?;
        Ok(Some(hash))
    }

    /// Look up a document within the database. This will only ever check the local database
    /// itself.
    pub fn get_doc(&self, doc_hash: &Hash) -> Result<Option<Arc<Document>>, DbError> {
        // Verify the document is still in the database
        if !self.docs.contains_key(doc_hash)? { return Ok(None); }

        // Attempt to retrieve from the cache
        if let Some(doc) = self.doc_cache.get(doc_hash) {
            return Ok(Some(doc));
        }

        // If not in cache, attempt to retrieve from DB and parse the document
        // If this returns nothing, it's not necessarily an error - the document could have been 
        // removed between the `contains_key` check and now.
        let raw = if let Some(raw) = self.docs.get(doc_hash)? { raw.to_vec() } else {
            return Ok(None);
        };
        let schema_hash = fog_pack::get_doc_schema(&raw).map_err(|e| {
            DbError::Corruption(format!(
                "Document {} doesn't match format. fog-pack error: {}",
                doc_hash, e
            ))
        })?;
        let doc = match schema_hash {
            None => NoSchema::trusted_decode_doc(raw).map_err(|e| {
                DbError::Corruption(format!(
                    "Document {} in database is corrupted. fog-pack error: {}",
                    doc_hash, e
                ))
            })?,
            Some(schema_hash) => {
                let schema = self.get_schema(&schema_hash)?.ok_or_else(||
                    DbError::Corruption(format!(
                        "Document {} is missing its schema {}", doc_hash, schema_hash
                    ))
                )?;
                schema.trusted_decode_doc(raw).map_err(|e| {
                    DbError::Corruption(format!(
                        "Document {} in database is corrupted. fog-pack error: {}",
                        doc_hash, e
                    ))
                })?
            }
        };

        // Load through the cache
        Ok(Some(self.doc_cache.insert(doc_hash, doc)))
    }

    fn construct_entry_key(entry: &EntryRef) -> Vec<u8> {
        let key_len: [u8; 8] = entry.key.len().to_le_bytes();
        [entry.parent.as_ref(), &key_len[..3], entry.key.as_bytes(), entry.hash.as_ref()].concat()
    }

    fn get_ttl_entries(&self, time: i64) -> Result<Vec<EntryRef>, DbError> {
        let time_key = time.max(0).to_be_bytes();
        let raw = if let Some(raw) = self.ttl_by_time.get(&time_key)? { raw } else {
            return Ok(Vec::new());
        };

        let mut parser: &[u8] = raw.as_ref();

        // We encode as a sequence of hash-string-hash, and repeat that.
        let mut v = Vec::new();
        while !parser.is_empty() {
            let mut len = [0u8; 4];
            parser.read_exact(&mut len).map_err(|_| DbError::Corruption(format!(
                "TTL time {} is corrupted, couldn't fetch a full length of the next document hash", time
            )))?;
            let len = u32::from_le_bytes(len) as usize;
            if len > parser.len() { return Err(DbError::Corruption(format!(
                "TTL time {} is corrupted, document hash length is longer than remaining bytes", time
            ))); }
            let (bytes, data) = parser.split_at(len);
            parser = data;
            let parent = Hash::try_from(bytes).map_err(|_| DbError::Corruption(format!(
                "TTL time {} is corrupted, Couldn't parse a document hash", time
            )))?;

            let mut len = [0u8; 4];
            parser.read_exact(&mut len).map_err(|_| DbError::Corruption(format!(
                "TTL time {} is corrupted, couldn't fetch a full length of the next key string", time
            )))?;
            let len = u32::from_le_bytes(len) as usize;
            if len > parser.len() { return Err(DbError::Corruption(format!(
                "TTL time {} is corrupted, key length is longer than remaining bytes", time
            ))); }
            let (bytes, data) = parser.split_at(len);
            parser = data;
            let key = std::str::from_utf8(bytes).map_err(|_| DbError::Corruption(format!(
                "TTL time {} is corrupted, Couldn't parse a key string", time
            )))?.to_string();

            let mut len = [0u8; 4];
            parser.read_exact(&mut len).map_err(|_| DbError::Corruption(format!(
                "TTL time {} is corrupted, couldn't fetch a full length of the next entry hash", time
            )))?;
            let len = u32::from_le_bytes(len) as usize;
            if len > parser.len() { return Err(DbError::Corruption(format!(
                "TTL time {} is corrupted, entry hash length is longer than remaining bytes", time
            ))); }
            let (bytes, data) = parser.split_at(len);
            parser = data;
            let hash = Hash::try_from(bytes).map_err(|_| DbError::Corruption(format!(
                "TTL time {} is corrupted, Couldn't parse an entry hash", time
            )))?;
            
            v.push(EntryRef {
                parent,
                key,
                hash
            });
        }
        Ok(v)
    }

    fn construct_ttl_entries(entries: &[EntryRef]) -> Vec<u8> {
        let mut v = Vec::new();
        for entry in entries {
            v.reserve(12 + entry.parent.as_ref().len() + entry.key.len() + entry.hash.as_ref().len());
            let len = (entry.parent.as_ref().len() as u32).to_le_bytes();
            v.extend_from_slice(&len);
            v.extend_from_slice(entry.parent.as_ref());
            let len = (entry.key.len() as u32).to_le_bytes();
            v.extend_from_slice(&len);
            v.extend_from_slice(entry.key.as_bytes());
            let len = (entry.hash.as_ref().len() as u32).to_le_bytes();
            v.extend_from_slice(&len);
            v.extend_from_slice(entry.hash.as_ref());
        }
        v
    }

    /// Load an entry from the database. Returns Ok(None) if the entry can't be found.
    pub fn get_entry(&self, entry: &EntryRef) -> Result<Option<Entry>, DbError> {
        // Try to find the raw entry
        let entry_key = Self::construct_entry_key(entry);
        let raw = if let Some(raw) = self.entries.get(&entry_key)? { raw.to_vec() } else { return Ok(None); };

        // Load the parent document - needed for the schema and required for later decoding
        let parent = match self.get_doc(&entry.parent) {
            Ok(Some(doc)) => doc,
            Ok(None) => return Err(DbError::Corruption(format!(
                "Entry in database but is missing parent document (entry: {})", entry
            ))),
            Err(e) => return Err(e),
        };

        // Load the parent document's schema
        let schema_hash = parent.schema_hash().ok_or_else(|| DbError::Corruption(format!(
            "Entry's parent document has no schema (entry: {})", entry
        )))?;
        let schema = if let Some(schema) = self.get_schema(schema_hash)? { schema } else {
            return Err(DbError::Corruption(format!(
                    "Entry {} in database has parent document with missing schema (schema: {})",
                    entry,
                    schema_hash
            )));
        };

        // Decode the entry using the schema & parent document
        let entry = schema
            .trusted_decode_entry(raw, &entry.key, &parent, &entry.hash)
            .map_err(|e| {
                DbError::Corruption(format!(
                        "Entry {} in database is corrupted. fog-pack error: {}", entry, e
                ))
            })?;
        Ok(Some(entry))
    }

    /// Get the hash of the schema used by a given document. Returns Ok(None) if the document isn't 
    /// present. If the document is present but no schema is used, returns Ok(Some(None)).
    fn get_doc_schema(&self, doc_hash: &Hash) -> Result<Option<Option<Hash>>, DbError> {
        let raw = if let Some(raw) = self.docs.get(doc_hash)? { raw } else {
            return Ok(None);
        };
        let schema_hash = fog_pack::get_doc_schema(raw.as_ref()).map_err(|e| {
            DbError::Corruption(format!(
                "Document {} in database is corrupted. fog-pack error: {}",
                doc_hash, e
            ))
        })?;
        Ok(Some(schema_hash))
    }

    /// Run through the deletion checking process.
    fn run_delete_check(&self) -> Result<(), DbError> {
        // The straightforward approach here is a depth-first search through each backref, 
        // terminating in failure if we detect a loop and in success if we hit a non-hash backref

        let raw_hash = if let Some((key,_)) = self.delete_check.first()? { key } else { return Ok(()); };
        let doc_hash = Hash::try_from(raw_hash.as_ref()).map_err(|_| DbError::Corruption(format!(
            "Found a key in the delete_check tree that wasn't a hash: {:?}", raw_hash
        )))?;
        
        let mut trace = Vec::new();
        if self.should_keep(&mut trace, doc_hash.clone())? {
            self.delete_check.remove(raw_hash)?;
            return Ok(());
        }

        // By this point, we know this document can be dropped. We don't have to clear out *its* 
        // backrefs, as that will be done when the documents referencing it disappear in turn. We 
        // do, however, need to clear out the backref entries that it originally created.
        let mut backrefs = HashMap::new();
        let mut delete_check = HashSet::new();
        let mut backrefs_batch = sled::Batch::default();
        let mut entries_batch = sled::Batch::default();
        let mut delete_check_batch = sled::Batch::default();

        // Get the document we're going to remove
        let doc = if let Some(doc) = self.get_doc(&doc_hash)? { doc } else {
            return Err(DbError::Corruption(format!(
                "Document is in delete_check but doesn't exist anymore: {}", doc_hash
            )));
        };

        // Update the schema reference count if it has a schema
        if let Some(schema_hash) = doc.schema_hash() {
            let mut backref_idx: Vec<u8> = Vec::with_capacity(schema_hash.as_ref().len() + 1);
            backref_idx.extend(schema_hash.as_ref());
            backref_idx.push(BACKREF_SCHEMA_COUNT);
            let mut schema_count = if let Some(v) = self.backrefs.get(&backref_idx)? {
                let count: [u8; 8] = v.as_ref().try_into().map_err(|_| DbError::Corruption(format!(
                    "Schema count is not a u64 value for schema {}", schema_hash
                )))?;
                u64::from_le_bytes(count)
            }
            else {
                return Err(DbError::Corruption(format!(
                    "Document's schema doesn't have any schema count despite being referenced by doc. Schema: {}",
                    schema_hash
                )));
            };
            schema_count -= 1;
            if schema_count == 0 {
                backrefs_batch.remove(backref_idx);
                delete_check.insert(schema_hash.clone());
            }
            else {
                backrefs_batch.insert(backref_idx, &schema_count.to_le_bytes());
            }
        }
        
        let refs = find_doc_hashes(&doc).map_err(|e|
            DbError::Corruption(format!(
                "Document {} in database is corrupted. fog-pack error: {}",
                doc_hash, e
            ))
        )?;

        for h in refs {
            *backrefs.entry(h.clone()).or_insert(0i64) -= 1;
            delete_check.insert(h);
        }

        // Find any attached entries and gather their hashes
        if let Some(schema_hash) = doc.schema_hash() {
            let schema = if let Some(schema) = self.get_schema(schema_hash)? { schema } else {
                return Err(DbError::Corruption(format!(
                        "Document {} in database is missing schema (schema: {})",
                        doc.hash(),
                        schema_hash
                )));
            };
            for scan in self.entries.scan_prefix(doc_hash.as_ref()) {
                // Decode the entry key
                let (key, raw) = scan?;
                entries_batch.remove(key.clone());
                let mut key_parser: &[u8] = &key.as_ref()[doc_hash.as_ref().len()..];
                let mut len = [0u8; 4];
                key_parser.read_exact(&mut len[..3]).map_err(|_| DbError::Corruption(format!(
                    "Couldn't parse an entry key for document {}", doc_hash
                )))?;
                let len = u32::from_le_bytes(len) as usize;
                if len > key_parser.len() { return Err(DbError::Corruption(format!(
                    "Couldn't parse an entry key for document {}", doc_hash
                ))); }
                let (bytes, raw_entry_hash) = key_parser.split_at(len);
                let key = std::str::from_utf8(bytes).map_err(|_| DbError::Corruption(format!(
                    "Couldn't parse an entry key's key string for document {}", doc_hash
                )))?;
                let entry_hash = Hash::try_from(raw_entry_hash).map_err(|_| DbError::Corruption(format!(
                    "Couldn't parse an entry key's entry hash for document {}", doc_hash
                )))?;

                // Decode the entry
                let entry = schema.trusted_decode_entry(raw.to_vec(), key, &doc, &entry_hash)
                    .map_err(|e| DbError::Corruption(format!(
                        "Entry {} in database is corrupted. fog-pack error: {}", 
                        EntryRef { parent: doc.hash().clone(), key: key.to_string(), hash: entry_hash.clone() },
                        e
                    )))?;

                // Find the hashes the entry refers to
                let refs = find_entry_hashes(&entry).map_err(|e| DbError::Corruption(format!(
                    "Entry {} in database is corrupted (found on decoding to get hashes). fog-pack error: {}",
                    EntryRef { parent: doc.hash().clone(), key: key.to_string(), hash: entry_hash.clone() },
                    e
                )))?;

                // Update the backrefs
                for h in refs {
                    *backrefs.entry(h.clone()).or_insert(0i64) -= 1;
                    delete_check.insert(h);
                }
            }
        }

        // Convert backref updates into a batch
        let mut backref_idx = Vec::new();
        for (hash, diff) in backrefs {
            backref_idx.clear();
            backref_idx.extend_from_slice(hash.as_ref());
            backref_idx.extend_from_slice(doc_hash.as_ref());
            let new_val = if let Some(raw) = self.backrefs.get(&backref_idx)? {
                let prev_val: [u8; 8] = raw.as_ref().try_into().map_err(|_| DbError::Corruption(format!(
                    "Backref count for document {}, referencer {} is corrupted, couldn't get value.",
                    hash, doc_hash
                )))?;
                i64::from_le_bytes(prev_val) + diff
            }
            else {
                return Err(DbError::Corruption(format!(
                    "Backref count for document {}, referencer {} is missing, but we should have been able to decrement it by {}",
                    hash, doc_hash, -diff
                )));
            };
            if new_val < 0 {
                return Err(DbError::Corruption(format!(
                    "Deletion attempted to update backref count for document {}, referencer {}, to be negative.",
                    hash, doc_hash
                )));
            }
            if new_val == 0 {
                backrefs_batch.remove(backref_idx.clone());
            }
            else {
                backrefs_batch.insert(backref_idx.clone(), &new_val.to_le_bytes());
            }
        }

        delete_check.remove(&doc_hash);
        for check in delete_check {
            delete_check_batch.insert(check.as_ref(), &[]);
        }
        delete_check_batch.remove(doc_hash.as_ref());

        ( &self.docs,
          &self.entries,
          &self.backrefs,
          &self.delete_check,
        )
            .transaction(
                |(docs, entries, backrefs, delete_check)| {
                    docs.remove(doc_hash.as_ref())?;
                    entries.apply_batch(&entries_batch)?;
                    backrefs.apply_batch(&backrefs_batch)?;
                    delete_check.apply_batch(&delete_check_batch)?;
                    Ok(())
                }
            )?;
        Ok(())
    }

    fn should_keep(&self, trace: &mut Vec<Hash>, point: Hash) -> Result<bool, DbError> {
        let hash_len = point.as_ref().len();
        let mut idx = Vec::with_capacity(hash_len * 2);
        idx.extend_from_slice(point.as_ref());
        let mut key = if let Some((k,_)) = self.backrefs.get_gt(&idx)? { k } else { return Ok(false); };
        if key[..hash_len] != idx { return Ok(false); } // Check that this is a backref for the current hash.
        if key.len() == (hash_len+1) { return Ok(true); } // Named, schema, etc. It is fixed.
        trace.push(point);
        loop {
            idx.clear();
            idx.extend_from_slice(&key);
            let hash = Hash::try_from(&idx[hash_len..]).map_err(|_| DbError::Corruption(format!(
                        "Found a key in the backrefs tree whose 2nd part isn't one byte or a hash: {:?}", idx
            )))?;
            // Loop detection
            if trace.contains(&hash) { break }
            if self.should_keep(trace, hash)? { return Ok(true); }
            key = if let Some((k,_)) = self.backrefs.get_gt(&idx)? { k } else { break };
            if key[..hash_len] != idx[..hash_len] { break }
        }
        trace.pop();
        Ok(false)
    }

    /// Name a document, pinning it in the database and allowing by-name retrieval. Note: the
    /// document does not need to be in the database to become named - in fact, it is expected that
    /// in most cases, one will call `add_name` and then follow immediately with a transaction
    /// adding the document.
    pub async fn add_name(&self, name: &str, doc: &Hash) -> Result<Option<Hash>, DbError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(WriteCmd::AddName {
                name: String::from(name),
                doc: doc.clone(),
                result: tx,
            })
            .await
            .map_err(|_| DbError::Internal("DB Writer task died".into()))?;
        rx.await
            .map_err(|_| DbError::Internal("Db Writer task died".into()))?
    }

    /// Drop a name reference from the database. This does not necessarily delete the document, but
    /// it will no longer be pinned in place by the name reference.
    pub async fn del_name(&self, name: &str) -> Result<Option<Hash>, DbError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(WriteCmd::DelName {
                name: String::from(name),
                result: tx,
            })
            .await
            .map_err(|_| DbError::Internal("DB Writer task died".into()))?;
        rx.await
            .map_err(|_| DbError::Internal("Db Writer task died".into()))?
    }

    /// Add a schema to the database. The Schema must not itself adhere to a schema.
    pub async fn add_schema(&self, schema: Document) -> Result<(), DbError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(WriteCmd::AddSchema { schema: Box::new(schema), result: tx })
            .await
            .map_err(|_| DbError::Internal("DB Writer task died".into()))?;
        rx.await
            .map_err(|_| DbError::Internal("Db Writer task died".into()))?
    }

    /// Remove a schema from the database. A schema document will continue to be treated as a
    /// schema so long as it is
    pub async fn del_schema(&self, schema: &Hash) -> Result<(), DbError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(WriteCmd::DelSchema {
                schema: schema.clone(),
                result: tx,
            })
            .await
            .map_err(|_| DbError::Internal("DB Writer task died".into()))?;
        rx.await
            .map_err(|_| DbError::Internal("Db Writer task died".into()))?
    }

    /// Check to see if a hash is registered as a schema in the database
    fn is_schema(&self, schema_hash: &Hash) -> Result<bool, DbError> {
        if !self.docs.contains_key(schema_hash)? { return Ok(false); }
        let mut backref_idx = Vec::with_capacity(schema_hash.as_ref().len() + 1);
        backref_idx.extend_from_slice(schema_hash.as_ref());
        backref_idx.push(BACKREF_IS_SCHEMA);
        if self.backrefs.contains_key(&backref_idx)? {
            return Ok(true);
        }
        backref_idx.pop();
        backref_idx.push(BACKREF_SCHEMA_COUNT);
        Ok(self.backrefs.contains_key(&backref_idx)?)
    }

    /// Look up a schema in the database. Returns None if the document isn't present.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Invalid`] if the document is present but isn't a schema. Other errors 
    /// are from the database internals.
    pub fn get_schema(&self, schema_hash: &Hash) -> Result<Option<Arc<Schema>>, DbError> {
        // Fetch from cache
        if let Some(schema) = self.schema_cache.get(schema_hash) {
            return Ok(Some(schema));
        }

        // If not in cache, verify it is a schema and attempt to load it
        let schema_doc = if let Some(doc) = self.get_doc(schema_hash)? { doc } else {
            return Ok(None);
        };
        if !self.is_schema(schema_hash)? {
            return Err(DbError::Invalid(
                "Not entered as a schema in the database".into(),
            ));
        }
        let schema = Schema::from_doc(&schema_doc).map_err(|_| {
            DbError::Invalid(format!("Document is not a schema (doc: {})", schema_hash))
        })?;

        // Load through the cache
        Ok(Some(self.schema_cache.insert(schema_hash, schema)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fog_pack::validator::*;

    #[tokio::test]
    async fn new_schema() {
        let db = Database::temp().unwrap();

        let schema = fog_pack::schema::SchemaBuilder::new(StrValidator::new().build()).build().unwrap();
        let schema_hash = schema.hash().clone();
        println!("Schema hash is {}", schema_hash);

        db.add_schema(schema).await.unwrap();
        let loaded_schema = db.get_schema(&schema_hash).unwrap().unwrap();
        assert_eq!(loaded_schema.hash(), &schema_hash);
    }

    #[tokio::test]
    async fn new_schema_document() {
        let db = Database::temp().unwrap();

        // Load a schema in
        let schema = fog_pack::schema::SchemaBuilder::new(StrValidator::new().build()).build().unwrap();
        let schema_hash = schema.hash().clone();
        println!("Schema hash is {}", schema_hash);
        db.add_schema(schema).await.unwrap();

        // Create a document and submit it
        let doc = fog_pack::document::NewDocument::new(Some(&schema_hash), "I am a test document").unwrap();
        let doc_hash = doc.hash().clone();
        db.add_name("test", &doc_hash).await.unwrap(); // Name it so that it won't be immediately removed
        let t = Transaction::new().add_new_doc(doc);
        db.submit_transaction(t).await.unwrap();

        // Make sure the database actually has the document in it
        let doc = db.get_doc(&doc_hash).unwrap().unwrap();
        assert_eq!(doc.hash(), &doc_hash);

        let doc: String = doc.deserialize().unwrap();
        assert_eq!(&doc, "I am a test document");
    }
}
