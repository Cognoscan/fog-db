use std::fmt;

use fog_pack::entry::EntryRef;
use fog_pack::types::Hash;

#[derive(Debug)]
#[non_exhaustive]
pub enum DbError {
    Internal(String),
    WriterDied,
    Db(sled::Error),
    Corruption(String),
    Invalid(String),
    FogPack {
        info: String,
        err: fog_pack::error::Error,
    },
}

impl From<sled::Error> for DbError {
    fn from(err: sled::Error) -> Self {
        DbError::Db(err)
    }
}

impl From<sled::transaction::TransactionError<DbError>> for DbError {
    fn from(err: sled::transaction::TransactionError<DbError>) -> Self {
        match err {
            sled::transaction::TransactionError::Abort(e) => e,
            sled::transaction::TransactionError::Storage(err) => err.into(),
        }
    }
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use self::DbError::*;
        match self {
            Internal(s) => write!(f, "Internal system error: {}", s),
            WriterDied => write!(f, "Database writer task died unexpectedly"),
            Db(_) => write!(f, "Sled database error"),
            Corruption(s) => write!(f, "Database Corruption: {}", s),
            Invalid(s) => write!(f, "Invalid database operation: {}", s),
            FogPack { info, .. } => write!(f, "fog-pack error while processing: {}", info),
        }
    }
}

impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use self::DbError::*;
        match self {
            Db(e) => Some(e),
            FogPack { err, .. } => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum TransactionError {
    /// A database error halted the transaction.
    DbError(DbError),
    /// Missing a schema for a document.
    MissingDocSchema { doc: Hash, schema: Hash },
    /// Missing a schema for an entry.
    MissingEntrySchema { entry: EntryRef, schema: Hash },
    /// Document is using a non-schema document as its schema.
    InvalidSchema { doc: Hash, schema: Hash },
    /// At least one document failed to validate.
    DocValidationFailed {
        doc: Hash,
        err: fog_pack::error::Error,
    },
    /// An entry was supplied for a parent document that has no schema
    EntryForSchemalessDoc(EntryRef),
    /// At least one entry failed to validate.
    EntryValidationFailed {
        entry: EntryRef,
        err: fog_pack::error::Error,
    },
    /// An entry was supplied without its parent document being in the database or the transaction.
    EntryWithoutDoc(EntryRef),
    /// An entry's documents needed for validation were neither supplied with the transaction nor
    /// in the database.
    EntryMissingRequiredDoc { entry: EntryRef, required: Hash },
    /// Tried setting the properties for a document that isn't in the database or the transaction.
    MissingDoc(Hash),
    /// Tried setting the properties for an entry that isn't in the database or the transaction.
    MissingEntry(EntryRef),
    /// Tried deleting an entry that is not in the database.
    EntryAlreadyDeleted(EntryRef),
    /// Miscellaneous FogPack error during transaction processing
    FogPack {
        info: String,
        err: fog_pack::error::Error,
    },
}

impl From<sled::Error> for TransactionError {
    fn from(err: sled::Error) -> Self {
        TransactionError::DbError(DbError::Db(err))
    }
}

impl From<sled::transaction::TransactionError<DbError>> for TransactionError {
    fn from(err: sled::transaction::TransactionError<DbError>) -> Self {
        match err {
            sled::transaction::TransactionError::Abort(e) => e.into(),
            sled::transaction::TransactionError::Storage(err) => err.into(),
        }
    }
}

impl From<DbError> for TransactionError {
    fn from(err: DbError) -> Self {
        TransactionError::DbError(err)
    }
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use self::TransactionError::*;
        match self {
            DbError(_) => write!(f, "Database error"),
            MissingDocSchema { doc, schema } => write!(f, "A schema for a document could not be found in the database (doc: {}, schema: {})", doc, schema),
            MissingEntrySchema { entry, schema } => write!(f, "A schema for an entry could not be found in the database (entry: {}, schema: {})", entry, schema),
            InvalidSchema { doc, schema } => write!(f, "A document is trying to use a non-schema document as its schema (doc: {}, schema: {})", doc, schema),
            DocValidationFailed { doc, .. } => write!(f, "A document failed validation (hash: {})", doc),
            EntryForSchemalessDoc(entry) => write!(f, "An entry was provided for a document with no schema (entry: {})", entry),
            EntryValidationFailed { entry, .. } => write!(f, "An entry failed validation: (entry: {})", entry),
            EntryWithoutDoc(entry) => write!(f, "The parent document for an entry is missing (entry: {})", entry),
            EntryMissingRequiredDoc { entry, required } => write!(f, "A document needed for validation of an entry was not in the transaction or database (entry: {}, required: {})", entry, required),
            EntryAlreadyDeleted(entry) => write!(f, "Tried deleting an entry that isn't in the database (entry: {})", entry),
            MissingDoc(hash) => write!(f, "Can't set properties of document not in DB (hash: {})", hash),
            MissingEntry(entry) => write!(f, "Can't set properties of entry not in DB (entry: {})", entry),
            FogPack { info, .. } => write!(f, "fog-pack error while processing: {}", info),
        }
    }
}

impl std::error::Error for TransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use self::TransactionError::*;
        match self {
            DbError(e) => Some(e),
            DocValidationFailed { err, .. } => Some(err),
            EntryValidationFailed { err, .. } => Some(err),
            FogPack { err, .. } => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum CursorError {
    /// A database error is preventing navigation.
    DbError(DbError),
    /// Hash given to `forward` operation isn't present in current document.
    InvalidForward,
    /// Checked only the database and didn't find it immediately
    NotInDatabase,
    /// Can't use the `back` operation on a Cursor that is already at its original start point.
    BackAtTopLevel,
    /// The supplied query is invalid for the currently selected document.
    InvalidQuery,
    /// Miscellaneous FogPack error during cursor handling
    FogPack {
        info: String,
        err: fog_pack::error::Error,
    },
}

impl From<sled::Error> for CursorError {
    fn from(err: sled::Error) -> Self {
        CursorError::DbError(DbError::Db(err))
    }
}

impl From<DbError> for CursorError {
    fn from(err: DbError) -> Self {
        CursorError::DbError(err)
    }
}


impl std::fmt::Display for CursorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use self::CursorError::*;
        match self {
            DbError(_) => write!(f, "Database Error"),
            InvalidForward => write!(f, "Can't move to new document not referenced by current one"),
            NotInDatabase => write!(f, "Document wasn't in the database at time of attempted fetch"),
            BackAtTopLevel => write!(f, "Tried to move back a level when at top level of cursor"),
            InvalidQuery => write!(f, "Given query is invalid for the currently selected document"),
            FogPack { info, .. } => write!(f, "fog-pack error while handling cursor: {}", info),
        }
    }
}


impl std::error::Error for CursorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use self::CursorError::*;
        match self {
            DbError(e) => Some(e),
            FogPack { err, .. } => Some(err),
            _ => None,
        }
    }
}
