// TODO: Remove this, once this library is further along and doesn't have so much skeleton code
#![allow(dead_code)]

pub mod cert;
pub mod db;
pub mod error;
pub mod group;
pub mod cursor;

mod transaction;

pub use transaction::Transaction;

pub enum NetType {
    /// Databases held within this machine.
    Machine,
    /// Direct connection to a nearby device
    Direct,
    /// "Local" Network
    Local,
    /// The global internet
    Internet,
}

