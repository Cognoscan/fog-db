fog-db
======

fog-db is an embedded database built on top of [rocksdb], using [fog-pack] as 
the data format. Through fog-pack, it supports storage of immutable data 
(Documents) with attached mutable data (Entries). Documents may follow a schema 
to enable querying of their entries, as well as to enable strict format 
requirements and improved compression.

This is a work in progress and is not ready for general use.

[rocksdb]: https://rocksdb.org/
[fog-pack]: https://crates.io/crates/fog-pack
