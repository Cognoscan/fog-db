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

## License

Licensed under either of

- Apache License, Version 2.0
	([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license
	([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
