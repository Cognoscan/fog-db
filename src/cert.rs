use fog_pack::{
    document::Document,
    types::{Identity, Timestamp},
};
use serde::{Deserialize, Serialize};

pub struct CertErr;

pub struct CertDb;

/// A Certificate Database. Stores certificates, allows searches, and performs certificate chain
/// verification.
///
/// A certificate consists of a subject Identity, a key string, a value string, an issuer, a start
/// time, and an end time. For each subject-issuer-key tuple, only one certificate will be stored.
/// When an addition would create a conflict, the certificate with the highest start time is used.
/// If start times are identical during an add operation, the added certificate takes priority.
///
/// Each time a certificate is added, its end time is checked. For each subject-issuer-key tuple,
/// the highest end time seen is stored. By tracking the highest known end time, the database can
/// determine when a certificate may be removed from the database, as all matching certificates
/// will have expired. When fetching a certificate, this end time is also provided.
impl CertDb {
    /// Add a certificate document to the database. Fails if the document is not a valid, signed
    /// certificate. On success, returns true if the cert is now the newest.
    pub async fn add(&mut self, cert: Document) -> Result<bool, CertErr> {
        todo!()
    }

    /// Add a set of certificate documents to the database. Fails if any of the documents are not
    /// valid, signed certificates.
    pub async fn add_vec(&mut self, certs: Vec<Document>) -> Result<(), CertErr> {
        todo!()
    }

    /// Iterator over all certificates matching the given subject. Returns the currently stored
    /// certificate and the highest end time seen for that certificate.
    pub async fn find_by_subject(
        &self,
        subject: Identity,
    ) -> Box<dyn Iterator<Item = (Document, Timestamp)>> {
        todo!()
    }

    /// Iterator over all certificates matching the given issuer. Returns the currently stored
    /// certificate and the highest end time seen for that certificate.
    pub async fn find_by_issuer(
        &self,
        issuer: Identity,
    ) -> Box<dyn Iterator<Item = (Document, Timestamp)>> {
        todo!()
    }

    /// Look up a certificate, given the subject, issuer, and key string. Returns the currently
    /// stored certificate and the highest end time seen for that certificate.
    pub async fn find_tuple(
        &self,
        subject: Identity,
        issuer: Identity,
        key: &str,
    ) -> Option<(Document, Timestamp)> {
        todo!()
    }

    /// Check to see if a subject meets the requirements of a certificate rule. The returned token
    /// will be dynamically updated as the database changes.
    pub async fn verify(&mut self, subject: Identity, chain: CertRule) -> CertToken {
        todo!()
    }

    /// Check to see if a subject meets the requirements of a certificate rule. Checks only once,
    /// so the result is only valid at the exact point in time when this function is called.
    pub async fn verify_once(&self, subject: Identity, chain: CertRule) -> bool {
        todo!()
    }

    /// Try to prove that a subject meets the requirements of a certificate rule. If it does, all
    /// the certificates that were used to show the requirements are met will be returned. By
    /// splitting up a `CertRule`, this function may be used to dynamically explore the database.
    pub async fn prove(&self, subject: Identity, chain: CertRule) -> Option<Vec<Document>> {
        todo!()
    }
}

/// An authentication token. It indicates if a given subject Identity matches a certificate rule,
/// according to the CertDb that issued it. It dynamically updates whenever CertDb receives new
/// certificates. the CertDb `add` & `add_vec` functions are guaranteed to not return until all
/// CertTokens have been appropriately updated.
pub struct CertToken;

impl CertToken {
    pub fn valid(&self) -> bool {
        todo!()
    }
}

/// Returns the Schema Document used for certificates.
pub fn certificate_schema() -> Document {
    use fog_pack::schema::SchemaBuilder;
    use fog_pack::validator::*;
    SchemaBuilder::new(
        MapValidator::new()
            .req_add("subject", IdentityValidator::new().build())
            .req_add("key", StrValidator::new().max_len(255).build())
            .req_add("value", StrValidator::new().max_len(255).build())
            .req_add("start", TimeValidator::new().build())
            .req_add("end", TimeValidator::new().build())
            .build(),
    )
    .build()
    .unwrap()
}

/// Get the Schema Document that [`CertRule`] structs all follow.
pub fn cert_rule_schema() -> Document {
    use fog_pack::schema::SchemaBuilder;
    use fog_pack::validator::*;
    SchemaBuilder::new(
        MapValidator::new()
            .req_add(
                "roots",
                ArrayValidator::new()
                    .items(IdentityValidator::new().build())
                    .build(),
            )
            .req_add(
                "chains",
                ArrayValidator::new()
                    .items(
                        ArrayValidator::new()
                            .items(
                                MapValidator::new()
                                    .req_add("key", StrValidator::new().max_len(255).build())
                                    .req_add("value", StrValidator::new().max_len(255).build())
                                    .req_add(
                                        "min_issuers",
                                        IntValidator::new().min(0).max(255).build(),
                                    )
                                    .build(),
                            )
                            .build(),
                    )
                    .build(),
            )
            .build(),
    )
    .build()
    .unwrap()
}

/// A link in a certificate chain. Consists of a key-value pair, and how many Identities meeting 
/// the previous rule must have issued a certificate asserting the key-value pair for an Identity.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CertChainLink {
    pub key: String,
    pub value: String,
    pub min_issuers: u8,
}

/// A Certificate Chain.
///
/// Certificate chains are used to specify requirements an Identity must meet. They consist of a
/// set of root Identities and an optional sequence of chain links, with each link specifying a
/// required value for a key and how many issuers must have certified that key-value pair.
///
/// If no chain links are provided, the Identity must instead be amongst the list of root
/// Identities.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CertRule {
    roots: Vec<Identity>,
    chains: Vec<Vec<CertChainLink>>,
}

impl CertRule {
    /// Create a new Certificate Rule Chain with one root signer.
    pub fn with_root(root: Identity) -> Self {
        Self {
            roots: vec![root],
            chains: Vec::new(),
        }
    }

    /// Create a new Certificate Rule Chain with multiple root signers.
    pub fn with_roots(roots: Vec<Identity>) -> Self {
        Self {
            roots,
            chains: Vec::new(),
        }
    }

    /// Add a new root signer to the list.
    pub fn add_root(&mut self, root: Identity) {
        self.roots.push(root);
    }

    /// Add a new accepted certificate chain for this rule.
    pub fn add_chain(&mut self, chain: Vec<CertChainLink>) {
        self.chains.push(chain);
    }

    /// Iterate over the known roots.
    pub fn roots(&self) -> std::slice::Iter<Identity> {
        self.roots.iter()
    }

}
