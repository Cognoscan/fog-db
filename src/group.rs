use fog_crypto::{
    identity::IdentityKey,
    hash::Hash,
};
use crate::NetType;
use crate::cert::CertRule;
use crate::cursor::Cursor;

pub struct GroupBuilder {
    self_id: Option<IdentityKey>,
    nets: Vec<NetType>,
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
    pub fn add_net(mut self, net: NetType) -> Self {
        self.nets.push(net);
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






