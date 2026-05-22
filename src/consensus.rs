//! Distributed BFT consensus over a replicated operation log.
//!
//! Every node runs an identical [`ConsensusEngine`]. The consensus log is a
//! hash-linked chain of [`OpBlock`]s, each carrying a batch of [`LedgerOp`]s.
//! A block is appended only once a Byzantine-fault-tolerant quorum of validators
//! has signed it. On commit, each node applies the block's ops to its local
//! [`Network`] replica via [`Network::apply_op`], so all nodes converge on
//! byte-identical state (balances, projects, tasks, reputation).
//!
//! This module is pure logic — no I/O. The network layer relays every inbound
//! [`ConsensusMsg`] across the mesh (with seen-set dedup); the engine only
//! originates new messages (its own votes, proposals, commit certificates).
//!
//! Round per height H = `next_index()`:
//!   1. Proposer for H (round-robin `validators[H % n]`) drains its mempool into
//!      a candidate [`OpBlock`], signs it, emits `Propose` + its own `Vote`.
//!   2. Each validator validates the proposal and emits a signed `Vote`.
//!   3. When votes for one block hash reach quorum, the block is sealed with that
//!      certificate, appended, its ops applied to the replica, and broadcast as
//!      `Committed`.
//!
//! Out of scope (coursework simulation): view changes / proposer failover,
//! equivocation slashing, dynamic-validator-set reconfiguration safety.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::blockchain::{BlockVote, NodeKey};
use crate::identity::Identity;
use crate::network::{Network, NetworkConfig};
use crate::ops::LedgerOp;

/// A consensus-log block: a batch of operations plus the BFT commit
/// certificate. `hash` commits to everything except `votes` (votes sign `hash`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpBlock {
    pub index: u64,
    pub prev_hash: [u8; 32],
    pub proposer: NodeKey,
    pub ops: Vec<LedgerOp>,
    /// Number of validator signatures required to seal this block, fixed by the
    /// proposer at propose time. Stored so a node that later learns of more
    /// validators (larger quorum) still accepts blocks sealed earlier under a
    /// smaller validator set — otherwise late joiners reject early blocks and
    /// never converge.
    #[serde(default)]
    pub quorum: usize,
    pub hash: [u8; 32],
    #[serde(default)]
    pub votes: Vec<BlockVote>,
}

impl OpBlock {
    pub fn compute_hash(
        index: u64,
        prev_hash: &[u8; 32],
        proposer: &NodeKey,
        ops: &[LedgerOp],
        quorum: usize,
    ) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(index.to_le_bytes());
        hasher.update(prev_hash);
        hasher.update(proposer);
        // Stable serialization: serde_json field order is fixed by the structs.
        hasher.update(serde_json::to_vec(ops).unwrap_or_default());
        hasher.update((quorum as u64).to_le_bytes());
        hasher.finalize().into()
    }

    fn genesis() -> Self {
        let proposer = [0u8; 32];
        let ops = Vec::new();
        let hash = Self::compute_hash(0, &[0u8; 32], &proposer, &ops, 0);
        Self {
            index: 0,
            prev_hash: [0u8; 32],
            proposer,
            ops,
            quorum: 0,
            hash,
            votes: Vec::new(),
        }
    }

    pub fn proposed(
        index: u64,
        prev_hash: [u8; 32],
        proposer: NodeKey,
        ops: Vec<LedgerOp>,
        quorum: usize,
    ) -> Self {
        let hash = Self::compute_hash(index, &prev_hash, &proposer, &ops, quorum);
        Self {
            index,
            prev_hash,
            proposer,
            ops,
            quorum,
            hash,
            votes: Vec::new(),
        }
    }
}

/// BFT quorum threshold for `n` validators: `floor(2n/3) + 1`.
/// n=1→1, n=2→2, n=3→3, n=4→3, n=7→5.
pub fn quorum_for(n: usize) -> usize {
    (2 * n) / 3 + 1
}

/// Messages exchanged between consensus nodes over the gossip mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusMsg {
    /// An operation submitted to the network, destined for the mempool.
    Op(LedgerOp),
    /// A proposer's candidate block for height `block.index` (votes empty).
    Propose(OpBlock),
    /// A validator's signature over `block_hash` at `height`.
    Vote {
        height: u64,
        block_hash: [u8; 32],
        voter: NodeKey,
        signature: Vec<u8>,
    },
    /// A block sealed with its quorum certificate.
    Committed(OpBlock),
    /// Request all blocks from `from_height` onward (catch-up).
    SyncRequest { from_height: u64 },
    /// Reply to a [`ConsensusMsg::SyncRequest`]: certified blocks in order.
    SyncResponse { blocks: Vec<OpBlock> },
}

/// Votes accumulated at one height: block hash → (voter → signature).
type HeightVotes = HashMap<[u8; 32], HashMap<NodeKey, Vec<u8>>>;

pub struct ConsensusEngine {
    identity: Identity,
    me: NodeKey,
    /// The replicated state machine, advanced by applying committed ops.
    state: Network,
    /// Hash-linked consensus log of op blocks (block 0 = genesis).
    chain: Vec<OpBlock>,
    mempool: Vec<LedgerOp>,
    /// Known validators (includes self). Grows as proposers/voters are observed.
    validators: BTreeSet<NodeKey>,
    proposals: HashMap<u64, HashMap<[u8; 32], OpBlock>>,
    votes: HashMap<u64, HeightVotes>,
    voted: HashMap<u64, [u8; 32]>,
}

impl ConsensusEngine {
    pub fn new(identity: Identity, initial_validators: impl IntoIterator<Item = NodeKey>) -> Self {
        let me = identity.public_key_bytes();
        let mut validators: BTreeSet<NodeKey> = initial_validators.into_iter().collect();
        validators.insert(me);
        Self {
            identity,
            me,
            // Task-result quorum 1: a single worker's report finalizes a task.
            // All replicas use the same config, so state stays deterministic.
            // (This is the compute layer; block insertion still uses BFT quorum.)
            state: Network::with_config(NetworkConfig {
                consensus_quorum: 1,
                ..NetworkConfig::default()
            }),
            chain: vec![OpBlock::genesis()],
            mempool: Vec::new(),
            validators,
            proposals: HashMap::new(),
            votes: HashMap::new(),
            voted: HashMap::new(),
        }
    }

    pub fn me(&self) -> NodeKey {
        self.me
    }

    /// Read-only access to the replicated state.
    pub fn state(&self) -> &Network {
        &self.state
    }

    /// Canonical hash of the replicated state — convergence check across nodes.
    pub fn state_fingerprint(&self) -> [u8; 32] {
        self.state.state_fingerprint()
    }

    pub fn mempool(&self) -> &[LedgerOp] {
        &self.mempool
    }

    pub fn block_count(&self) -> usize {
        self.chain.len()
    }

    pub fn head_hash(&self) -> [u8; 32] {
        self.chain.last().map(|b| b.hash).unwrap_or([0u8; 32])
    }

    pub fn next_index(&self) -> u64 {
        self.chain.len() as u64
    }

    pub fn validator_count(&self) -> usize {
        self.validators.len()
    }

    pub fn quorum(&self) -> usize {
        quorum_for(self.validators.len())
    }

    pub fn add_validator(&mut self, key: NodeKey) {
        self.validators.insert(key);
    }

    /// All known validator keys (for membership gossip).
    pub fn validators(&self) -> Vec<NodeKey> {
        self.validators.iter().copied().collect()
    }

    pub fn proposer_for(&self, height: u64) -> Option<NodeKey> {
        let n = self.validators.len();
        if n == 0 {
            return None;
        }
        let idx = (height % n as u64) as usize;
        self.validators.iter().nth(idx).copied()
    }

    fn am_proposer_for(&self, height: u64) -> bool {
        self.proposer_for(height) == Some(self.me)
    }

    /// Submit a locally-originated operation. Adds it to the mempool and returns
    /// the gossip message announcing it.
    pub fn submit_local_op(&mut self, op: LedgerOp) -> Vec<ConsensusMsg> {
        if self.add_to_mempool(op.clone()) {
            vec![ConsensusMsg::Op(op)]
        } else {
            vec![]
        }
    }

    /// If this node is the proposer for the next height and has pending ops,
    /// build and broadcast a candidate block plus its self-vote.
    pub fn try_propose(&mut self) -> Vec<ConsensusMsg> {
        let height = self.next_index();
        if !self.am_proposer_for(height) || self.mempool.is_empty() {
            return vec![];
        }
        if self
            .proposals
            .get(&height)
            .map(|m| m.values().any(|b| b.proposer == self.me))
            .unwrap_or(false)
        {
            return vec![];
        }

        let ops = self.mempool.clone();
        let block = OpBlock::proposed(height, self.head_hash(), self.me, ops, self.quorum());

        self.store_proposal(block.clone());
        let mut out = vec![ConsensusMsg::Propose(block.clone())];
        out.extend(self.cast_vote(height, block.hash));
        out.extend(self.maybe_commit(height, block.hash));
        out
    }

    /// Handle an inbound gossip message. Returns messages this node newly
    /// originates; relaying the inbound message is the network layer's job.
    pub fn on_message(&mut self, msg: ConsensusMsg) -> Vec<ConsensusMsg> {
        match msg {
            ConsensusMsg::Op(op) => {
                self.add_to_mempool(op);
                vec![]
            }
            ConsensusMsg::Propose(block) => self.on_propose(block),
            ConsensusMsg::Vote {
                height,
                block_hash,
                voter,
                signature,
            } => self.on_vote(height, block_hash, voter, signature),
            ConsensusMsg::Committed(block) => self.on_committed(block),
            ConsensusMsg::SyncRequest { from_height } => {
                let blocks: Vec<OpBlock> = self
                    .chain
                    .iter()
                    .filter(|b| b.index >= from_height)
                    .cloned()
                    .collect();
                if blocks.is_empty() {
                    vec![]
                } else {
                    vec![ConsensusMsg::SyncResponse { blocks }]
                }
            }
            ConsensusMsg::SyncResponse { blocks } => {
                self.apply_sync_blocks(blocks);
                vec![]
            }
        }
    }

    // ── handlers ──────────────────────────────────────────────────────────

    fn on_propose(&mut self, block: OpBlock) -> Vec<ConsensusMsg> {
        self.add_validator(block.proposer);
        let height = block.index;

        if height != self.next_index() {
            if height > self.next_index() {
                return vec![ConsensusMsg::SyncRequest {
                    from_height: self.next_index(),
                }];
            }
            return vec![];
        }
        if block.prev_hash != self.head_hash() {
            return vec![];
        }
        if self.proposer_for(height) != Some(block.proposer) {
            return vec![];
        }
        let expected = OpBlock::compute_hash(
            block.index,
            &block.prev_hash,
            &block.proposer,
            &block.ops,
            block.quorum,
        );
        if block.hash != expected {
            return vec![];
        }

        let block_hash = block.hash;
        self.store_proposal(block);
        let mut out = self.cast_vote(height, block_hash);
        out.extend(self.maybe_commit(height, block_hash));
        out
    }

    fn on_vote(
        &mut self,
        height: u64,
        block_hash: [u8; 32],
        voter: NodeKey,
        signature: Vec<u8>,
    ) -> Vec<ConsensusMsg> {
        self.add_validator(voter);
        if height < self.next_index() {
            return vec![];
        }
        if !Identity::verify(&voter, &block_hash, &signature) {
            return vec![];
        }
        self.votes
            .entry(height)
            .or_default()
            .entry(block_hash)
            .or_default()
            .insert(voter, signature);
        self.maybe_commit(height, block_hash)
    }

    fn on_committed(&mut self, block: OpBlock) -> Vec<ConsensusMsg> {
        self.add_validator(block.proposer);
        let next = self.next_index();
        if block.index < next {
            return vec![];
        }
        if block.index > next {
            return vec![ConsensusMsg::SyncRequest { from_height: next }];
        }
        if self.verify_certificate(&block) {
            self.append_and_apply(block);
        }
        vec![]
    }

    fn apply_sync_blocks(&mut self, blocks: Vec<OpBlock>) {
        let mut sorted = blocks;
        sorted.sort_by_key(|b| b.index);
        for block in sorted {
            if block.index != self.next_index() {
                continue;
            }
            let ok = block.index == 0 || self.verify_certificate(&block);
            if ok {
                self.append_and_apply(block);
            }
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────

    fn cast_vote(&mut self, height: u64, block_hash: [u8; 32]) -> Vec<ConsensusMsg> {
        if self.voted.contains_key(&height) {
            return vec![];
        }
        let signature = self.identity.sign_nonce(&block_hash);
        self.voted.insert(height, block_hash);
        self.votes
            .entry(height)
            .or_default()
            .entry(block_hash)
            .or_default()
            .insert(self.me, signature.clone());
        vec![ConsensusMsg::Vote {
            height,
            block_hash,
            voter: self.me,
            signature,
        }]
    }

    fn maybe_commit(&mut self, height: u64, block_hash: [u8; 32]) -> Vec<ConsensusMsg> {
        if height != self.next_index() {
            return vec![];
        }
        // Seal against the quorum the proposer fixed in the block, not the
        // current validator count (which may have grown since).
        let Some(mut block) = self
            .proposals
            .get(&height)
            .and_then(|m| m.get(&block_hash))
            .cloned()
        else {
            return vec![]; // have votes but not the block yet
        };
        let tally = self
            .votes
            .get(&height)
            .and_then(|m| m.get(&block_hash))
            .map(|v| v.len())
            .unwrap_or(0);
        if tally < block.quorum.max(1) {
            return vec![];
        }

        block.votes = self
            .votes
            .get(&height)
            .and_then(|m| m.get(&block_hash))
            .map(|sigs| {
                sigs.iter()
                    .map(|(voter, signature)| BlockVote {
                        voter: *voter,
                        signature: signature.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        self.append_and_apply(block.clone());
        vec![ConsensusMsg::Committed(block)]
    }

    /// Link the block onto the chain and apply its ops to the replica.
    fn append_and_apply(&mut self, block: OpBlock) {
        if block.index != self.next_index() || block.prev_hash != self.head_hash() {
            return;
        }
        for op in &block.ops {
            self.state.apply_op(op);
        }
        self.mempool.retain(|op| !block.ops.contains(op));
        let index = block.index;
        self.chain.push(block);
        self.proposals.remove(&index);
        self.votes.remove(&index);
        self.voted.remove(&index);
    }

    fn store_proposal(&mut self, block: OpBlock) {
        self.proposals
            .entry(block.index)
            .or_default()
            .insert(block.hash, block);
    }

    fn add_to_mempool(&mut self, op: LedgerOp) -> bool {
        if self.mempool.contains(&op) {
            return false;
        }
        let already_committed = self.chain.iter().any(|b| b.ops.contains(&op));
        if already_committed {
            return false;
        }
        self.mempool.push(op);
        true
    }

    /// Validate a block's commit certificate: at least a quorum of distinct
    /// known validators signed the block hash. Genesis needs no certificate.
    pub fn verify_certificate(&self, block: &OpBlock) -> bool {
        if block.index == 0 {
            return true;
        }
        // Validate against the quorum the block was sealed under (stored in the
        // block), not our current validator count.
        let quorum = block.quorum.max(1);
        let valid = block
            .votes
            .iter()
            .filter(|v| {
                self.validators.contains(&v.voter)
                    && Identity::verify(&v.voter, &block.hash, &v.signature)
            })
            .map(|v| v.voter)
            .collect::<BTreeSet<_>>();
        valid.len() >= quorum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_to_quiescence(engines: &mut [ConsensusEngine], mut queue: Vec<(usize, ConsensusMsg)>) {
        let mut guard = 0;
        while let Some((origin, msg)) = queue.pop() {
            guard += 1;
            assert!(guard < 100_000, "consensus did not converge");
            for (i, eng) in engines.iter_mut().enumerate() {
                if i == origin {
                    continue;
                }
                let produced = eng.on_message(msg.clone());
                for m in produced {
                    queue.push((i, m));
                }
            }
        }
    }

    fn keys(engines: &[ConsensusEngine]) -> Vec<NodeKey> {
        engines.iter().map(|e| e.me()).collect()
    }

    fn make_network(n: usize) -> Vec<ConsensusEngine> {
        let identities: Vec<Identity> = (0..n).map(|_| Identity::generate()).collect();
        let all: Vec<NodeKey> = identities.iter().map(|i| i.public_key_bytes()).collect();
        identities
            .into_iter()
            .map(|id| ConsensusEngine::new(id, all.clone()))
            .collect()
    }

    /// Drive the proposer for the next height to propose, then settle.
    fn propose_and_settle(engines: &mut [ConsensusEngine]) {
        let height = engines[0].next_index();
        let proposer = engines[0].proposer_for(height).unwrap();
        let p_idx = keys(engines).iter().position(|k| *k == proposer).unwrap();
        let out = engines[p_idx].try_propose();
        run_to_quiescence(engines, out.into_iter().map(|m| (p_idx, m)).collect());
    }

    #[test]
    fn quorum_thresholds() {
        assert_eq!(quorum_for(1), 1);
        assert_eq!(quorum_for(2), 2);
        assert_eq!(quorum_for(3), 3);
        assert_eq!(quorum_for(4), 3);
        assert_eq!(quorum_for(7), 5);
    }

    #[test]
    fn single_node_commits_register_op() {
        let mut engines = make_network(1);
        let out = engines[0].submit_local_op(LedgerOp::RegisterParticipant {
            id: 42,
            name: "A".into(),
            public_key: [9u8; 32],
            initial_balance: 100,
        });
        run_to_quiescence(&mut engines, out.into_iter().map(|m| (0, m)).collect());
        propose_and_settle(&mut engines);

        assert_eq!(engines[0].block_count(), 2);
        assert_eq!(engines[0].state().balance_of(42), 100);
    }

    #[test]
    fn four_nodes_converge_on_identical_state() {
        let mut engines = make_network(4);
        // Two participants, then a transfer between them.
        let ops = vec![
            LedgerOp::RegisterParticipant {
                id: 1,
                name: "A".into(),
                public_key: [1u8; 32],
                initial_balance: 100,
            },
            LedgerOp::RegisterParticipant {
                id: 2,
                name: "B".into(),
                public_key: [2u8; 32],
                initial_balance: 100,
            },
            LedgerOp::Transfer {
                from: 1,
                to: 2,
                amount: 30,
                memo: "pay".into(),
            },
        ];
        for op in ops {
            let out = engines[0].submit_local_op(op);
            run_to_quiescence(&mut engines, out.into_iter().map(|m| (0, m)).collect());
            propose_and_settle(&mut engines);
        }

        let reference = engines[0].state_fingerprint();
        for eng in &engines {
            assert_eq!(eng.block_count(), engines[0].block_count());
            assert_eq!(eng.head_hash(), engines[0].head_hash());
            assert_eq!(eng.state_fingerprint(), reference);
            assert_eq!(eng.state().balance_of(1), 70);
            assert_eq!(eng.state().balance_of(2), 130);
        }
    }

    #[test]
    fn forged_vote_is_rejected_by_certificate_check() {
        let engines = make_network(4);
        let mut block = OpBlock::proposed(1, engines[0].head_hash(), engines[0].me(), vec![], 3);
        block.votes = engines
            .iter()
            .map(|e| BlockVote {
                voter: e.me(),
                signature: vec![0u8; 64],
            })
            .collect();
        assert!(!engines[0].verify_certificate(&block));
    }

    #[test]
    fn late_joiner_catches_up_via_sync() {
        let mut engines = make_network(3);
        let out = engines[0].submit_local_op(LedgerOp::RegisterParticipant {
            id: 7,
            name: "X".into(),
            public_key: [7u8; 32],
            initial_balance: 9,
        });
        run_to_quiescence(&mut engines, out.into_iter().map(|m| (0, m)).collect());
        propose_and_settle(&mut engines);

        let joiner_id = Identity::generate();
        let mut all = keys(&engines);
        all.push(joiner_id.public_key_bytes());
        let mut joiner = ConsensusEngine::new(joiner_id, all);
        let reply = engines[0].on_message(ConsensusMsg::SyncRequest { from_height: 0 });
        for m in reply {
            joiner.on_message(m);
        }
        assert_eq!(joiner.block_count(), engines[0].block_count());
        assert_eq!(joiner.state_fingerprint(), engines[0].state_fingerprint());
        assert_eq!(joiner.state().balance_of(7), 9);
    }
}
