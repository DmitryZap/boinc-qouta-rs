//! Distributed BFT consensus over a replicated operation log.
//!
//! Every node runs an identical [`ConsensusEngine`]. The consensus log is a
//! hash-linked chain of [`OpBlock`]s, each carrying a batch of [`LedgerOp`]s.
//! A block is appended only once a Byzantine-fault-tolerant quorum of validators
//! has signed it. On commit, each node applies the block's ops to its local
//! [`Network`] replica via [`Network::apply_op`], so all nodes converge on
//! byte-identical state (balances, projects, tasks, reputation).
//!
//! This module is pure logic with no I/O. The network layer relays every inbound
//! [`ConsensusMsg`] across the mesh (with seen-set dedup); the engine only
//! originates new messages (its own votes, proposals, commit certificates).
//!
//! Round per height H = `next_index()`:
//!   1. Proposer for H (round-robin `validators[H % n]`) drains its mempool into
//!      a candidate [`OpBlock`], signs it, emits `Propose` plus its own `Vote`.
//!   2. Each validator validates the proposal and emits a signed `Vote`.
//!   3. When votes for one block hash reach quorum, the block is sealed with that
//!      certificate, appended, its ops applied to the replica, and broadcast as
//!      `Committed`.
//!
//! Out of scope (coursework simulation): view changes, proposer failover,
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
    /// smaller validator set; otherwise late joiners reject early blocks and
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
/// n=1 gives 1, n=2 gives 2, n=3 gives 3, n=4 gives 3, n=7 gives 5.
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

/// Votes accumulated at one height, keyed by block hash then by voter.
type HeightVotes = HashMap<[u8; 32], HashMap<NodeKey, Vec<u8>>>;

pub struct ConsensusEngine {
    identity: Identity,
    me: NodeKey,
    /// The replicated state machine, advanced by applying committed ops.
    state: Network,
    /// Hash-linked consensus log of op blocks (block 0 = genesis).
    chain: Vec<OpBlock>,
    mempool: Vec<LedgerOp>,
    /// Every validator we have ever learned of (live or historical). Used only
    /// to verify commit certificates: a block signed by a now-offline validator
    /// must still validate. Grows monotonically and is never gossiped.
    validators: BTreeSet<NodeKey>,
    /// Currently-active validators (always includes self). This is the set the
    /// BFT quorum and the proposer rotation are computed over, so an offline node
    /// neither inflates the quorum nor holds a proposer slot hostage. A node
    /// enters on connect or on a live consensus message and leaves when its link
    /// drops.
    active: BTreeSet<NodeKey>,
    proposals: HashMap<u64, HashMap<[u8; 32], OpBlock>>,
    votes: HashMap<u64, HeightVotes>,
    voted: HashMap<u64, [u8; 32]>,
}

impl ConsensusEngine {
    pub fn new(identity: Identity, initial_validators: impl IntoIterator<Item = NodeKey>) -> Self {
        let me = identity.public_key_bytes();
        let mut validators: BTreeSet<NodeKey> = initial_validators.into_iter().collect();
        validators.insert(me);
        // At construction every known validator is presumed active; liveness is
        // refined afterwards as links connect and drop.
        let active = validators.clone();
        Self {
            identity,
            me,
            // Task-result quorum 1: a single worker's report finalizes a task.
            // All replicas use the same config, so state stays deterministic.
            // (This is the compute layer; block insertion still uses BFT quorum.)
            state: Self::fresh_state(),
            chain: vec![OpBlock::genesis()],
            mempool: Vec::new(),
            validators,
            active,
            proposals: HashMap::new(),
            votes: HashMap::new(),
            voted: HashMap::new(),
        }
    }

    /// A blank replica with the canonical config, used at startup and when
    /// rebuilding state during a chain reorg.
    fn fresh_state() -> Network {
        Network::with_config(NetworkConfig {
            consensus_quorum: 1,
            ..NetworkConfig::default()
        })
    }

    pub fn me(&self) -> NodeKey {
        self.me
    }

    /// Read-only access to the replicated state.
    pub fn state(&self) -> &Network {
        &self.state
    }

    /// Canonical hash of the replicated state, used to check convergence across nodes.
    pub fn state_fingerprint(&self) -> [u8; 32] {
        self.state.state_fingerprint()
    }

    pub fn mempool(&self) -> &[LedgerOp] {
        &self.mempool
    }

    /// Snapshot the committed chain for persistence.
    pub fn chain_blocks(&self) -> Vec<OpBlock> {
        self.chain.clone()
    }

    /// Replay a persisted chain (trusted: our own saved data, so it links and
    /// applies ops without re-checking certificates). Genesis is already present,
    /// so only blocks at the expected next index are applied, in order.
    pub fn restore_chain(&mut self, blocks: Vec<OpBlock>) {
        for block in blocks {
            if block.index != self.next_index() {
                continue;
            }
            // Historical membership only: a restored chain must not resurrect
            // long-gone validators into the live quorum (that is what previously
            // wedged a solo node behind an unreachable quorum).
            self.note_validator(block.proposer);
            let voters: Vec<NodeKey> = block.votes.iter().map(|v| v.voter).collect();
            for v in voters {
                self.note_validator(v);
            }
            self.append_and_apply(block);
        }
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

    /// Total validators ever known (live + historical).
    pub fn validator_count(&self) -> usize {
        self.validators.len()
    }

    /// Currently-active validators, the set quorum and proposer are based on.
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    pub fn quorum(&self) -> usize {
        quorum_for(self.active.len())
    }

    /// Register a validator we are actively in contact with (a new link, a live
    /// consensus message, or membership gossip). Counts toward the active quorum
    /// and the proposer rotation.
    pub fn add_validator(&mut self, key: NodeKey) {
        self.validators.insert(key);
        self.active.insert(key);
    }

    /// Register a validator known only from historical chain data (chain restore
    /// or fork adoption). Needed so its past commit certificates still verify,
    /// but it does NOT become active: an offline node that once voted cannot
    /// inflate the live quorum or claim a proposer slot.
    fn note_validator(&mut self, key: NodeKey) {
        self.validators.insert(key);
    }

    /// Mark a validator inactive when its link drops. Self is never removed, so a
    /// solo node keeps an active set of one and can still make progress.
    pub fn mark_inactive(&mut self, key: NodeKey) {
        if key != self.me {
            self.active.remove(&key);
        }
    }

    /// Active validator keys (for liveness gossip to new peers). Historical-only
    /// validators are deliberately excluded so stale membership never spreads.
    pub fn validators(&self) -> Vec<NodeKey> {
        self.active.iter().copied().collect()
    }

    pub fn proposer_for(&self, height: u64) -> Option<NodeKey> {
        let n = self.active.len();
        if n == 0 {
            return None;
        }
        let idx = (height % n as u64) as usize;
        self.active.iter().nth(idx).copied()
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
            ConsensusMsg::SyncResponse { blocks } => self.apply_sync_blocks(blocks),
        }
    }

    // handlers

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
        // A committed block at an index we already hold but with a different hash
        // means our chain has forked from the network's. Pull the peer's full
        // chain so fork choice can decide which to keep.
        if block.index < next {
            if self.chain[block.index as usize].hash != block.hash {
                return vec![ConsensusMsg::SyncRequest { from_height: 0 }];
            }
            return vec![];
        }
        if block.index > next {
            return vec![ConsensusMsg::SyncRequest { from_height: next }];
        }
        // index == next: extends our head only if it links. A non-linking block
        // at the next height is also a fork, so fetch the full chain.
        if block.prev_hash != self.head_hash() {
            return vec![ConsensusMsg::SyncRequest { from_height: 0 }];
        }
        if self.verify_certificate(&block) {
            self.append_and_apply(block);
        }
        vec![]
    }

    fn apply_sync_blocks(&mut self, blocks: Vec<OpBlock>) -> Vec<ConsensusMsg> {
        let mut sorted = blocks;
        sorted.sort_by_key(|b| b.index);

        // A response carrying the full chain from genesis lets fork choice adopt
        // it wholesale, the only way to recover from a divergent local chain.
        if sorted.first().map(|b| b.index) == Some(0) {
            self.try_adopt_chain(&sorted);
            return vec![];
        }

        // Partial response: extend our head with contiguous, certified blocks.
        let mut linked_any = false;
        for block in &sorted {
            if block.index != self.next_index() || block.prev_hash != self.head_hash() {
                continue;
            }
            if block.index == 0 || self.verify_certificate(block) {
                self.append_and_apply(block.clone());
                linked_any = true;
            }
        }

        // Got blocks ahead of us that wouldn't link, so our chain has forked.
        // Pull the peer's full chain so fork choice can decide.
        if !linked_any && sorted.iter().any(|b| b.index >= self.next_index()) {
            return vec![ConsensusMsg::SyncRequest { from_height: 0 }];
        }
        vec![]
    }

    /// Fork choice. If `blocks` is a valid chain from genesis that is *strictly
    /// longer* than ours, rebuild the replica from it and replace our chain.
    /// Returns whether it was adopted.
    ///
    /// Equal-length forks are deliberately NOT adopted: doing so would let a peer
    /// that finalized a competing same-height block make us discard our own
    /// committed block (e.g. a just-submitted task). Only a strictly longer chain
    /// wins, since it carries strictly more committed work.
    fn try_adopt_chain(&mut self, blocks: &[OpBlock]) -> bool {
        if blocks.first().map(|b| b.index) != Some(0) {
            return false;
        }
        // Validate links and hashes end to end.
        let mut prev_hash = [0u8; 32];
        for (i, b) in blocks.iter().enumerate() {
            if b.index != i as u64 || b.prev_hash != prev_hash {
                return false;
            }
            let expected =
                OpBlock::compute_hash(b.index, &b.prev_hash, &b.proposer, &b.ops, b.quorum);
            if b.hash != expected {
                return false;
            }
            prev_hash = b.hash;
        }
        // Certificate checks need the validator set; register the chain's
        // proposers and voters as historical members (not active liveness; the
        // live peer that served this chain is marked active via its own link).
        for b in blocks {
            self.note_validator(b.proposer);
            for v in &b.votes {
                self.note_validator(v.voter);
            }
        }
        if !blocks.iter().all(|b| self.verify_certificate(b)) {
            return false;
        }
        if blocks.len() <= self.chain.len() {
            return false;
        }
        // Rebuild the replica deterministically from the adopted chain.
        let mut state = Self::fresh_state();
        for b in blocks {
            for op in &b.ops {
                state.apply_op(op);
            }
        }
        self.state = state;
        self.chain = blocks.to_vec();
        let mempool = std::mem::take(&mut self.mempool);
        self.mempool = mempool
            .into_iter()
            .filter(|op| !self.chain.iter().any(|b| b.ops.contains(op)))
            .collect();
        self.proposals.clear();
        self.votes.clear();
        self.voted.clear();
        true
    }

    // helpers

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

    /// Drive a single-validator engine to commit one op into a block.
    fn commit_solo(e: &mut ConsensusEngine, op: LedgerOp) {
        e.submit_local_op(op);
        e.try_propose();
    }

    #[test]
    fn divergent_chain_reorgs_to_longer_via_sync() {
        // Two nodes that each built their own chain offline, forking at height 1.
        let mut a = ConsensusEngine::new(Identity::generate(), []);
        let mut b = ConsensusEngine::new(Identity::generate(), []);

        // A's chain is longer (two blocks); B's diverges at height 1 (one block).
        commit_solo(&mut a, LedgerOp::RegisterParticipant {
            id: 1, name: "A".into(), public_key: [1u8; 32], initial_balance: 100,
        });
        commit_solo(&mut a, LedgerOp::RegisterParticipant {
            id: 2, name: "B".into(), public_key: [2u8; 32], initial_balance: 50,
        });
        commit_solo(&mut b, LedgerOp::RegisterParticipant {
            id: 9, name: "Z".into(), public_key: [9u8; 32], initial_balance: 7,
        });

        assert_eq!(a.block_count(), 3);
        assert_eq!(b.block_count(), 2);
        assert_ne!(a.head_hash(), b.head_hash());

        // B receives A's full chain and adopts it via fork choice (longer wins).
        let out = b.on_message(ConsensusMsg::SyncResponse { blocks: a.chain_blocks() });
        assert!(out.is_empty());

        assert_eq!(b.block_count(), a.block_count());
        assert_eq!(b.head_hash(), a.head_hash());
        assert_eq!(b.state_fingerprint(), a.state_fingerprint());
        // B's old divergent state is gone; A's state is now live on B.
        assert_eq!(b.state().balance_of(1), 100);
        assert_eq!(b.state().balance_of(2), 50);
        assert_eq!(b.state().balance_of(9), 0);
    }

    #[test]
    fn equal_length_fork_is_not_adopted() {
        // Two nodes with same-length but divergent chains must NOT steal each
        // other's committed work — otherwise a peer's competing same-height block
        // would silently drop our own (e.g. a just-submitted task).
        let mut a = ConsensusEngine::new(Identity::generate(), []);
        let mut b = ConsensusEngine::new(Identity::generate(), []);

        commit_solo(&mut a, LedgerOp::RegisterParticipant {
            id: 1, name: "A".into(), public_key: [1u8; 32], initial_balance: 100,
        });
        commit_solo(&mut b, LedgerOp::RegisterParticipant {
            id: 2, name: "B".into(), public_key: [2u8; 32], initial_balance: 50,
        });
        assert_eq!(a.block_count(), b.block_count());

        let a_head = a.head_hash();
        let a_fp = a.state_fingerprint();
        // A receives B's equal-length chain — keeps its own, regardless of hash order.
        let out = a.on_message(ConsensusMsg::SyncResponse { blocks: b.chain_blocks() });
        assert!(out.is_empty());
        assert_eq!(a.head_hash(), a_head);
        assert_eq!(a.state_fingerprint(), a_fp);
        assert_eq!(a.state().balance_of(1), 100);
        assert_eq!(a.state().balance_of(2), 0);
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
    fn inactive_validators_shrink_quorum() {
        // Three validators known, but two have dropped their links. Quorum and
        // the proposer rotation must follow the active set, not the full one.
        let me = Identity::generate();
        let others: Vec<NodeKey> = (0..2).map(|_| Identity::generate().public_key_bytes()).collect();
        let mut e = ConsensusEngine::new(me, others.clone());
        assert_eq!(e.active_count(), 3);
        assert_eq!(e.quorum(), quorum_for(3));

        for k in &others {
            e.mark_inactive(*k);
        }
        assert_eq!(e.active_count(), 1, "self stays active");
        assert_eq!(e.validator_count(), 3, "historical set unchanged");
        assert_eq!(e.quorum(), 1);

        // With quorum 1 the lone active node finalizes its own op.
        e.submit_local_op(LedgerOp::RegisterParticipant {
            id: 5, name: "S".into(), public_key: [5u8; 32], initial_balance: 42,
        });
        e.try_propose();
        assert_eq!(e.block_count(), 2);
        assert_eq!(e.state().balance_of(5), 42);
    }

    #[test]
    fn restore_does_not_reactivate_offline_validators() {
        // A chain authored by other (now-offline) validators is restored. Those
        // validators must be known (so old certificates verify) but NOT active,
        // so a solo node restarting alone still reaches quorum and progresses.
        let mut donor = ConsensusEngine::new(Identity::generate(), []);
        commit_solo(&mut donor, LedgerOp::RegisterParticipant {
            id: 1, name: "A".into(), public_key: [1u8; 32], initial_balance: 100,
        });
        let saved = donor.chain_blocks();

        let mut node = ConsensusEngine::new(Identity::generate(), []);
        node.restore_chain(saved);
        assert_eq!(node.block_count(), 2, "restored donor's block");
        assert_eq!(node.active_count(), 1, "only self is active after restore");
        assert_eq!(node.quorum(), 1);
        assert!(node.validator_count() >= 2, "donor remembered for cert checks");

        // The solo node can still commit fresh ops on top of the restored chain.
        node.submit_local_op(LedgerOp::RegisterParticipant {
            id: 2, name: "B".into(), public_key: [2u8; 32], initial_balance: 7,
        });
        node.try_propose();
        assert_eq!(node.block_count(), 3);
        assert_eq!(node.state().balance_of(2), 7);
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
