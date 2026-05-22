use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub type Address = u64;

/// Ed25519 public key identifying a consensus node (proposer / voter).
pub type NodeKey = [u8; 32];

/// A single validator's signed approval of a block. Signature is over the
/// block's `hash` (which itself commits to index/tick/prev_hash/txs/proposer).
/// Collected into `Block::votes` as the BFT commit certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockVote {
    pub voter: NodeKey,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Transaction {
    Mint {
        to: Address,
        amount: u64,
    },
    Transfer {
        from: Address,
        to: Address,
        amount: u64,
        memo: String,
    },
    Burn {
        from: Address,
        amount: u64,
    },
}

impl Transaction {
    fn serialize(&self) -> Vec<u8> {
        match self {
            Transaction::Mint { to, amount } => {
                let mut v = vec![0u8];
                v.extend_from_slice(&to.to_le_bytes());
                v.extend_from_slice(&amount.to_le_bytes());
                v
            }
            Transaction::Transfer {
                from,
                to,
                amount,
                memo,
            } => {
                let mut v = vec![1u8];
                v.extend_from_slice(&from.to_le_bytes());
                v.extend_from_slice(&to.to_le_bytes());
                v.extend_from_slice(&amount.to_le_bytes());
                v.extend_from_slice(memo.as_bytes());
                v
            }
            Transaction::Burn { from, amount } => {
                let mut v = vec![2u8];
                v.extend_from_slice(&from.to_le_bytes());
                v.extend_from_slice(&amount.to_le_bytes());
                v
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub index: u64,
    pub tick: u64,
    pub prev_hash: [u8; 32],
    pub transactions: Vec<Transaction>,
    /// Public key of the node that proposed this block. Zero for genesis and
    /// for locally-committed (non-consensus) blocks.
    #[serde(default)]
    pub proposer: NodeKey,
    pub hash: [u8; 32],
    /// BFT commit certificate: validator signatures over `hash`. Empty for
    /// genesis and locally-committed blocks; populated when a block is sealed
    /// through distributed consensus.
    #[serde(default)]
    pub votes: Vec<BlockVote>,
}

impl Block {
    /// Hash commits to everything that defines the block *except* the vote
    /// certificate — votes are gathered after the hash is fixed, so they sign it.
    pub fn compute_hash(
        index: u64,
        tick: u64,
        prev_hash: &[u8; 32],
        transactions: &[Transaction],
        proposer: &NodeKey,
    ) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(index.to_le_bytes());
        hasher.update(tick.to_le_bytes());
        hasher.update(prev_hash);
        for tx in transactions {
            hasher.update(tx.serialize());
        }
        hasher.update(proposer);
        hasher.finalize().into()
    }

    /// Build a block with a zero proposer and no votes (legacy local commit).
    fn new(index: u64, tick: u64, prev_hash: [u8; 32], transactions: Vec<Transaction>) -> Self {
        Self::proposed(index, tick, prev_hash, transactions, [0u8; 32])
    }

    /// Build an unsigned candidate block attributed to `proposer`. The commit
    /// certificate (`votes`) is filled in later once a quorum signs `hash`.
    pub fn proposed(
        index: u64,
        tick: u64,
        prev_hash: [u8; 32],
        transactions: Vec<Transaction>,
        proposer: NodeKey,
    ) -> Self {
        let hash = Self::compute_hash(index, tick, &prev_hash, &transactions, &proposer);
        Self {
            index,
            tick,
            prev_hash,
            transactions,
            proposer,
            hash,
            votes: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Blockchain {
    blocks: Vec<Block>,
}

impl Default for Blockchain {
    fn default() -> Self {
        Self::new()
    }
}

impl Blockchain {
    pub fn new() -> Self {
        let genesis = Block::new(0, 0, [0u8; 32], vec![]);
        Self {
            blocks: vec![genesis],
        }
    }

    pub fn commit_block(&mut self, tick: u64, transactions: Vec<Transaction>) -> &Block {
        let index = self.blocks.len() as u64;
        let prev_hash = self.blocks.last().map(|b| b.hash).unwrap_or([0u8; 32]);
        let block = Block::new(index, tick, prev_hash, transactions);
        self.blocks.push(block);
        self.blocks.last().unwrap()
    }

    /// Hash of the current chain tip; what the next block must point back to.
    pub fn head_hash(&self) -> [u8; 32] {
        self.blocks.last().map(|b| b.hash).unwrap_or([0u8; 32])
    }

    /// Index the next appended block must carry.
    pub fn next_index(&self) -> u64 {
        self.blocks.len() as u64
    }

    /// Append a block that was sealed elsewhere (consensus / sync). Validates
    /// the index, prev-hash linkage, and that the stored hash matches the
    /// recomputed one. The vote certificate is NOT checked here — the caller
    /// (consensus engine) verifies signatures against the validator set, since
    /// the chain itself does not know who the validators are.
    pub fn append_committed(&mut self, block: Block) -> std::result::Result<(), &'static str> {
        if block.index != self.next_index() {
            return Err("unexpected block index");
        }
        if block.prev_hash != self.head_hash() {
            return Err("prev_hash does not match chain head");
        }
        let expected = Block::compute_hash(
            block.index,
            block.tick,
            &block.prev_hash,
            &block.transactions,
            &block.proposer,
        );
        if block.hash != expected {
            return Err("block hash mismatch");
        }
        self.blocks.push(block);
        Ok(())
    }

    pub fn balance_of(&self, address: Address) -> u64 {
        let mut balance: i128 = 0;
        for block in &self.blocks {
            for tx in &block.transactions {
                match tx {
                    Transaction::Mint { to, amount } if *to == address => {
                        balance += *amount as i128;
                    }
                    Transaction::Transfer {
                        from, to, amount, ..
                    } => {
                        if *from == address {
                            balance -= *amount as i128;
                        }
                        if *to == address {
                            balance += *amount as i128;
                        }
                    }
                    Transaction::Burn { from, amount } if *from == address => {
                        balance -= *amount as i128;
                    }
                    _ => {}
                }
            }
        }
        balance.max(0) as u64
    }

    pub fn verify_integrity(&self) -> bool {
        for (i, block) in self.blocks.iter().enumerate() {
            let expected_hash = Block::compute_hash(
                block.index,
                block.tick,
                &block.prev_hash,
                &block.transactions,
                &block.proposer,
            );
            if block.hash != expected_hash {
                return false;
            }
            let expected_prev = if i == 0 {
                [0u8; 32]
            } else {
                self.blocks[i - 1].hash
            };
            if block.prev_hash != expected_prev {
                return false;
            }
        }
        true
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn blocks(&self) -> &[Block] {
        &self.blocks
    }

    pub fn history_for(&self, address: Address) -> Vec<&Transaction> {
        self.blocks
            .iter()
            .flat_map(|b| &b.transactions)
            .filter(|tx| match tx {
                Transaction::Mint { to, .. } => *to == address,
                Transaction::Transfer { from, to, .. } => *from == address || *to == address,
                Transaction::Burn { from, .. } => *from == address,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn genesis_block_has_zero_prev_hash() {
        let chain = Blockchain::new();
        assert_eq!(chain.blocks[0].prev_hash, [0u8; 32]);
        assert_eq!(chain.block_count(), 1);
    }

    #[test]
    fn balance_of_accumulates_mint() {
        let mut chain = Blockchain::new();
        chain.commit_block(1, vec![Transaction::Mint { to: 1, amount: 100 }]);
        chain.commit_block(2, vec![Transaction::Mint { to: 1, amount: 50 }]);
        assert_eq!(chain.balance_of(1), 150);
    }

    #[test]
    fn transfer_moves_balance_between_accounts() {
        let mut chain = Blockchain::new();
        chain.commit_block(1, vec![Transaction::Mint { to: 1, amount: 100 }]);
        chain.commit_block(
            2,
            vec![Transaction::Transfer {
                from: 1,
                to: 2,
                amount: 40,
                memo: "pay".to_string(),
            }],
        );
        assert_eq!(chain.balance_of(1), 60);
        assert_eq!(chain.balance_of(2), 40);
    }

    #[test]
    fn burn_reduces_balance() {
        let mut chain = Blockchain::new();
        chain.commit_block(1, vec![Transaction::Mint { to: 5, amount: 20 }]);
        chain.commit_block(2, vec![Transaction::Burn { from: 5, amount: 8 }]);
        assert_eq!(chain.balance_of(5), 12);
    }

    #[test]
    fn balance_never_goes_negative() {
        let mut chain = Blockchain::new();
        chain.commit_block(
            1,
            vec![Transaction::Burn {
                from: 99,
                amount: 1000,
            }],
        );
        assert_eq!(chain.balance_of(99), 0);
    }

    #[test]
    fn verify_integrity_passes_valid_chain() {
        let mut chain = Blockchain::new();
        chain.commit_block(1, vec![Transaction::Mint { to: 1, amount: 10 }]);
        chain.commit_block(
            2,
            vec![Transaction::Transfer {
                from: 1,
                to: 2,
                amount: 5,
                memo: "t".to_string(),
            }],
        );
        assert!(chain.verify_integrity());
    }

    #[test]
    fn verify_integrity_fails_on_tampered_amount() {
        let mut chain = Blockchain::new();
        chain.commit_block(1, vec![Transaction::Mint { to: 1, amount: 10 }]);
        // Tamper: change amount in the transaction directly
        if let Transaction::Mint { ref mut amount, .. } = chain.blocks[1].transactions[0] {
            *amount = 9999;
        }
        assert!(!chain.verify_integrity());
    }

    #[test]
    fn verify_integrity_fails_on_broken_prev_hash() {
        let mut chain = Blockchain::new();
        chain.commit_block(1, vec![Transaction::Mint { to: 1, amount: 10 }]);
        chain.commit_block(2, vec![Transaction::Mint { to: 2, amount: 5 }]);
        // Break prev_hash link in block 2
        chain.blocks[2].prev_hash[0] ^= 0xff;
        assert!(!chain.verify_integrity());
    }

    #[test]
    fn history_for_returns_only_relevant_transactions() {
        let mut chain = Blockchain::new();
        chain.commit_block(
            1,
            vec![
                Transaction::Mint { to: 1, amount: 100 },
                Transaction::Mint { to: 2, amount: 50 },
            ],
        );
        chain.commit_block(
            2,
            vec![Transaction::Transfer {
                from: 1,
                to: 2,
                amount: 10,
                memo: "x".to_string(),
            }],
        );
        let hist = chain.history_for(1);
        assert_eq!(hist.len(), 2);
        let hist2 = chain.history_for(2);
        assert_eq!(hist2.len(), 2);
    }
}
