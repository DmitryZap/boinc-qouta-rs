//! `LedgerOp` — the high-level operations that make up the replicated consensus
//! log. Every node applies an identical, ordered sequence of these to its local
//! [`Network`](crate::network::Network) replica via
//! [`Network::apply_op`](crate::network::Network::apply_op), so all nodes
//! converge on byte-identical state (balances, projects, tasks, reputation).
//!
//! Operations are *intents*, not effects. Id allocation for projects and tasks
//! happens deterministically at apply time (sequential counters advance in lock
//! step because the op order is identical everywhere), so those ops carry no id.
//! Participant ids are the pubkey-derived account address, known to a node
//! before it joins, so `RegisterParticipant` carries the id explicitly.

use serde::{Deserialize, Serialize};

use crate::identity::EncryptedBlob;
use crate::model::{ParticipantId, ProjectId, TaskId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LedgerOp {
    /// Add a participant and mint their initial balance. `id` is the
    /// pubkey-derived account address. Idempotent: ignored if `id` already known.
    RegisterParticipant {
        id: ParticipantId,
        name: String,
        public_key: [u8; 32],
        initial_balance: u64,
    },
    /// Create a project owned by `owner_id`. Project id is allocated at apply.
    CreateProject {
        owner_id: ParticipantId,
        name: String,
        owner_encryption_pubkey: Option<[u8; 32]>,
    },
    /// Direct token transfer between two participant accounts.
    Transfer {
        from: ParticipantId,
        to: ParticipantId,
        amount: u64,
        memo: String,
    },
    /// Owner moves their own balance into a project's quota.
    FundProject {
        owner_id: ParticipantId,
        project_id: ProjectId,
        amount: u64,
    },
    /// Any participant donates balance into a project's quota.
    DonateToProject {
        supporter_id: ParticipantId,
        project_id: ProjectId,
        amount: u64,
    },
    /// Owner submits a task under a project. Task id is allocated at apply.
    SubmitTask {
        owner_id: ParticipantId,
        project_id: ProjectId,
        reward: u64,
        payload: String,
    },
    /// Worker claims the next eligible pending task (deterministic assignment).
    /// `nonce` only makes repeated requests distinct for mempool dedup; it does
    /// not affect how the op is applied.
    RequestTask { worker_id: ParticipantId, nonce: u64 },
    /// Worker reports a result for an assigned task.
    SubmitResult {
        worker_id: ParticipantId,
        task_id: TaskId,
        result_digest: String,
        actual_cost: u64,
        encrypted_result: Option<EncryptedBlob>,
    },
    /// Advance the network clock by `n` ticks (drives lease expiry).
    Tick { n: u64 },
}
