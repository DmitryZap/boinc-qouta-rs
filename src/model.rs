use serde::{Deserialize, Serialize};

use crate::identity::EncryptedBlob;

pub type ParticipantId = u64;
pub type ProjectId = u64;
pub type TaskId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Participant {
    pub id: ParticipantId,
    pub name: String,
    pub reputation: u64,
    pub public_key: Option<[u8; 32]>, // None = legacy (coordinator's own account)
}

impl Participant {
    pub fn new(id: ParticipantId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            reputation: 1,
            public_key: None,
        }
    }

    pub fn with_public_key(mut self, key: [u8; 32]) -> Self {
        self.public_key = Some(key);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub owner_id: ParticipantId,
    pub name: String,
    pub quota_available: u64,
    pub quota_locked: u64,
    /// Project owner's X25519 public key, used by workers to encrypt
    /// task result stdout so only the owner can decrypt it.
    #[serde(default)]
    pub owner_encryption_pubkey: Option<[u8; 32]>,
}

impl Project {
    pub fn new(id: ProjectId, owner_id: ParticipantId, name: impl Into<String>) -> Self {
        Self {
            id,
            owner_id,
            name: name.into(),
            quota_available: 0,
            quota_locked: 0,
            owner_encryption_pubkey: None,
        }
    }

    pub fn with_encryption_pubkey(mut self, key: [u8; 32]) -> Self {
        self.owner_encryption_pubkey = Some(key);
        self
    }

    pub fn priority_score(&self) -> u64 {
        self.quota_available + self.quota_locked
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Assigned {
        worker_id: ParticipantId,
        lease_expires_at_tick: u64,
    },
    Completed {
        accepted_digest: String,
        rewarded_workers: Vec<ParticipantId>,
    },
    QuotaExhausted {
        accepted_digest: String,
        reporting_workers: Vec<ParticipantId>,
    },
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskReport {
    pub worker_id: ParticipantId,
    pub result_digest: String,
    /// Token cost measured by the Estimator on the executor node (0 if not measured).
    #[serde(default)]
    pub actual_cost: u64,
    /// Stdout encrypted to the project owner's X25519 public key (None if owner
    /// did not publish an encryption key).
    #[serde(default)]
    pub encrypted_result: Option<EncryptedBlob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub project_id: ProjectId,
    pub reward: u64,
    pub payload: String,
    pub consensus_weight_required: u64,
    pub max_reports: usize,
    pub reports: Vec<TaskReport>,
    pub status: TaskStatus,
    /// Filled in when consensus is reached, copied from the report that
    /// matched the accepted digest. The project owner can decrypt this.
    #[serde(default)]
    pub encrypted_result: Option<EncryptedBlob>,
}

impl Task {
    pub fn new(
        id: TaskId,
        project_id: ProjectId,
        reward: u64,
        payload: impl Into<String>,
        consensus_weight_required: u64,
        max_reports: usize,
    ) -> Self {
        Self {
            id,
            project_id,
            reward,
            payload: payload.into(),
            consensus_weight_required,
            max_reports,
            reports: Vec::new(),
            status: TaskStatus::Pending,
            encrypted_result: None,
        }
    }
}
