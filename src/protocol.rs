use serde::{Deserialize, Serialize};

use crate::identity::EncryptedBlob;
use crate::model::{ParticipantId, ProjectId, TaskId};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TransactionView {
    pub kind: String,
    pub from: Option<u64>,
    pub to: Option<u64>,
    pub amount: u64,
    pub memo: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BlockView {
    pub index: u64,
    pub tick: u64,
    pub hash: String,
    pub prev_hash: String,
    pub transactions: Vec<TransactionView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ParticipantView {
    pub id: ParticipantId,
    pub name: String,
    pub balance: u64,
    pub reputation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectView {
    pub id: ProjectId,
    pub owner_id: ParticipantId,
    pub name: String,
    pub quota_available: u64,
    pub quota_locked: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskView {
    pub id: TaskId,
    pub project_id: ProjectId,
    pub reward: u64,
    pub payload: String,
    pub status_label: String,
    #[serde(default)]
    pub assigned_worker_id: Option<ParticipantId>,
    #[serde(default)]
    pub reported_worker_ids: Vec<ParticipantId>,
    #[serde(default)]
    pub has_encrypted_result: bool,
    #[serde(default)]
    pub encrypted_result: Option<EncryptedBlob>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NetworkSnapshot {
    pub participants: Vec<ParticipantView>,
    pub projects: Vec<ProjectView>,
    pub tasks: Vec<TaskView>,
    #[serde(default)]
    pub blocks: Vec<BlockView>,
    pub block_count: usize,
    pub blockchain_valid: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum NodeMode {
    #[default]
    Coordinator,
    Worker,
}

// App UI → NetworkActor (local channel)
#[derive(Debug)]
pub enum AppCommand {
    ConnectCoordinator {
        listen_addr: String,
        name: String,
        balance: u64,
        quorum: u64,
    },
    ConnectWorker {
        coord_addr: String,
        name: String,
        balance: u64,
    },
    CreateProject {
        name: String,
        owner_encryption_pubkey: Option<[u8; 32]>,
    },
    FundProject {
        project_id: ProjectId,
        amount: u64,
    },
    DonateToProject {
        project_id: ProjectId,
        amount: u64,
    },
    SubmitTask {
        project_id: ProjectId,
        reward: u64,
        payload: String,
    },
    StartExecutor {
        reliability: u8,
        compute_ticks: u64,
        allowed_packages: Vec<String>,
    },
    StopExecutor,
    Disconnect,
}

// NetworkActor → App UI (local channel)
#[derive(Debug, Clone)]
pub enum AppEvent {
    Connected,
    Disconnected { reason: String },
    Registered { participant_id: ParticipantId },
    StateUpdate(NetworkSnapshot),
    ExecutorStarted,
    ExecutorStopped,
    Log(String),
    Error(String),
}

// Worker → Coordinator (TCP, line-delimited JSON)
#[derive(Debug, Serialize, Deserialize)]
pub enum PeerRequest {
    // Auth handshake (replaces Register)
    Hello {
        name: String,
        public_key: [u8; 32],
        initial_balance: u64,
    },
    Auth {
        public_key: [u8; 32],
        nonce_signature: Vec<u8>,
    },
    // Existing operations
    CreateProject {
        name: String,
        owner_encryption_pubkey: Option<[u8; 32]>,
    },
    FundProject {
        project_id: ProjectId,
        amount: u64,
    },
    DonateToProject {
        project_id: ProjectId,
        amount: u64,
    },
    SubmitTask {
        project_id: ProjectId,
        reward: u64,
        payload: String,
    },
    RequestTask {
        worker_id: ParticipantId,
    },
    SubmitResult {
        worker_id: ParticipantId,
        task_id: TaskId,
        result_digest: String,
        #[serde(default)]
        actual_cost: u64,
        encrypted_result: Option<EncryptedBlob>,
    },
    Heartbeat {
        worker_id: ParticipantId,
        task_id: TaskId,
    },
    GetState,
}

// Coordinator → Worker (TCP, line-delimited JSON)
#[derive(Debug, Serialize, Deserialize)]
pub enum PeerResponse {
    // Auth
    Challenge {
        participant_id: ParticipantId,
        nonce: [u8; 32],
    },
    Denied {
        reason: String,
    },
    // Existing responses
    Welcome {
        participant_id: ParticipantId,
    },
    ProjectCreated {
        project_id: ProjectId,
    },
    TaskSubmitted {
        task_id: TaskId,
    },
    TaskAssigned {
        task_id: TaskId,
        project_id: ProjectId,
        reward: u64,
        payload: String,
        owner_encryption_pubkey: Option<[u8; 32]>,
    },
    ResultAck {
        consensus: bool,
    },
    StateUpdate(NetworkSnapshot),
    NoPendingTasks,
    Error {
        msg: String,
    },
    Ok,
}
