use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::blockchain::{Address, Blockchain, Transaction};
use crate::identity::EncryptedBlob;
use crate::model::{
    Participant, ParticipantId, Project, ProjectId, Task, TaskId, TaskReport, TaskStatus,
};
use crate::ops::LedgerOp;

pub const PROJECT_ID_OFFSET: u64 = 1_000_000_000;

pub fn project_address(project_id: ProjectId) -> Address {
    project_id + PROJECT_ID_OFFSET
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub lease_timeout_ticks: u64,
    pub consensus_quorum: u64,
    pub max_reports_per_task: usize,
    pub lease_slash: u64,
    pub reputation_reward: u64,
    pub reputation_penalty: u64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            lease_timeout_ticks: 3,
            consensus_quorum: 2,
            max_reports_per_task: 5,
            lease_slash: 1,
            reputation_reward: 1,
            reputation_penalty: 1,
        }
    }
}

#[derive(Debug)]
pub enum NetworkError {
    ParticipantNotFound(ParticipantId),
    ProjectNotFound(ProjectId),
    TaskNotFound(TaskId),
    NotEnoughBalance {
        participant_id: ParticipantId,
        requested: u64,
        available: u64,
    },
    NotEnoughProjectQuota {
        project_id: ProjectId,
        requested: u64,
        available: u64,
    },
    NotTaskOwner {
        participant_id: ParticipantId,
        project_id: ProjectId,
    },
    TaskNotAssignedToWorker {
        task_id: TaskId,
        worker_id: ParticipantId,
    },
    NoPendingTasks,
}

pub type Result<T> = std::result::Result<T, NetworkError>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct TaskLease {
    worker_id: ParticipantId,
    expires_at_tick: u64,
}

struct ReportState {
    reports: Vec<TaskReport>,
    max_reports: usize,
    consensus_weight_required: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Network {
    participants: HashMap<ParticipantId, Participant>,
    projects: HashMap<ProjectId, Project>,
    tasks: HashMap<TaskId, Task>,
    pending_tasks: VecDeque<TaskId>,
    leases: HashMap<TaskId, TaskLease>,
    current_tick: u64,
    config: NetworkConfig,
    next_participant_id: ParticipantId,
    next_project_id: ProjectId,
    next_task_id: TaskId,
    blockchain: Blockchain,
}

impl Default for Network {
    fn default() -> Self {
        Self::with_config(NetworkConfig::default())
    }
}

impl Network {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new() -> Self {
        Self::with_config(NetworkConfig::default())
    }

    pub fn with_config(config: NetworkConfig) -> Self {
        Self {
            participants: HashMap::new(),
            projects: HashMap::new(),
            tasks: HashMap::new(),
            pending_tasks: VecDeque::new(),
            leases: HashMap::new(),
            current_tick: 0,
            config,
            next_participant_id: 1,
            next_project_id: 1,
            next_task_id: 1,
            blockchain: Blockchain::new(),
        }
    }

    pub fn set_config(&mut self, config: NetworkConfig) {
        self.config = config;
    }

    pub fn register_participant(
        &mut self,
        name: impl Into<String>,
        initial_balance: u64,
        public_key: Option<[u8; 32]>,
    ) -> ParticipantId {
        let id = self.next_participant_id;
        self.next_participant_id += 1;
        let mut participant = Participant::new(id, name);
        if let Some(pk) = public_key {
            participant = participant.with_public_key(pk);
        }
        self.participants.insert(id, participant);
        if initial_balance > 0 {
            self.blockchain.commit_block(
                self.current_tick,
                vec![Transaction::Mint {
                    to: id,
                    amount: initial_balance,
                }],
            );
        }
        id
    }

    /// Register a participant with an explicit, externally-chosen id (the
    /// pubkey-derived account address). Idempotent: registering a known id is a
    /// no-op, so applying the same op on every replica (or re-applying during
    /// sync) is safe. Mints `initial_balance` on first sight.
    pub fn register_participant_with_id(
        &mut self,
        id: ParticipantId,
        name: impl Into<String>,
        initial_balance: u64,
        public_key: Option<[u8; 32]>,
    ) {
        if self.participants.contains_key(&id) {
            return;
        }
        let mut participant = Participant::new(id, name);
        if let Some(pk) = public_key {
            participant = participant.with_public_key(pk);
        }
        self.participants.insert(id, participant);
        self.next_participant_id = self.next_participant_id.max(id + 1);
        if initial_balance > 0 {
            self.blockchain.commit_block(
                self.current_tick,
                vec![Transaction::Mint {
                    to: id,
                    amount: initial_balance,
                }],
            );
        }
    }

    /// Apply one consensus-ordered operation to the replica. Deterministic:
    /// given identical prior state and op, every node reaches identical state.
    /// Operation errors (e.g. insufficient balance) are deterministic too and so
    /// are silently ignored: a failed op leaves state unchanged on every node.
    pub fn apply_op(&mut self, op: &LedgerOp) {
        match op {
            LedgerOp::RegisterParticipant {
                id,
                name,
                public_key,
                initial_balance,
            } => {
                self.register_participant_with_id(
                    *id,
                    name.clone(),
                    *initial_balance,
                    Some(*public_key),
                );
            }
            LedgerOp::CreateProject {
                owner_id,
                name,
                owner_encryption_pubkey,
            } => {
                let _ = self.create_project(*owner_id, name.clone(), *owner_encryption_pubkey);
            }
            LedgerOp::Transfer {
                from,
                to,
                amount,
                memo,
            } => {
                let _ = self.transfer(*from, *to, *amount, memo.clone());
            }
            LedgerOp::FundProject {
                owner_id,
                project_id,
                amount,
            } => {
                let _ = self.fund_project_from_owner(*owner_id, *project_id, *amount);
            }
            LedgerOp::DonateToProject {
                supporter_id,
                project_id,
                amount,
            } => {
                let _ = self.donate_to_project(*supporter_id, *project_id, *amount);
            }
            LedgerOp::SubmitTask {
                owner_id,
                project_id,
                reward,
                payload,
            } => {
                let _ = self.submit_task(*owner_id, *project_id, *reward, payload.clone());
            }
            LedgerOp::RequestTask { worker_id, .. } => {
                let _ = self.request_task(*worker_id);
            }
            LedgerOp::SubmitResult {
                worker_id,
                task_id,
                result_digest,
                actual_cost,
                encrypted_result,
            } => {
                let _ = self.submit_result(
                    *worker_id,
                    *task_id,
                    result_digest.clone(),
                    *actual_cost,
                    encrypted_result.clone(),
                );
            }
            LedgerOp::Tick { n } => self.tick(*n),
        }
    }

    /// Canonical, order-independent hash of the full replicated state. Two nodes
    /// that applied the same op log must produce the same fingerprint, the
    /// convergence check used by tests and the smoke binary.
    pub fn state_fingerprint(&self) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(self.current_tick.to_le_bytes());
        h.update(self.next_project_id.to_le_bytes());
        h.update(self.next_task_id.to_le_bytes());

        let mut participants: Vec<&Participant> = self.participants.values().collect();
        participants.sort_by_key(|p| p.id);
        for p in participants {
            h.update(p.id.to_le_bytes());
            h.update(p.name.as_bytes());
            h.update(p.reputation.to_le_bytes());
            h.update(self.blockchain.balance_of(p.id).to_le_bytes());
        }

        let mut projects: Vec<&Project> = self.projects.values().collect();
        projects.sort_by_key(|p| p.id);
        for p in projects {
            h.update(p.id.to_le_bytes());
            h.update(p.owner_id.to_le_bytes());
            h.update(p.quota_available.to_le_bytes());
            h.update(p.quota_locked.to_le_bytes());
        }

        let mut tasks: Vec<&Task> = self.tasks.values().collect();
        tasks.sort_by_key(|t| t.id);
        for t in tasks {
            h.update(t.id.to_le_bytes());
            h.update(t.project_id.to_le_bytes());
            h.update(t.reward.to_le_bytes());
            hash_status(&mut h, &t.status);
            for r in &t.reports {
                h.update(r.worker_id.to_le_bytes());
                h.update(r.result_digest.as_bytes());
            }
        }
        h.finalize().into()
    }

    pub fn find_participant_by_pubkey(&self, public_key: &[u8; 32]) -> Option<ParticipantId> {
        self.participants
            .values()
            .find(|p| p.public_key.as_ref() == Some(public_key))
            .map(|p| p.id)
    }

    pub fn create_project(
        &mut self,
        owner_id: ParticipantId,
        name: impl Into<String>,
        owner_encryption_pubkey: Option<[u8; 32]>,
    ) -> Result<ProjectId> {
        self.ensure_participant(owner_id)?;
        let project_id = self.next_project_id;
        self.next_project_id += 1;
        let mut project = Project::new(project_id, owner_id, name);
        if let Some(pk) = owner_encryption_pubkey {
            project = project.with_encryption_pubkey(pk);
        }
        self.projects.insert(project_id, project);
        Ok(project_id)
    }

    pub fn donate_to_project(
        &mut self,
        supporter_id: ParticipantId,
        project_id: ProjectId,
        amount: u64,
    ) -> Result<()> {
        self.ensure_participant(supporter_id)?;
        self.project_or_err(project_id)?;

        let available = self.blockchain.balance_of(supporter_id);
        if available < amount {
            return Err(NetworkError::NotEnoughBalance {
                participant_id: supporter_id,
                requested: amount,
                available,
            });
        }

        self.transfer_to_project(supporter_id, project_id, amount);
        self.project_mut_or_err(project_id)?.quota_available += amount;
        Ok(())
    }

    /// Direct token transfer between participant accounts.
    pub fn transfer(
        &mut self,
        from: ParticipantId,
        to: ParticipantId,
        amount: u64,
        memo: impl Into<String>,
    ) -> Result<()> {
        let available = self.blockchain.balance_of(from);
        if available < amount {
            return Err(NetworkError::NotEnoughBalance {
                participant_id: from,
                requested: amount,
                available,
            });
        }
        self.blockchain.commit_block(
            self.current_tick,
            vec![Transaction::Transfer {
                from,
                to,
                amount,
                memo: memo.into(),
            }],
        );
        Ok(())
    }

    pub fn fund_project_from_owner(
        &mut self,
        owner_id: ParticipantId,
        project_id: ProjectId,
        amount: u64,
    ) -> Result<()> {
        self.ensure_project_owner(owner_id, project_id)?;
        self.donate_to_project(owner_id, project_id, amount)
    }

    pub fn submit_task(
        &mut self,
        owner_id: ParticipantId,
        project_id: ProjectId,
        reward: u64,
        payload: impl Into<String>,
    ) -> Result<TaskId> {
        self.ensure_project_owner(owner_id, project_id)?;
        self.reserve_project_quota(project_id, reward)?;

        let task_id = self.next_task_id;
        self.next_task_id += 1;

        self.tasks.insert(
            task_id,
            Task::new(
                task_id,
                project_id,
                reward,
                payload,
                self.config.consensus_quorum.max(1),
                self.config.max_reports_per_task.max(1),
            ),
        );
        self.enqueue_pending(task_id);
        Ok(task_id)
    }

    pub fn request_task(&mut self, worker_id: ParticipantId) -> Result<Task> {
        self.ensure_participant(worker_id)?;
        self.reclaim_expired_leases();

        let task_id = self.take_next_task_for(worker_id)?;
        self.assign_task(worker_id, task_id)
    }

    pub fn heartbeat(&mut self, worker_id: ParticipantId, task_id: TaskId) -> Result<()> {
        self.ensure_participant(worker_id)?;
        self.reclaim_expired_leases();

        let lease_expires_at_tick = self.current_tick + self.config.lease_timeout_ticks;
        let lease = self.lease_for_worker_mut(worker_id, task_id)?;
        lease.expires_at_tick = lease_expires_at_tick;

        let task = self.task_mut_or_err(task_id)?;
        task.status = TaskStatus::Assigned {
            worker_id,
            lease_expires_at_tick,
        };
        Ok(())
    }

    /// Returns true when consensus is reached and task is finalized.
    pub fn submit_result(
        &mut self,
        worker_id: ParticipantId,
        task_id: TaskId,
        result_digest: String,
        actual_cost: u64,
        encrypted_result: Option<EncryptedBlob>,
    ) -> Result<bool> {
        self.ensure_participant(worker_id)?;
        self.reclaim_expired_leases();

        let lease = self.lease_for_worker(worker_id, task_id)?;
        if lease.worker_id != worker_id {
            return Err(NetworkError::TaskNotAssignedToWorker { task_id, worker_id });
        }

        self.leases.remove(&task_id);
        let report = TaskReport {
            worker_id,
            result_digest,
            actual_cost,
            encrypted_result,
        };
        let report_state = self.record_report(task_id, report)?;

        if let Some(accepted_digest) = self.consensus_digest(
            &report_state.reports,
            report_state.consensus_weight_required,
        ) {
            self.finalize_task(task_id, &accepted_digest)?;
            Ok(true)
        } else if report_state.reports.len() >= report_state.max_reports {
            self.reject_task(task_id)?;
            Ok(true)
        } else {
            self.return_task_to_queue(task_id)?;
            Ok(false)
        }
    }

    pub fn tick(&mut self, ticks: u64) {
        self.current_tick = self.current_tick.saturating_add(ticks);
        self.reclaim_expired_leases();
    }

    pub fn now_tick(&self) -> u64 {
        self.current_tick
    }

    pub fn participant(&self, id: ParticipantId) -> Option<&Participant> {
        self.participants.get(&id)
    }

    pub fn balance_of(&self, id: ParticipantId) -> u64 {
        self.blockchain.balance_of(id)
    }

    pub fn blockchain(&self) -> &Blockchain {
        &self.blockchain
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn set_participant_reputation(
        &mut self,
        participant_id: ParticipantId,
        reputation: u64,
    ) -> Result<()> {
        let participant = self
            .participants
            .get_mut(&participant_id)
            .ok_or(NetworkError::ParticipantNotFound(participant_id))?;
        participant.reputation = reputation.max(1);
        Ok(())
    }

    pub fn project(&self, id: ProjectId) -> Option<&Project> {
        self.projects.get(&id)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn task(&self, id: TaskId) -> Option<&Task> {
        self.tasks.get(&id)
    }

    pub fn pending_count(&self) -> usize {
        self.pending_tasks.len()
    }

    /// Number of outstanding task leases. Used to decide whether the clock needs
    /// to advance for lease expiry; no leases means ticking is pointless.
    pub fn lease_count(&self) -> usize {
        self.leases.len()
    }

    pub fn all_participants(&self) -> Vec<&crate::model::Participant> {
        self.participants.values().collect()
    }

    pub fn all_projects(&self) -> Vec<&crate::model::Project> {
        self.projects.values().collect()
    }

    pub fn all_tasks(&self) -> Vec<&crate::model::Task> {
        self.tasks.values().collect()
    }

    pub fn completed_task_results(&self, project_id: ProjectId) -> Result<Vec<(TaskId, String)>> {
        self.ensure_project(project_id)?;
        let mut results: Vec<(TaskId, String)> = self
            .tasks
            .iter()
            .filter_map(|(task_id, task)| {
                if task.project_id != project_id {
                    return None;
                }
                if let TaskStatus::Completed {
                    accepted_digest, ..
                } = &task.status
                {
                    Some((*task_id, accepted_digest.clone()))
                } else {
                    None
                }
            })
            .collect();
        results.sort_by_key(|(task_id, _)| *task_id);
        Ok(results)
    }

    fn project_or_err(&self, project_id: ProjectId) -> Result<&Project> {
        self.projects
            .get(&project_id)
            .ok_or(NetworkError::ProjectNotFound(project_id))
    }

    fn project_mut_or_err(&mut self, project_id: ProjectId) -> Result<&mut Project> {
        self.projects
            .get_mut(&project_id)
            .ok_or(NetworkError::ProjectNotFound(project_id))
    }

    fn task_or_err(&self, task_id: TaskId) -> Result<&Task> {
        self.tasks
            .get(&task_id)
            .ok_or(NetworkError::TaskNotFound(task_id))
    }

    fn task_mut_or_err(&mut self, task_id: TaskId) -> Result<&mut Task> {
        self.tasks
            .get_mut(&task_id)
            .ok_or(NetworkError::TaskNotFound(task_id))
    }

    fn ensure_project_owner(
        &self,
        participant_id: ParticipantId,
        project_id: ProjectId,
    ) -> Result<()> {
        let project = self.project_or_err(project_id)?;
        if project.owner_id == participant_id {
            Ok(())
        } else {
            Err(NetworkError::NotTaskOwner {
                participant_id,
                project_id,
            })
        }
    }

    fn transfer_to_project(
        &mut self,
        supporter_id: ParticipantId,
        project_id: ProjectId,
        amount: u64,
    ) {
        self.blockchain.commit_block(
            self.current_tick,
            vec![Transaction::Transfer {
                from: supporter_id,
                to: project_address(project_id),
                amount,
                memo: format!("fund:project={project_id}"),
            }],
        );
    }

    fn reserve_project_quota(&mut self, project_id: ProjectId, reward: u64) -> Result<()> {
        let project = self.project_mut_or_err(project_id)?;
        if project.quota_available < reward {
            return Err(NetworkError::NotEnoughProjectQuota {
                project_id,
                requested: reward,
                available: project.quota_available,
            });
        }

        project.quota_available -= reward;
        project.quota_locked += reward;
        Ok(())
    }

    fn release_locked_quota(&mut self, project_id: ProjectId, amount: u64) -> Result<()> {
        let project = self.project_mut_or_err(project_id)?;
        project.quota_locked = project.quota_locked.saturating_sub(amount);
        Ok(())
    }

    fn lease_for_worker(&self, worker_id: ParticipantId, task_id: TaskId) -> Result<TaskLease> {
        let lease = self
            .leases
            .get(&task_id)
            .copied()
            .ok_or(NetworkError::TaskNotAssignedToWorker { task_id, worker_id })?;
        if lease.worker_id == worker_id {
            Ok(lease)
        } else {
            Err(NetworkError::TaskNotAssignedToWorker { task_id, worker_id })
        }
    }

    fn lease_for_worker_mut(
        &mut self,
        worker_id: ParticipantId,
        task_id: TaskId,
    ) -> Result<&mut TaskLease> {
        let lease = self
            .leases
            .get_mut(&task_id)
            .ok_or(NetworkError::TaskNotAssignedToWorker { task_id, worker_id })?;
        if lease.worker_id == worker_id {
            Ok(lease)
        } else {
            Err(NetworkError::TaskNotAssignedToWorker { task_id, worker_id })
        }
    }

    fn take_next_task_for(&mut self, worker_id: ParticipantId) -> Result<TaskId> {
        let task_id = self
            .pending_tasks
            .iter()
            .copied()
            .filter(|task_id| self.worker_can_take_task(worker_id, *task_id))
            .max_by_key(|task_id| self.task_priority_key(*task_id))
            .ok_or(NetworkError::NoPendingTasks)?;

        self.pending_tasks.retain(|id| *id != task_id);
        Ok(task_id)
    }

    fn task_priority_key(&self, task_id: TaskId) -> (u64, std::cmp::Reverse<TaskId>) {
        self.tasks
            .get(&task_id)
            .and_then(|task| self.projects.get(&task.project_id))
            .map(|project| (project.priority_score(), std::cmp::Reverse(task_id)))
            .unwrap_or((0, std::cmp::Reverse(u64::MAX)))
    }

    fn assign_task(&mut self, worker_id: ParticipantId, task_id: TaskId) -> Result<Task> {
        let lease_expires_at_tick = self.current_tick + self.config.lease_timeout_ticks;
        self.leases.insert(
            task_id,
            TaskLease {
                worker_id,
                expires_at_tick: lease_expires_at_tick,
            },
        );

        let task = self.task_mut_or_err(task_id)?;
        task.status = TaskStatus::Assigned {
            worker_id,
            lease_expires_at_tick,
        };
        Ok(task.clone())
    }

    fn record_report(&mut self, task_id: TaskId, report: TaskReport) -> Result<ReportState> {
        let task = self.task_mut_or_err(task_id)?;
        task.reports.push(report);

        Ok(ReportState {
            reports: task.reports.clone(),
            max_reports: task.max_reports,
            consensus_weight_required: task.consensus_weight_required,
        })
    }

    fn return_task_to_queue(&mut self, task_id: TaskId) -> Result<()> {
        self.task_mut_or_err(task_id)?.status = TaskStatus::Pending;
        self.enqueue_pending(task_id);
        Ok(())
    }

    fn finalize_task(&mut self, task_id: TaskId, accepted_digest: &str) -> Result<()> {
        let (project_id, task_reward, reports_snapshot) = {
            let task = self.task_or_err(task_id)?;
            (task.project_id, task.reward, task.reports.clone())
        };

        let rewarded_workers: Vec<ParticipantId> = reports_snapshot
            .iter()
            .filter(|report| report.result_digest == accepted_digest)
            .map(|report| report.worker_id)
            .collect();

        // Pick the first encrypted_result attached to a winning report.
        let accepted_encrypted: Option<EncryptedBlob> = reports_snapshot
            .iter()
            .find(|r| r.result_digest == accepted_digest && r.encrypted_result.is_some())
            .and_then(|r| r.encrypted_result.clone());

        if rewarded_workers.is_empty() {
            return self.reject_task(task_id);
        }

        if is_budget_exhausted_digest(accepted_digest) {
            return self.finalize_budget_exhausted_task(
                task_id,
                project_id,
                task_reward,
                accepted_digest,
                rewarded_workers,
                accepted_encrypted,
            );
        }

        self.release_locked_quota(project_id, task_reward)?;

        let mut weighted_winners: Vec<(ParticipantId, u64)> = rewarded_workers
            .iter()
            .copied()
            .map(|winner_id| {
                let weight = self
                    .participants
                    .get(&winner_id)
                    .map(|p| p.reputation.max(1))
                    .unwrap_or(1);
                (winner_id, weight)
            })
            .collect();
        let total_weight: u64 = weighted_winners.iter().map(|(_, w)| *w).sum();
        if total_weight == 0 {
            return self.reject_task(task_id);
        }

        let reward_txs = distribute_reward(task_reward, total_weight, &mut weighted_winners);

        let proj_addr = project_address(project_id);
        let transactions: Vec<Transaction> = reward_txs
            .iter()
            .filter(|(_, share)| *share > 0)
            .map(|(winner_id, share)| Transaction::Transfer {
                from: proj_addr,
                to: *winner_id,
                amount: *share,
                memo: format!("reward:task={task_id}"),
            })
            .collect();

        if !transactions.is_empty() {
            self.blockchain
                .commit_block(self.current_tick, transactions);
        }

        for winner_id in &rewarded_workers {
            self.adjust_reputation(*winner_id, self.config.reputation_reward as i64);
        }
        for report in &reports_snapshot {
            if report.result_digest != accepted_digest {
                self.adjust_reputation(report.worker_id, -(self.config.reputation_penalty as i64));
            }
        }

        let task = self.task_mut_or_err(task_id)?;
        task.status = TaskStatus::Completed {
            accepted_digest: accepted_digest.to_string(),
            rewarded_workers,
        };
        task.encrypted_result = accepted_encrypted;
        Ok(())
    }

    fn finalize_budget_exhausted_task(
        &mut self,
        task_id: TaskId,
        project_id: ProjectId,
        task_reward: u64,
        accepted_digest: &str,
        reporting_workers: Vec<ParticipantId>,
        accepted_encrypted: Option<EncryptedBlob>,
    ) -> Result<()> {
        self.release_locked_quota(project_id, task_reward)?;
        if task_reward > 0 {
            self.blockchain.commit_block(
                self.current_tick,
                vec![Transaction::Burn {
                    from: project_address(project_id),
                    amount: task_reward,
                }],
            );
        }

        let task = self.task_mut_or_err(task_id)?;
        task.status = TaskStatus::QuotaExhausted {
            accepted_digest: accepted_digest.to_string(),
            reporting_workers,
        };
        task.encrypted_result = accepted_encrypted;
        Ok(())
    }

    fn reject_task(&mut self, task_id: TaskId) -> Result<()> {
        let (project_id, task_reward, reports_snapshot) = {
            let task = self.task_or_err(task_id)?;
            (task.project_id, task.reward, task.reports.clone())
        };
        self.release_locked_quota(project_id, task_reward)?;
        self.project_mut_or_err(project_id)?.quota_available += task_reward;

        for report in &reports_snapshot {
            self.adjust_reputation(report.worker_id, -(self.config.reputation_penalty as i64));
        }

        let task = self.task_mut_or_err(task_id)?;
        task.status = TaskStatus::Rejected;
        Ok(())
    }

    fn worker_can_take_task(&self, worker_id: ParticipantId, task_id: TaskId) -> bool {
        self.tasks
            .get(&task_id)
            .map(|task| {
                matches!(task.status, TaskStatus::Pending)
                    && !task
                        .reports
                        .iter()
                        .any(|report| report.worker_id == worker_id)
            })
            .unwrap_or(false)
    }

    fn consensus_digest(
        &self,
        reports: &[TaskReport],
        consensus_weight_required: u64,
    ) -> Option<String> {
        let mut digest_weights: HashMap<&str, u64> = HashMap::new();
        for report in reports {
            let weight = self
                .participants
                .get(&report.worker_id)
                .map(|p| p.reputation.max(1))
                .unwrap_or(1);
            let sum = digest_weights
                .entry(report.result_digest.as_str())
                .or_insert(0);
            *sum += weight;
            if *sum >= consensus_weight_required {
                return Some(report.result_digest.clone());
            }
        }
        None
    }

    fn enqueue_pending(&mut self, task_id: TaskId) {
        if !self.pending_tasks.contains(&task_id) {
            self.pending_tasks.push_back(task_id);
        }
    }

    fn reclaim_expired_leases(&mut self) {
        let mut expired: Vec<TaskId> = Vec::new();
        for (task_id, lease) in &self.leases {
            if lease.expires_at_tick <= self.current_tick {
                expired.push(*task_id);
            }
        }
        // HashMap iteration order is non-deterministic; sort so the emitted
        // slash transactions (and thus block contents / replica state) are
        // identical on every node, as the replicated state machine requires.
        expired.sort_unstable();

        let mut slash_txs: Vec<Transaction> = Vec::new();
        for task_id in expired {
            let lost_lease = self.leases.remove(&task_id);
            let was_assigned = self
                .tasks
                .get(&task_id)
                .map(|task| matches!(task.status, TaskStatus::Assigned { .. }))
                .unwrap_or(false);
            if was_assigned {
                if let Some(lease) = lost_lease {
                    self.adjust_reputation(
                        lease.worker_id,
                        -(self.config.reputation_penalty as i64),
                    );
                    let worker_balance = self.blockchain.balance_of(lease.worker_id);
                    let slash = self.config.lease_slash.min(worker_balance);
                    if slash > 0 {
                        slash_txs.push(Transaction::Burn {
                            from: lease.worker_id,
                            amount: slash,
                        });
                    }
                }
                if let Some(task) = self.tasks.get_mut(&task_id) {
                    task.status = TaskStatus::Pending;
                }
                self.enqueue_pending(task_id);
            }
        }

        if !slash_txs.is_empty() {
            self.blockchain.commit_block(self.current_tick, slash_txs);
        }
    }

    fn adjust_reputation(&mut self, participant_id: ParticipantId, delta: i64) {
        if let Some(participant) = self.participants.get_mut(&participant_id) {
            if delta >= 0 {
                participant.reputation = participant.reputation.saturating_add(delta as u64);
            } else {
                let decrease = (-delta) as u64;
                participant.reputation = participant.reputation.saturating_sub(decrease).max(1);
            }
        }
    }

    fn ensure_participant(&self, id: ParticipantId) -> Result<()> {
        if self.participants.contains_key(&id) {
            Ok(())
        } else {
            Err(NetworkError::ParticipantNotFound(id))
        }
    }

    fn ensure_project(&self, id: ProjectId) -> Result<()> {
        if self.projects.contains_key(&id) {
            Ok(())
        } else {
            Err(NetworkError::ProjectNotFound(id))
        }
    }
}

/// Canonical byte encoding of a task status into the state fingerprint hasher.
fn hash_status(h: &mut Sha256, status: &TaskStatus) {
    match status {
        TaskStatus::Pending => h.update([0u8]),
        TaskStatus::Assigned {
            worker_id,
            lease_expires_at_tick,
        } => {
            h.update([1u8]);
            h.update(worker_id.to_le_bytes());
            h.update(lease_expires_at_tick.to_le_bytes());
        }
        TaskStatus::Completed {
            accepted_digest,
            rewarded_workers,
        } => {
            h.update([2u8]);
            h.update(accepted_digest.as_bytes());
            for w in rewarded_workers {
                h.update(w.to_le_bytes());
            }
        }
        TaskStatus::QuotaExhausted {
            accepted_digest,
            reporting_workers,
        } => {
            h.update([3u8]);
            h.update(accepted_digest.as_bytes());
            for w in reporting_workers {
                h.update(w.to_le_bytes());
            }
        }
        TaskStatus::Rejected => h.update([4u8]),
    }
}

fn distribute_reward(
    task_reward: u64,
    total_weight: u64,
    weighted_winners: &mut [(ParticipantId, u64)],
) -> Vec<(ParticipantId, u64)> {
    let mut reward_txs: Vec<(ParticipantId, u64)> = weighted_winners
        .iter()
        .map(|(winner_id, weight)| {
            (
                *winner_id,
                task_reward.saturating_mul(*weight) / total_weight,
            )
        })
        .collect();

    let distributed: u64 = reward_txs.iter().map(|(_, share)| *share).sum();
    let mut remainder = task_reward.saturating_sub(distributed);
    weighted_winners.sort_by_key(|(_, weight)| std::cmp::Reverse(*weight));

    for (winner_id, _) in weighted_winners {
        if remainder == 0 {
            break;
        }
        if let Some((_, share)) = reward_txs.iter_mut().find(|(id, _)| id == winner_id) {
            *share += 1;
            remainder -= 1;
        }
    }

    reward_txs
}

fn is_budget_exhausted_digest(digest: &str) -> bool {
    digest.contains("-budget-exhausted-")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::LedgerOp;

    /// A representative op log touching every op kind and the multi-expired-lease
    /// reclaim path (the determinism hot spot).
    fn representative_log() -> Vec<LedgerOp> {
        vec![
            LedgerOp::RegisterParticipant {
                id: 100,
                name: "Owner".into(),
                public_key: [1u8; 32],
                initial_balance: 200,
            },
            LedgerOp::RegisterParticipant {
                id: 1,
                name: "W1".into(),
                public_key: [2u8; 32],
                initial_balance: 5,
            },
            LedgerOp::RegisterParticipant {
                id: 2,
                name: "W2".into(),
                public_key: [3u8; 32],
                initial_balance: 5,
            },
            LedgerOp::CreateProject {
                owner_id: 100,
                name: "P".into(),
                owner_encryption_pubkey: None,
            },
            LedgerOp::FundProject {
                owner_id: 100,
                project_id: 1,
                amount: 100,
            },
            LedgerOp::SubmitTask {
                owner_id: 100,
                project_id: 1,
                reward: 10,
                payload: "a".into(),
            },
            LedgerOp::SubmitTask {
                owner_id: 100,
                project_id: 1,
                reward: 10,
                payload: "b".into(),
            },
            LedgerOp::SubmitTask {
                owner_id: 100,
                project_id: 1,
                reward: 10,
                payload: "c".into(),
            },
            LedgerOp::RequestTask {
                worker_id: 1,
                nonce: 1,
            }, // W1 → task 1
            LedgerOp::RequestTask {
                worker_id: 2,
                nonce: 2,
            }, // W2 → task 2
            LedgerOp::SubmitResult {
                worker_id: 1,
                task_id: 1,
                result_digest: "done".into(),
                actual_cost: 0,
                encrypted_result: None,
            },
            LedgerOp::RequestTask {
                worker_id: 1,
                nonce: 3,
            }, // W1 → task 3
            LedgerOp::Tick { n: 5 },                // task2 + task3 leases expire together
        ]
    }

    fn replica(cfg: NetworkConfig, log: &[LedgerOp]) -> Network {
        let mut net = Network::with_config(cfg);
        for op in log {
            net.apply_op(op);
        }
        net
    }

    #[test]
    fn identical_op_log_yields_identical_state_across_replicas() {
        let cfg = NetworkConfig {
            consensus_quorum: 1,
            lease_timeout_ticks: 2,
            lease_slash: 1,
            ..NetworkConfig::default()
        };
        let log = representative_log();

        let reference = replica(cfg, &log).state_fingerprint();
        // Many independent replicas applying the same ordered log must converge.
        for _ in 0..5 {
            assert_eq!(replica(cfg, &log).state_fingerprint(), reference);
        }
        // Sanity: the log actually produced non-trivial state.
        assert_ne!(reference, Network::with_config(cfg).state_fingerprint());

        // Spot-check the deterministic outcome: task 1 finalized, W1 rewarded.
        let net = replica(cfg, &log);
        assert!(matches!(
            net.task(1).unwrap().status,
            TaskStatus::Completed { .. }
        ));
        assert!(net.blockchain().verify_integrity());
    }

    #[test]
    fn donation_reduces_supporter_balance_and_increases_project_quota() {
        let mut n = Network::new();
        let alice = n.register_participant("Alice", 100, None);
        let owner = n.register_participant("Owner", 0, None);
        let project = n.create_project(owner, "Climate", None).unwrap();

        n.donate_to_project(alice, project, 40).unwrap();

        assert_eq!(n.balance_of(alice), 60);
        assert_eq!(n.project(project).unwrap().quota_available, 40);
    }

    #[test]
    fn cannot_submit_task_when_project_has_not_enough_quota() {
        let mut n = Network::new();
        let owner = n.register_participant("Owner", 0, None);
        let project = n.create_project(owner, "Protein", None).unwrap();

        let err = n.submit_task(owner, project, 1, "job").unwrap_err();
        assert!(matches!(
            err,
            NetworkError::NotEnoughProjectQuota {
                project_id: _,
                requested: 1,
                available: 0
            }
        ));
    }

    #[test]
    fn scheduler_prefers_project_with_higher_priority_score() {
        let mut n = Network::new();
        let owner1 = n.register_participant("Owner1", 100, None);
        let owner2 = n.register_participant("Owner2", 100, None);
        let worker = n.register_participant("Worker", 0, None);

        let p1 = n.create_project(owner1, "Big", None).unwrap();
        let p2 = n.create_project(owner2, "Small", None).unwrap();

        n.fund_project_from_owner(owner1, p1, 60).unwrap();
        n.fund_project_from_owner(owner2, p2, 20).unwrap();

        let t1 = n.submit_task(owner1, p1, 10, "task-big").unwrap();
        let _t2 = n.submit_task(owner2, p2, 10, "task-small").unwrap();

        let assigned = n.request_task(worker).unwrap();
        assert_eq!(assigned.id, t1);
    }

    #[test]
    fn consensus_rewards_winning_workers_and_unlocks_project_quota() {
        let mut n = Network::with_config(NetworkConfig {
            lease_timeout_ticks: 3,
            consensus_quorum: 2,
            max_reports_per_task: 5,
            lease_slash: 1,
            reputation_reward: 1,
            reputation_penalty: 1,
        });
        let owner = n.register_participant("Owner", 20, None);
        let w1 = n.register_participant("W1", 0, None);
        let w2 = n.register_participant("W2", 0, None);
        let p = n.create_project(owner, "Astro", None).unwrap();

        n.fund_project_from_owner(owner, p, 10).unwrap();
        let task_id = n.submit_task(owner, p, 8, "fft").unwrap();

        n.request_task(w1).unwrap();
        let finalized = n
            .submit_result(w1, task_id, "digest-ok".to_string(), 0, None)
            .unwrap();
        assert!(!finalized);

        n.request_task(w2).unwrap();
        let finalized = n
            .submit_result(w2, task_id, "digest-ok".to_string(), 0, None)
            .unwrap();
        assert!(finalized);

        assert_eq!(n.balance_of(w1), 4);
        assert_eq!(n.balance_of(w2), 4);
        assert_eq!(n.project(p).unwrap().quota_locked, 0);
        assert!(matches!(
            n.task(task_id).unwrap().status,
            TaskStatus::Completed { .. }
        ));
    }

    #[test]
    fn expired_lease_returns_task_back_to_queue() {
        let mut n = Network::with_config(NetworkConfig {
            lease_timeout_ticks: 2,
            consensus_quorum: 2,
            max_reports_per_task: 5,
            lease_slash: 2,
            reputation_reward: 1,
            reputation_penalty: 1,
        });
        let owner = n.register_participant("Owner", 20, None);
        let w1 = n.register_participant("W1", 0, None);
        let w2 = n.register_participant("W2", 0, None);
        let p = n.create_project(owner, "Geo", None).unwrap();

        n.fund_project_from_owner(owner, p, 10).unwrap();
        n.submit_task(owner, p, 6, "map").unwrap();

        n.request_task(w1).unwrap();
        n.tick(2);

        let reassigned = n.request_task(w2).unwrap();
        assert_eq!(reassigned.project_id, p);
        assert_eq!(n.pending_count(), 0);
        assert_eq!(n.balance_of(w1), 0);
    }

    #[test]
    fn weighted_consensus_can_finalize_with_high_reputation_executor() {
        let mut n = Network::with_config(NetworkConfig {
            lease_timeout_ticks: 3,
            consensus_quorum: 4,
            max_reports_per_task: 5,
            lease_slash: 1,
            reputation_reward: 1,
            reputation_penalty: 1,
        });
        let owner = n.register_participant("Owner", 30, None);
        let high_rep = n.register_participant("HighRep", 0, None);
        let p = n.create_project(owner, "Chem", None).unwrap();
        n.set_participant_reputation(high_rep, 4).unwrap();

        n.fund_project_from_owner(owner, p, 20).unwrap();
        let task_id = n.submit_task(owner, p, 12, "mol").unwrap();
        n.request_task(high_rep).unwrap();
        let finalized = n
            .submit_result(high_rep, task_id, "digest-stable".to_string(), 0, None)
            .unwrap();

        assert!(finalized);
        assert_eq!(n.balance_of(high_rep), 12);
        assert!(matches!(
            n.task(task_id).unwrap().status,
            TaskStatus::Completed { .. }
        ));
    }

    #[test]
    fn task_is_auto_rejected_when_max_reports_reached_without_consensus() {
        let mut n = Network::with_config(NetworkConfig {
            lease_timeout_ticks: 3,
            consensus_quorum: 10,
            max_reports_per_task: 2,
            lease_slash: 1,
            reputation_reward: 1,
            reputation_penalty: 1,
        });
        let owner = n.register_participant("Owner", 20, None);
        let w1 = n.register_participant("W1", 0, None);
        let w2 = n.register_participant("W2", 0, None);
        let p = n.create_project(owner, "Math", None).unwrap();

        n.fund_project_from_owner(owner, p, 10).unwrap();
        let task_id = n.submit_task(owner, p, 8, "integral").unwrap();

        n.request_task(w1).unwrap();
        assert!(!n
            .submit_result(w1, task_id, "digest-a".to_string(), 0, None)
            .unwrap());
        n.request_task(w2).unwrap();
        assert!(n
            .submit_result(w2, task_id, "digest-b".to_string(), 0, None)
            .unwrap());

        assert!(matches!(
            n.task(task_id).unwrap().status,
            TaskStatus::Rejected
        ));
        assert_eq!(n.project(p).unwrap().quota_locked, 0);
        assert_eq!(n.project(p).unwrap().quota_available, 10);
    }

    #[test]
    fn budget_exhausted_consensus_burns_reward_without_paying_workers() {
        let mut n = Network::with_config(NetworkConfig {
            consensus_quorum: 2,
            ..NetworkConfig::default()
        });
        let owner = n.register_participant("Owner", 20, None);
        let w1 = n.register_participant("W1", 0, None);
        let w2 = n.register_participant("W2", 0, None);
        let p = n.create_project(owner, "Budget", None).unwrap();

        n.fund_project_from_owner(owner, p, 10).unwrap();
        let task_id = n.submit_task(owner, p, 8, "python-loop").unwrap();

        n.request_task(w1).unwrap();
        assert!(!n
            .submit_result(
                w1,
                task_id,
                "python-budget-exhausted-1".to_string(),
                8,
                None
            )
            .unwrap());
        n.request_task(w2).unwrap();
        assert!(n
            .submit_result(
                w2,
                task_id,
                "python-budget-exhausted-1".to_string(),
                8,
                None
            )
            .unwrap());

        assert_eq!(n.balance_of(w1), 0);
        assert_eq!(n.balance_of(w2), 0);
        assert_eq!(n.project(p).unwrap().quota_locked, 0);
        assert_eq!(n.project(p).unwrap().quota_available, 2);
        assert_eq!(n.blockchain().balance_of(project_address(p)), 2);
        assert!(matches!(
            n.task(task_id).unwrap().status,
            TaskStatus::QuotaExhausted { .. }
        ));
    }

    #[test]
    fn lease_timeout_slashes_worker_balance_and_reputation() {
        let mut n = Network::with_config(NetworkConfig {
            lease_timeout_ticks: 1,
            consensus_quorum: 2,
            max_reports_per_task: 5,
            lease_slash: 2,
            reputation_reward: 1,
            reputation_penalty: 1,
        });
        let owner = n.register_participant("Owner", 20, None);
        let w1 = n.register_participant("W1", 5, None);
        let p = n.create_project(owner, "Geo", None).unwrap();
        n.set_participant_reputation(w1, 3).unwrap();

        n.fund_project_from_owner(owner, p, 10).unwrap();
        n.submit_task(owner, p, 6, "mesh").unwrap();
        n.request_task(w1).unwrap();
        n.tick(1);

        assert_eq!(n.balance_of(w1), 3);
        assert_eq!(n.participant(w1).unwrap().reputation, 2);
        assert_eq!(n.pending_count(), 1);
    }

    // --- Blockchain-specific tests ---

    #[test]
    fn blockchain_balance_matches_initial_mint() {
        let mut n = Network::new();
        let alice = n.register_participant("Alice", 150, None);
        assert_eq!(n.balance_of(alice), 150);
        assert!(n.blockchain().verify_integrity());
    }

    #[test]
    fn funding_project_reduces_participant_balance_on_chain() {
        let mut n = Network::new();
        let owner = n.register_participant("Owner", 100, None);
        let p = n.create_project(owner, "Proj", None).unwrap();
        n.fund_project_from_owner(owner, p, 60).unwrap();
        assert_eq!(n.balance_of(owner), 40);
        assert!(n.blockchain().verify_integrity());
    }

    #[test]
    fn task_reward_increases_worker_balance_on_chain() {
        let mut n = Network::with_config(NetworkConfig {
            consensus_quorum: 1,
            ..NetworkConfig::default()
        });
        let owner = n.register_participant("Owner", 50, None);
        let worker = n.register_participant("Worker", 0, None);
        let p = n.create_project(owner, "P", None).unwrap();
        n.fund_project_from_owner(owner, p, 20).unwrap();
        let task_id = n.submit_task(owner, p, 10, "work").unwrap();
        n.request_task(worker).unwrap();
        n.submit_result(worker, task_id, "ok".to_string(), 0, None)
            .unwrap();
        assert_eq!(n.balance_of(worker), 10);
        assert!(n.blockchain().verify_integrity());
    }

    #[test]
    fn slash_reduces_worker_balance_on_chain() {
        let mut n = Network::with_config(NetworkConfig {
            lease_timeout_ticks: 1,
            lease_slash: 3,
            ..NetworkConfig::default()
        });
        let owner = n.register_participant("Owner", 30, None);
        let worker = n.register_participant("Worker", 10, None);
        let p = n.create_project(owner, "P", None).unwrap();
        n.fund_project_from_owner(owner, p, 15).unwrap();
        n.submit_task(owner, p, 10, "job").unwrap();
        n.request_task(worker).unwrap();
        n.tick(1);
        assert_eq!(n.balance_of(worker), 7);
        assert!(n.blockchain().verify_integrity());
    }

    #[test]
    fn blockchain_integrity_holds_after_full_lifecycle() {
        let mut n = Network::with_config(NetworkConfig {
            lease_timeout_ticks: 10,
            consensus_quorum: 2,
            max_reports_per_task: 5,
            lease_slash: 1,
            reputation_reward: 1,
            reputation_penalty: 1,
        });
        let owner = n.register_participant("Owner", 100, None);
        let w1 = n.register_participant("W1", 0, None);
        let w2 = n.register_participant("W2", 0, None);
        let p = n.create_project(owner, "Full", None).unwrap();
        n.fund_project_from_owner(owner, p, 50).unwrap();
        let task_id = n.submit_task(owner, p, 20, "calc").unwrap();
        n.request_task(w1).unwrap();
        n.submit_result(w1, task_id, "res".to_string(), 0, None)
            .unwrap();
        n.request_task(w2).unwrap();
        n.submit_result(w2, task_id, "res".to_string(), 0, None)
            .unwrap();
        assert!(n.blockchain().verify_integrity());
        assert_eq!(n.balance_of(w1) + n.balance_of(w2), 20);
    }
}
