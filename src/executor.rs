use crate::estimator::{Estimator, EstimatorError, ResourceMetrics};
use crate::identity::Identity;
use crate::model::ParticipantId;
use crate::network::{Network, NetworkError, Result as NetworkResult};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64, Engine as _};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

/// Default wall-clock cap handed to the isolated interpreter when no budget
/// override applies.
const DEFAULT_TIMEOUT_SECS: u64 = 10;

/// Computed task result: digest used for consensus, plus the raw stdout
/// captured during execution (used for encrypted-result delivery) and the
/// dynamic resource metrics measured by the interpreter.
#[derive(Debug, Clone)]
pub struct ComputedResult {
    pub digest: String,
    pub stdout: String,
    pub budget_exhausted: bool,
    pub metrics: ResourceMetrics,
}

enum Payload<'a> {
    Python { code_b64: &'a str, gpu: bool },
    Unknown,
}

struct ExecutionBudget {
    reward: u64,
}

#[derive(Clone)]
pub struct Executor {
    worker_id: ParticipantId,
    label: String,
    /// Deterministic "quality" in percents [0..100].
    reliability_percent: u8,
    compute_ticks: u64,
    heartbeat_interval_ticks: u64,
    estimator: Option<Arc<Estimator>>,
    /// User-managed import allowlist passed to the isolated interpreter.
    allowed_packages: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorEvent {
    Idle,
    ProcessedTask {
        worker_id: ParticipantId,
        worker_label: String,
        task_id: u64,
        project_id: u64,
        consensus_reached: bool,
        lease_lost: bool,
        reward: u64,
        result_digest: String,
        /// Measured token cost from the Estimator (0 if no estimator attached).
        actual_cost: u64,
    },
}

impl Executor {
    pub fn new(
        worker_id: ParticipantId,
        label: impl Into<String>,
        reliability_percent: u8,
        compute_ticks: u64,
        heartbeat_interval_ticks: u64,
        allowed_packages: Vec<String>,
    ) -> Self {
        Self {
            worker_id,
            label: label.into(),
            reliability_percent: reliability_percent.min(100),
            compute_ticks: compute_ticks.max(1),
            heartbeat_interval_ticks,
            estimator: Some(Arc::new(Estimator::default())),
            allowed_packages,
        }
    }

    pub fn process_next_task(&self, network: &mut Network) -> NetworkResult<ExecutorEvent> {
        let task = match network.request_task(self.worker_id) {
            Ok(task) => task,
            Err(NetworkError::NoPendingTasks) => return Ok(ExecutorEvent::Idle),
            Err(err) => return Err(err),
        };

        for elapsed in 0..self.compute_ticks {
            network.tick(1);

            let heartbeat_due = self.heartbeat_interval_ticks > 0
                && (elapsed + 1) % self.heartbeat_interval_ticks == 0
                && (elapsed + 1) < self.compute_ticks;
            if heartbeat_due {
                match network.heartbeat(self.worker_id, task.id) {
                    Ok(()) => {}
                    Err(NetworkError::TaskNotAssignedToWorker { .. }) => {
                        return Ok(ExecutorEvent::ProcessedTask {
                            worker_id: self.worker_id,
                            worker_label: self.label.clone(),
                            task_id: task.id,
                            project_id: task.project_id,
                            consensus_reached: false,
                            lease_lost: true,
                            reward: 0,
                            result_digest: "lease-lost".to_string(),
                            actual_cost: 0,
                        });
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        let (computed, actual_cost) =
            self.execute_task_payload(task.id, &task.payload, task.reward);
        let result_digest = computed.digest.clone();
        let stdout = computed.stdout;

        // If the project has an owner encryption pubkey, encrypt stdout for them.
        let owner_pk = network
            .project(task.project_id)
            .and_then(|p| p.owner_encryption_pubkey);
        let encrypted_result = owner_pk.map(|pk| Identity::encrypt_for(&pk, stdout.as_bytes()));

        let consensus_reached = match network.submit_result(
            self.worker_id,
            task.id,
            result_digest.clone(),
            actual_cost,
            encrypted_result,
        ) {
            Ok(done) => done,
            Err(NetworkError::TaskNotAssignedToWorker { .. }) => {
                return Ok(ExecutorEvent::ProcessedTask {
                    worker_id: self.worker_id,
                    worker_label: self.label.clone(),
                    task_id: task.id,
                    project_id: task.project_id,
                    consensus_reached: false,
                    lease_lost: true,
                    reward: 0,
                    result_digest,
                    actual_cost,
                });
            }
            Err(err) => return Err(err),
        };

        Ok(ExecutorEvent::ProcessedTask {
            worker_id: self.worker_id,
            worker_label: self.label.clone(),
            task_id: task.id,
            project_id: task.project_id,
            consensus_reached,
            lease_lost: false,
            reward: task.reward,
            result_digest,
            actual_cost,
        })
    }


    pub fn execute_task_payload(
        &self,
        task_id: u64,
        payload: &str,
        reward: u64,
    ) -> (ComputedResult, u64) {
        let result =
            self.compute_result_with_budget(task_id, payload, Some(ExecutionBudget { reward }));
        let actual_cost = self.actual_cost(&result.metrics, reward, result.budget_exhausted);
        (result, actual_cost)
    }

    fn compute_result_with_budget(
        &self,
        task_id: u64,
        payload: &str,
        budget: Option<ExecutionBudget>,
    ) -> ComputedResult {
        match Payload::parse(payload) {
            Payload::Python { code_b64, gpu } => {
                self.run_python_payload(task_id, code_b64, gpu, budget)
            }
            Payload::Unknown => digest_only(format!("error:unknown-payload-{task_id:x}")),
        }
    }

    /// Run a Python payload in the isolated interpreter.
    ///
    /// `python-gpu:` payloads run through the same interpreter (no real GPU).
    /// The interpreter enforces the import allowlist, blocks submodule access,
    /// and caps operations so infinite loops surface as a timeout result.
    fn run_python_payload(
        &self,
        task_id: u64,
        b64_code: &str,
        gpu: bool,
        budget: Option<ExecutionBudget>,
    ) -> ComputedResult {
        let prefix = if gpu { "python-gpu" } else { "python" };
        let code = match Self::decode_python_source(task_id, b64_code, "python") {
            Ok(code) => code,
            Err(result) => return result,
        };

        let budgeted = budget.is_some();
        let timeout_secs = self.python_timeout(&code, budget);

        if self.generates_corrupted_result(task_id, b64_code) {
            return digest_only(format!("{prefix}-corrupt-{task_id:x}"));
        }

        let Some(estimator) = &self.estimator else {
            return self.run_python_via_sandbox(task_id, prefix, &code, timeout_secs, budgeted);
        };

        match estimator.measure_via_sandbox(
            &code,
            Duration::from_secs(timeout_secs),
            &self.allowed_packages,
        ) {
            Ok(run) => ComputedResult {
                digest: Self::digest_from_stdout(&run.stdout),
                stdout: run.stdout,
                budget_exhausted: false,
                metrics: run.metrics,
            },
            Err(EstimatorError::Timeout(partial)) => {
                python_timeout_result(prefix, task_id, budgeted, partial.stdout, partial.metrics)
            }
            Err(e) => {
                eprintln!("[executor] Interpreter failed: {e}");
                digest_only(format!("{prefix}-sandbox-error-{task_id:x}"))
            }
        }
    }

    /// Wall-clock cap for the interpreter: derived from the reward budget when
    /// running a task, otherwise from the static estimate.
    fn python_timeout(&self, code: &str, budget: Option<ExecutionBudget>) -> u64 {
        let Some(estimator) = &self.estimator else {
            return DEFAULT_TIMEOUT_SECS;
        };

        if let Some(budget) = budget {
            return estimator.timeout_for_budget(budget.reward);
        }

        estimator
            .estimate_python(code)
            .suggested_timeout_secs
            .clamp(1, DEFAULT_TIMEOUT_SECS)
    }

    fn decode_python_source(
        task_id: u64,
        b64_code: &str,
        prefix: &str,
    ) -> Result<String, ComputedResult> {
        let bytes = BASE64
            .decode(b64_code)
            .map_err(|_| digest_only(format!("{prefix}-error:bad-base64-{task_id:x}")))?;
        String::from_utf8(bytes)
            .map_err(|_| digest_only(format!("{prefix}-error:invalid-utf8-{task_id:x}")))
    }

    fn digest_from_stdout(stdout: &str) -> String {
        let truncated = &stdout[..stdout.len().min(1024)];
        let mut hasher = Sha256::new();
        hasher.update(truncated.as_bytes());
        let hash = hasher.finalize();
        let digest = u64::from_be_bytes(hash[..8].try_into().unwrap());
        format!("python-ok-{digest:016x}")
    }

    /// Fallback when no estimator is attached: run the interpreter directly
    /// without dynamic metrics.
    fn run_python_via_sandbox(
        &self,
        task_id: u64,
        prefix: &str,
        code: &str,
        timeout_secs: u64,
        budgeted: bool,
    ) -> ComputedResult {
        match crate::sandbox::run_python_sandboxed(code, timeout_secs, &self.allowed_packages) {
            Ok(result) => ComputedResult {
                digest: Self::digest_from_stdout(&result.stdout),
                stdout: result.stdout,
                budget_exhausted: false,
                metrics: ResourceMetrics::default(),
            },
            Err(crate::sandbox::SandboxError::Timeout) => {
                python_timeout_result(prefix, task_id, budgeted, String::new(), ResourceMetrics::default())
            }
            Err(e) => {
                eprintln!("[executor] Sandbox failed: {e}");
                digest_only(format!("{prefix}-sandbox-error-{task_id:x}"))
            }
        }
    }

    fn actual_cost(&self, metrics: &ResourceMetrics, reward: u64, budget_exhausted: bool) -> u64 {
        if budget_exhausted {
            return reward;
        }

        self.estimator
            .as_ref()
            .map(|est| est.calculate_cost(metrics).min(reward))
            .unwrap_or(0)
    }

    fn generates_corrupted_result(&self, task_id: u64, payload: &str) -> bool {
        let payload_factor = (payload.len() as u64).wrapping_mul(11);
        let worker_factor = self.worker_id.wrapping_mul(13);
        let quality_index = (task_id.wrapping_mul(37) + payload_factor + worker_factor) % 100;
        quality_index >= u64::from(self.reliability_percent)
    }
}

impl<'a> Payload<'a> {
    fn parse(payload: &'a str) -> Self {
        if let Some(code_b64) = payload.strip_prefix("python-gpu:") {
            return Self::Python {
                code_b64,
                gpu: true,
            };
        }

        if let Some(code_b64) = payload.strip_prefix("python:") {
            return Self::Python {
                code_b64,
                gpu: false,
            };
        }

        Self::Unknown
    }
}

fn digest_only(digest: String) -> ComputedResult {
    ComputedResult {
        stdout: digest.clone(),
        digest,
        budget_exhausted: false,
        metrics: ResourceMetrics::default(),
    }
}

fn python_timeout_result(
    prefix: &str,
    task_id: u64,
    budgeted: bool,
    partial_stdout: String,
    metrics: ResourceMetrics,
) -> ComputedResult {
    let reason = if budgeted {
        "budget-exhausted"
    } else {
        "timeout"
    };
    let digest = format!("{prefix}-{reason}-{task_id:x}");
    let stdout = if partial_stdout.is_empty() {
        digest.clone()
    } else {
        partial_stdout
    };
    ComputedResult {
        stdout,
        digest,
        budget_exhausted: budgeted,
        metrics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_gets_reward_when_result_accepted() {
        let mut network = Network::new();
        let owner = network.register_participant("Owner", 50, None);
        let worker = network.register_participant("Worker", 0, None);
        let validator = network.register_participant("Validator", 0, None);
        let project = network.create_project(owner, "Physics", None).unwrap();

        network.fund_project_from_owner(owner, project, 20).unwrap();
        let payload = format!("python:{}", BASE64.encode("print('result-1')"));
        network
            .submit_task(owner, project, 10, &payload)
            .unwrap();

        let executor = Executor::new(worker, "w1", 100, 1, 1, crate::sandbox::default_allowed_packages());
        let validator_executor = Executor::new(validator, "w2", 100, 1, 1, crate::sandbox::default_allowed_packages());
        let event = executor.process_next_task(&mut network).unwrap();
        validator_executor.process_next_task(&mut network).unwrap();

        assert!(matches!(
            event,
            ExecutorEvent::ProcessedTask {
                consensus_reached: false,
                reward: 10,
                ..
            }
        ));
        assert_eq!(network.balance_of(worker), 5);
    }

    #[test]
    fn executor_can_lose_lease_without_heartbeat() {
        let mut network = Network::new();
        let owner = network.register_participant("Owner", 50, None);
        let worker = network.register_participant("Worker", 0, None);
        let project = network.create_project(owner, "Bio", None).unwrap();

        network.fund_project_from_owner(owner, project, 20).unwrap();
        let payload = format!("python:{}", BASE64.encode("print('result-2')"));
        network
            .submit_task(owner, project, 10, &payload)
            .unwrap();

        let executor = Executor::new(worker, "w2", 100, 5, 0, crate::sandbox::default_allowed_packages());
        let event = executor.process_next_task(&mut network).unwrap();

        assert!(matches!(
            event,
            ExecutorEvent::ProcessedTask {
                lease_lost: true,
                ..
            }
        ));
        assert_eq!(network.balance_of(worker), 0);
        assert_eq!(network.project(project).unwrap().quota_locked, 10);
        assert_eq!(network.pending_count(), 1);
    }

    #[test]
    fn executor_becomes_idle_when_no_tasks_left() {
        let mut network = Network::new();
        let worker = network.register_participant("Worker", 0, None);
        let executor = Executor::new(worker, "w3", 100, 1, 1, crate::sandbox::default_allowed_packages());

        let event = executor.process_next_task(&mut network).unwrap();
        assert_eq!(event, ExecutorEvent::Idle);
    }

    #[test]
    fn reward_budget_stops_obvious_infinite_python() {
        let executor = Executor::new(1, "w4", 100, 1, 1, crate::sandbox::default_allowed_packages());
        let payload = format!("python:{}", BASE64.encode("while True:\n    pass\n"));

        let (result, actual_cost) = executor.execute_task_payload(42, &payload, 1);

        assert_eq!(result.digest, "python-budget-exhausted-2a");
        assert!(result.budget_exhausted);
        assert_eq!(actual_cost, 1);
    }
}
