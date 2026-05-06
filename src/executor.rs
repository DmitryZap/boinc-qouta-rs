use crate::compute::{ComputeError, ComputeModule, ResourceConfig};
use crate::estimator::{Estimator, ResourceMetrics};
use crate::identity::Identity;
use crate::model::ParticipantId;
use crate::network::{Network, NetworkError, Result as NetworkResult};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64, Engine as _};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};

const GPU_IMAGE: &str = "pytorch/pytorch:2.3.1-cuda12.1-cudnn8-runtime";

/// Computed task result: digest used for consensus, plus the raw stdout
/// captured during execution (used for encrypted-result delivery).
#[derive(Debug, Clone)]
pub struct ComputedResult {
    pub digest: String,
    pub stdout: String,
    pub budget_exhausted: bool,
}

enum Payload<'a> {
    Python { code_b64: &'a str, gpu: bool },
    Inference(&'a str),
    Deterministic(&'a str),
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
    resource_config: ResourceConfig,
    compute_module: Option<Arc<ComputeModule>>,
    estimator: Option<Arc<Estimator>>,
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
    ) -> Self {
        let resource_config = ResourceConfig::default();
        let compute_module = ComputeModule::new(resource_config.clone())
            .map(Arc::new)
            .map_err(|e| eprintln!("[executor] Docker unavailable: {e}. Using fake compute."))
            .ok();
        Self {
            worker_id,
            label: label.into(),
            reliability_percent: reliability_percent.min(100),
            compute_ticks: compute_ticks.max(1),
            heartbeat_interval_ticks,
            resource_config,
            compute_module,
            estimator: Some(Arc::new(Estimator::default())),
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
        let started_at = Instant::now();
        let result =
            self.compute_result_with_budget(task_id, payload, Some(ExecutionBudget { reward }));
        let actual_cost = self.actual_cost(started_at.elapsed(), reward, result.budget_exhausted);
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
            Payload::Inference(features) => self.run_inference_payload(task_id, features),
            Payload::Deterministic(text) => self.run_deterministic_payload(task_id, text),
        }
    }


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
        let resource_config = self.estimate_python_execution(&code, budget);

        if !gpu && self.generates_corrupted_result(task_id, b64_code) {
            return digest_only(format!("{prefix}-corrupt-{task_id:x}"));
        }

        if gpu {
            return self.run_python_gpu_source(task_id, &code, &resource_config, budgeted);
        }

        let Some(module) = &self.compute_module else {
            return self.run_python_without_docker(
                task_id,
                prefix,
                &code,
                &resource_config,
                budgeted,
            );
        };

        let fut = module.execute_python(&code, self.worker_id, task_id, &resource_config);
        let result = block_on_compute(fut);

        match result {
            Ok(output) => ComputedResult {
                digest: Self::digest_from_stdout(&output.stdout),
                stdout: output.stdout,
                budget_exhausted: false,
            },
            Err(ComputeError::Timeout(partial)) => {
                python_timeout_result(prefix, task_id, budgeted, partial.stdout)
            }
            Err(ComputeError::NonZeroExit(r)) => {
                let d = format!("python-nonzero-{}-{task_id:x}", r.exit_code);
                ComputedResult {
                    stdout: r.stdout.clone(),
                    digest: d,
                    budget_exhausted: false,
                }
            }
            Err(e) => {
                eprintln!("[executor] Docker error: {e}. Falling back to sandbox.");
                self.run_python_without_docker(task_id, prefix, &code, &resource_config, budgeted)
            }
        }
    }

    fn run_python_gpu_source(
        &self,
        task_id: u64,
        code: &str,
        resource_config: &ResourceConfig,
        budgeted: bool,
    ) -> ComputedResult {
        let Some(module) = &self.compute_module else {
            return digest_only(format!("python-gpu-no-docker-{task_id:x}"));
        };

        let fut =
            module.execute_python_gpu(code, self.worker_id, task_id, resource_config, GPU_IMAGE);
        let result = block_on_compute(fut);

        match result {
            Ok(output) => ComputedResult {
                digest: Self::digest_from_stdout(&output.stdout),
                stdout: output.stdout,
                budget_exhausted: false,
            },
            Err(crate::compute::ComputeError::Timeout(partial)) => {
                python_timeout_result("python-gpu", task_id, budgeted, partial.stdout)
            }
            Err(crate::compute::ComputeError::NonZeroExit(r)) => {
                let d = format!("python-gpu-nonzero-{}-{task_id:x}", r.exit_code);
                let stdout = if r.stdout.trim().is_empty() {
                    r.stderr.clone()
                } else if r.stderr.trim().is_empty() {
                    r.stdout.clone()
                } else {
                    format!("{}\n{}", r.stdout.trim_end(), r.stderr.trim_end())
                };
                ComputedResult {
                    stdout,
                    digest: d,
                    budget_exhausted: false,
                }
            }
            Err(e) => {
                eprintln!("[executor] GPU docker error: {e}");
                digest_only(format!("python-gpu-error-{task_id:x}"))
            }
        }
    }

    fn estimate_python_execution(
        &self,
        code: &str,
        budget: Option<ExecutionBudget>,
    ) -> ResourceConfig {
        let mut config = self.resource_config.clone();
        let Some(estimator) = &self.estimator else {
            return config;
        };

        if let Some(budget) = budget {
            config.timeout_secs = estimator.timeout_for_budget(budget.reward);
            return config;
        }

        let estimate = estimator.estimate_python(code);
        config.timeout_secs = estimate
            .suggested_timeout_secs
            .clamp(1, self.resource_config.timeout_secs.max(1));
        config
    }

    fn run_python_without_docker(
        &self,
        task_id: u64,
        prefix: &str,
        code: &str,
        resource_config: &ResourceConfig,
        budgeted: bool,
    ) -> ComputedResult {
        let Some(estimator) = &self.estimator else {
            return Self::run_python_via_sandbox(
                task_id,
                code,
                resource_config.timeout_secs,
                budgeted,
            );
        };

        match estimator.run_and_measure_output_with_timeout(
            code,
            Duration::from_secs(resource_config.timeout_secs),
        ) {
            Ok(run) => ComputedResult {
                digest: Self::digest_from_stdout(&run.stdout),
                stdout: run.stdout,
                budget_exhausted: false,
            },
            Err(crate::estimator::EstimatorError::Timeout(partial)) => {
                python_timeout_result(prefix, task_id, budgeted, partial.stdout)
            }
            Err(e) => {
                eprintln!("[executor] Python subprocess failed: {e}. Falling back to sandbox.");
                Self::run_python_via_sandbox(task_id, code, resource_config.timeout_secs, budgeted)
            }
        }
    }

    fn run_inference_payload(&self, task_id: u64, payload: &str) -> ComputedResult {
        let Some((predicted_class, confidence)) = Self::inference_prediction(payload) else {
            return self.run_deterministic_payload(task_id, payload);
        };

        let class = if self.generates_corrupted_result(task_id, payload) {
            1usize.saturating_sub(predicted_class.min(1))
        } else {
            predicted_class
        };
        digest_only(format!("infer:class={class};confidence={confidence:.4}"))
    }

    fn run_deterministic_payload(&self, task_id: u64, payload: &str) -> ComputedResult {
        let payload_score: u64 = payload.bytes().map(u64::from).sum();
        let baseline = payload_score ^ (task_id * 31);
        let digest = if self.generates_corrupted_result(task_id, payload) {
            format!("digest-bad-{:x}", baseline ^ 0x9e3779b97f4a7c15)
        } else {
            format!("digest-ok-{baseline:x}")
        };
        digest_only(digest)
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

    fn run_python_via_sandbox(
        task_id: u64,
        code: &str,
        timeout_secs: u64,
        budgeted: bool,
    ) -> ComputedResult {
        match crate::sandbox::run_python_sandboxed(code, timeout_secs) {
            Ok(result) => ComputedResult {
                digest: Self::digest_from_stdout(&result.stdout),
                stdout: result.stdout,
                budget_exhausted: false,
            },
            Err(crate::sandbox::SandboxError::Timeout) => {
                python_timeout_result("python", task_id, budgeted, String::new())
            }
            Err(e) => {
                eprintln!("[executor] Sandbox failed: {e}");
                let d = format!("python-sandbox-error-{task_id:x}");
                ComputedResult {
                    stdout: d.clone(),
                    digest: d,
                    budget_exhausted: false,
                }
            }
        }
    }

    fn actual_cost(&self, elapsed: Duration, reward: u64, budget_exhausted: bool) -> u64 {
        if budget_exhausted {
            return reward;
        }

        self.estimator
            .as_ref()
            .map(|est| {
                est.calculate_cost(&ResourceMetrics {
                    cpu_seconds: elapsed.as_secs_f64(),
                    peak_memory_mb: 0,
                    wall_clock_seconds: elapsed.as_secs_f64(),
                })
                .min(reward)
            })
            .unwrap_or(0)
    }

    fn inference_prediction(payload: &str) -> Option<(usize, f32)> {
        let features = payload.strip_prefix("infer:")?;
        let mut parsed = [0.0_f32; 3];
        let mut count = 0usize;
        for chunk in features.split(',') {
            if count >= parsed.len() {
                return None;
            }
            parsed[count] = chunk.trim().parse::<f32>().ok()?;
            count += 1;
        }
        if count != parsed.len() {
            return None;
        }

        let h1 = (0.8 * parsed[0] - 0.4 * parsed[1] + 0.3 * parsed[2] + 0.1).max(0.0);
        let h2 = (-0.2 * parsed[0] + 0.9 * parsed[1] + 0.5 * parsed[2] - 0.3).max(0.0);
        let logits = [1.2 * h1 - 0.7 * h2 + 0.2, -0.6 * h1 + 1.1 * h2 - 0.1];
        let (predicted_class, confidence) = if logits[0] >= logits[1] {
            (0, sigmoid(logits[0] - logits[1]))
        } else {
            (1, sigmoid(logits[1] - logits[0]))
        };

        Some((predicted_class, confidence))
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

        if payload.starts_with("infer:") {
            return Self::Inference(payload);
        }

        Self::Deterministic(payload)
    }
}

fn digest_only(digest: String) -> ComputedResult {
    ComputedResult {
        stdout: digest.clone(),
        digest,
        budget_exhausted: false,
    }
}

fn python_timeout_result(
    prefix: &str,
    task_id: u64,
    budgeted: bool,
    partial_stdout: String,
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
    }
}

fn block_on_compute<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(future),
    }
}

fn sigmoid(x: f32) -> f32 {
    1.0 / (1.0 + (-x).exp())
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
        network
            .submit_task(owner, project, 10, "simulate-1")
            .unwrap();

        let executor = Executor::new(worker, "w1", 100, 1, 1);
        let validator_executor = Executor::new(validator, "w2", 100, 1, 1);
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
        network
            .submit_task(owner, project, 10, "simulate-2")
            .unwrap();

        let executor = Executor::new(worker, "w2", 100, 5, 0);
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
        let executor = Executor::new(worker, "w3", 100, 1, 1);

        let event = executor.process_next_task(&mut network).unwrap();
        assert_eq!(event, ExecutorEvent::Idle);
    }

    #[test]
    fn reward_budget_stops_obvious_infinite_python() {
        let executor = Executor::new(1, "w4", 100, 1, 1);
        let payload = format!("python:{}", BASE64.encode("while True:\n    pass\n"));

        let (result, actual_cost) = executor.execute_task_payload(42, &payload, 1);

        assert_eq!(result.digest, "python-budget-exhausted-2a");
        assert!(result.budget_exhausted);
        assert_eq!(actual_cost, 1);
    }
}
