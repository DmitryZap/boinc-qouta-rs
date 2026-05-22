use crate::sandbox::{self, SandboxError};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

const DEFAULT_RUN_TIMEOUT_SECS: u64 = 10;
const MIN_SUGGESTED_TIMEOUT_SECS: u64 = 1;

/// Interpreter operations treated as one CPU-second of work. The isolated
/// interpreter caps loops at ~1M operations, so 1M ops ≈ one second of budget.
const OPS_PER_CPU_SEC: f64 = 1_000_000.0;

#[derive(Debug, Clone, Default)]
pub struct ResourceMetrics {
    pub cpu_seconds: f64,
    pub peak_memory_mb: u64,
    pub wall_clock_seconds: f64,
}

#[derive(Debug, Clone)]
pub struct CostConfig {
    /// Flat fee charged per task regardless of resource usage.
    pub base_fee: u64,
    /// Tokens per CPU-second consumed.
    pub cpu_rate: u64,
    /// Tokens per peak megabyte of RAM.
    pub memory_rate: u64,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self {
            base_fee: 1,
            cpu_rate: 10,
            memory_rate: 1,
        }
    }
}

#[derive(Debug)]
pub enum EstimatorError {
    /// Interpreter op-cap or wall-clock timeout tripped; partial run attached.
    Timeout(MeasuredRun),
    /// Interpreter could not run the code (e.g. smolagents unavailable).
    Sandbox(String),
}

impl std::fmt::Display for EstimatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout(_) => write!(f, "script timed out"),
            Self::Sandbox(msg) => write!(f, "sandbox: {msg}"),
        }
    }
}

#[derive(Debug, Default)]
pub struct Estimator {
    pub config: CostConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionEstimate {
    pub estimated_cost: u64,
    pub suggested_timeout_secs: u64,
    pub unbounded_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MeasuredRun {
    pub digest: String,
    pub stdout: String,
    pub metrics: ResourceMetrics,
}

impl Estimator {
    pub fn new(config: CostConfig) -> Self {
        Self { config }
    }

    /// Static pre-estimate from Python source without executing it.
    /// Counts lines and applies keyword multipliers (loops, ML libs, file I/O).
    pub fn pre_estimate(&self, source: &str) -> u64 {
        let lines = source.lines().count() as u64;
        let base = self.config.base_fee.saturating_add(lines);

        let mut multiplier = 1.0_f64;
        if source.contains("for ") || source.contains("while ") {
            multiplier *= 1.5;
        }
        if source.contains("import torch")
            || source.contains("import tensorflow")
            || source.contains("import tf")
        {
            multiplier *= 3.0;
        }
        if source.contains("model.fit")
            || source.contains(".train(")
            || source.contains("matmul")
            || source.contains("conv2d")
        {
            multiplier *= 2.0;
        }
        if source.contains("open(") {
            multiplier *= 1.2;
        }

        ((base as f64) * multiplier) as u64
    }

    pub fn estimate_python(&self, source: &str) -> ExecutionEstimate {
        let estimated_cost = self.pre_estimate(source);
        let suggested_timeout_secs = self.suggest_timeout(estimated_cost);
        let unbounded_reason = detect_unbounded_python(source);

        ExecutionEstimate {
            estimated_cost,
            suggested_timeout_secs,
            unbounded_reason,
        }
    }

    /// Convert measured ResourceMetrics to token cost.
    ///
    /// Formula: base_fee + cpu_seconds * cpu_rate + peak_memory_mb * memory_rate
    pub fn calculate_cost(&self, metrics: &ResourceMetrics) -> u64 {
        let cpu_cost = (metrics.cpu_seconds * self.config.cpu_rate as f64) as u64;
        let mem_cost = metrics
            .peak_memory_mb
            .saturating_mul(self.config.memory_rate);
        self.config
            .base_fee
            .saturating_add(cpu_cost)
            .saturating_add(mem_cost)
    }

    /// Execute Python source in the isolated interpreter, measure work done,
    /// return a SHA-256 digest of stdout and the observed ResourceMetrics.
    pub fn run_and_measure(
        &self,
        source: &str,
    ) -> Result<(String, ResourceMetrics), EstimatorError> {
        self.run_and_measure_with_timeout(source, Duration::from_secs(DEFAULT_RUN_TIMEOUT_SECS))
    }

    pub fn run_and_measure_with_timeout(
        &self,
        source: &str,
        timeout: Duration,
    ) -> Result<(String, ResourceMetrics), EstimatorError> {
        let run =
            self.measure_via_sandbox(source, timeout, &sandbox::default_allowed_packages())?;
        Ok((run.digest, run.metrics))
    }

    /// Run `source` in the isolated interpreter and derive dynamic
    /// `ResourceMetrics` from the actual execution: the interpreter's operation
    /// counter (deterministic, execution-path dependent) maps to CPU-seconds,
    /// and wall-clock is measured around the call. `allowed_packages` is the
    /// user-managed import allowlist handed to the interpreter.
    pub fn measure_via_sandbox(
        &self,
        source: &str,
        timeout: Duration,
        allowed_packages: &[String],
    ) -> Result<MeasuredRun, EstimatorError> {
        let start = Instant::now();
        match sandbox::run_python_sandboxed(source, timeout.as_secs().max(1), allowed_packages) {
            Ok(result) => {
                let wall_clock_seconds = start.elapsed().as_secs_f64();
                Ok(MeasuredRun {
                    digest: digest_stdout(result.stdout.as_bytes()),
                    stdout: result.stdout,
                    metrics: ResourceMetrics {
                        cpu_seconds: result.operations as f64 / OPS_PER_CPU_SEC,
                        peak_memory_mb: 0,
                        wall_clock_seconds,
                    },
                })
            }
            Err(SandboxError::Timeout) => {
                let wall_clock_seconds = start.elapsed().as_secs_f64();
                Err(EstimatorError::Timeout(MeasuredRun {
                    digest: digest_stdout(b""),
                    stdout: String::new(),
                    metrics: ResourceMetrics {
                        cpu_seconds: wall_clock_seconds,
                        peak_memory_mb: 0,
                        wall_clock_seconds,
                    },
                }))
            }
            Err(e) => Err(EstimatorError::Sandbox(e.to_string())),
        }
    }

    pub fn timeout_for_budget(&self, reward: u64) -> u64 {
        let cpu_rate = self.config.cpu_rate.max(1);
        reward
            .saturating_sub(self.config.base_fee)
            .div_ceil(cpu_rate)
            .max(MIN_SUGGESTED_TIMEOUT_SECS)
    }

    fn suggest_timeout(&self, estimated_cost: u64) -> u64 {
        let variable_cost = estimated_cost.saturating_sub(self.config.base_fee);
        let cpu_rate = self.config.cpu_rate.max(1);
        variable_cost
            .div_ceil(cpu_rate)
            .max(MIN_SUGGESTED_TIMEOUT_SECS)
    }
}

fn detect_unbounded_python(source: &str) -> Option<String> {
    let compact = source
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();

    if compact.contains("whiletrue:") || compact.contains("while1:") {
        return Some("unbounded while loop".to_string());
    }
    if compact.contains("itertools.count(") {
        return Some("unbounded itertools.count loop".to_string());
    }
    if compact.contains("repeat(None)") {
        return Some("unbounded repeat loop".to_string());
    }

    None
}

fn digest_stdout(stdout: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(stdout);
    format!("py-{:x}", hasher.finalize())
}


// TODO: Выставлять ограничение по времени выполнения таски
// Slurm 

////
/// Подумать над ключевой особенностью проекта.
/// 


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculate_cost_formula() {
        let est = Estimator::new(CostConfig {
            base_fee: 5,
            cpu_rate: 10,
            memory_rate: 2,
        });
        let metrics = ResourceMetrics {
            cpu_seconds: 2.0,
            peak_memory_mb: 100,
            wall_clock_seconds: 3.0,
        };
        // 5 + (2.0 * 10) + (100 * 2) = 225
        assert_eq!(est.calculate_cost(&metrics), 225);
    }

    #[test]
    fn estimate_flags_obvious_infinite_loop() {
        let est = Estimator::default();
        let estimate = est.estimate_python("while True:\n    pass\n");
        assert_eq!(
            estimate.unbounded_reason.as_deref(),
            Some("unbounded while loop")
        );
    }

    #[test]
    fn run_and_measure_times_out() {
        let est = Estimator::default();
        let err = est
            .run_and_measure_with_timeout("while True:\n    pass\n", Duration::from_secs(30))
            .unwrap_err();
        assert!(matches!(err, EstimatorError::Timeout(_)));
    }

    #[test]
    fn run_and_measure_simple_script() {
        let est = Estimator::default();
        let script = "print(sum(range(100)))\n";
        let (digest, metrics) = est.run_and_measure(script).expect("run");
        assert!(digest.starts_with("py-"), "digest={digest}");
        assert!(metrics.wall_clock_seconds >= 0.0);
        assert!(metrics.cpu_seconds >= 0.0);
    }

    #[test]
    fn run_and_measure_deterministic_digest() {
        let est = Estimator::default();
        let script = "print(42)\n";
        let (d1, _) = est.run_and_measure(script).expect("run1");
        let (d2, _) = est.run_and_measure(script).expect("run2");
        assert_eq!(d1, d2, "same script must produce same digest");
    }
}
