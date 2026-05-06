use sha2::{Digest, Sha256};
use std::io::Read;
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const DEFAULT_RUN_TIMEOUT_SECS: u64 = 10;
const MIN_SUGGESTED_TIMEOUT_SECS: u64 = 1;

#[derive(Debug, Clone)]
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
    Io(std::io::Error),
    PythonNotFound,
    Timeout(MeasuredRun),
    ScriptFailed {
        exit_code: Option<i32>,
        stderr: String,
    },
}

impl std::fmt::Display for EstimatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::PythonNotFound => write!(f, "python3 not found"),
            Self::Timeout(_) => write!(f, "script timed out"),
            Self::ScriptFailed { exit_code, stderr } => {
                write!(f, "script failed (exit={exit_code:?}): {stderr}")
            }
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

    /// Execute Python source in a subprocess, measure resource usage, return
    /// a SHA-256 digest of stdout and the observed ResourceMetrics.
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
        let run = self.run_and_measure_output_with_timeout(source, timeout)?;
        Ok((run.digest, run.metrics))
    }

    pub fn run_and_measure_output_with_timeout(
        &self,
        source: &str,
        timeout: Duration,
    ) -> Result<MeasuredRun, EstimatorError> {
        let mut tmp = NamedTempFile::new().map_err(EstimatorError::Io)?;
        tmp.write_all(source.as_bytes())
            .map_err(EstimatorError::Io)?;
        tmp.flush().map_err(EstimatorError::Io)?;

        let start = Instant::now();
        let mut child = Command::new("python3")
            .arg(tmp.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    EstimatorError::PythonNotFound
                } else {
                    EstimatorError::Io(e)
                }
            })?;

        let deadline = Instant::now() + timeout;
        loop {
            if child.try_wait().map_err(EstimatorError::Io)?.is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                let mut stdout = Vec::new();
                if let Some(mut pipe) = child.stdout.take() {
                    pipe.read_to_end(&mut stdout).map_err(EstimatorError::Io)?;
                }
                let wall_clock_seconds = start.elapsed().as_secs_f64();
                let (cpu_seconds, peak_memory_mb) = measure_rusage();
                let digest = digest_stdout(&stdout);
                return Err(EstimatorError::Timeout(MeasuredRun {
                    digest,
                    stdout: String::from_utf8_lossy(&stdout).into_owned(),
                    metrics: ResourceMetrics {
                        cpu_seconds,
                        peak_memory_mb,
                        wall_clock_seconds,
                    },
                }));
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let mut stdout = Vec::new();
        if let Some(mut pipe) = child.stdout.take() {
            pipe.read_to_end(&mut stdout).map_err(EstimatorError::Io)?;
        }
        let mut stderr = Vec::new();
        if let Some(mut pipe) = child.stderr.take() {
            pipe.read_to_end(&mut stderr).map_err(EstimatorError::Io)?;
        }
        let status = child.wait().map_err(EstimatorError::Io)?;
        let wall_clock_seconds = start.elapsed().as_secs_f64();

        if !status.success() {
            return Err(EstimatorError::ScriptFailed {
                exit_code: status.code(),
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
            });
        }

        let (cpu_seconds, peak_memory_mb) = measure_rusage();

        let digest = digest_stdout(&stdout);

        Ok(MeasuredRun {
            digest,
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            metrics: ResourceMetrics {
                cpu_seconds,
                peak_memory_mb,
                wall_clock_seconds,
            },
        })
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

#[cfg(unix)]
fn measure_rusage() -> (f64, u64) {
    unsafe {
        let mut usage = std::mem::zeroed::<libc::rusage>();
        if libc::getrusage(libc::RUSAGE_CHILDREN, &mut usage) == 0 {
            let cpu_seconds = usage.ru_utime.tv_sec as f64
                + usage.ru_utime.tv_usec as f64 / 1_000_000.0
                + usage.ru_stime.tv_sec as f64
                + usage.ru_stime.tv_usec as f64 / 1_000_000.0;
            // macOS reports ru_maxrss in bytes; Linux in kilobytes.
            #[cfg(target_os = "macos")]
            let peak_memory_mb = (usage.ru_maxrss as u64) / (1024 * 1024);
            #[cfg(not(target_os = "macos"))]
            let peak_memory_mb = (usage.ru_maxrss as u64) / 1024;
            (cpu_seconds, peak_memory_mb)
        } else {
            (0.0, 0)
        }
    }
}

#[cfg(not(unix))]
fn measure_rusage() -> (f64, u64) {
    (0.0, 0)
}

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
            .run_and_measure_with_timeout("while True:\n    pass\n", Duration::from_millis(50))
            .unwrap_err();
        assert!(matches!(err, EstimatorError::Timeout(_)));
    }

    #[test]
    fn run_and_measure_simple_script() {
        let est = Estimator::default();
        let script = "print(sum(range(100)))\n";
        match est.run_and_measure(script) {
            Ok((digest, metrics)) => {
                assert!(digest.starts_with("py-"), "digest={digest}");
                assert!(metrics.wall_clock_seconds >= 0.0);
            }
            Err(EstimatorError::PythonNotFound) => {
                eprintln!("python3 not found — skipping");
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    #[test]
    fn run_and_measure_deterministic_digest() {
        let est = Estimator::default();
        let script = "print(42)\n";
        let r1 = est.run_and_measure(script);
        let r2 = est.run_and_measure(script);
        match (r1, r2) {
            (Ok((d1, _)), Ok((d2, _))) => {
                assert_eq!(d1, d2, "same script must produce same digest")
            }
            (Err(EstimatorError::PythonNotFound), _) | (_, Err(EstimatorError::PythonNotFound)) => {
                eprintln!("python3 not found — skipping");
            }
            (Err(e), _) | (_, Err(e)) => panic!("unexpected error: {e}"),
        }
    }
}
