use bollard::container::LogOutput;
use bollard::models::{ContainerCreateBody, HostConfig};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, LogsOptionsBuilder,
    RemoveContainerOptionsBuilder, WaitContainerOptionsBuilder,
};
use bollard::Docker;
use futures::StreamExt;
use std::fmt;
use std::time::Instant;
use tokio::process::Command as AsyncCommand;
use tokio::time::{timeout, Duration};

#[derive(Debug, Clone)]
pub struct ResourceConfig {
    /// Docker CPU shares, relative weight 0–1024 (512 ≈ 0.5 CPU)
    pub cpu_shares: u32,
    /// Memory limit in megabytes
    pub memory_mb: u64,
    /// Wall-clock timeout in seconds
    pub timeout_secs: u64,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            cpu_shares: 512,
            memory_mb: 256,
            timeout_secs: 10,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ComputeResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i64,
    pub duration_ms: u64,
}

#[derive(Debug)]
pub enum ComputeError {
    DockerError(bollard::errors::Error),
    Timeout(ComputeResult),
    NonZeroExit(ComputeResult),
    PayloadParseError(String),
    IoError(String),
}

impl fmt::Display for ComputeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComputeError::DockerError(e) => write!(f, "docker: {e}"),
            ComputeError::Timeout(_) => write!(f, "execution timed out"),
            ComputeError::NonZeroExit(r) => write!(f, "exit code {}", r.exit_code),
            ComputeError::PayloadParseError(msg) => write!(f, "payload parse: {msg}"),
            ComputeError::IoError(msg) => write!(f, "io: {msg}"),
        }
    }
}

impl std::error::Error for ComputeError {}

impl From<bollard::errors::Error> for ComputeError {
    fn from(e: bollard::errors::Error) -> Self {
        ComputeError::DockerError(e)
    }
}

pub struct ComputeModule {
    docker: Docker,
    pub config: ResourceConfig,
}

impl ComputeModule {
    /// Connect to Docker via platform-default socket. Returns Err if Docker unreachable.
    pub fn new(config: ResourceConfig) -> Result<Self, ComputeError> {
        let docker = Docker::connect_with_local_defaults()?;
        Ok(Self { docker, config })
    }

    async fn ensure_image(&self, image: &str) -> Result<(), ComputeError> {
        if self.docker.inspect_image(image).await.is_ok() {
            return Ok(());
        }
        eprintln!("[compute] Pulling {image}...");
        let opts = CreateImageOptionsBuilder::default()
            .from_image(image)
            .build();
        let mut stream = self.docker.create_image(Some(opts), None, None);
        while let Some(item) = stream.next().await {
            item?;
        }
        Ok(())
    }

    pub async fn execute_python(
        &self,
        code: &str,
        worker_id: u64,
        task_id: u64,
        config: &ResourceConfig,
    ) -> Result<ComputeResult, ComputeError> {
        self.ensure_image("python:3.11-alpine").await?;
        let container_name = format!("boinc-py-{worker_id}-{task_id}");
        let started_at = Instant::now();

        let host_cfg = HostConfig {
            memory: Some((config.memory_mb * 1024 * 1024) as i64),
            cpu_shares: Some(config.cpu_shares as i64),
            network_mode: Some("none".to_string()),
            ..Default::default()
        };

        let body = ContainerCreateBody {
            image: Some("python:3.11-alpine".to_string()),
            cmd: Some(vec![
                "python3".to_string(),
                "-c".to_string(),
                code.to_string(),
            ]),
            host_config: Some(host_cfg),
            ..Default::default()
        };

        let create_opts = CreateContainerOptionsBuilder::new()
            .name(&container_name)
            .build();

        let id = self
            .docker
            .create_container(Some(create_opts), body)
            .await?
            .id;

        self.docker.start_container(&id, None).await?;

        let wait_opts = WaitContainerOptionsBuilder::new().build();
        let wait_result = timeout(
            Duration::from_secs(config.timeout_secs),
            self.docker.wait_container(&id, Some(wait_opts)).next(),
        )
        .await;

        let duration_ms = started_at.elapsed().as_millis() as u64;
        let (stdout, stderr) = self.collect_logs(&id).await.unwrap_or_default();

        let remove_opts = RemoveContainerOptionsBuilder::new().force(true).build();
        let _ = self.docker.remove_container(&id, Some(remove_opts)).await;

        match wait_result {
            Err(_elapsed) => Err(ComputeError::Timeout(ComputeResult {
                stdout,
                stderr,
                exit_code: -1,
                duration_ms,
            })),
            Ok(None) => Err(ComputeError::Timeout(ComputeResult {
                stdout,
                stderr,
                exit_code: -1,
                duration_ms,
            })),
            Ok(Some(Err(e))) => Err(ComputeError::DockerError(e)),
            Ok(Some(Ok(resp))) => {
                let exit_code = resp.status_code;
                let result = ComputeResult {
                    stdout,
                    stderr,
                    exit_code,
                    duration_ms,
                };
                if exit_code != 0 {
                    Err(ComputeError::NonZeroExit(result))
                } else {
                    Ok(result)
                }
            }
        }
    }

    async fn collect_logs(&self, container_id: &str) -> Result<(String, String), ComputeError> {
        let opts = LogsOptionsBuilder::new()
            .stdout(true)
            .stderr(true)
            .follow(false)
            .build();

        let mut stdout_buf = String::new();
        let mut stderr_buf = String::new();
        let mut stream = self.docker.logs(container_id, Some(opts));

        while let Some(chunk) = stream.next().await {
            match chunk? {
                LogOutput::StdOut { message } => {
                    stdout_buf.push_str(&String::from_utf8_lossy(&message));
                }
                LogOutput::StdErr { message } => {
                    stderr_buf.push_str(&String::from_utf8_lossy(&message));
                }
                _ => {}
            }
        }

        Ok((stdout_buf, stderr_buf))
    }

    /// Execute Python code in a GPU-enabled Docker container via docker CLI.
    /// Uses `--gpus all` for NVIDIA GPU passthrough; falls back gracefully if no GPU.
    /// Security: --network none, memory limit, auto-remove.
    pub async fn execute_python_gpu(
        &self,
        code: &str,
        worker_id: u64,
        task_id: u64,
        config: &ResourceConfig,
        image: &str,
    ) -> Result<ComputeResult, ComputeError> {
        self.ensure_image(image).await?;

        let container_name = format!("boinc-gpu-{worker_id}-{task_id}");
        let mem_limit = format!("{}m", config.memory_mb.max(4096));
        let timeout_secs = config.timeout_secs.max(60);
        let started_at = Instant::now();

        let run_result =
            Self::run_gpu_container(code, image, &container_name, &mem_limit, timeout_secs, true)
                .await;

        let run_result = match run_result {
            Ok(Ok(out)) if out.status.success() => Ok(Ok(out)),
            Ok(Ok(out)) if Self::gpu_runtime_unavailable(&out.stderr) => {
                let _ = AsyncCommand::new("docker")
                    .args(["rm", "-f", &container_name])
                    .output()
                    .await;
                Self::run_gpu_container(
                    code,
                    image,
                    &container_name,
                    &mem_limit,
                    timeout_secs,
                    false,
                )
                .await
            }
            other => other,
        };

        // Force-remove container in case it's still running after timeout
        let _ = AsyncCommand::new("docker")
            .args(["rm", "-f", &container_name])
            .output()
            .await;

        let duration_ms = started_at.elapsed().as_millis() as u64;

        match run_result {
            Err(_) => Err(ComputeError::Timeout(ComputeResult {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: -1,
                duration_ms,
            })),
            Ok(Err(e)) => Err(ComputeError::IoError(e.to_string())),
            Ok(Ok(out)) => {
                let stdout = String::from_utf8_lossy(&out.stdout).to_string();
                let stderr = Self::clean_docker_stderr(&out.stderr);
                let exit_code = out.status.code().unwrap_or(-1) as i64;
                let result = ComputeResult {
                    stdout,
                    stderr,
                    exit_code,
                    duration_ms,
                };
                if exit_code != 0 {
                    Err(ComputeError::NonZeroExit(result))
                } else {
                    Ok(result)
                }
            }
        }
    }

    async fn run_gpu_container(
        code: &str,
        image: &str,
        container_name: &str,
        mem_limit: &str,
        timeout_secs: u64,
        request_gpus: bool,
    ) -> Result<std::io::Result<std::process::Output>, tokio::time::error::Elapsed> {
        let mut cmd = AsyncCommand::new("docker");
        cmd.args(["run", "--rm", "--name", container_name]);
        cmd.args(["--platform", "linux/amd64"]);
        if request_gpus {
            cmd.args(["--gpus", "all"]);
        }
        cmd.args([
            "--network",
            "none",
            "-m",
            mem_limit,
            image,
            "python3",
            "-c",
            code,
        ]);

        timeout(Duration::from_secs(timeout_secs), cmd.output()).await
    }

    fn gpu_runtime_unavailable(stderr: &[u8]) -> bool {
        let stderr = String::from_utf8_lossy(stderr).to_ascii_lowercase();
        stderr.contains("could not select device driver")
            || stderr.contains("could not find gpu")
            || stderr.contains("failed to discover gpu")
            || stderr.contains("nvidia-container-cli")
            || stderr.contains("unknown flag: --gpus")
            || stderr.contains("could not initialize nvml")
    }

    fn clean_docker_stderr(stderr: &[u8]) -> String {
        String::from_utf8_lossy(stderr)
            .lines()
            .filter(|line| !line.contains("WARNING: The requested image's platform"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}
