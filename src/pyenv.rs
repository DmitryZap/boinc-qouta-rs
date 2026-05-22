//! Runtime Python environment bootstrap.
//!
//! For end-user deployment we cannot rely on a developer-created venv at a fixed
//! path. Instead, on first use we locate a system Python (>=3.10), create a
//! private virtualenv under the OS data directory, and install `smolagents`
//! into it. The isolated interpreter then resolves packages from this venv, and
//! [`crate::sandbox`] pip-installs any additional user-allowed packages here.
//!
//! Note: PyO3 links a specific `libpython` at build time, so the embedded
//! interpreter must be ABI-compatible (same minor version) with the system
//! Python found here. A release build should therefore target the Python
//! version it expects to find (or bundle one).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// Candidate system interpreters, most-specific first.
const PYTHON_CANDIDATES: &[&str] =
    &["python3.12", "python3.11", "python3.10", "python3", "python"];

/// Resolved venv interpreter + its site-packages, computed once per process.
#[derive(Debug, Clone)]
pub struct PyEnv {
    pub python: PathBuf,
    pub site_packages: PathBuf,
}

static PYENV: OnceLock<Option<PyEnv>> = OnceLock::new();

/// The bootstrapped environment, or `None` if setup failed (no suitable Python,
/// venv creation failed, …). Result is cached for the process lifetime.
pub fn get() -> Option<&'static PyEnv> {
    PYENV.get_or_init(|| bootstrap().map_err(|e| eprintln!("[pyenv] {e}")).ok())
        .as_ref()
}

/// venv interpreter path, if the environment is ready.
pub fn python() -> Option<PathBuf> {
    get().map(|e| e.python.clone())
}

/// venv site-packages path, if the environment is ready.
pub fn site_packages() -> Option<PathBuf> {
    get().map(|e| e.site_packages.clone())
}

/// site-packages path **without** triggering bootstrap: returns a value only if
/// the venv was already bootstrapped this process or exists on disk. Used for
/// seeding `sys.path` cheaply (e.g. during tests) without creating a venv or
/// hitting the network.
pub fn site_packages_if_ready() -> Option<PathBuf> {
    if let Some(Some(env)) = PYENV.get() {
        return Some(env.site_packages.clone());
    }
    let venv_py = venv_python(&venv_dir());
    venv_py.exists().then(|| query_site_packages(&venv_py).ok()).flatten()
}

/// Force bootstrap and report a human-readable error on failure. Triggers venv
/// creation + smolagents install on first call.
pub fn ensure_ready() -> Result<(), String> {
    match get() {
        Some(_) => Ok(()),
        None => Err("Python environment unavailable — install Python 3.10+ and retry".into()),
    }
}

fn bootstrap() -> Result<PyEnv, String> {
    let venv_dir = venv_dir();
    let venv_py = venv_python(&venv_dir);

    if !venv_py.exists() {
        let system = find_system_python()
            .ok_or("no system Python >=3.10 found (tried python3.12 … python3)")?;
        if let Some(parent) = venv_dir.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("create data dir: {e}"))?;
        }
        run_ok(
            Command::new(&system).arg("-m").arg("venv").arg(&venv_dir),
            "create venv",
        )?;
    }

    if !venv_py.exists() {
        return Err(format!("venv python missing after creation: {}", venv_py.display()));
    }

    ensure_smolagents(&venv_py)?;

    let site_packages = query_site_packages(&venv_py)?;
    Ok(PyEnv {
        python: venv_py,
        site_packages,
    })
}

/// `<data_dir>/boinc-quota/venv`
fn venv_dir() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("boinc-quota")
        .join("venv")
}

/// Interpreter path inside a venv (platform-specific layout).
fn venv_python(venv: &Path) -> PathBuf {
    if cfg!(windows) {
        venv.join("Scripts").join("python.exe")
    } else {
        venv.join("bin").join("python3")
    }
}

fn find_system_python() -> Option<PathBuf> {
    PYTHON_CANDIDATES.iter().find_map(|cand| {
        let ok = Command::new(cand)
            .arg("-c")
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)")
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        ok.then(|| PathBuf::from(cand))
    })
}

fn ensure_smolagents(venv_py: &Path) -> Result<(), String> {
    let present = Command::new(venv_py)
        .args(["-c", "import smolagents"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if present {
        return Ok(());
    }
    run_ok(
        Command::new(venv_py).args([
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "-q",
            "smolagents",
        ]),
        "install smolagents",
    )
}

fn query_site_packages(venv_py: &Path) -> Result<PathBuf, String> {
    let out = Command::new(venv_py)
        .args(["-c", "import site; print(site.getsitepackages()[0])"])
        .output()
        .map_err(|e| format!("query site-packages: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "query site-packages failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if path.is_empty() {
        return Err("empty site-packages path".into());
    }
    Ok(PathBuf::from(path))
}

fn run_ok(cmd: &mut Command, what: &str) -> Result<(), String> {
    let out = cmd
        .output()
        .map_err(|e| format!("{what}: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        let err = String::from_utf8_lossy(&out.stderr);
        let last = err.lines().last().unwrap_or("").trim();
        Err(format!("{what} failed: {last}"))
    }
}
