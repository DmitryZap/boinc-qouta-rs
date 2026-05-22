//! Bundled Python environment.
//!
//! The app ships a self-contained [python-build-standalone] interpreter in a
//! `python/` directory next to the binary (dev: `vendor/python` in the repo).
//! PyO3 links its `libpython` at build time (see `.cargo/config.toml`); at
//! runtime we point the embedded interpreter at the bundle's stdlib via
//! `PYTHONHOME`, and keep installable packages (smolagents plus user-allowed
//! ones) in a writable venv under the OS data directory.
//!
//! With no bundle we fall back to a system Python >=3.10 so dev/source runs
//! still work.
//!
//! [python-build-standalone]: https://github.com/astral-sh/python-build-standalone

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// System interpreters tried only when no bundle is present, most-specific first.
const PYTHON_CANDIDATES: &[&str] =
    &["python3.12", "python3.11", "python3.10", "python3", "python"];

/// Resolved venv interpreter + its site-packages, computed once per process.
#[derive(Debug, Clone)]
pub struct PyEnv {
    pub python: PathBuf,
    pub site_packages: PathBuf,
}

static PYENV: OnceLock<Option<PyEnv>> = OnceLock::new();

/// Point the embedded interpreter at the bundled stdlib by setting `PYTHONHOME`,
/// unless it is already set (dev `.cargo/config.toml` or a user override).
///
/// MUST be called before the first `Python::attach` (i.e. at process start),
/// because PyO3 reads `PYTHONHOME` when it initializes the interpreter.
pub fn prepare_embedded_python() {
    if std::env::var_os("PYTHONHOME").is_some() {
        return;
    }
    if let Some(dir) = bundled_python_dir() {
        std::env::set_var("PYTHONHOME", dir);
    }
}

/// The bootstrapped environment, or `None` if setup failed. Cached for the
/// process lifetime.
pub fn get() -> Option<&'static PyEnv> {
    PYENV
        .get_or_init(|| bootstrap().map_err(|e| eprintln!("[pyenv] {e}")).ok())
        .as_ref()
}

/// venv interpreter path, if the environment is ready.
pub fn python() -> Option<PathBuf> {
    get().map(|e| e.python.clone())
}

/// site-packages path without triggering bootstrap: returns a value only if the
/// venv was already bootstrapped this process or exists on disk. Lets us seed
/// `sys.path` cheaply (e.g. during tests) without creating a venv.
pub fn site_packages_if_ready() -> Option<PathBuf> {
    if let Some(Some(env)) = PYENV.get() {
        return Some(env.site_packages.clone());
    }
    let venv_py = venv_python(&venv_dir());
    venv_py
        .exists()
        .then(|| query_site_packages(&venv_py).ok())
        .flatten()
}

/// Force bootstrap and report a human-readable error on failure. Triggers venv
/// creation + smolagents install on first call.
pub fn ensure_ready() -> Result<(), String> {
    match get() {
        Some(_) => Ok(()),
        None => Err("Python environment unavailable (no bundled or system Python 3.10+)".into()),
    }
}

fn bootstrap() -> Result<PyEnv, String> {
    let base = base_python().ok_or(
        "no Python found: bundle missing and no system python3.10+ (tried python3.12 ... python3)",
    )?;

    let venv_dir = venv_dir();
    let venv_py = venv_python(&venv_dir);

    if !venv_py.exists() {
        if let Some(parent) = venv_dir.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("create data dir: {e}"))?;
        }
        run_ok(
            Command::new(&base).arg("-m").arg("venv").arg(&venv_dir),
            "create venv",
        )?;
    }
    if !venv_py.exists() {
        return Err(format!("venv python missing after creation: {}", venv_py.display()));
    }

    ensure_smolagents(&venv_py)?;
    let site_packages = query_site_packages(&venv_py)?;
    Ok(PyEnv { python: venv_py, site_packages })
}

/// Interpreter used to *create* the venv: the bundled one if present, else a
/// system Python >=3.10.
fn base_python() -> Option<PathBuf> {
    if let Some(dir) = bundled_python_dir() {
        let py = bundle_interpreter(&dir);
        if py.exists() {
            return Some(py);
        }
    }
    find_system_python()
}

/// Locate the bundled python directory (the prefix containing `bin`/`lib`).
/// Searches next to the executable, the repo `vendor` directory, and the cwd.
fn bundled_python_dir() -> Option<PathBuf> {
    let mut roots: Vec<PathBuf> = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            roots.push(dir.to_path_buf()); // alongside binary
            roots.push(dir.join("..").join("Resources")); // macOS .app bundle
            for up in [dir.join(".."), dir.join("..").join("..")] {
                roots.push(up); // dev: target/debug -> repo root
            }
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd);
    }

    for root in roots {
        for name in ["python", "vendor/python"] {
            let cand = root.join(name);
            if bundle_interpreter(&cand).exists() {
                return Some(cand);
            }
        }
    }
    None
}

/// Interpreter path inside a python-build-standalone prefix.
fn bundle_interpreter(prefix: &Path) -> PathBuf {
    if cfg!(windows) {
        prefix.join("python.exe")
    } else {
        prefix.join("bin").join("python3")
    }
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
    let present = venv_command(venv_py)
        .args(["-c", "import smolagents"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if present {
        return Ok(());
    }
    run_ok(
        venv_command(venv_py).args([
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

/// Command for a venv interpreter with `PYTHONHOME` cleared: an inherited
/// `PYTHONHOME` (set for the embedded interpreter) would override the venv and
/// make pip install into the bundle instead.
pub(crate) fn venv_command(venv_py: &Path) -> Command {
    let mut cmd = Command::new(venv_py);
    cmd.env_remove("PYTHONHOME");
    cmd
}

fn query_site_packages(venv_py: &Path) -> Result<PathBuf, String> {
    let out = venv_command(venv_py)
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
    let out = cmd.output().map_err(|e| format!("{what}: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        let err = String::from_utf8_lossy(&out.stderr);
        let last = err.lines().last().unwrap_or("").trim();
        Err(format!("{what} failed: {last}"))
    }
}
