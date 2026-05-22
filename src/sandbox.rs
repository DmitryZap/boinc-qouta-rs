use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::ffi::CString;
use std::process::Command;
use std::sync::{mpsc, LazyLock};
use std::time::Duration;

/// Modules user code is allowed to import inside the isolated interpreter.
///
/// smolagents' `LocalPythonExecutor` verifies at construction time that every
/// authorized import is actually installed, so this list must contain only
/// importable modules (all stdlib here). Submodule access (e.g. `random._os`)
/// is blocked by the interpreter regardless of this list.
/// Default *additional* imports beyond smolagents' built-in safe baseline.
///
/// `LocalPythonExecutor` always permits a base set of safe stdlib modules
/// (math, random, datetime, collections, itertools, re, statistics, time, …);
/// the list passed to it is *additional* on top of that. These are the extra
/// stdlib modules we enable by default — all importable, so interpreter setup
/// never fails on them.
const ALLOWED: &[&str] = &[
    "cmath", "decimal", "fractions", "functools", "heapq", "bisect", "operator", "string", "io",
    "json", "struct", "hashlib", "copy", "abc", "enum", "typing", "dataclasses", "numbers",
    "array", "pprint", "textwrap",
];

/// Default additional-import allowlist offered to the user as a starting point.
/// Callers may pass any list to [`run_python_sandboxed`]; modules in the
/// interpreter's safe baseline are allowed regardless of this list.
pub fn default_allowed_packages() -> Vec<String> {
    ALLOWED.iter().map(|s| s.to_string()).collect()
}

/// Python glue that runs user code through smolagents' isolated interpreter.
///
/// Inputs (set in the run globals before execution):
///   * `__user_code__` — the code string to execute.
///   * `__allowed__`   — list of authorized import names.
///   * `__timeout__`   — wall-clock cap (seconds) handed to the interpreter.
///
/// Outputs (read back from the same globals):
///   * `__stdout__`     — captured print output (also on failure: partial).
///   * `__success__`    — bool, True if code ran without raising.
///   * `__operations__` — interpreter operation counter (dynamic work metric).
///   * `__timed_out__`  — bool, True when the op-cap / timeout tripped.
///   * `__err__`        — error message (empty on success).
///
/// `LocalPythonExecutor` enforces: import allowlist, blocked submodule access,
/// and a hard cap on executed operations (stops infinite loops without a kill).
const GLUE: &str = r#"
import sys as _sys
for _p in __extra_paths__:
    if _p and _p not in _sys.path:
        _sys.path.insert(0, _p)

from smolagents.local_python_executor import LocalPythonExecutor, InterpreterError

_executor = LocalPythonExecutor(__allowed__, timeout_seconds=__timeout__)
_executor.send_tools({})

__success__ = False
__err__ = ""
try:
    _out = _executor(__user_code__)
    __stdout__ = _out.logs or ""
    __success__ = True
except InterpreterError as e:
    __err__ = str(e)
    __stdout__ = str(_executor.state.get("_print_outputs", ""))
except Exception as e:  # noqa: BLE001 — surface any interpreter-level failure
    __err__ = "{}: {}".format(type(e).__name__, e)
    __stdout__ = str(_executor.state.get("_print_outputs", ""))

__operations__ = int(_executor.state.get("_operations_count", {}).get("counter", 0))

_low = __err__.lower()
__timed_out__ = (
    "maximum number of" in _low
    or "timed out" in _low
    or ("operations" in _low and "exceeded" in _low)
)
"#;

static GLUE_CSTR: LazyLock<CString> =
    LazyLock::new(|| CString::new(GLUE).expect("GLUE contains no null bytes"));

// ── Public types ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum SandboxError {
    Timeout,
    PythonError(String),
    Unavailable(String),
}

impl std::fmt::Display for SandboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SandboxError::Timeout => write!(f, "sandbox: execution timed out"),
            SandboxError::PythonError(msg) => write!(f, "sandbox: python error: {msg}"),
            SandboxError::Unavailable(msg) => write!(f, "sandbox: unavailable: {msg}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SandboxResult {
    pub stdout: String,
    pub success: bool,
    /// Operations executed by the interpreter — a deterministic, execution-path
    /// dependent measure of computational work (used for dynamic cost).
    pub operations: u64,
}

// ── Public entry-point ───────────────────────────────────────────────────────

/// Run `code` in the smolagents isolated interpreter.
///
/// * Printed output is captured into `SandboxResult::stdout`.
/// * Non-allowlisted imports and submodule access (`random._os`) raise inside
///   the interpreter and surface as `success = false`.
/// * Infinite loops trip the interpreter's operation cap and return
///   `SandboxError::Timeout`; `timeout_secs` is also passed to the interpreter
///   and backstopped by a wall-clock thread timeout.
/// * If smolagents is not installed, returns `SandboxError::Unavailable`.
pub fn run_python_sandboxed(
    code: &str,
    timeout_secs: u64,
    allowed_packages: &[String],
) -> Result<SandboxResult, SandboxError> {
    let code = code.to_string();
    let allowed: Vec<String> = allowed_packages.to_vec();
    let (tx, rx) = mpsc::channel::<Result<SandboxResult, SandboxError>>();

    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Python::attach(|py| run_in_gil(py, &code, timeout_secs, &allowed))
        }))
        .unwrap_or_else(|_| Err(SandboxError::Unavailable("panic inside Python GIL".into())));

        let _ = tx.send(result);
    });

    // Wall-clock backstop: the interpreter's own timeout/op-cap should fire
    // first; add a small margin so we prefer the structured Timeout over this.
    rx.recv_timeout(Duration::from_secs(timeout_secs.saturating_add(2)))
        .map_err(|_| SandboxError::Timeout)?
}

// ── Internals ────────────────────────────────────────────────────────────────

/// Accept only safe module/package names (block python-code injection via the
/// `import <name>` probe and stray pip args).
fn is_valid_package_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

/// Outcome of auto-installing one package.
#[derive(Debug, Clone)]
pub enum InstallOutcome {
    /// Module already importable (stdlib or previously installed) — skipped.
    AlreadyPresent,
    /// `pip install` succeeded.
    Installed,
    /// `pip install` failed (or name rejected); carries the reason.
    Failed(String),
}

/// Ensure each package is importable by the venv interpreter, pip-installing the
/// missing ones into the venv. Stdlib and already-installed modules are skipped.
/// Returns one outcome per input package.
pub fn ensure_packages_installed(packages: &[String]) -> Vec<(String, InstallOutcome)> {
    packages
        .iter()
        .map(|pkg| (pkg.clone(), ensure_one(pkg)))
        .collect()
}

fn ensure_one(pkg: &str) -> InstallOutcome {
    if !is_valid_package_name(pkg) {
        return InstallOutcome::Failed("invalid package name".to_string());
    }

    let Some(venv_python) = crate::pyenv::python() else {
        return InstallOutcome::Failed("Python environment unavailable".to_string());
    };

    let importable = Command::new(&venv_python)
        .args(["-c", &format!("import {pkg}")])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if importable {
        return InstallOutcome::AlreadyPresent;
    }

    match Command::new(&venv_python)
        .args(["-m", "pip", "install", "--disable-pip-version-check", "-q", pkg])
        .output()
    {
        Ok(o) if o.status.success() => InstallOutcome::Installed,
        Ok(o) => {
            let err = String::from_utf8_lossy(&o.stderr);
            let last = err.lines().last().unwrap_or("pip install failed").trim();
            InstallOutcome::Failed(last.to_string())
        }
        Err(e) => InstallOutcome::Failed(e.to_string()),
    }
}

/// Candidate import roots to seed onto `sys.path`: every entry of the runtime
/// `PYTHONPATH` plus the bootstrapped venv's site-packages.
fn extra_python_paths() -> Vec<String> {
    let mut paths: Vec<String> = std::env::var("PYTHONPATH")
        .unwrap_or_default()
        .split(':')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    if let Some(site) = crate::pyenv::site_packages_if_ready() {
        paths.push(site.to_string_lossy().into_owned());
    }
    paths
}

fn run_in_gil(
    py: Python<'_>,
    user_code: &str,
    timeout_secs: u64,
    allowed_packages: &[String],
) -> Result<SandboxResult, SandboxError> {
    let globals = PyDict::new(py);

    globals
        .set_item("__user_code__", user_code)
        .map_err(|e| SandboxError::Unavailable(format!("set __user_code__: {e}")))?;
    globals
        .set_item("__timeout__", timeout_secs.max(1))
        .map_err(|e| SandboxError::Unavailable(format!("set __timeout__: {e}")))?;
    let allowed = PyList::new(py, allowed_packages)
        .map_err(|e| SandboxError::Unavailable(format!("build allowlist: {e}")))?;
    globals
        .set_item("__allowed__", allowed)
        .map_err(|e| SandboxError::Unavailable(format!("set __allowed__: {e}")))?;

    // Ensure smolagents is importable regardless of how the binary was launched:
    // seed sys.path from runtime PYTHONPATH plus the project venv site-packages.
    let extra = PyList::new(py, extra_python_paths())
        .map_err(|e| SandboxError::Unavailable(format!("build extra paths: {e}")))?;
    globals
        .set_item("__extra_paths__", extra)
        .map_err(|e| SandboxError::Unavailable(format!("set __extra_paths__: {e}")))?;

    // A failure here means the interpreter itself could not be set up
    // (e.g. smolagents not installed) — distinct from user-code errors, which
    // the glue catches internally.
    py.run(GLUE_CSTR.as_c_str(), Some(&globals), None)
        .map_err(|e| SandboxError::Unavailable(format!("interpreter setup failed: {e}")))?;

    let get_bool = |key: &str| -> bool {
        globals
            .get_item(key)
            .ok()
            .flatten()
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(false)
    };
    let get_string = |key: &str| -> String {
        globals
            .get_item(key)
            .ok()
            .flatten()
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_default()
    };

    if get_bool("__timed_out__") {
        return Err(SandboxError::Timeout);
    }

    let operations = globals
        .get_item("__operations__")
        .ok()
        .flatten()
        .and_then(|v| v.extract::<u64>().ok())
        .unwrap_or(0);

    Ok(SandboxResult {
        stdout: get_string("__stdout__"),
        success: get_bool("__success__"),
        operations,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Run with the built-in default allowlist.
    fn run(code: &str, timeout: u64) -> Result<SandboxResult, SandboxError> {
        run_python_sandboxed(code, timeout, &default_allowed_packages())
    }

    #[test]
    fn sandbox_captures_stdout() {
        let result = run("print('hello sandbox')", 5).unwrap();
        assert!(result.success, "expected success");
        assert_eq!(result.stdout.trim(), "hello sandbox");
    }

    #[test]
    fn sandbox_blocks_os_import() {
        let result = run("import os", 5).unwrap();
        assert!(!result.success, "import os must fail");
    }

    #[test]
    fn sandbox_blocks_subprocess_import() {
        let result = run("import subprocess", 5).unwrap();
        assert!(!result.success, "import subprocess must fail");
    }

    #[test]
    fn sandbox_blocks_submodule_access() {
        // smolagents-specific: even allowlisted `random` cannot reach `random._os`.
        let result = run("import random\nrandom._os.system('echo bad')", 5).unwrap();
        assert!(!result.success, "submodule access must fail");
    }

    #[test]
    fn sandbox_allows_math() {
        let result = run("import math\nprint(math.sqrt(16))", 5).unwrap();
        assert!(result.success);
        assert_eq!(result.stdout.trim(), "4.0");
    }

    #[test]
    fn sandbox_allows_json() {
        let result = run("import json\nprint(json.dumps({'a':1}))", 5).unwrap();
        assert!(result.success);
        assert_eq!(result.stdout.trim(), r#"{"a": 1}"#);
    }

    #[test]
    fn sandbox_captures_error_output() {
        // Prints then raises: partial stdout retained, success = false.
        let result = run("print('before')\nraise ValueError('oops')", 5).unwrap();
        assert!(!result.success, "expected failure");
        assert!(result.stdout.contains("before"), "got: {}", result.stdout);
    }

    #[test]
    fn sandbox_reports_operations() {
        let result = run("x = 0\nfor i in range(10):\n    x += i\nprint(x)", 5).unwrap();
        assert!(result.success);
        assert!(result.operations > 0, "expected nonzero operation count");
    }

    #[test]
    fn sandbox_caps_infinite_loop() {
        // Op-cap fires before the wall-clock backstop — returns Timeout fast.
        let err = run("while True:\n    pass", 30).unwrap_err();
        assert!(matches!(err, SandboxError::Timeout));
    }

    #[test]
    fn sandbox_honors_custom_allowlist() {
        // `json` is not in the interpreter's safe baseline, so it imports only
        // when present in the caller's additional-import list.
        let allowed = vec!["json".to_string()];
        let ok = run_python_sandboxed("import json\nprint(json.dumps({'a':1}))", 5, &allowed)
            .unwrap();
        assert!(ok.success, "json should import when allowed");

        // Empty additional list: json is blocked (baseline does not cover it).
        let blocked = run_python_sandboxed("import json", 5, &[]).unwrap();
        assert!(!blocked.success, "json must fail when not in allowlist");
    }
}
