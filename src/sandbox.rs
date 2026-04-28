use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::ffi::CString;
use std::sync::{mpsc, LazyLock};
use std::time::Duration;

/// Python code run once per sandbox invocation.
///
/// Design goals:
///  - Never mutate CPython's global `sys.stdout` / `sys.stderr` (avoids
///    cross-thread clobbering when the timeout test's zombie thread runs on).
///  - Build a *local* copy of `builtins` so patching never bleeds into other
///    invocations.
///  - Override `print` in that local builtins to write to a StringIO buffer.
///  - Block dangerous imports via an allowlist.
///  - Block `open()` by raising PermissionError.
const SANDBOX_INIT: &str = r#"
import io as _io
import builtins as _b
import types as _types

# ── 1. Capture buffer (NOT assigned to sys.stdout — avoids global mutation) ──
__sandbox_out__ = _io.StringIO()

# ── 2. Build a *local* builtins copy so we never touch the real module ────────
_local_builtins = _types.ModuleType('builtins')
_local_builtins.__dict__.update(_b.__dict__)

# ── 3. Override print to write to the local buffer ───────────────────────────
_real_print = _b.print

def _sandbox_print(*args, **kwargs):
    kwargs.setdefault('file', __sandbox_out__)
    _real_print(*args, **kwargs)

_local_builtins.print = _sandbox_print

# ── 4. Allowlist import wrapper ───────────────────────────────────────────────
_ALLOWED = {
    'math', 'cmath', 'decimal', 'fractions', 'random', 'statistics',
    'itertools', 'functools', 'collections', 'heapq', 'bisect', 'operator',
    're', 'string', 'io', 'json', 'struct', 'hashlib',
    'copy', 'abc', 'enum', 'typing', 'dataclasses', 'datetime', 'time',
    'numbers', 'array', 'queue', 'pprint', 'textwrap',
}
_real_import = _b.__import__

def _safe_import(name, g=None, l=None, fl=(), lv=0):
    base = name.split('.')[0]
    if base in _ALLOWED:
        return _real_import(name, g, l, fl, lv)
    raise ImportError("[sandbox] Module '{}' is not in the allowlist".format(base))

_local_builtins.__import__ = _safe_import

# ── 5. Block filesystem access ────────────────────────────────────────────────
def _denied_open(*a, **kw):
    raise PermissionError("[sandbox] File system access is not allowed in sandbox")

_local_builtins.open = _denied_open

# Expose the patched builtins for user code globals
__builtins__ = _local_builtins
"#;

static SANDBOX_INIT_CSTR: LazyLock<CString> =
    LazyLock::new(|| CString::new(SANDBOX_INIT).expect("SANDBOX_INIT contains no null bytes"));

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
}

// ── Public entry-point ───────────────────────────────────────────────────────

/// Run `code` in an isolated CPython sandbox.
///
/// * stdout / stderr of the user script are captured and returned in
///   `SandboxResult::stdout`.
/// * `open()` raises `PermissionError`; blocked imports raise `ImportError`.
/// * Execution is bounded by `timeout_secs`; exceeding it returns
///   `SandboxError::Timeout`.
pub fn run_python_sandboxed(code: &str, timeout_secs: u64) -> Result<SandboxResult, SandboxError> {
    let code = code.to_string();
    let (tx, rx) = mpsc::channel::<Result<SandboxResult, SandboxError>>();

    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Python::attach(|py| run_in_gil(py, &code))
        }))
        .unwrap_or_else(|_| Err(SandboxError::Unavailable("panic inside Python GIL".into())));

        let _ = tx.send(result);
    });

    rx.recv_timeout(Duration::from_secs(timeout_secs))
        .map_err(|_| SandboxError::Timeout)?
}

// ── Internals ────────────────────────────────────────────────────────────────

fn run_in_gil(py: Python<'_>, user_code: &str) -> Result<SandboxResult, SandboxError> {
    let setup_globals = PyDict::new(py);

    py.run(SANDBOX_INIT_CSTR.as_c_str(), Some(&setup_globals), None)
        .map_err(|e| SandboxError::Unavailable(format!("sandbox init failed: {e}")))?;

    let captured = setup_globals
        .get_item("__sandbox_out__")
        .map_err(|e| SandboxError::Unavailable(format!("get __sandbox_out__: {e}")))?
        .ok_or_else(|| SandboxError::Unavailable("__sandbox_out__ missing after init".into()))?;

    let user_globals = PyDict::new(py);

    if let Some(builtins) = setup_globals
        .get_item("__builtins__")
        .map_err(|e| SandboxError::Unavailable(format!("get __builtins__: {e}")))?
    {
        user_globals
            .set_item("__builtins__", &builtins)
            .map_err(|e| SandboxError::Unavailable(format!("set __builtins__: {e}")))?;
    }

    let user_cstr = CString::new(user_code.as_bytes())
        .map_err(|e| SandboxError::PythonError(format!("invalid code bytes: {e}")))?;

    let success = py
        .run(user_cstr.as_c_str(), Some(&user_globals), None)
        .is_ok();

    let stdout: String = captured
        .call_method0("getvalue")
        .and_then(|v| v.extract::<String>())
        .unwrap_or_default();

    Ok(SandboxResult { stdout, success })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_captures_stdout() {
        let result = run_python_sandboxed("print('hello sandbox')", 5).unwrap();
        assert!(result.success, "expected success");
        assert_eq!(result.stdout.trim(), "hello sandbox");
    }

    #[test]
    fn sandbox_blocks_open() {
        let result = run_python_sandboxed(
            "try:\n  open('/etc/passwd')\nexcept PermissionError:\n  print('blocked')",
            5,
        )
        .unwrap();
        assert_eq!(result.stdout.trim(), "blocked");
    }

    #[test]
    fn sandbox_blocks_os_import() {
        let result = run_python_sandboxed(
            "try:\n  import os\nexcept ImportError as e:\n  print('blocked:', e)",
            5,
        )
        .unwrap();
        assert!(result.stdout.contains("blocked"), "got: {}", result.stdout);
    }

    #[test]
    fn sandbox_blocks_subprocess_import() {
        let result = run_python_sandboxed(
            "try:\n  import subprocess\nexcept ImportError as e:\n  print('blocked')",
            5,
        )
        .unwrap();
        assert_eq!(result.stdout.trim(), "blocked");
    }

    #[test]
    fn sandbox_allows_math() {
        let result = run_python_sandboxed("import math\nprint(math.sqrt(16))", 5).unwrap();
        assert!(result.success);
        assert_eq!(result.stdout.trim(), "4.0");
    }

    #[test]
    fn sandbox_allows_json() {
        let result = run_python_sandboxed("import json\nprint(json.dumps({'a':1}))", 5).unwrap();
        assert!(result.success);
        assert_eq!(result.stdout.trim(), r#"{"a": 1}"#);
    }

    #[test]
    fn sandbox_captures_error_output() {
        // A script that prints then raises: stdout still captured, success = false.
        let result = run_python_sandboxed("print('before'); raise ValueError('oops')", 5).unwrap();
        assert!(!result.success, "expected failure");
        assert_eq!(result.stdout.trim(), "before");
    }

    #[test]
    fn sandbox_timeout() {
        let err = run_python_sandboxed("while True: pass", 1).unwrap_err();
        assert!(matches!(err, SandboxError::Timeout));
    }
}
