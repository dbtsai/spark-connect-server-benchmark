//! Shared artifact identity and atomic JSON persistence for the Rust harness.

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

static SIGNAL_OWNER: AtomicBool = AtomicBool::new(false);

/// POSIX handlers are process-wide even when orchestration is called from Rust.
/// Only one suite/comparison coordinator may own them in a process at a time.
pub(crate) struct SignalLease;

impl SignalLease {
    pub(crate) fn acquire() -> Result<Self, String> {
        SIGNAL_OWNER
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| {
                "another benchmark coordinator owns this process's signal handlers".to_string()
            })?;
        Ok(Self)
    }
}

impl Drop for SignalLease {
    fn drop(&mut self) {
        SIGNAL_OWNER.store(false, Ordering::SeqCst);
    }
}

pub fn sha_file(path: &Path) -> Result<String, String> {
    let mut file = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(|e| e.to_string())?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

pub fn now_utc() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

pub fn save(path: &Path, value: &Value) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let temporary = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|e| e.to_string())?;
        serde_json::to_writer_pretty(&mut output, value).map_err(|e| e.to_string())?;
        output.write_all(b"\n").map_err(|e| e.to_string())?;
        output.sync_all().map_err(|e| e.to_string())?;
        fs::rename(&temporary, path).map_err(|e| e.to_string())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub fn harness_identity() -> Value {
    json!({
        "kind": "rust",
        "schema_version": 1,
        "package_version": env!("CARGO_PKG_VERSION"),
        "binary_sha256": std::env::current_exe().ok().and_then(|p| sha_file(&p).ok()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_hash_uses_standard_sha256_and_missing_files_fail() {
        let root = std::env::temp_dir().join(format!("harness-util-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        fs::write(root.join("input"), b"abc").unwrap();
        assert_eq!(
            sha_file(&root.join("input")).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(sha_file(&root.join("missing")).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn atomic_save_replaces_json_without_leaving_temporary_files() {
        let root = std::env::temp_dir().join(format!("harness-save-{}", uuid::Uuid::new_v4()));
        let path = root.join("result.json");
        save(&path, &json!({"valid":false})).unwrap();
        save(&path, &json!({"valid":true,"label":"π😀"})).unwrap();
        let value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(value, json!({"valid":true,"label":"π😀"}));
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
        assert!(chrono::DateTime::parse_from_rfc3339(&now_utc()).is_ok());
        fs::remove_dir_all(root).unwrap();
    }
}
