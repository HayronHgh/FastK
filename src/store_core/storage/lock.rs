use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{FastKError, Result};
use crate::storage::fs::ensure_dir;

const LOCK_FILE_NAME: &str = ".fastk.write.lock";

/// Coarse single-writer guard used around mutating store operations.
#[derive(Debug)]
pub struct StoreWriteLock {
    path: PathBuf,
    _file: File,
}

impl StoreWriteLock {
    pub fn acquire(root: &Path) -> Result<Self> {
        ensure_dir(root)?;
        let path = root.join(LOCK_FILE_NAME);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::AlreadyExists {
                    FastKError::InvalidInput(format!(
                        "FastK write lock is already held: {}",
                        path.display()
                    ))
                } else {
                    FastKError::Io(err)
                }
            })?;

        writeln!(
            file,
            "pid={},ts_ms={}",
            std::process::id(),
            now_timestamp_ms()
        )?;
        file.sync_all()?;

        Ok(Self { path, _file: file })
    }
}

impl Drop for StoreWriteLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn now_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
