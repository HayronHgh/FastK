use crate::error::{FastKError, Result};
use crate::storage::fs::ensure_dir;
use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;

pub(crate) const LOCK_FILE_NAME: &str = ".fastk.write.lock";
pub(crate) const SNAPSHOT_FILE_NAME: &str = ".fastk.snapshot.json";

/// Persistent rendezvous file. Ownership belongs to the OS handle, not its contents.
/// Never unlink it: doing so could create two independently locked inodes.
#[derive(Debug)]
pub struct StoreWriteLock {
    _file: File,
}
impl StoreWriteLock {
    pub fn acquire(root: &Path) -> Result<Self> {
        ensure_dir(root)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join(LOCK_FILE_NAME))?;
        file.try_lock().map_err(lock_error)?;
        let guard = Self { _file: file };
        if root.join(SNAPSHOT_FILE_NAME).exists() {
            return Err(FastKError::InvalidInput(
                "sealed snapshot is read-only; create a new version".into(),
            ));
        }
        Ok(guard)
    }
}

impl Drop for StoreWriteLock {
    fn drop(&mut self) {
        // Explicit unlock also releases the open-file-description lock if an
        // unrelated concurrent fork temporarily inherited a CLOEXEC descriptor.
        let _ = self._file.unlock();
    }
}
#[derive(Debug)]
pub struct StoreReadLock {
    _file: File,
}
impl StoreReadLock {
    pub fn acquire(root: &Path) -> Result<Option<Self>> {
        if !root.exists() {
            return Ok(None);
        }
        let file = match File::open(root.join(LOCK_FILE_NAME)) {
            Ok(file) => file,
            Err(err)
                if err.kind() == std::io::ErrorKind::NotFound && !root.join("series").exists() =>
            {
                return Ok(None)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(FastKError::InvalidInput("legacy store has no persistent lock; initialize it once with a writable handle".into()));
            }
            Err(err) => return Err(err.into()),
        };
        file.try_lock_shared().map_err(lock_error)?;
        Ok(Some(Self { _file: file }))
    }
}

impl Drop for StoreReadLock {
    fn drop(&mut self) {
        let _ = self._file.unlock();
    }
}
fn lock_error(err: TryLockError) -> FastKError {
    match err {
        TryLockError::WouldBlock => FastKError::InvalidInput(
            "FastK store is busy (reader, writer, or recovery holds its lock)".into(),
        ),
        TryLockError::Error(err) => err.into(),
    }
}
