//! Immutable physical snapshots. Dataset identity is independent of its root path.
//! Seal freezes the existing format; it does not add a second storage backend.
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{FastKError, Result};
use crate::storage::{
    fs as storage_fs,
    lock::{StoreReadLock, StoreWriteLock, SNAPSHOT_FILE_NAME},
    manifest, recovery,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotFile {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub format_version: u32,
    pub digest: String,
    pub files: Vec<SnapshotFile>,
}

/// Freezes all series bytes, including schema/scales and indexes. Further changes
/// require a new root/version. Existing sealed roots are verified idempotently.
pub fn seal_snapshot(root: impl AsRef<Path>) -> Result<SnapshotManifest> {
    let root = root.as_ref();
    if root.join(SNAPSHOT_FILE_NAME).exists() {
        return verify_snapshot(root, None);
    }
    let _lock = StoreWriteLock::acquire(root)?;
    manifest::invalidate_root(root);
    recovery::startup_recover_locked(root, recovery::RecoveryOptions::default())?;
    let reports = recovery::scrub_store(
        root,
        recovery::ValidationOptions {
            verbose: true,
            revalidate_checksums: true,
        },
    )?;
    if reports.is_empty() || reports.iter().any(|report| !report.validation.is_clean()) {
        return Err(FastKError::InvalidData(
            "snapshot requires a non-empty, clean store; scrub/repair it first".into(),
        ));
    }
    // Finalize already-durable deltas for sealed replay before fixing inventory.
    for series in recovery::discover_series_dirs(&root.join("series"))? {
        let mut meta = (*manifest::load_series_meta_shared_mode(&series, true)?.meta).clone();
        let mut changed = false;
        for chunk in &mut meta.chunks {
            match chunk.state {
                crate::types::ChunkState::Active => {
                    chunk.state = crate::types::ChunkState::Sealed;
                    changed = true;
                }
                crate::types::ChunkState::Sealed => {}
                _ => {
                    return Err(FastKError::InvalidData(
                        "resolve transitional chunks before sealing".into(),
                    ))
                }
            }
        }
        if changed {
            meta.active_chunk_id = None;
            meta.manifest_seq = meta.manifest_seq.saturating_add(1);
            meta.updated_at = chrono::Utc::now().timestamp_millis();
            manifest::save_series_meta(&series, &meta)?;
        }
    }
    let files = collect_files(root)?;
    let snapshot = SnapshotManifest {
        format_version: 1,
        digest: files_digest(&files)?,
        files,
    };
    let bytes = serde_json::to_vec_pretty(&snapshot)?;
    storage_fs::atomic_write_new(&root.join(SNAPSHOT_FILE_NAME), |writer| {
        writer.write_all(&bytes)?;
        Ok(())
    })?;
    Ok(snapshot)
}

/// Verifies every byte and the complete series file inventory. Supplying a pinned
/// digest also detects replacement of the snapshot manifest itself.
pub fn verify_snapshot(
    root: impl AsRef<Path>,
    expected_digest: Option<&str>,
) -> Result<SnapshotManifest> {
    let root = root.as_ref();
    let _lock = StoreReadLock::acquire(root)?;
    if recovery::has_pending_recovery(root)? {
        return Err(FastKError::InvalidData(
            "snapshot has pending recovery".into(),
        ));
    }
    let snapshot: SnapshotManifest =
        serde_json::from_slice(&fs::read(root.join(SNAPSHOT_FILE_NAME))?)?;
    if snapshot.format_version != 1
        || files_digest(&snapshot.files)? != snapshot.digest
        || expected_digest.is_some_and(|expected| expected != snapshot.digest)
        || collect_files(root)? != snapshot.files
    {
        return Err(FastKError::InvalidData(
            "snapshot digest or file inventory mismatch".into(),
        ));
    }
    Ok(snapshot)
}

fn files_digest(files: &[SnapshotFile]) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(files)?)))
}

fn collect_files(root: &Path) -> Result<Vec<SnapshotFile>> {
    fn visit(root: &Path, dir: &Path, files: &mut Vec<SnapshotFile>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Err(FastKError::InvalidInput(
                    "snapshot cannot contain symlinks".into(),
                ));
            }
            if kind.is_dir() {
                visit(root, &entry.path(), files)?;
            } else if kind.is_file() {
                let mut file = File::open(entry.path())?;
                let mut hash = Sha256::new();
                let mut buf = [0u8; 65536];
                loop {
                    let n = file.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hash.update(&buf[..n]);
                }
                files.push(SnapshotFile {
                    path: entry
                        .path()
                        .strip_prefix(root)
                        .map_err(|err| FastKError::InvalidInput(err.to_string()))?
                        .to_str()
                        .ok_or_else(|| {
                            FastKError::InvalidInput("snapshot path must be UTF-8".into())
                        })?
                        .replace('\\', "/"),
                    bytes: file.metadata()?.len(),
                    sha256: format!("{:x}", hash.finalize()),
                });
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(root, &root.join("series"), &mut files)?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}
