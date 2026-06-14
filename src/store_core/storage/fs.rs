use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{FastKError, Result};

const CHECKSUM_OFFSET: u64 = 0xcbf29ce484222325;
const CHECKSUM_PRIME: u64 = 0x100000001b3;

/// Ensures a directory exists.
pub fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    Ok(())
}

/// Returns a stable 64-bit checksum suitable for manifests and chunk payloads.
pub fn checksum64(bytes: &[u8]) -> u64 {
    let mut digest = CHECKSUM_OFFSET;
    for byte in bytes {
        digest ^= *byte as u64;
        digest = digest.wrapping_mul(CHECKSUM_PRIME);
    }
    digest
}

/// Writes a brand-new file via temp file + atomic rename in the same directory.
pub fn atomic_write_new<F>(dest: &Path, write_fn: F) -> Result<()>
where
    F: FnMut(&mut BufWriter<File>) -> Result<()>,
{
    atomic_write(dest, false, write_fn)
}

/// Replaces an existing file via temp file + atomic replace.
pub fn atomic_write_replace<F>(dest: &Path, write_fn: F) -> Result<()>
where
    F: FnMut(&mut BufWriter<File>) -> Result<()>,
{
    atomic_write(dest, true, write_fn)
}

fn atomic_write<F>(dest: &Path, allow_replace: bool, mut write_fn: F) -> Result<()>
where
    F: FnMut(&mut BufWriter<File>) -> Result<()>,
{
    let parent = dest.parent().ok_or_else(|| {
        FastKError::InvalidInput(format!("path has no parent: {}", dest.display()))
    })?;
    ensure_dir(parent)?;

    if !allow_replace && dest.exists() {
        return Err(FastKError::InvalidInput(format!(
            "destination already exists: {}",
            dest.display()
        )));
    }

    let temp_path = temp_path_for(dest)?;
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)?;

    let mut writer = BufWriter::new(file);
    let write_result = write_fn(&mut writer);
    let flush_result = writer.flush().map_err(FastKError::from);
    let sync_result = writer.get_ref().sync_all().map_err(FastKError::from);
    drop(writer);

    if let Err(err) = write_result.and(flush_result).and(sync_result) {
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }

    replace_file_atomically(&temp_path, dest, allow_replace)?;
    sync_parent_dir(parent)?;
    Ok(())
}

fn temp_path_for(dest: &Path) -> Result<PathBuf> {
    let file_name = dest
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            FastKError::InvalidInput(format!("invalid file name: {}", dest.display()))
        })?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| FastKError::InvalidData(format!("system time error: {err}")))?
        .as_nanos();
    Ok(dest.with_file_name(format!("{file_name}.{nanos}.tmp")))
}

#[cfg(not(windows))]
fn replace_file_atomically(temp_path: &Path, dest: &Path, _allow_replace: bool) -> Result<()> {
    fs::rename(temp_path, dest)?;
    Ok(())
}

#[cfg(windows)]
fn replace_file_atomically(temp_path: &Path, dest: &Path, allow_replace: bool) -> Result<()> {
    use std::iter;
    use std::os::windows::ffi::OsStrExt;

    type Bool = i32;
    type Dword = u32;
    type Lpcwstr = *const u16;

    const MOVEFILE_REPLACE_EXISTING: Dword = 0x1;
    const MOVEFILE_WRITE_THROUGH: Dword = 0x8;

    extern "system" {
        fn MoveFileExW(existing_file_name: Lpcwstr, new_file_name: Lpcwstr, flags: Dword) -> Bool;
    }

    if !allow_replace && dest.exists() {
        return Err(FastKError::InvalidInput(format!(
            "destination already exists: {}",
            dest.display()
        )));
    }

    let from: Vec<u16> = temp_path
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect();
    let to: Vec<u16> = dest
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect();
    let mut flags = MOVEFILE_WRITE_THROUGH;
    if allow_replace {
        flags |= MOVEFILE_REPLACE_EXISTING;
    }

    let result = unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), flags) };
    if result == 0 {
        let err = std::io::Error::last_os_error();
        let _ = fs::remove_file(temp_path);
        return Err(FastKError::Io(err));
    }

    Ok(())
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
fn sync_parent_dir(path: &Path) -> Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;

    let dir = match OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
    {
        Ok(dir) => dir,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
        Err(err) => return Err(FastKError::Io(err)),
    };

    if let Err(err) = dir.sync_all() {
        if err.kind() != std::io::ErrorKind::PermissionDenied {
            return Err(FastKError::Io(err));
        }
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn sync_parent_dir(_path: &Path) -> Result<()> {
    Ok(())
}
