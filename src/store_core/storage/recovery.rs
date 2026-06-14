use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::chunk::header::{
    ChunkHeader, BBO_SCHEMA_ID, BOOK_DELTA_SCHEMA_ID, KLINE_SCHEMA_ID, SCALAR_SCHEMA_ID,
    TRADE_SCHEMA_ID,
};
use crate::engine::Catalog;
use crate::error::{FastKError, Result};
use crate::index::{ValueIndexEntry, ZoneMapEntry};
use crate::storage::{fs as storage_fs, manifest, path};
use crate::types::{
    BboRecord, BookDeltaRecord, ChunkMeta, ChunkState, PartitionPolicy, RecordType,
    ScalarSeriesKey, SeriesMeta, SidecarMeta, TradeRecord,
};

const RECOVERY_MARKER_FILE: &str = ".fastk.recovery.pending";

/// Summary emitted by startup recovery or manual repair.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RecoveryReport {
    pub removed_temp_files: usize,
    pub rebuilt_manifests: usize,
    pub adopted_chunks: usize,
    pub dry_run: bool,
    pub removed_temp_paths: Vec<PathBuf>,
    pub rebuilt_manifest_paths: Vec<PathBuf>,
    pub adopted_chunk_paths: Vec<PathBuf>,
    pub overlap_resolutions: Vec<SeriesOverlapExplanation>,
}

/// Validation report for one series directory.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ManifestValidation {
    pub series_dir: PathBuf,
    pub manifest_exists: bool,
    pub missing_chunks: Vec<String>,
    pub missing_sidecars: Vec<String>,
    pub checksum_mismatches: Vec<String>,
    pub chunk_metadata_mismatches: Vec<String>,
    pub sidecar_consistency_issues: Vec<String>,
    pub untracked_chunks: Vec<String>,
    pub untracked_sidecars: Vec<String>,
    pub overlap_groups: Vec<Vec<String>>,
}

impl ManifestValidation {
    pub fn is_clean(&self) -> bool {
        self.manifest_exists
            && self.missing_chunks.is_empty()
            && self.missing_sidecars.is_empty()
            && self.checksum_mismatches.is_empty()
            && self.chunk_metadata_mismatches.is_empty()
            && self.sidecar_consistency_issues.is_empty()
            && self.untracked_chunks.is_empty()
            && self.untracked_sidecars.is_empty()
            && self.overlap_groups.is_empty()
    }
}

/// Filesystem artifact not currently tracked by the manifest.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OrphanArtifact {
    pub series_dir: PathBuf,
    pub relative_path: String,
    pub kind: String,
    pub reason: String,
}

/// Resolution returned when overlapping chunks are examined.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OverlapResolution {
    pub month_key: String,
    pub winner_relative_path: Option<String>,
    pub participants: Vec<String>,
    pub reason: String,
}

/// Root-level overlap explanation with series context attached.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SeriesOverlapExplanation {
    pub series_dir: PathBuf,
    pub resolution: OverlapResolution,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ValidationOptions {
    pub verbose: bool,
    pub revalidate_checksums: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ScrubSeriesReport {
    pub validation: ManifestValidation,
    pub checked_chunk_count: usize,
    pub checked_sidecar_count: usize,
    pub overlap_resolutions: Vec<OverlapResolution>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryOptions {
    pub dry_run: bool,
}

#[derive(Debug)]
pub struct RecoveryMarkerGuard {
    path: PathBuf,
}

impl RecoveryMarkerGuard {
    pub fn arm(root: &Path) -> Result<Self> {
        storage_fs::ensure_dir(root)?;
        let path = recovery_marker_path(root);
        fs::write(&path, format!("dirty_since={}\n", now_timestamp_ms()))?;
        Ok(Self { path })
    }

    pub fn commit(self) -> Result<()> {
        clear_recovery_marker_path(&self.path)
    }
}

pub fn startup_recover(root: &Path) -> Result<RecoveryReport> {
    startup_recover_with_options(root, RecoveryOptions::default())
}

pub fn startup_recover_with_options(
    root: &Path,
    options: RecoveryOptions,
) -> Result<RecoveryReport> {
    let mut report = RecoveryReport {
        dry_run: options.dry_run,
        ..RecoveryReport::default()
    };
    if !root.exists() {
        return Ok(report);
    }

    let removed = scan_temp_files(root)?;
    report.removed_temp_files = removed.len();
    report.removed_temp_paths = removed.clone();
    if !options.dry_run {
        for path in removed {
            let _ = fs::remove_file(path);
        }
    }
    let series_root = path::series_root(root);
    if !series_root.exists() {
        if !options.dry_run {
            clear_recovery_marker(root)?;
        }
        return Ok(report);
    }

    for series_dir in discover_series_dirs(&series_root)? {
        recover_series_dir(root, &series_dir, &mut report, options)?;
    }

    if !options.dry_run {
        clear_recovery_marker(root)?;
    }
    Ok(report)
}

pub fn has_pending_recovery(root: &Path) -> Result<bool> {
    Ok(recovery_marker_path(root).exists())
}

pub fn clear_recovery_marker(root: &Path) -> Result<()> {
    clear_recovery_marker_path(&recovery_marker_path(root))
}

pub fn validate_store(root: &Path) -> Result<()> {
    let validations = validate_manifest_vs_fs(root)?;
    if let Some(report) = validations.iter().find(|report| !report.is_clean()) {
        return Err(FastKError::InvalidData(format!(
            "store validation failed for {}",
            report.series_dir.display()
        )));
    }
    Ok(())
}

/// Returns manifest-vs-filesystem validation details for every discovered series.
pub fn validate_manifest_vs_fs(root: &Path) -> Result<Vec<ManifestValidation>> {
    validate_manifest_vs_fs_with_options(root, ValidationOptions::default())
}

pub fn validate_manifest_vs_fs_with_options(
    root: &Path,
    options: ValidationOptions,
) -> Result<Vec<ManifestValidation>> {
    let series_root = path::series_root(root);
    if !series_root.exists() {
        return Ok(Vec::new());
    }

    discover_series_dirs(&series_root)?
        .into_iter()
        .map(|series_dir| validate_series_dir(&series_dir, options))
        .collect()
}

pub fn scrub_store(root: &Path, options: ValidationOptions) -> Result<Vec<ScrubSeriesReport>> {
    let series_root = path::series_root(root);
    if !series_root.exists() {
        return Ok(Vec::new());
    }

    let mut reports = Vec::new();
    for series_dir in discover_series_dirs(&series_root)? {
        let validation = validate_series_dir(&series_dir, options)?;
        let chunks = scan_chunk_files(&series_dir)?;
        let overlaps = overlap_groups(&chunks)
            .into_iter()
            .map(|group| choose_surviving_chunk_for_overlap(&group))
            .collect::<Vec<_>>();
        let diagnostics = build_scrub_diagnostics(&validation, &overlaps, options.verbose);
        reports.push(ScrubSeriesReport {
            checked_chunk_count: chunks.len(),
            checked_sidecar_count: chunks.iter().map(|chunk| chunk.sidecars.len()).sum(),
            validation,
            overlap_resolutions: overlaps,
            diagnostics,
        });
    }
    Ok(reports)
}

/// Scans chunk directories and returns artifacts not currently tracked by manifests.
pub fn scan_orphan_artifacts(root: &Path) -> Result<Vec<OrphanArtifact>> {
    let series_root = path::series_root(root);
    if !series_root.exists() {
        return Ok(Vec::new());
    }

    let mut artifacts = Vec::new();
    for series_dir in discover_series_dirs(&series_root)? {
        let validation = validate_series_dir(&series_dir, ValidationOptions::default())?;
        artifacts.extend(
            validation
                .untracked_chunks
                .into_iter()
                .map(|relative_path| OrphanArtifact {
                    series_dir: series_dir.clone(),
                    relative_path,
                    kind: "chunk".to_string(),
                    reason: "filesystem chunk is not referenced by manifest".to_string(),
                }),
        );
        artifacts.extend(
            validation
                .untracked_sidecars
                .into_iter()
                .map(|relative_path| OrphanArtifact {
                    series_dir: series_dir.clone(),
                    relative_path,
                    kind: "sidecar".to_string(),
                    reason: "filesystem sidecar is not referenced by manifest".to_string(),
                }),
        );
    }
    Ok(artifacts)
}

/// Chooses the chunk that should survive inside an overlap set.
pub fn choose_surviving_chunk_for_overlap(chunks: &[ChunkMeta]) -> OverlapResolution {
    let mut participants: Vec<String> = chunks
        .iter()
        .map(|chunk| chunk.relative_path.clone())
        .collect();
    participants.sort();

    if chunks.is_empty() {
        return OverlapResolution {
            month_key: String::new(),
            winner_relative_path: None,
            participants,
            reason: "empty overlap set".to_string(),
        };
    }

    let min_start = chunks
        .iter()
        .map(|chunk| chunk.start_ts)
        .min()
        .unwrap_or(chunks[0].start_ts);
    let max_end = chunks
        .iter()
        .map(|chunk| chunk.end_ts)
        .max()
        .unwrap_or(chunks[0].end_ts);

    let mut ranked: Vec<&ChunkMeta> = chunks.iter().collect();
    ranked.sort_by(|left, right| {
        (
            right.generation,
            right.end_ts.saturating_sub(right.start_ts),
            right.count,
            &right.relative_path,
        )
            .cmp(&(
                left.generation,
                left.end_ts.saturating_sub(left.start_ts),
                left.count,
                &left.relative_path,
            ))
    });

    let winner = ranked
        .into_iter()
        .find(|chunk| chunk.start_ts <= min_start && chunk.end_ts >= max_end);
    match winner {
        Some(chunk) => OverlapResolution {
            month_key: chunk.month_key.clone(),
            winner_relative_path: Some(chunk.relative_path.clone()),
            participants,
            reason: format!(
                "generation {} fully covers overlap window [{}..={}]",
                chunk.generation, min_start, max_end
            ),
        },
        None => OverlapResolution {
            month_key: chunks[0].month_key.clone(),
            winner_relative_path: None,
            participants,
            reason: "no single chunk fully covers the overlap window".to_string(),
        },
    }
}

/// Rebuilds one series manifest from chunk files currently present on disk.
pub fn rebuild_manifest_from_fs(root: &Path, series_dir: &Path) -> Result<SeriesMeta> {
    let chunks = scan_chunk_files(series_dir)?;
    if chunks.is_empty() {
        return Err(FastKError::NotFound(format!(
            "no chunk files found under {}",
            series_dir.display()
        )));
    }

    let meta =
        build_series_meta_from_chunks(root, series_dir, &select_non_overlapping_chunks(chunks)?)?;
    manifest::save_series_meta(series_dir, &meta)?;
    Ok(meta)
}

fn rebuild_manifest_preview(root: &Path, series_dir: &Path) -> Result<SeriesMeta> {
    let chunks = scan_chunk_files(series_dir)?;
    if chunks.is_empty() {
        return Err(FastKError::NotFound(format!(
            "no chunk files found under {}",
            series_dir.display()
        )));
    }

    build_series_meta_from_chunks(root, series_dir, &select_non_overlapping_chunks(chunks)?)
}

/// Rebuilds every manifest reachable from `root`.
pub fn rebuild_all_manifests_from_fs(root: &Path) -> Result<Vec<PathBuf>> {
    let series_root = path::series_root(root);
    if !series_root.exists() {
        return Ok(Vec::new());
    }

    let mut rebuilt = Vec::new();
    for series_dir in discover_series_dirs(&series_root)? {
        let chunks = scan_chunk_files(&series_dir)?;
        if chunks.is_empty() {
            continue;
        }
        let meta = build_series_meta_from_chunks(
            root,
            &series_dir,
            &select_non_overlapping_chunks(chunks)?,
        )?;
        manifest::save_series_meta(&series_dir, &meta)?;
        rebuilt.push(series_dir);
    }
    Ok(rebuilt)
}

/// Recursively discovers every series directory under `series/`.
pub fn discover_series_dirs(series_root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    collect_series_dirs(series_root, &mut out)?;
    Ok(out)
}

/// Explains every overlap group reachable from the store root.
pub fn explain_overlaps(root: &Path) -> Result<Vec<SeriesOverlapExplanation>> {
    let series_root = path::series_root(root);
    if !series_root.exists() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for series_dir in discover_series_dirs(&series_root)? {
        let chunks = match manifest::load_series_meta(&series_dir) {
            Ok(meta) => meta.chunks,
            Err(_) => scan_chunk_files(&series_dir)?,
        };
        for group in overlap_groups(&chunks) {
            out.push(SeriesOverlapExplanation {
                series_dir: series_dir.clone(),
                resolution: choose_surviving_chunk_for_overlap(&group),
            });
        }
    }
    Ok(out)
}

fn recovery_marker_path(root: &Path) -> PathBuf {
    root.join(RECOVERY_MARKER_FILE)
}

fn clear_recovery_marker_path(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(FastKError::Io(err)),
    }
}

fn scan_temp_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    collect_temp_files(root, &mut out)?;
    Ok(out)
}

fn recover_series_dir(
    root: &Path,
    series_dir: &Path,
    report: &mut RecoveryReport,
    options: RecoveryOptions,
) -> Result<()> {
    let candidates = scan_chunk_files(series_dir)?;
    if candidates.is_empty() {
        return Ok(());
    }

    let mut meta = match manifest::load_series_meta(series_dir) {
        Ok(meta) => meta,
        Err(_) => {
            report.rebuilt_manifests += 1;
            report.rebuilt_manifest_paths.push(series_dir.to_path_buf());
            if options.dry_run {
                rebuild_manifest_preview(root, series_dir)?
            } else {
                rebuild_manifest_from_fs(root, series_dir)?
            }
        }
    };

    let mut changed = normalize_dangling_merging_states(&mut meta);
    for candidate in candidates {
        if meta
            .chunks
            .iter()
            .any(|chunk| chunk.relative_path == candidate.relative_path)
        {
            continue;
        }

        let overlap_indices: Vec<_> = meta
            .chunks
            .iter()
            .enumerate()
            .filter_map(|(index, chunk)| {
                ranges_overlap(
                    chunk.start_ts,
                    chunk.end_ts,
                    candidate.start_ts,
                    candidate.end_ts,
                )
                .then_some(index)
            })
            .collect();

        if overlap_indices.is_empty() {
            meta.chunks.push(candidate);
            changed = true;
            report.adopted_chunks += 1;
            continue;
        }

        let overlap_group: Vec<ChunkMeta> = overlap_indices
            .iter()
            .map(|index| meta.chunks[*index].clone())
            .chain(std::iter::once(candidate.clone()))
            .collect();
        let resolution = choose_surviving_chunk_for_overlap(&overlap_group);
        report.overlap_resolutions.push(SeriesOverlapExplanation {
            series_dir: series_dir.to_path_buf(),
            resolution: resolution.clone(),
        });
        if resolution.winner_relative_path.as_deref() == Some(candidate.relative_path.as_str()) {
            meta.chunks = meta
                .chunks
                .iter()
                .enumerate()
                .filter_map(|(index, chunk)| {
                    (!overlap_indices.contains(&index)).then_some(chunk.clone())
                })
                .collect();
            meta.chunks.push(candidate);
            changed = true;
            report.adopted_chunks += 1;
            report
                .adopted_chunk_paths
                .push(series_dir.join(resolution.winner_relative_path.clone().unwrap_or_default()));
        }
    }

    if changed {
        meta.chunks.sort_by_key(|chunk| chunk.start_ts);
        meta.active_chunk_id = meta
            .chunks
            .iter()
            .rev()
            .find(|chunk| chunk.state == ChunkState::Active)
            .map(|chunk| chunk.chunk_id);
        meta.manifest_seq = meta.manifest_seq.saturating_add(1);
        meta.updated_at = now_timestamp_ms();
        if !options.dry_run {
            manifest::save_series_meta(series_dir, &meta)?;
        }
    }

    Ok(())
}

fn validate_series_dir(
    series_dir: &Path,
    options: ValidationOptions,
) -> Result<ManifestValidation> {
    let manifest_path = path::series_meta_path(series_dir);
    let manifest_exists = manifest_path.exists();
    let fs_chunks = scan_chunk_files(series_dir)?;
    let fs_chunk_map: HashMap<_, _> = fs_chunks
        .iter()
        .map(|chunk| (chunk.relative_path.clone(), chunk.clone()))
        .collect();
    let fs_sidecars = scan_sidecar_files(series_dir)?;
    let fs_sidecar_map: HashMap<_, _> = fs_sidecars
        .iter()
        .map(|sidecar| (sidecar.relative_path.clone(), sidecar.clone()))
        .collect();

    if !manifest_exists {
        return Ok(ManifestValidation {
            series_dir: series_dir.to_path_buf(),
            manifest_exists: false,
            missing_chunks: Vec::new(),
            missing_sidecars: Vec::new(),
            checksum_mismatches: Vec::new(),
            chunk_metadata_mismatches: Vec::new(),
            sidecar_consistency_issues: Vec::new(),
            untracked_chunks: fs_chunk_map.keys().cloned().collect(),
            untracked_sidecars: fs_sidecar_map.keys().cloned().collect(),
            overlap_groups: overlap_groups_as_strings(&fs_chunks),
        });
    }

    let meta = manifest::load_series_meta(series_dir)?;
    let mut missing_chunks = Vec::new();
    let mut missing_sidecars = Vec::new();
    let mut checksum_mismatches = Vec::new();
    let mut chunk_metadata_mismatches = Vec::new();
    let mut sidecar_consistency_issues = Vec::new();
    let tracked_chunks: HashSet<_> = meta
        .chunks
        .iter()
        .map(|chunk| chunk.relative_path.clone())
        .collect();
    let tracked_sidecars: HashSet<_> = meta
        .chunks
        .iter()
        .flat_map(|chunk| {
            chunk
                .sidecars
                .iter()
                .map(|sidecar| sidecar.relative_path.clone())
        })
        .collect();

    for chunk in &meta.chunks {
        match fs_chunk_map.get(&chunk.relative_path) {
            Some(actual) => {
                if options.revalidate_checksums
                    && chunk.chunk_checksum != 0
                    && chunk.chunk_checksum != actual.chunk_checksum
                {
                    checksum_mismatches.push(chunk.relative_path.clone());
                }
                if chunk.chunk_id != actual.chunk_id
                    || chunk.generation != actual.generation
                    || chunk.count != actual.count
                    || chunk.start_ts != actual.start_ts
                    || chunk.end_ts != actual.end_ts
                    || chunk.layout_version != actual.layout_version
                    || chunk.header_len != actual.header_len
                    || chunk.sparse_index_every != actual.sparse_index_every
                    || chunk.sparse_index_offset != actual.sparse_index_offset
                    || chunk.sparse_index_len != actual.sparse_index_len
                {
                    chunk_metadata_mismatches.push(chunk.relative_path.clone());
                }
            }
            None => missing_chunks.push(chunk.relative_path.clone()),
        }

        for sidecar in &chunk.sidecars {
            match fs_sidecar_map.get(&sidecar.relative_path) {
                Some(actual) => {
                    if options.revalidate_checksums
                        && sidecar.checksum != 0
                        && sidecar.checksum != actual.checksum
                    {
                        checksum_mismatches.push(sidecar.relative_path.clone());
                    }
                    if sidecar.kind != actual.kind
                        || sidecar.generation != 0
                            && actual.generation != 0
                            && sidecar.generation != actual.generation
                    {
                        sidecar_consistency_issues.push(sidecar.relative_path.clone());
                    }
                    validate_sidecar_content(
                        series_dir,
                        sidecar,
                        actual,
                        &mut sidecar_consistency_issues,
                    )?;
                }
                None => missing_sidecars.push(sidecar.relative_path.clone()),
            }
        }
    }

    let mut untracked_chunks: Vec<_> = fs_chunk_map
        .keys()
        .filter(|relative_path| !tracked_chunks.contains(*relative_path))
        .cloned()
        .collect();
    let mut untracked_sidecars: Vec<_> = fs_sidecar_map
        .keys()
        .filter(|relative_path| !tracked_sidecars.contains(*relative_path))
        .cloned()
        .collect();
    untracked_chunks.sort();
    untracked_sidecars.sort();

    Ok(ManifestValidation {
        series_dir: series_dir.to_path_buf(),
        manifest_exists,
        missing_chunks,
        missing_sidecars,
        checksum_mismatches,
        chunk_metadata_mismatches,
        sidecar_consistency_issues,
        untracked_chunks,
        untracked_sidecars,
        overlap_groups: overlap_groups_as_strings(&meta.chunks),
    })
}

fn collect_series_dirs(current: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !current.is_dir() {
        return Ok(());
    }

    let has_chunks = current.join(path::CHUNKS_DIR).is_dir();
    let has_manifest = current.join(path::MANIFEST_FILE).is_file();
    if has_chunks || has_manifest {
        out.push(current.to_path_buf());
        return Ok(());
    }

    for entry in fs::read_dir(current)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            collect_series_dirs(&entry.path(), out)?;
        }
    }
    Ok(())
}

fn collect_temp_files(current: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !current.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_temp_files(&path, out)?;
            continue;
        }

        let is_tmp = path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.eq_ignore_ascii_case("tmp"))
            .unwrap_or(false);
        if is_tmp {
            out.push(path);
        }
    }
    Ok(())
}

fn scan_chunk_files(series_dir: &Path) -> Result<Vec<ChunkMeta>> {
    let chunks_dir = path::chunks_dir(series_dir);
    if !chunks_dir.exists() {
        return Ok(Vec::new());
    }

    let sidecars = scan_sidecar_files(series_dir)?;
    let mut sidecars_by_chunk: HashMap<String, Vec<SidecarMeta>> = HashMap::new();
    for sidecar in sidecars {
        let chunk_relative_path = parent_chunk_relative_path(&sidecar.relative_path)?;
        sidecars_by_chunk
            .entry(chunk_relative_path)
            .or_default()
            .push(sidecar);
    }

    let mut chunks = Vec::new();
    for entry in fs::read_dir(&chunks_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if !file_name.ends_with(".chunk") {
            continue;
        }

        let file_path = entry.path();
        let bytes = fs::read(&file_path)?;
        let mut cursor = std::io::Cursor::new(&bytes);
        let header = ChunkHeader::read_from(&mut cursor)?;
        let month_key = path::month_key(header.start_ts)?;
        let relative_path = path::chunk_relative_path(&file_name);
        let mut chunk_sidecars = sidecars_by_chunk.remove(&relative_path).unwrap_or_default();
        chunk_sidecars.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        chunks.push(ChunkMeta {
            chunk_id: header.chunk_id,
            month_key,
            start_ts: header.start_ts,
            end_ts: header.end_ts,
            count: header.count,
            state: decode_chunk_state(&file_name),
            layout_version: header.version,
            header_len: header.header_size,
            sparse_index_every: header.sparse_index_every as u32,
            sparse_index_offset: header.index_offset,
            sparse_index_len: header.index_len as u32,
            chunk_checksum: storage_fs::checksum64(&bytes),
            generation: header.generation as u32,
            relative_path,
            sidecars: chunk_sidecars,
        });
    }

    chunks.sort_by_key(|chunk| chunk.start_ts);
    Ok(chunks)
}

fn scan_sidecar_files(series_dir: &Path) -> Result<Vec<SidecarMeta>> {
    let chunks_dir = path::chunks_dir(series_dir);
    if !chunks_dir.exists() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for entry in fs::read_dir(&chunks_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy().to_string();
        if file_name.ends_with(".chunk") || !file_name.contains(".chunk.") {
            continue;
        }

        let relative_path = path::chunk_relative_path(&file_name);
        let kind = file_name.rsplit('.').next().unwrap_or_default().to_string();
        let bytes = fs::read(entry.path())?;
        let record_count = match kind.as_str() {
            "zmap" => (bytes.len() / ZoneMapEntry::BYTE_SIZE) as u64,
            "vix" => (bytes.len() / ValueIndexEntry::BYTE_SIZE) as u64,
            _ => 0,
        };
        out.push(SidecarMeta {
            kind,
            relative_path,
            generation: 0,
            checksum: storage_fs::checksum64(&bytes),
            block_size: 0,
            record_count,
        });
    }
    Ok(out)
}

fn validate_sidecar_content(
    series_dir: &Path,
    expected: &SidecarMeta,
    actual: &SidecarMeta,
    issues: &mut Vec<String>,
) -> Result<()> {
    let sidecar_path = path::resolve_relative_path(series_dir, &actual.relative_path);
    match actual.kind.as_str() {
        "zmap" => match crate::index::zmap::read_entries(&sidecar_path) {
            Ok(entries) => {
                if expected.record_count != 0 && expected.record_count != entries.len() as u64 {
                    issues.push(actual.relative_path.clone());
                }
                if expected.block_size != 0
                    && actual.block_size != 0
                    && expected.block_size != actual.block_size
                {
                    issues.push(actual.relative_path.clone());
                }
            }
            Err(_) => issues.push(actual.relative_path.clone()),
        },
        "vix" => match crate::index::vix::read_entries(&sidecar_path) {
            Ok(entries) => {
                if expected.record_count != 0 && expected.record_count != entries.len() as u64 {
                    issues.push(actual.relative_path.clone());
                }
            }
            Err(_) => issues.push(actual.relative_path.clone()),
        },
        _ => {
            issues.push(actual.relative_path.clone());
        }
    }
    Ok(())
}

fn parent_chunk_relative_path(sidecar_relative_path: &str) -> Result<String> {
    let chunk_relative_path = sidecar_relative_path
        .rsplit_once('.')
        .map(|(prefix, _)| prefix.to_string())
        .ok_or_else(|| {
            FastKError::InvalidData(format!(
                "invalid sidecar relative path: {sidecar_relative_path}",
            ))
        })?;
    if !chunk_relative_path.ends_with(".chunk") {
        return Err(FastKError::InvalidData(format!(
            "sidecar path does not resolve to a chunk: {sidecar_relative_path}",
        )));
    }
    Ok(chunk_relative_path)
}

fn select_non_overlapping_chunks(chunks: Vec<ChunkMeta>) -> Result<Vec<ChunkMeta>> {
    let groups = overlap_groups(&chunks);
    if groups.is_empty() {
        return Ok(chunks);
    }

    let mut discarded = HashSet::new();
    for group in groups {
        let resolution = choose_surviving_chunk_for_overlap(&group);
        let Some(winner) = resolution.winner_relative_path else {
            return Err(FastKError::InvalidData(format!(
                "cannot rebuild manifest because overlap is ambiguous: {}",
                resolution.reason
            )));
        };
        for chunk in group {
            if chunk.relative_path != winner {
                discarded.insert(chunk.relative_path);
            }
        }
    }

    let mut survivors: Vec<_> = chunks
        .into_iter()
        .filter(|chunk| !discarded.contains(&chunk.relative_path))
        .collect();
    survivors.sort_by_key(|chunk| chunk.start_ts);
    Ok(survivors)
}

fn build_series_meta_from_chunks(
    _root: &Path,
    series_dir: &Path,
    chunks: &[ChunkMeta],
) -> Result<SeriesMeta> {
    let first_chunk_path = path::resolve_relative_path(series_dir, &chunks[0].relative_path);
    let mut file = std::fs::File::open(first_chunk_path)?;
    let header = ChunkHeader::read_from(&mut file)?;

    let name = series_dir
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| FastKError::InvalidData("invalid series dir name".to_string()))?
        .to_string();
    let category = series_dir
        .parent()
        .and_then(|value| value.file_name())
        .and_then(|value| value.to_str())
        .ok_or_else(|| FastKError::InvalidData("invalid category dir name".to_string()))?
        .to_string();
    let symbol = series_dir
        .parent()
        .and_then(|value| value.parent())
        .and_then(|value| value.file_name())
        .and_then(|value| value.to_str())
        .ok_or_else(|| FastKError::InvalidData("invalid symbol dir name".to_string()))?
        .to_string();

    let record_type = match header.schema_id {
        KLINE_SCHEMA_ID => RecordType::Kline,
        SCALAR_SCHEMA_ID => RecordType::Scalar,
        TRADE_SCHEMA_ID => RecordType::Trade,
        BBO_SCHEMA_ID => RecordType::Bbo,
        BOOK_DELTA_SCHEMA_ID => RecordType::BookDelta,
        other => {
            return Err(FastKError::InvalidData(format!(
                "unsupported schema_id during recovery: {other}",
            )))
        }
    };

    let mut meta = match record_type {
        RecordType::Kline => Catalog::build_kline_meta(&symbol, &name, header.timeframe_ms, 1, 1),
        RecordType::Scalar => Catalog::build_scalar_meta(
            &ScalarSeriesKey {
                symbol: symbol.clone(),
                category: category.clone(),
                name: name.clone(),
            },
            header.timeframe_ms,
        ),
        RecordType::Trade => Catalog::build_fixed_meta::<TradeRecord>(
            &symbol,
            &category,
            &name,
            header.timeframe_ms,
            &PartitionPolicy::hour(),
        ),
        RecordType::Bbo => Catalog::build_fixed_meta::<BboRecord>(
            &symbol,
            &category,
            &name,
            header.timeframe_ms,
            &PartitionPolicy::day(),
        ),
        RecordType::BookDelta => Catalog::build_fixed_meta::<BookDeltaRecord>(
            &symbol,
            &category,
            &name,
            header.timeframe_ms,
            &PartitionPolicy::hour(),
        ),
    };
    meta.category = category;
    meta.record_size = header.record_size;
    meta.schema_id = header.schema_id;
    meta.series_id = header
        .series_id
        .max(Catalog::series_id_for(&symbol, &meta.category, &name));
    meta.chunks = chunks.to_vec();
    meta.active_chunk_id = meta
        .chunks
        .iter()
        .rev()
        .find(|chunk| chunk.state == ChunkState::Active)
        .map(|chunk| chunk.chunk_id);
    meta.manifest_seq = 1;
    meta.updated_at = now_timestamp_ms();
    meta.created_at = meta.updated_at;
    meta.validate()?;
    Ok(meta)
}

fn normalize_dangling_merging_states(meta: &mut SeriesMeta) -> bool {
    let mut changed = false;
    let readable_chunks = meta.chunks.clone();
    for chunk in &mut meta.chunks {
        if chunk.state != ChunkState::Merging {
            continue;
        }

        let overlaps: Vec<_> = readable_chunks
            .iter()
            .filter(|candidate| {
                candidate.relative_path != chunk.relative_path
                    && candidate.generation > chunk.generation
                    && ranges_overlap(
                        candidate.start_ts,
                        candidate.end_ts,
                        chunk.start_ts,
                        chunk.end_ts,
                    )
            })
            .cloned()
            .collect();
        let resolution = choose_surviving_chunk_for_overlap(
            &overlaps
                .into_iter()
                .chain(std::iter::once(chunk.clone()))
                .collect::<Vec<_>>(),
        );
        if resolution.winner_relative_path.is_none()
            || resolution.winner_relative_path.as_deref() == Some(chunk.relative_path.as_str())
        {
            chunk.state = if meta.active_chunk_id == Some(chunk.chunk_id) {
                ChunkState::Active
            } else {
                ChunkState::Sealed
            };
            changed = true;
        }
    }
    changed
}

fn overlap_groups_as_strings(chunks: &[ChunkMeta]) -> Vec<Vec<String>> {
    overlap_groups(chunks)
        .into_iter()
        .map(|group| {
            let mut entries: Vec<_> = group.into_iter().map(|chunk| chunk.relative_path).collect();
            entries.sort();
            entries
        })
        .collect()
}

fn build_scrub_diagnostics(
    validation: &ManifestValidation,
    overlaps: &[OverlapResolution],
    verbose: bool,
) -> Vec<String> {
    if !verbose {
        return Vec::new();
    }

    let mut diagnostics = Vec::new();
    if !validation.manifest_exists {
        diagnostics.push("manifest missing; rebuild recommended".to_string());
    }
    if !validation.missing_chunks.is_empty() {
        diagnostics.push(format!(
            "missing chunks: {}",
            validation.missing_chunks.join(", ")
        ));
    }
    if !validation.missing_sidecars.is_empty() {
        diagnostics.push(format!(
            "missing sidecars: {}",
            validation.missing_sidecars.join(", ")
        ));
    }
    if !validation.chunk_metadata_mismatches.is_empty() {
        diagnostics.push(format!(
            "chunk metadata mismatches: {}",
            validation.chunk_metadata_mismatches.join(", ")
        ));
    }
    if !validation.sidecar_consistency_issues.is_empty() {
        diagnostics.push(format!(
            "sidecar consistency issues: {}",
            validation.sidecar_consistency_issues.join(", ")
        ));
    }
    for overlap in overlaps {
        diagnostics.push(format!(
            "overlap {} -> winner {:?} ({})",
            overlap.month_key, overlap.winner_relative_path, overlap.reason
        ));
    }
    diagnostics
}

fn overlap_groups(chunks: &[ChunkMeta]) -> Vec<Vec<ChunkMeta>> {
    let mut sorted = chunks.to_vec();
    sorted.sort_by_key(|chunk| chunk.start_ts);
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_end = i64::MIN;

    for chunk in sorted {
        if current.is_empty() {
            current_end = chunk.end_ts;
            current.push(chunk);
            continue;
        }

        if chunk.start_ts <= current_end {
            current_end = current_end.max(chunk.end_ts);
            current.push(chunk);
            continue;
        }

        if current.len() > 1 {
            groups.push(current);
        }
        current = vec![chunk];
        current_end = current[0].end_ts;
    }
    if current.len() > 1 {
        groups.push(current);
    }
    groups
}

fn decode_chunk_state(file_name: &str) -> ChunkState {
    if file_name.contains(".delta.") {
        ChunkState::Active
    } else {
        ChunkState::Sealed
    }
}

fn ranges_overlap(left_start: i64, left_end: i64, right_start: i64, right_end: i64) -> bool {
    left_start <= right_end && left_end >= right_start
}

fn now_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::chunk::kline_writer::{self, WriteKlineChunkOptions};
    use crate::engine::Catalog;
    use crate::index::{vix, zmap};
    use crate::storage::{manifest, path, recovery};
    use crate::types::{ChunkMeta, ChunkState, KlineRecord, ScalarRecord, SidecarMeta};

    #[test]
    fn startup_recovery_removes_temp_artifacts() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let stray = temp_dir.path().join("stray.tmp");
        std::fs::write(&stray, b"tmp").expect("temp file should write");

        let report = recovery::startup_recover(temp_dir.path()).expect("recovery should succeed");
        assert_eq!(report.removed_temp_files, 1);
        assert!(!stray.exists());
    }

    #[test]
    fn startup_recovery_dry_run_reports_without_mutating() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let stray = temp_dir.path().join("stray.tmp");
        std::fs::write(&stray, b"tmp").expect("temp file should write");

        let report = recovery::startup_recover_with_options(
            temp_dir.path(),
            recovery::RecoveryOptions { dry_run: true },
        )
        .expect("dry-run recovery should succeed");

        assert!(report.dry_run);
        assert_eq!(report.removed_temp_files, 1);
        assert!(stray.exists());
    }

    #[test]
    fn startup_recovery_adopts_chunk_written_before_manifest_update() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let root = temp_dir.path();
        let series_dir = path::kline_series_dir(root, "BTCUSDT", "1m");
        std::fs::create_dir_all(path::chunks_dir(&series_dir)).expect("chunks dir should exist");
        let meta = Catalog::build_kline_meta("BTCUSDT", "1m", 60_000, 100_000, 100_000);
        manifest::save_series_meta(&series_dir, &meta).expect("manifest should save");

        let chunk_path = path::chunk_path(&series_dir, "2024-02.chunk");
        let chunk_meta = kline_writer::write_chunk(
            &chunk_path,
            &meta,
            &[KlineRecord {
                ts: 1_706_745_600_000,
                open: 1,
                high: 2,
                low: 0,
                close: 1,
                volume: 1,
            }],
            &WriteKlineChunkOptions {
                chunk_id: 1,
                generation: 1,
                state: ChunkState::Sealed,
                relative_path: "chunks/2024-02.chunk".to_string(),
                sparse_index_every: 128,
            },
        )
        .expect("chunk should write");

        let report = recovery::startup_recover(root).expect("recovery should succeed");
        assert_eq!(report.adopted_chunks, 1);

        let repaired = manifest::load_series_meta(&series_dir).expect("manifest should reload");
        assert_eq!(repaired.chunks, vec![chunk_meta]);
    }

    #[test]
    fn orphan_artifacts_are_detected() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let root = temp_dir.path();
        let series_dir = path::kline_series_dir(root, "BTCUSDT", "1m");
        std::fs::create_dir_all(path::chunks_dir(&series_dir)).expect("chunks dir should exist");
        let meta = Catalog::build_kline_meta("BTCUSDT", "1m", 60_000, 100_000, 100_000);
        manifest::save_series_meta(&series_dir, &meta).expect("manifest should save");

        let orphan_path = path::chunk_path(&series_dir, "2024-02.chunk");
        kline_writer::write_chunk(
            &orphan_path,
            &meta,
            &[KlineRecord {
                ts: 1_706_745_600_000,
                open: 1,
                high: 2,
                low: 0,
                close: 1,
                volume: 1,
            }],
            &WriteKlineChunkOptions {
                chunk_id: 1,
                generation: 1,
                state: ChunkState::Sealed,
                relative_path: "chunks/2024-02.chunk".to_string(),
                sparse_index_every: 128,
            },
        )
        .expect("chunk should write");

        let artifacts = recovery::scan_orphan_artifacts(root).expect("orphan scan should succeed");
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].kind, "chunk");
    }

    #[test]
    fn overlap_resolution_prefers_newer_covering_chunk() {
        let resolution = recovery::choose_surviving_chunk_for_overlap(&vec![
            sample_chunk("chunks/2024-02.chunk", 100, 150, 1, ChunkState::Sealed),
            sample_chunk(
                "chunks/2024-02.g00000002.chunk",
                100,
                200,
                2,
                ChunkState::Sealed,
            ),
            sample_chunk(
                "chunks/2024-02.delta.g00000003.chunk",
                151,
                200,
                3,
                ChunkState::Active,
            ),
        ]);

        assert_eq!(
            resolution.winner_relative_path.as_deref(),
            Some("chunks/2024-02.g00000002.chunk")
        );
    }

    #[test]
    fn rebuild_manifest_from_fs_restores_scalar_sidecars() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let root = temp_dir.path();
        let series_dir = path::scalar_series_dir(root, "BTCUSDT", "indicator", "rsi14");
        std::fs::create_dir_all(path::chunks_dir(&series_dir)).expect("chunks dir should exist");
        let meta = Catalog::build_scalar_meta(
            &crate::types::ScalarSeriesKey {
                symbol: "BTCUSDT".to_string(),
                category: "indicator".to_string(),
                name: "rsi14".to_string(),
            },
            60_000,
        );
        let records = vec![
            ScalarRecord {
                ts: 1_706_745_600_000,
                value: 10,
            },
            ScalarRecord {
                ts: 1_706_745_660_000,
                value: 20,
            },
            ScalarRecord {
                ts: 1_706_745_720_000,
                value: 30,
            },
        ];
        let chunk_path = path::chunk_path(&series_dir, "2024-02.chunk");
        let chunk_meta = crate::chunk::scalar_writer::write_chunk(
            &chunk_path,
            &meta,
            &records,
            &crate::chunk::scalar_writer::WriteScalarChunkOptions {
                chunk_id: 1,
                generation: 1,
                state: ChunkState::Sealed,
                relative_path: "chunks/2024-02.chunk".to_string(),
                sparse_index_every: 128,
            },
        )
        .expect("scalar chunk should write");

        let zmap_entries = zmap::build_entries(&records, 2).expect("zmap should build");
        let zmap_path = path::resolve_relative_path(
            &series_dir,
            &path::sidecar_relative_path(&chunk_meta.relative_path, "zmap"),
        );
        zmap::write_entries(&zmap_path, &zmap_entries).expect("zmap should write");

        let vix_entries = vix::build_entries(&records);
        let vix_path = path::resolve_relative_path(
            &series_dir,
            &path::sidecar_relative_path(&chunk_meta.relative_path, "vix"),
        );
        vix::write_entries(&vix_path, &vix_entries).expect("vix should write");

        let rebuilt = recovery::rebuild_manifest_from_fs(root, &series_dir)
            .expect("manifest rebuild should succeed");
        assert_eq!(rebuilt.chunks.len(), 1);
        assert_eq!(rebuilt.chunks[0].sidecars.len(), 2);
    }

    #[test]
    fn scrub_reports_corrupt_sidecar_consistency_issues() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let root = temp_dir.path();
        let series_dir = path::scalar_series_dir(root, "BTCUSDT", "indicator", "rsi14");
        std::fs::create_dir_all(path::chunks_dir(&series_dir)).expect("chunks dir should exist");
        let meta = Catalog::build_scalar_meta(
            &crate::types::ScalarSeriesKey {
                symbol: "BTCUSDT".to_string(),
                category: "indicator".to_string(),
                name: "rsi14".to_string(),
            },
            60_000,
        );
        let records = vec![
            ScalarRecord {
                ts: 1_706_745_600_000,
                value: 10,
            },
            ScalarRecord {
                ts: 1_706_745_660_000,
                value: 20,
            },
            ScalarRecord {
                ts: 1_706_745_720_000,
                value: 30,
            },
        ];
        let chunk_path = path::chunk_path(&series_dir, "2024-02.chunk");
        let mut chunk_meta = crate::chunk::scalar_writer::write_chunk(
            &chunk_path,
            &meta,
            &records,
            &crate::chunk::scalar_writer::WriteScalarChunkOptions {
                chunk_id: 1,
                generation: 1,
                state: ChunkState::Sealed,
                relative_path: "chunks/2024-02.chunk".to_string(),
                sparse_index_every: 128,
            },
        )
        .expect("scalar chunk should write");

        let zmap_entries = zmap::build_entries(&records, 2).expect("zmap should build");
        let zmap_relative = path::sidecar_relative_path(&chunk_meta.relative_path, "zmap");
        let zmap_path = path::resolve_relative_path(&series_dir, &zmap_relative);
        zmap::write_entries(&zmap_path, &zmap_entries).expect("zmap should write");
        let vix_entries = vix::build_entries(&records);
        let vix_relative = path::sidecar_relative_path(&chunk_meta.relative_path, "vix");
        let vix_path = path::resolve_relative_path(&series_dir, &vix_relative);
        vix::write_entries(&vix_path, &vix_entries).expect("vix should write");

        chunk_meta.sidecars = vec![
            SidecarMeta {
                kind: "zmap".to_string(),
                relative_path: zmap_relative.clone(),
                generation: 1,
                checksum: crate::storage::fs::checksum64(
                    &std::fs::read(&zmap_path).expect("zmap bytes"),
                ),
                block_size: 2,
                record_count: zmap_entries.len() as u64,
            },
            SidecarMeta {
                kind: "vix".to_string(),
                relative_path: vix_relative.clone(),
                generation: 1,
                checksum: crate::storage::fs::checksum64(
                    &std::fs::read(&vix_path).expect("vix bytes"),
                ),
                block_size: 0,
                record_count: vix_entries.len() as u64,
            },
        ];
        let mut persisted = meta.clone();
        persisted.chunks.push(chunk_meta);
        manifest::save_series_meta(&series_dir, &persisted).expect("manifest should save");

        std::fs::write(&zmap_path, b"bad").expect("corrupt zmap should write");
        let reports = recovery::scrub_store(
            root,
            recovery::ValidationOptions {
                verbose: true,
                revalidate_checksums: true,
            },
        )
        .expect("scrub should succeed");

        assert_eq!(reports.len(), 1);
        assert!(!reports[0].validation.sidecar_consistency_issues.is_empty());
        assert!(!reports[0].diagnostics.is_empty());
    }

    #[test]
    fn recovery_normalizes_dangling_merging_state() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let root = temp_dir.path();
        let series_dir = path::kline_series_dir(root, "BTCUSDT", "1m");
        std::fs::create_dir_all(path::chunks_dir(&series_dir)).expect("chunks dir should exist");
        let mut meta = Catalog::build_kline_meta("BTCUSDT", "1m", 60_000, 100_000, 100_000);
        let chunk_path = path::chunk_path(&series_dir, "2024-02.chunk");
        let chunk_meta = kline_writer::write_chunk(
            &chunk_path,
            &meta,
            &[KlineRecord {
                ts: 1_706_745_600_000,
                open: 1,
                high: 2,
                low: 0,
                close: 1,
                volume: 1,
            }],
            &WriteKlineChunkOptions {
                chunk_id: 1,
                generation: 1,
                state: ChunkState::Sealed,
                relative_path: "chunks/2024-02.chunk".to_string(),
                sparse_index_every: 128,
            },
        )
        .expect("chunk should write");
        let mut merging_chunk = chunk_meta.clone();
        merging_chunk.state = ChunkState::Merging;
        meta.chunks.push(merging_chunk);
        manifest::save_series_meta(&series_dir, &meta).expect("manifest should save");

        recovery::startup_recover(root).expect("recovery should succeed");
        let repaired = manifest::load_series_meta(&series_dir).expect("manifest should load");
        assert_eq!(repaired.chunks[0].state, ChunkState::Sealed);
    }

    fn sample_chunk(
        relative_path: &str,
        start_ts: i64,
        end_ts: i64,
        generation: u32,
        state: ChunkState,
    ) -> ChunkMeta {
        ChunkMeta {
            chunk_id: generation as u64,
            month_key: "2024-02".to_string(),
            start_ts,
            end_ts,
            count: 10,
            state,
            layout_version: 2,
            header_len: 128,
            sparse_index_every: 128,
            sparse_index_offset: 256,
            sparse_index_len: 1,
            chunk_checksum: 1,
            generation,
            relative_path: relative_path.to_string(),
            sidecars: vec![SidecarMeta {
                kind: "zmap".to_string(),
                relative_path: format!("{relative_path}.zmap"),
                generation,
                checksum: 1,
                block_size: 0,
                record_count: 1,
            }],
        }
    }
}
