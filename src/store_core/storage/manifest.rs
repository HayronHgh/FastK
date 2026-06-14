use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::chunk::sparse_index::DEFAULT_SPARSE_INDEX_EVERY;
use crate::error::{FastKError, Result};
use crate::storage::fs;
use crate::storage::path;
use crate::types::{ChunkMeta, ChunkState, RecordType, SeriesMeta, SidecarMeta};

const MANIFEST_MAGIC: [u8; 8] = *b"FKMETA02";
const MANIFEST_VERSION: u32 = 2;
const MANIFEST_HEADER_LEN: usize = 24;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ManifestLoadStats {
    pub shared_cache_hit: bool,
    pub file_read_ns: u64,
    pub decode_ns: u64,
    pub chunk_materialize_ns: u64,
    pub sidecar_materialize_ns: u64,
}

#[derive(Debug, Clone)]
pub struct ManifestLoadReport {
    pub meta: Arc<SeriesMeta>,
    pub stats: ManifestLoadStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ManifestFingerprint {
    len: u64,
    modified_ns: u128,
}

#[derive(Debug, Clone)]
struct CachedManifest {
    fingerprint: ManifestFingerprint,
    meta: Arc<SeriesMeta>,
}

/// Loads `series.meta`, auto-migrating legacy JSON manifests.
pub fn load_series_meta(series_dir: &Path) -> Result<SeriesMeta> {
    Ok((*load_series_meta_shared(series_dir)?.meta).clone())
}

/// Returns the shared in-process manifest cache entry, if it already exists.
///
/// This path intentionally skips filesystem fingerprint validation and is meant for
/// read-heavy attach/bootstrap flows that run in the same process as FastK writes.
/// External modifications that bypass FastK are not observed until the validated
/// `load_series_meta_shared()` path is used.
pub fn load_series_meta_shared_cached(series_dir: &Path) -> Option<ManifestLoadReport> {
    let meta_path = path::series_meta_path(series_dir);
    let cache_key = meta_path.to_string_lossy().into_owned();
    manifest_cache()
        .read()
        .expect("manifest cache poisoned")
        .get(&cache_key)
        .map(|cached| ManifestLoadReport {
            meta: cached.meta.clone(),
            stats: ManifestLoadStats {
                shared_cache_hit: true,
                ..ManifestLoadStats::default()
            },
        })
}

/// Loads `series.meta` into a shared process cache, auto-migrating legacy JSON manifests.
pub fn load_series_meta_shared(series_dir: &Path) -> Result<ManifestLoadReport> {
    let meta_path = path::series_meta_path(series_dir);
    if !meta_path.exists() {
        return Err(FastKError::NotFound(format!(
            "series meta not found: {}",
            meta_path.display()
        )));
    }

    let cache_key = meta_path.to_string_lossy().into_owned();
    let fingerprint = manifest_fingerprint(&meta_path)?;
    if let Some(cached) = manifest_cache()
        .read()
        .expect("manifest cache poisoned")
        .get(&cache_key)
    {
        if cached.fingerprint == fingerprint {
            return Ok(ManifestLoadReport {
                meta: cached.meta.clone(),
                stats: ManifestLoadStats {
                    shared_cache_hit: true,
                    ..ManifestLoadStats::default()
                },
            });
        }
    }

    let read_started = Instant::now();
    let bytes = std::fs::read(&meta_path)?;
    let file_read_ns = read_started.elapsed().as_nanos() as u64;
    let (meta, stats) = if bytes.starts_with(&MANIFEST_MAGIC) {
        let (meta, stats) = decode_manifest_with_stats(&bytes)?;
        (meta, stats)
    } else {
        let decode_started = Instant::now();
        let legacy: LegacySeriesMeta = serde_json::from_slice(&bytes)?;
        let migrated = migrate_legacy_json(legacy, series_dir)?;
        save_series_meta(series_dir, &migrated)?;
        (
            migrated,
            ManifestLoadStats {
                shared_cache_hit: false,
                file_read_ns,
                decode_ns: decode_started.elapsed().as_nanos() as u64,
                ..ManifestLoadStats::default()
            },
        )
    };

    meta.validate()?;
    let meta = Arc::new(meta);
    manifest_cache()
        .write()
        .expect("manifest cache poisoned")
        .insert(
            cache_key,
            CachedManifest {
                fingerprint: manifest_fingerprint(&meta_path)?,
                meta: meta.clone(),
            },
        );
    Ok(ManifestLoadReport {
        meta,
        stats: ManifestLoadStats {
            file_read_ns,
            ..stats
        },
    })
}

/// Persists the binary series manifest.
pub fn save_series_meta(series_dir: &Path, meta: &SeriesMeta) -> Result<()> {
    meta.validate()?;
    let meta_path = path::series_meta_path(series_dir);
    let bytes = encode_manifest(meta)?;
    fs::atomic_write_replace(&meta_path, |writer| {
        writer.write_all(&bytes)?;
        Ok(())
    })?;
    manifest_cache()
        .write()
        .expect("manifest cache poisoned")
        .insert(
            meta_path.to_string_lossy().into_owned(),
            CachedManifest {
                fingerprint: manifest_fingerprint(&meta_path)?,
                meta: Arc::new(meta.clone()),
            },
        );
    Ok(())
}

/// Inserts or updates a chunk entry while keeping the manifest sorted by start timestamp.
pub fn upsert_chunk_meta(meta: &mut SeriesMeta, chunk_meta: ChunkMeta) -> Result<()> {
    if let Some(existing) = meta
        .chunks
        .iter_mut()
        .find(|candidate| candidate.chunk_id == chunk_meta.chunk_id)
    {
        *existing = chunk_meta;
    } else {
        meta.chunks.push(chunk_meta);
    }

    meta.chunks.sort_by_key(|chunk| chunk.start_ts);
    meta.manifest_seq = meta.manifest_seq.saturating_add(1);
    meta.updated_at = now_timestamp_ms();
    meta.validate()
}

pub fn replace_month_chunks(
    meta: &mut SeriesMeta,
    month_key: &str,
    replacement: ChunkMeta,
) -> Result<Vec<ChunkMeta>> {
    let mut removed = Vec::new();
    meta.chunks.retain(|chunk| {
        let keep = chunk.month_key != month_key;
        if !keep {
            removed.push(chunk.clone());
        }
        keep
    });
    meta.chunks.push(replacement);
    meta.chunks.sort_by_key(|chunk| chunk.start_ts);
    meta.active_chunk_id = meta
        .chunks
        .iter()
        .rev()
        .find(|chunk| chunk.state == ChunkState::Active)
        .map(|chunk| chunk.chunk_id);
    meta.manifest_seq = meta.manifest_seq.saturating_add(1);
    meta.updated_at = now_timestamp_ms();
    meta.validate()?;
    Ok(removed)
}

pub fn encode_manifest(meta: &SeriesMeta) -> Result<Vec<u8>> {
    meta.validate()?;

    let mut body = Vec::new();
    put_u32(&mut body, meta.record_type.as_u32());
    put_u32(&mut body, meta.record_size);
    put_u32(&mut body, meta.schema_id);
    put_u64(&mut body, meta.series_id);
    put_u64(&mut body, meta.manifest_seq);
    put_i64(&mut body, meta.created_at);
    put_i64(&mut body, meta.updated_at);
    put_i64(&mut body, meta.timeframe_ms);
    put_i64(&mut body, meta.price_scale);
    put_i64(&mut body, meta.volume_scale);
    put_str(&mut body, &meta.chunk_unit)?;
    put_u64(&mut body, meta.active_chunk_id.unwrap_or(0));
    put_str(&mut body, &meta.symbol)?;
    put_str(&mut body, &meta.category)?;
    put_str(&mut body, &meta.name)?;
    put_u32(&mut body, meta.chunks.len() as u32);

    for chunk in &meta.chunks {
        put_u64(&mut body, chunk.chunk_id);
        put_str(&mut body, &chunk.month_key)?;
        put_i64(&mut body, chunk.start_ts);
        put_i64(&mut body, chunk.end_ts);
        put_u64(&mut body, chunk.count);
        put_u8(&mut body, chunk.state.as_u8());
        put_u32(&mut body, chunk.layout_version);
        put_u32(&mut body, chunk.header_len);
        put_u32(&mut body, chunk.sparse_index_every);
        put_u64(&mut body, chunk.sparse_index_offset);
        put_u32(&mut body, chunk.sparse_index_len);
        put_u64(&mut body, chunk.chunk_checksum);
        put_u32(&mut body, chunk.generation);
        put_str(&mut body, &chunk.relative_path)?;
        put_u32(&mut body, chunk.sidecars.len() as u32);
        for sidecar in &chunk.sidecars {
            put_str(&mut body, &sidecar.kind)?;
            put_str(&mut body, &sidecar.relative_path)?;
            put_u32(&mut body, sidecar.generation);
            put_u64(&mut body, sidecar.checksum);
            put_u32(&mut body, sidecar.block_size);
            put_u64(&mut body, sidecar.record_count);
        }
    }

    let checksum = fs::checksum64(&body);
    let mut bytes = Vec::with_capacity(MANIFEST_HEADER_LEN + body.len());
    bytes.extend_from_slice(&MANIFEST_MAGIC);
    put_u32(&mut bytes, MANIFEST_VERSION);
    put_u32(&mut bytes, meta.flags);
    put_u64(&mut bytes, checksum);
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

#[cfg(test)]
pub fn decode_manifest(bytes: &[u8]) -> Result<SeriesMeta> {
    Ok(decode_manifest_with_stats(bytes)?.0)
}

fn decode_manifest_with_stats(bytes: &[u8]) -> Result<(SeriesMeta, ManifestLoadStats)> {
    if bytes.len() < MANIFEST_HEADER_LEN {
        return Err(FastKError::InvalidData("manifest is too short".to_string()));
    }
    if bytes[..8] != MANIFEST_MAGIC {
        return Err(FastKError::InvalidData(
            "invalid manifest magic".to_string(),
        ));
    }

    let mut cursor = Cursor::new(&bytes[8..]);
    let version = cursor.read_u32()?;
    let flags = cursor.read_u32()?;
    let checksum = cursor.read_u64()?;
    if version != MANIFEST_VERSION {
        return Err(FastKError::InvalidData(format!(
            "unsupported manifest version: {version}",
        )));
    }

    let body = &bytes[MANIFEST_HEADER_LEN..];
    if fs::checksum64(body) != checksum {
        return Err(FastKError::InvalidData(
            "manifest checksum mismatch".to_string(),
        ));
    }

    let decode_started = Instant::now();
    let mut body_cursor = Cursor::new(body);
    let record_type = RecordType::from_u32(body_cursor.read_u32()?)?;
    let record_size = body_cursor.read_u32()?;
    let schema_id = body_cursor.read_u32()?;
    let series_id = body_cursor.read_u64()?;
    let manifest_seq = body_cursor.read_u64()?;
    let created_at = body_cursor.read_i64()?;
    let updated_at = body_cursor.read_i64()?;
    let timeframe_ms = body_cursor.read_i64()?;
    let price_scale = body_cursor.read_i64()?;
    let volume_scale = body_cursor.read_i64()?;
    let chunk_unit = body_cursor.read_string()?;
    let active_chunk_id = match body_cursor.read_u64()? {
        0 => None,
        value => Some(value),
    };
    let symbol = body_cursor.read_string()?;
    let category = body_cursor.read_string()?;
    let name = body_cursor.read_string()?;
    let chunk_count = body_cursor.read_u32()? as usize;
    let mut chunks = Vec::with_capacity(chunk_count);
    let mut chunk_materialize_ns = 0u64;
    let mut sidecar_materialize_ns = 0u64;

    for _ in 0..chunk_count {
        let chunk_started = Instant::now();
        let chunk_id = body_cursor.read_u64()?;
        let month_key = body_cursor.read_string()?;
        let start_ts = body_cursor.read_i64()?;
        let end_ts = body_cursor.read_i64()?;
        let count = body_cursor.read_u64()?;
        let state = ChunkState::from_u8(body_cursor.read_u8()?)?;
        let layout_version = body_cursor.read_u32()?;
        let header_len = body_cursor.read_u32()?;
        let sparse_index_every = body_cursor.read_u32()?;
        let sparse_index_offset = body_cursor.read_u64()?;
        let sparse_index_len = body_cursor.read_u32()?;
        let chunk_checksum = body_cursor.read_u64()?;
        let generation = body_cursor.read_u32()?;
        let relative_path = body_cursor.read_string()?;
        let sidecar_count = body_cursor.read_u32()? as usize;
        let mut sidecars = Vec::with_capacity(sidecar_count);
        chunk_materialize_ns =
            chunk_materialize_ns.saturating_add(chunk_started.elapsed().as_nanos() as u64);
        let sidecar_started = Instant::now();
        for _ in 0..sidecar_count {
            sidecars.push(SidecarMeta {
                kind: body_cursor.read_string()?,
                relative_path: body_cursor.read_string()?,
                generation: body_cursor.read_u32()?,
                checksum: body_cursor.read_u64()?,
                block_size: body_cursor.read_u32()?,
                record_count: body_cursor.read_u64()?,
            });
        }
        sidecar_materialize_ns =
            sidecar_materialize_ns.saturating_add(sidecar_started.elapsed().as_nanos() as u64);

        chunks.push(ChunkMeta {
            chunk_id,
            month_key,
            start_ts,
            end_ts,
            count,
            state,
            layout_version,
            header_len,
            sparse_index_every,
            sparse_index_offset,
            sparse_index_len,
            chunk_checksum,
            generation,
            relative_path,
            sidecars,
        });
    }

    let meta = SeriesMeta {
        symbol,
        category,
        name,
        timeframe_ms,
        record_type,
        record_size,
        schema_id,
        price_scale,
        volume_scale,
        chunk_unit,
        series_id,
        manifest_seq,
        created_at,
        updated_at,
        flags,
        active_chunk_id,
        chunks,
    };

    Ok((
        meta,
        ManifestLoadStats {
            shared_cache_hit: false,
            file_read_ns: 0,
            decode_ns: decode_started.elapsed().as_nanos() as u64,
            chunk_materialize_ns,
            sidecar_materialize_ns,
        },
    ))
}

fn manifest_cache() -> &'static RwLock<HashMap<String, CachedManifest>> {
    static MANIFEST_CACHE: OnceLock<RwLock<HashMap<String, CachedManifest>>> = OnceLock::new();
    MANIFEST_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn manifest_fingerprint(path: &Path) -> Result<ManifestFingerprint> {
    let metadata = std::fs::metadata(path)?;
    let modified_ns = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    Ok(ManifestFingerprint {
        len: metadata.len(),
        modified_ns,
    })
}

fn migrate_legacy_json(legacy: LegacySeriesMeta, series_dir: &Path) -> Result<SeriesMeta> {
    let now = now_timestamp_ms();
    let chunks = legacy
        .chunks
        .into_iter()
        .enumerate()
        .map(|(index, chunk)| {
            let month_key = path::month_key(chunk.start_ts)?;
            Ok(ChunkMeta {
                chunk_id: (index + 1) as u64,
                month_key,
                start_ts: chunk.start_ts,
                end_ts: chunk.end_ts,
                count: chunk.count,
                state: ChunkState::Sealed,
                layout_version: 1,
                header_len: 88,
                sparse_index_every: DEFAULT_SPARSE_INDEX_EVERY,
                sparse_index_offset: 0,
                sparse_index_len: 0,
                chunk_checksum: 0,
                generation: 1,
                relative_path: path::chunk_relative_path(&chunk.file_name),
                sidecars: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let meta = SeriesMeta {
        symbol: legacy.symbol,
        category: legacy.category,
        name: legacy.name,
        timeframe_ms: legacy.timeframe_ms,
        record_type: RecordType::Kline,
        record_size: legacy.record_size,
        schema_id: legacy.schema_id,
        price_scale: legacy.price_scale,
        volume_scale: legacy.volume_scale,
        chunk_unit: legacy.chunk_unit,
        series_id: legacy.series_id,
        manifest_seq: 1,
        created_at: now,
        updated_at: now,
        flags: 0,
        active_chunk_id: None,
        chunks,
    };

    meta.validate()?;
    let _ = series_dir;
    Ok(meta)
}

fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|_| {
        FastKError::InvalidInput(format!(
            "string field too long for manifest: {} bytes",
            bytes.len()
        ))
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn now_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.offset + len > self.bytes.len() {
            return Err(FastKError::InvalidData(
                "manifest ended unexpectedly".to_string(),
            ));
        }
        let slice = &self.bytes[self.offset..self.offset + len];
        self.offset += len;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| FastKError::InvalidData("invalid u32 field".to_string()))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| FastKError::InvalidData("invalid u64 field".to_string()))?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| FastKError::InvalidData("invalid i64 field".to_string()))?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_string(&mut self) -> Result<String> {
        let len_bytes: [u8; 2] = self
            .read_exact(2)?
            .try_into()
            .map_err(|_| FastKError::InvalidData("invalid string length".to_string()))?;
        let len = u16::from_le_bytes(len_bytes) as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|err| FastKError::InvalidData(format!("invalid utf8 string: {err}")))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LegacyChunkMeta {
    file_name: String,
    start_ts: i64,
    end_ts: i64,
    count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LegacySeriesMeta {
    symbol: String,
    category: String,
    name: String,
    timeframe_ms: i64,
    record_size: u32,
    schema_id: u32,
    price_scale: i64,
    volume_scale: i64,
    chunk_unit: String,
    series_id: u64,
    chunks: Vec<LegacyChunkMeta>,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::storage::manifest::{
        decode_manifest, encode_manifest, load_series_meta, load_series_meta_shared,
    };
    use crate::storage::path;
    use crate::types::{ChunkMeta, ChunkState, RecordType, SeriesMeta};

    #[test]
    fn manifest_roundtrip_preserves_all_fields() {
        let meta = sample_meta();
        let encoded = encode_manifest(&meta).expect("encode should succeed");
        let decoded = decode_manifest(&encoded).expect("decode should succeed");

        assert_eq!(decoded, meta);
    }

    #[test]
    fn manifest_checksum_mismatch_is_rejected() {
        let meta = sample_meta();
        let mut encoded = encode_manifest(&meta).expect("encode should succeed");
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;

        let err = decode_manifest(&encoded).expect_err("checksum mismatch should fail");
        assert!(matches!(err, crate::FastKError::InvalidData(_)));
    }

    #[test]
    fn manifest_version_mismatch_is_rejected() {
        let meta = sample_meta();
        let mut encoded = encode_manifest(&meta).expect("encode should succeed");
        encoded[8..12].copy_from_slice(&999u32.to_le_bytes());

        let err = decode_manifest(&encoded).expect_err("version mismatch should fail");
        assert!(matches!(err, crate::FastKError::InvalidData(_)));
    }

    #[test]
    fn legacy_json_is_migrated_to_binary_manifest() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let series_dir = path::kline_series_dir(temp_dir.path(), "BTCUSDT", "1m");
        std::fs::create_dir_all(path::chunks_dir(&series_dir))
            .expect("series dir should be created");
        let manifest_path = path::series_meta_path(&series_dir);
        std::fs::write(
            &manifest_path,
            serde_json::to_vec(&serde_json::json!({
                "symbol": "BTCUSDT",
                "category": "kline",
                "name": "1m",
                "timeframe_ms": 60000,
                "record_size": 48,
                "schema_id": 1,
                "price_scale": 100000,
                "volume_scale": 100000,
                "chunk_unit": "month",
                "series_id": 99,
                "chunks": [
                    {
                        "file_name": "2024-02.chunk",
                        "start_ts": 100,
                        "end_ts": 200,
                        "count": 2
                    }
                ]
            }))
            .expect("legacy json should serialize"),
        )
        .expect("legacy manifest should write");

        let meta = load_series_meta(&series_dir).expect("legacy manifest should migrate");
        assert_eq!(meta.record_type, RecordType::Kline);
        assert_eq!(meta.chunks[0].relative_path, "chunks/2024-02.chunk");

        let bytes = std::fs::read(&manifest_path).expect("migrated binary manifest should exist");
        assert!(bytes.starts_with(b"FKMETA02"));
    }

    #[test]
    fn shared_manifest_cache_reuses_decoded_meta_for_reopen_like_loads() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let series_dir = path::kline_series_dir(temp_dir.path(), "BTCUSDT", "1m");
        std::fs::create_dir_all(path::chunks_dir(&series_dir))
            .expect("series dir should be created");
        let meta = sample_meta();
        crate::storage::manifest::save_series_meta(&series_dir, &meta)
            .expect("manifest should save");

        let first = load_series_meta_shared(&series_dir).expect("first load should succeed");
        assert!(first.stats.shared_cache_hit);

        let second = load_series_meta_shared(&series_dir).expect("second load should succeed");
        assert!(second.stats.shared_cache_hit);
        assert_eq!(*second.meta, meta);
        assert_eq!(second.stats.file_read_ns, 0);
        assert_eq!(second.stats.decode_ns, 0);
    }

    fn sample_meta() -> SeriesMeta {
        SeriesMeta {
            symbol: "BTCUSDT".to_string(),
            category: "kline".to_string(),
            name: "1m".to_string(),
            timeframe_ms: 60_000,
            record_type: RecordType::Kline,
            record_size: 48,
            schema_id: 1,
            price_scale: 100_000,
            volume_scale: 100_000,
            chunk_unit: "month".to_string(),
            series_id: 42,
            manifest_seq: 7,
            created_at: 1000,
            updated_at: 2000,
            flags: 9,
            active_chunk_id: Some(5),
            chunks: vec![ChunkMeta {
                chunk_id: 5,
                month_key: "2024-02".to_string(),
                start_ts: 10,
                end_ts: 20,
                count: 2,
                state: ChunkState::Active,
                layout_version: 2,
                header_len: 128,
                sparse_index_every: 128,
                sparse_index_offset: 224,
                sparse_index_len: 1,
                chunk_checksum: 123,
                generation: 3,
                relative_path: "chunks/2024-02.chunk".to_string(),
                sidecars: Vec::new(),
            }],
        }
    }
}
