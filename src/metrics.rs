use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use serde::Serialize;

/// Instrumentation verbosity used by runtime metrics and benchmark runners.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[repr(u8)]
pub enum MetricsLevel {
    Off = 0,
    Basic = 1,
    Detailed = 2,
}

impl MetricsLevel {
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Off,
            2 => Self::Detailed,
            _ => Self::Basic,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Basic => "basic",
            Self::Detailed => "detailed",
        }
    }
}

impl Default for MetricsLevel {
    fn default() -> Self {
        Self::Basic
    }
}

/// Runtime counters used for benchmark instrumentation and acceptance checks.
#[derive(Debug)]
pub struct StoreMetrics {
    level: AtomicU8,
    bytes_read: AtomicU64,
    logical_bytes_written: AtomicU64,
    chunk_header_cache_hits: AtomicU64,
    chunk_header_cache_misses: AtomicU64,
    chunk_file_cache_hits: AtomicU64,
    chunk_file_cache_misses: AtomicU64,
    sidecar_cache_hits: AtomicU64,
    sidecar_cache_misses: AtomicU64,
    manifest_cache_hits: AtomicU64,
    manifest_cache_misses: AtomicU64,
    manifest_load_ns: AtomicU64,
    manifest_file_read_ns: AtomicU64,
    manifest_decode_ns: AtomicU64,
    manifest_chunk_materialize_ns: AtomicU64,
    manifest_sidecar_materialize_ns: AtomicU64,
    store_bootstrap_ns: AtomicU64,
    session_attach_ns: AtomicU64,
    session_prewarm_ns: AtomicU64,
    chunk_lookup_ns: AtomicU64,
    chunk_header_load_ns: AtomicU64,
    chunk_file_open_ns: AtomicU64,
    sparse_index_load_ns: AtomicU64,
    point_record_read_ns: AtomicU64,
    point_local_search_ns: AtomicU64,
    point_decode_ns: AtomicU64,
    sidecar_load_ns: AtomicU64,
}

impl Default for StoreMetrics {
    fn default() -> Self {
        Self::new(MetricsLevel::Basic)
    }
}

impl StoreMetrics {
    pub fn new(level: MetricsLevel) -> Self {
        Self {
            level: AtomicU8::new(level as u8),
            bytes_read: AtomicU64::new(0),
            logical_bytes_written: AtomicU64::new(0),
            chunk_header_cache_hits: AtomicU64::new(0),
            chunk_header_cache_misses: AtomicU64::new(0),
            chunk_file_cache_hits: AtomicU64::new(0),
            chunk_file_cache_misses: AtomicU64::new(0),
            sidecar_cache_hits: AtomicU64::new(0),
            sidecar_cache_misses: AtomicU64::new(0),
            manifest_cache_hits: AtomicU64::new(0),
            manifest_cache_misses: AtomicU64::new(0),
            manifest_load_ns: AtomicU64::new(0),
            manifest_file_read_ns: AtomicU64::new(0),
            manifest_decode_ns: AtomicU64::new(0),
            manifest_chunk_materialize_ns: AtomicU64::new(0),
            manifest_sidecar_materialize_ns: AtomicU64::new(0),
            store_bootstrap_ns: AtomicU64::new(0),
            session_attach_ns: AtomicU64::new(0),
            session_prewarm_ns: AtomicU64::new(0),
            chunk_lookup_ns: AtomicU64::new(0),
            chunk_header_load_ns: AtomicU64::new(0),
            chunk_file_open_ns: AtomicU64::new(0),
            sparse_index_load_ns: AtomicU64::new(0),
            point_record_read_ns: AtomicU64::new(0),
            point_local_search_ns: AtomicU64::new(0),
            point_decode_ns: AtomicU64::new(0),
            sidecar_load_ns: AtomicU64::new(0),
        }
    }

    pub fn shared() -> Arc<Self> {
        Self::shared_with_level(MetricsLevel::Basic)
    }

    pub fn shared_with_level(level: MetricsLevel) -> Arc<Self> {
        Arc::new(Self::new(level))
    }

    pub fn level(&self) -> MetricsLevel {
        MetricsLevel::from_u8(self.level.load(Ordering::Relaxed))
    }

    pub fn set_level(&self, level: MetricsLevel) {
        self.level.store(level as u8, Ordering::Relaxed);
    }

    pub fn reset(&self) {
        self.bytes_read.store(0, Ordering::Relaxed);
        self.logical_bytes_written.store(0, Ordering::Relaxed);
        self.chunk_header_cache_hits.store(0, Ordering::Relaxed);
        self.chunk_header_cache_misses.store(0, Ordering::Relaxed);
        self.chunk_file_cache_hits.store(0, Ordering::Relaxed);
        self.chunk_file_cache_misses.store(0, Ordering::Relaxed);
        self.sidecar_cache_hits.store(0, Ordering::Relaxed);
        self.sidecar_cache_misses.store(0, Ordering::Relaxed);
        self.manifest_cache_hits.store(0, Ordering::Relaxed);
        self.manifest_cache_misses.store(0, Ordering::Relaxed);
        self.manifest_load_ns.store(0, Ordering::Relaxed);
        self.manifest_file_read_ns.store(0, Ordering::Relaxed);
        self.manifest_decode_ns.store(0, Ordering::Relaxed);
        self.manifest_chunk_materialize_ns
            .store(0, Ordering::Relaxed);
        self.manifest_sidecar_materialize_ns
            .store(0, Ordering::Relaxed);
        self.store_bootstrap_ns.store(0, Ordering::Relaxed);
        self.session_attach_ns.store(0, Ordering::Relaxed);
        self.session_prewarm_ns.store(0, Ordering::Relaxed);
        self.chunk_lookup_ns.store(0, Ordering::Relaxed);
        self.chunk_header_load_ns.store(0, Ordering::Relaxed);
        self.chunk_file_open_ns.store(0, Ordering::Relaxed);
        self.sparse_index_load_ns.store(0, Ordering::Relaxed);
        self.point_record_read_ns.store(0, Ordering::Relaxed);
        self.point_local_search_ns.store(0, Ordering::Relaxed);
        self.point_decode_ns.store(0, Ordering::Relaxed);
        self.sidecar_load_ns.store(0, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StoreMetricsSnapshot {
        StoreMetricsSnapshot {
            metrics_level: self.level(),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            logical_bytes_written: self.logical_bytes_written.load(Ordering::Relaxed),
            chunk_header_cache_hits: self.chunk_header_cache_hits.load(Ordering::Relaxed),
            chunk_header_cache_misses: self.chunk_header_cache_misses.load(Ordering::Relaxed),
            chunk_file_cache_hits: self.chunk_file_cache_hits.load(Ordering::Relaxed),
            chunk_file_cache_misses: self.chunk_file_cache_misses.load(Ordering::Relaxed),
            sidecar_cache_hits: self.sidecar_cache_hits.load(Ordering::Relaxed),
            sidecar_cache_misses: self.sidecar_cache_misses.load(Ordering::Relaxed),
            manifest_cache_hits: self.manifest_cache_hits.load(Ordering::Relaxed),
            manifest_cache_misses: self.manifest_cache_misses.load(Ordering::Relaxed),
            manifest_load_ns: self.manifest_load_ns.load(Ordering::Relaxed),
            manifest_file_read_ns: self.manifest_file_read_ns.load(Ordering::Relaxed),
            manifest_decode_ns: self.manifest_decode_ns.load(Ordering::Relaxed),
            manifest_chunk_materialize_ns: self
                .manifest_chunk_materialize_ns
                .load(Ordering::Relaxed),
            manifest_sidecar_materialize_ns: self
                .manifest_sidecar_materialize_ns
                .load(Ordering::Relaxed),
            store_bootstrap_ns: self.store_bootstrap_ns.load(Ordering::Relaxed),
            session_attach_ns: self.session_attach_ns.load(Ordering::Relaxed),
            session_prewarm_ns: self.session_prewarm_ns.load(Ordering::Relaxed),
            chunk_lookup_ns: self.chunk_lookup_ns.load(Ordering::Relaxed),
            chunk_header_load_ns: self.chunk_header_load_ns.load(Ordering::Relaxed),
            chunk_file_open_ns: self.chunk_file_open_ns.load(Ordering::Relaxed),
            sparse_index_load_ns: self.sparse_index_load_ns.load(Ordering::Relaxed),
            point_record_read_ns: self.point_record_read_ns.load(Ordering::Relaxed),
            point_local_search_ns: self.point_local_search_ns.load(Ordering::Relaxed),
            point_decode_ns: self.point_decode_ns.load(Ordering::Relaxed),
            sidecar_load_ns: self.sidecar_load_ns.load(Ordering::Relaxed),
        }
    }

    pub fn record_bytes_read(&self, bytes: usize) {
        if self.level() != MetricsLevel::Off {
            self.bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    pub fn record_logical_bytes_written(&self, bytes: usize) {
        if self.level() != MetricsLevel::Off {
            self.logical_bytes_written
                .fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    pub fn record_chunk_header_hit(&self) {
        if self.level() != MetricsLevel::Off {
            self.chunk_header_cache_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_chunk_header_miss(&self) {
        if self.level() != MetricsLevel::Off {
            self.chunk_header_cache_misses
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_chunk_file_hit(&self) {
        if self.level() != MetricsLevel::Off {
            self.chunk_file_cache_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_chunk_file_miss(&self) {
        if self.level() != MetricsLevel::Off {
            self.chunk_file_cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_sidecar_hit(&self) {
        if self.level() != MetricsLevel::Off {
            self.sidecar_cache_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_sidecar_miss(&self) {
        if self.level() != MetricsLevel::Off {
            self.sidecar_cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_manifest_cache_hit(&self) {
        if self.level() != MetricsLevel::Off {
            self.manifest_cache_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_manifest_cache_miss(&self) {
        if self.level() != MetricsLevel::Off {
            self.manifest_cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_manifest_load_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.manifest_load_ns.fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_manifest_file_read_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.manifest_file_read_ns
                .fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_manifest_decode_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.manifest_decode_ns.fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_manifest_chunk_materialize_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.manifest_chunk_materialize_ns
                .fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_manifest_sidecar_materialize_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.manifest_sidecar_materialize_ns
                .fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_store_bootstrap_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.store_bootstrap_ns.fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_session_attach_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.session_attach_ns.fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_session_prewarm_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.session_prewarm_ns.fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_chunk_lookup_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.chunk_lookup_ns.fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_chunk_header_load_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.chunk_header_load_ns
                .fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_chunk_file_open_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.chunk_file_open_ns.fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_sparse_index_load_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.sparse_index_load_ns
                .fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_point_record_read_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.point_record_read_ns
                .fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_point_local_search_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.point_local_search_ns
                .fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_point_decode_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.point_decode_ns.fetch_add(nanos, Ordering::Relaxed);
        }
    }

    pub fn record_sidecar_load_ns(&self, nanos: u64) {
        if self.level() == MetricsLevel::Detailed {
            self.sidecar_load_ns.fetch_add(nanos, Ordering::Relaxed);
        }
    }
}

/// Snapshot of runtime counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct StoreMetricsSnapshot {
    pub metrics_level: MetricsLevel,
    pub bytes_read: u64,
    pub logical_bytes_written: u64,
    pub chunk_header_cache_hits: u64,
    pub chunk_header_cache_misses: u64,
    pub chunk_file_cache_hits: u64,
    pub chunk_file_cache_misses: u64,
    pub sidecar_cache_hits: u64,
    pub sidecar_cache_misses: u64,
    pub manifest_cache_hits: u64,
    pub manifest_cache_misses: u64,
    pub manifest_load_ns: u64,
    pub manifest_file_read_ns: u64,
    pub manifest_decode_ns: u64,
    pub manifest_chunk_materialize_ns: u64,
    pub manifest_sidecar_materialize_ns: u64,
    pub store_bootstrap_ns: u64,
    pub session_attach_ns: u64,
    pub session_prewarm_ns: u64,
    pub chunk_lookup_ns: u64,
    pub chunk_header_load_ns: u64,
    pub chunk_file_open_ns: u64,
    pub sparse_index_load_ns: u64,
    pub point_record_read_ns: u64,
    pub point_local_search_ns: u64,
    pub point_decode_ns: u64,
    pub sidecar_load_ns: u64,
}

impl StoreMetricsSnapshot {
    pub fn chunk_header_cache_hit_rate(self) -> f64 {
        ratio(self.chunk_header_cache_hits, self.chunk_header_cache_misses)
    }

    pub fn chunk_file_cache_hit_rate(self) -> f64 {
        ratio(self.chunk_file_cache_hits, self.chunk_file_cache_misses)
    }

    pub fn sidecar_cache_hit_rate(self) -> f64 {
        ratio(self.sidecar_cache_hits, self.sidecar_cache_misses)
    }

    pub fn manifest_cache_hit_rate(self) -> f64 {
        ratio(self.manifest_cache_hits, self.manifest_cache_misses)
    }
}

fn ratio(hits: u64, misses: u64) -> f64 {
    let total = hits + misses;
    if total == 0 {
        0.0
    } else {
        hits as f64 / total as f64
    }
}
