use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::chunk::file::ChunkFile;
use crate::chunk::header::ChunkHeader;
use crate::chunk::sparse_index::{self, SparseIndexEntry};
use crate::error::Result;
use crate::metrics::StoreMetrics;
use crate::types::ChunkMeta;

const DEFAULT_FILE_CACHE_CAPACITY: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChunkCacheKey {
    pub chunk_id: u64,
    pub generation: u64,
    pub series_id: u64,
    pub relative_path: String,
}

impl ChunkCacheKey {
    pub fn from_meta(chunk: &ChunkMeta, series_id: u64) -> Self {
        Self {
            chunk_id: chunk.chunk_id,
            generation: chunk.generation as u64,
            series_id,
            relative_path: chunk.relative_path.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CachedChunkLayout {
    pub header: ChunkHeader,
    pub sparse_index: Arc<Vec<SparseIndexEntry>>,
}

#[derive(Debug)]
pub struct ChunkHeaderCache {
    inner: Mutex<HashMap<ChunkCacheKey, Arc<CachedChunkLayout>>>,
}

impl ChunkHeaderCache {
    pub fn new(_metrics: Arc<StoreMetrics>) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &ChunkCacheKey) -> Option<Arc<CachedChunkLayout>> {
        self.inner
            .lock()
            .expect("header cache poisoned")
            .get(key)
            .cloned()
    }

    pub fn insert(&self, key: ChunkCacheKey, layout: Arc<CachedChunkLayout>) {
        self.inner
            .lock()
            .expect("header cache poisoned")
            .insert(key, layout);
    }

    pub fn invalidate(&self, key: &ChunkCacheKey) {
        self.inner
            .lock()
            .expect("header cache poisoned")
            .remove(key);
    }

    pub fn clear(&self) {
        self.inner.lock().expect("header cache poisoned").clear();
    }
}

#[derive(Debug)]
pub struct ChunkFileCache {
    capacity: usize,
    inner: RwLock<FileCacheState>,
    metrics: Arc<StoreMetrics>,
}

#[derive(Debug, Default)]
struct FileCacheState {
    order: VecDeque<ChunkCacheKey>,
    files: HashMap<ChunkCacheKey, CachedFile>,
}

#[derive(Debug)]
struct CachedFile {
    file: Arc<File>,
    referenced: AtomicBool,
}

impl CachedFile {
    fn reader(&self) -> ChunkFile {
        // A hot entry needs neither queue mutation nor repeated cache-line writes.
        if !self.referenced.load(Ordering::Relaxed) {
            self.referenced.store(true, Ordering::Relaxed);
        }
        ChunkFile::new(self.file.clone())
    }
}

impl Default for ChunkFileCache {
    fn default() -> Self {
        Self::new(DEFAULT_FILE_CACHE_CAPACITY, StoreMetrics::shared())
    }
}

impl ChunkFileCache {
    pub fn new(capacity: usize, metrics: Arc<StoreMetrics>) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: RwLock::new(FileCacheState::default()),
            metrics,
        }
    }

    pub fn open(&self, key: ChunkCacheKey, path: &Path) -> Result<ChunkFile> {
        {
            let state = self.inner.read().expect("file cache poisoned");
            if let Some(file) = state.files.get(&key) {
                self.metrics.record_chunk_file_hit();
                return Ok(file.reader());
            }
        }
        self.metrics.record_chunk_file_miss();
        let started = Instant::now();
        let opened = Arc::new(File::open(path)?);
        self.metrics
            .record_chunk_file_open_ns(started.elapsed().as_nanos() as u64);
        let mut state = self.inner.write().expect("file cache poisoned");
        if let Some(file) = state.files.get(&key) {
            return Ok(file.reader());
        }
        // Second chance eviction keeps hits read-only. Only a cache miss changes
        // the bounded queue; an in-flight reader retains its own Arc<File>.
        let evicted = if state.files.len() == self.capacity {
            loop {
                let candidate = state.order.pop_front().expect("nonempty file cache");
                if state.files[&candidate]
                    .referenced
                    .swap(false, Ordering::Relaxed)
                {
                    state.order.push_back(candidate);
                } else {
                    break state.files.remove(&candidate);
                }
            }
        } else {
            None
        };
        let reader = ChunkFile::new(opened.clone());
        state.order.push_back(key.clone());
        state.files.insert(
            key,
            CachedFile {
                file: opened,
                referenced: AtomicBool::new(false),
            },
        );
        drop(state);
        // Closing a file may enter the kernel; do it after releasing the cache lock.
        drop(evicted);
        Ok(reader)
    }

    pub fn invalidate(&self, key: &ChunkCacheKey) {
        let mut state = self.inner.write().expect("file cache poisoned");
        let removed = state.files.remove(key);
        state.order.retain(|candidate| candidate != key);
        drop(state);
        drop(removed);
    }

    pub fn clear(&self) {
        let mut state = self.inner.write().expect("file cache poisoned");
        let removed = std::mem::take(&mut *state);
        drop(state);
        drop(removed);
    }
}

#[derive(Debug)]
pub struct ChunkRuntime {
    headers: ChunkHeaderCache,
    files: ChunkFileCache,
    metrics: Arc<StoreMetrics>,
}

impl ChunkRuntime {
    pub fn new(file_cache_capacity: usize) -> Self {
        let metrics = StoreMetrics::shared();
        Self {
            headers: ChunkHeaderCache::new(metrics.clone()),
            files: ChunkFileCache::new(file_cache_capacity, metrics.clone()),
            metrics,
        }
    }
}

impl ChunkRuntime {
    pub fn get_layout(
        &self,
        chunk: &ChunkMeta,
        series_id: u64,
        series_dir: &Path,
    ) -> Result<Arc<CachedChunkLayout>> {
        let key = ChunkCacheKey::from_meta(chunk, series_id);
        if let Some(layout) = self.headers.get(&key) {
            self.metrics.record_chunk_header_hit();
            return Ok(layout);
        }

        self.metrics.record_chunk_header_miss();
        let path = series_dir.join(&chunk.relative_path);
        let mut file = self.files.open(key.clone(), &path)?;
        file.seek(SeekFrom::Start(0))?;

        let header_started = Instant::now();
        let header = ChunkHeader::read_from(&mut file)?;
        self.metrics
            .record_chunk_header_load_ns(header_started.elapsed().as_nanos() as u64);

        let sparse_started = Instant::now();
        let sparse_index = Arc::new(sparse_index::read_from_file(&mut file, &header)?);
        self.metrics
            .record_sparse_index_load_ns(sparse_started.elapsed().as_nanos() as u64);

        let layout = Arc::new(CachedChunkLayout {
            header,
            sparse_index,
        });
        self.headers.insert(key, layout.clone());
        Ok(layout)
    }

    pub fn open_file(
        &self,
        chunk: &ChunkMeta,
        series_id: u64,
        series_dir: &Path,
    ) -> Result<ChunkFile> {
        let key = ChunkCacheKey::from_meta(chunk, series_id);
        let path = series_dir.join(&chunk.relative_path);
        self.files.open(key, &path)
    }

    pub fn invalidate(&self, chunk: &ChunkMeta, series_id: u64) {
        let key = ChunkCacheKey::from_meta(chunk, series_id);
        self.headers.invalidate(&key);
        self.files.invalidate(&key);
    }

    pub fn metrics(&self) -> Arc<StoreMetrics> {
        self.metrics.clone()
    }

    pub fn clear(&self) {
        self.headers.clear();
        self.files.clear();
    }

    pub fn clear_layouts(&self) {
        self.headers.clear();
    }
}

impl Default for ChunkRuntime {
    fn default() -> Self {
        Self::new(DEFAULT_FILE_CACHE_CAPACITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn key(id: u64) -> ChunkCacheKey {
        ChunkCacheKey {
            chunk_id: id,
            generation: 0,
            series_id: 0,
            relative_path: id.to_string(),
        }
    }

    #[test]
    fn eviction_and_invalidation_keep_in_flight_files_alive_and_retry_failed_opens() {
        let root = tempfile::tempdir().unwrap();
        for id in 0..4 {
            std::fs::write(root.path().join(id.to_string()), [id as u8]).unwrap();
        }
        let cache = ChunkFileCache::new(2, StoreMetrics::shared());
        let mut pinned = cache.open(key(0), &root.path().join("0")).unwrap();
        drop(cache.open(key(1), &root.path().join("1")).unwrap());
        // Give the first entry a second chance; the untouched entry is evicted.
        drop(cache.open(key(0), &root.path().join("0")).unwrap());
        drop(cache.open(key(2), &root.path().join("2")).unwrap());
        {
            let state = cache.inner.read().unwrap();
            assert_eq!(state.files.len(), 2);
            assert!(state.files.contains_key(&key(0)));
            assert!(!state.files.contains_key(&key(1)));
        }
        cache.invalidate(&key(0));
        cache.clear();
        let mut byte = [255];
        pinned.read_exact(&mut byte).unwrap();
        assert_eq!(byte, [0]);
        assert!(cache.open(key(3), &root.path().join("missing")).is_err());
        drop(cache.open(key(3), &root.path().join("3")).unwrap());
        let state = cache.inner.read().unwrap();
        assert_eq!(state.files.len(), 1);
        assert_eq!(state.order.len(), 1);
    }

    #[test]
    fn concurrent_misses_hits_and_eviction_stay_bounded() {
        let root = tempfile::tempdir().unwrap();
        for id in 0..8 {
            std::fs::write(root.path().join(id.to_string()), [id as u8; 32]).unwrap();
        }
        let cache = ChunkFileCache::new(2, StoreMetrics::shared());
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for id in 0..8 {
                let (cache, barrier, root) = (&cache, &barrier, root.path());
                scope.spawn(move || {
                    barrier.wait();
                    for turn in 0..300 {
                        let id = (id + turn) % 8;
                        let mut reader = cache.open(key(id), &root.join(id.to_string())).unwrap();
                        let mut bytes = [0; 32];
                        reader.read_exact(&mut bytes).unwrap();
                        assert_eq!(bytes, [id as u8; 32]);
                        let state = cache.inner.read().unwrap();
                        assert!(state.files.len() <= 2);
                        assert_eq!(state.files.len(), state.order.len());
                    }
                });
            }
        });
    }
}
