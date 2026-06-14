use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

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
    inner: Mutex<FileCacheState>,
    metrics: Arc<StoreMetrics>,
}

#[derive(Debug, Default)]
struct FileCacheState {
    order: VecDeque<ChunkCacheKey>,
    files: HashMap<ChunkCacheKey, File>,
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
            inner: Mutex::new(FileCacheState::default()),
            metrics,
        }
    }

    pub fn open(&self, key: ChunkCacheKey, path: &Path) -> Result<File> {
        let mut state = self.inner.lock().expect("file cache poisoned");

        if let Some(file) = state.files.get(&key) {
            let clone = file.try_clone()?;
            touch(&mut state.order, &key);
            self.metrics.record_chunk_file_hit();
            return Ok(clone);
        }
        self.metrics.record_chunk_file_miss();

        let open_started = Instant::now();
        let file = File::open(path)?;
        self.metrics
            .record_chunk_file_open_ns(open_started.elapsed().as_nanos() as u64);
        let clone = file.try_clone()?;
        state.files.insert(key.clone(), file);
        touch(&mut state.order, &key);

        while state.files.len() > self.capacity {
            if let Some(evicted) = state.order.pop_front() {
                state.files.remove(&evicted);
            }
        }

        Ok(clone)
    }

    pub fn invalidate(&self, key: &ChunkCacheKey) {
        let mut state = self.inner.lock().expect("file cache poisoned");
        state.files.remove(key);
        state.order.retain(|candidate| candidate != key);
    }

    pub fn clear(&self) {
        let mut state = self.inner.lock().expect("file cache poisoned");
        state.files.clear();
        state.order.clear();
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

    pub fn open_file(&self, chunk: &ChunkMeta, series_id: u64, series_dir: &Path) -> Result<File> {
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

fn touch(order: &mut VecDeque<ChunkCacheKey>, key: &ChunkCacheKey) {
    if let Some(position) = order.iter().position(|candidate| candidate == key) {
        order.remove(position);
    }
    order.push_back(key.clone());
}
