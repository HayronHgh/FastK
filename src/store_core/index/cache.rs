use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::error::Result;
use crate::index::{vix, zmap, ValueIndexEntry, ZoneMapEntry};
use crate::metrics::StoreMetrics;

const DEFAULT_SIDECAR_CACHE_CAPACITY: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SidecarCacheKey {
    pub generation: u32,
    pub relative_path: String,
}

#[derive(Debug)]
pub struct ScalarSidecarRuntime {
    capacity: usize,
    zmap: Mutex<SidecarCacheState<Vec<ZoneMapEntry>>>,
    vix: Mutex<SidecarCacheState<Vec<ValueIndexEntry>>>,
    metrics: Arc<StoreMetrics>,
}

#[derive(Debug, Default)]
struct SidecarCacheState<T> {
    order: VecDeque<SidecarCacheKey>,
    values: HashMap<SidecarCacheKey, Arc<T>>,
}

impl ScalarSidecarRuntime {
    pub fn new(metrics: Arc<StoreMetrics>) -> Self {
        Self {
            capacity: DEFAULT_SIDECAR_CACHE_CAPACITY,
            zmap: Mutex::new(SidecarCacheState::default()),
            vix: Mutex::new(SidecarCacheState::default()),
            metrics,
        }
    }

    pub fn get_zmap(&self, key: SidecarCacheKey, path: &Path) -> Result<Arc<Vec<ZoneMapEntry>>> {
        {
            let mut state = self.zmap.lock().expect("zmap cache poisoned");
            if let Some(entries) = state.values.get(&key) {
                self.metrics.record_sidecar_hit();
                let entries = entries.clone();
                touch(&mut state.order, &key);
                return Ok(entries);
            }
        }

        self.metrics.record_sidecar_miss();
        let started = Instant::now();
        let entries = Arc::new(zmap::read_entries(path)?);
        self.metrics
            .record_sidecar_load_ns(started.elapsed().as_nanos() as u64);
        let mut state = self.zmap.lock().expect("zmap cache poisoned");
        state.values.insert(key.clone(), entries.clone());
        touch(&mut state.order, &key);
        trim(&mut state, self.capacity);
        Ok(entries)
    }

    pub fn get_vix(&self, key: SidecarCacheKey, path: &Path) -> Result<Arc<Vec<ValueIndexEntry>>> {
        {
            let mut state = self.vix.lock().expect("vix cache poisoned");
            if let Some(entries) = state.values.get(&key) {
                self.metrics.record_sidecar_hit();
                let entries = entries.clone();
                touch(&mut state.order, &key);
                return Ok(entries);
            }
        }

        self.metrics.record_sidecar_miss();
        let started = Instant::now();
        let entries = Arc::new(vix::read_entries(path)?);
        self.metrics
            .record_sidecar_load_ns(started.elapsed().as_nanos() as u64);
        let mut state = self.vix.lock().expect("vix cache poisoned");
        state.values.insert(key.clone(), entries.clone());
        touch(&mut state.order, &key);
        trim(&mut state, self.capacity);
        Ok(entries)
    }

    pub fn invalidate(&self, relative_path_prefix: &str) {
        invalidate_matching(
            &mut self.zmap.lock().expect("zmap cache poisoned"),
            relative_path_prefix,
        );
        invalidate_matching(
            &mut self.vix.lock().expect("vix cache poisoned"),
            relative_path_prefix,
        );
    }

    pub fn clear(&self) {
        clear_state(&mut self.zmap.lock().expect("zmap cache poisoned"));
        clear_state(&mut self.vix.lock().expect("vix cache poisoned"));
    }
}

fn invalidate_matching<T>(state: &mut SidecarCacheState<T>, relative_path_prefix: &str) {
    state
        .values
        .retain(|key, _| !key.relative_path.starts_with(relative_path_prefix));
    state
        .order
        .retain(|key| !key.relative_path.starts_with(relative_path_prefix));
}

fn trim<T>(state: &mut SidecarCacheState<T>, capacity: usize) {
    while state.values.len() > capacity {
        if let Some(evicted) = state.order.pop_front() {
            state.values.remove(&evicted);
        }
    }
}

fn clear_state<T>(state: &mut SidecarCacheState<T>) {
    state.order.clear();
    state.values.clear();
}

fn touch(order: &mut VecDeque<SidecarCacheKey>, key: &SidecarCacheKey) {
    if let Some(position) = order.iter().position(|candidate| candidate == key) {
        order.remove(position);
    }
    order.push_back(key.clone());
}
