use super::*;
use crate::chunk::file::ReadSpan;
use rayon::prelude::*;
use std::mem::{ManuallyDrop, MaybeUninit};

const PARALLEL_MIN_BYTES: usize = 4 * 1024 * 1024;
const TASK_BYTES: usize = 1024 * 1024;
// Platform IO and worker wake-up costs differ; keep useful batches coarse.
const MIN_WORKER_BYTES: usize = if cfg!(windows) { TASK_BYTES } else { PARALLEL_MIN_BYTES };

impl FastKStore {
    /// Bounds internal read workers. One thread is the default and creates no pool.
    pub fn set_read_threads(&mut self, threads: usize) -> Result<()> {
        if threads == 0 {
            return Err(FastKError::InvalidInput(
                "read threads must be positive".into(),
            ));
        }
        self.read_pool = if threads == 1 {
            None
        } else {
            Some(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .map_err(|error| FastKError::InvalidInput(error.to_string()))?,
            )
        };
        Ok(())
    }

    pub(super) fn should_plan_read(
        &self,
        meta: &SeriesMeta,
        chunks: &[crate::types::ChunkMeta],
        start: i64,
        end: i64,
    ) -> bool {
        let rows = chunks.iter().fold(0usize, |sum, chunk| {
            sum.saturating_add(chunk.count as usize)
        });
        let estimate = if meta.timeframe_ms > 0 {
            ((end.saturating_sub(start) / meta.timeframe_ms).saturating_add(1) as usize).min(rows)
        } else {
            rows
        };
        estimate.saturating_mul(meta.record_size as usize) >= PARALLEL_MIN_BYTES
    }

    /// Fill functions initialize every destination slot before success. Destinations
    /// are disjoint; failures drop only MaybeUninit storage and publish no records.
    pub(super) fn read_spans<R: Send>(
        &self,
        spans: Vec<ReadSpan>,
        record_size: usize,
        fill: fn(&ReadSpan, &mut [MaybeUninit<R>], &StoreMetrics) -> Result<()>,
    ) -> Result<Vec<R>> {
        let rows = spans.iter().try_fold(0usize, |total, span| {
            total
                .checked_add(span.rows)
                .ok_or_else(|| FastKError::InvalidData("read result size overflow".into()))
        })?;
        let bytes = rows
            .checked_mul(record_size)
            .ok_or_else(|| FastKError::InvalidData("read result byte length overflow".into()))?;
        let workers = self.read_pool.as_ref().map_or(1, |pool| {
            (bytes / MIN_WORKER_BYTES).max(1).min(pool.current_num_threads())
        });
        let mut out = Vec::with_capacity(rows);
        out.resize_with(rows, MaybeUninit::uninit);
        let mut remaining = out.as_mut_slice();
        let mut tasks = Vec::new();
        let task_rows = if workers > 1 { (TASK_BYTES / record_size).max(1) } else { usize::MAX };
        for span in spans {
            let mut start = 0;
            while start < span.rows {
                let count = (span.rows - start).min(task_rows);
                let (destination, rest) = remaining.split_at_mut(count);
                remaining = rest;
                let offset = span
                    .offset
                    .checked_add((start * record_size) as u64)
                    .ok_or_else(|| FastKError::InvalidData("read task offset overflow".into()))?;
                tasks.push((
                    ReadSpan {
                        file: span.file.clone(),
                        offset,
                        rows: count,
                    },
                    destination,
                ));
                start += count;
            }
        }
        if let Some(pool) = &self.read_pool {
            if workers > 1 && tasks.len() > 1 {
                let group_size = tasks.len().div_ceil(workers);
                let results: Vec<_> = pool.install(|| {
                    tasks
                        .par_chunks_mut(group_size)
                        .map(|group| group.iter_mut().try_for_each(|(span, destination)| fill(span, destination, &self.metrics)))
                        .collect()
                });
                for result in results {
                    result?;
                }
                return Ok(unsafe { initialized_records(out) });
            }
        }
        for (span, destination) in tasks {
            fill(&span, destination, &self.metrics)?;
        }
        Ok(unsafe { initialized_records(out) })
    }
}

/// Every slot must have been initialized by a successful fill. MaybeUninit<R>
/// and R have identical allocation layouts; ownership is transferred once.
unsafe fn initialized_records<R>(out: Vec<MaybeUninit<R>>) -> Vec<R> {
    let mut out = ManuallyDrop::new(out);
    unsafe { Vec::from_raw_parts(out.as_mut_ptr().cast(), out.len(), out.capacity()) }
}
