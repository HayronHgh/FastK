//! Batch append orchestration. OS ownership is always a stack-local guard.
use super::*;

#[derive(Debug, Clone)]
pub struct RangeWriteSummary {
    pub timeframe_ms: i64,
    pub registered: bool,
    pub requested_record_count: usize,
    pub written_record_count: usize,
    pub month_batches_written: usize,
}

impl FastKStore {
    pub(super) fn begin_mutation(&self) -> Result<StoreWriteLock> {
        self.acquire_write_ownership(true)
    }

    pub(super) fn acquire_write_ownership(&self, recover: bool) -> Result<StoreWriteLock> {
        if self.read_only {
            return Err(FastKError::InvalidInput("read-only store handle".into()));
        }
        let lock = StoreWriteLock::acquire(&self.root)?;
        if recover && recovery::has_pending_recovery(&self.root)? {
            recovery::startup_recover_locked(&self.root, recovery::RecoveryOptions::default())?;
        }
        self.clear_read_caches();
        manifest::invalidate_root(&self.root);
        Ok(lock)
    }

    /// Appends ordered records, committing each UTC month independently.
    pub fn append_kline_range(
        &mut self,
        symbol: &str,
        timeframe: &str,
        records: &[KlineRecord],
        timeframe_ms: i64,
        price_scale: i64,
        volume_scale: i64,
    ) -> Result<RangeWriteSummary> {
        validate_configuration(timeframe_ms, price_scale, volume_scale)?;
        let months = month_batches(records)?;
        let _lock = self.begin_mutation()?;
        let existing = match self.load_kline_meta(symbol, timeframe) {
            Ok(meta) => Some(meta),
            Err(FastKError::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        if let Some(meta) = &existing {
            validate_existing_series(meta, timeframe_ms, price_scale, volume_scale)?;
        }
        preflight_overlap(existing.as_deref(), &months, "write-kline-range")?;
        if !self.root.join("series").exists() {
            self.init_locked()?;
        }
        if existing.is_none() {
            self.register_kline_series_locked(
                symbol,
                timeframe,
                timeframe_ms,
                price_scale,
                volume_scale,
            )?;
        }
        let mut completed = Vec::new();
        for (month, rows) in &months {
            self.put_kline_chunk_locked(symbol, timeframe, rows)
                .map_err(|error| monthly_write_error(error, &completed))?;
            completed.push(month.clone());
        }
        Ok(RangeWriteSummary {
            timeframe_ms,
            registered: existing.is_none(),
            requested_record_count: records.len(),
            written_record_count: records.len(),
            month_batches_written: completed.len(),
        })
    }

    /// Appends scalar records without changing `put_scalar_chunk` replacement semantics.
    pub fn append_scalar_range(
        &mut self,
        key: &ScalarSeriesKey,
        records: &[ScalarRecord],
        timeframe_ms: Option<i64>,
    ) -> Result<RangeWriteSummary> {
        if timeframe_ms.is_some_and(|value| value <= 0) {
            return Err(FastKError::InvalidInput(
                "timeframe_ms must be positive".into(),
            ));
        }
        let months = month_batches(records)?;
        let _lock = self.begin_mutation()?;
        let existing = match self.load_scalar_meta(key) {
            Ok(meta) => Some(meta),
            Err(FastKError::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        let timeframe_ms = match (existing.as_deref(), timeframe_ms) {
            (Some(meta), Some(value)) if meta.timeframe_ms != value => {
                return Err(FastKError::InvalidInput(
                    "scalar timeframe does not match existing series".into(),
                ));
            }
            (Some(meta), _) => meta.timeframe_ms,
            (None, Some(value)) => value,
            (None, None) => {
                self.load_kline_meta(
                    &key.symbol,
                    &decode_scoped_scalar_key(key)
                        .ok_or_else(|| {
                            FastKError::InvalidInput(
                                "timeframe_ms required for unscoped scalar series".into(),
                            )
                        })?
                        .timeframe,
                )?
                .timeframe_ms
            }
        };
        preflight_overlap(existing.as_deref(), &months, "write-scalar-range")?;
        if !self.root.join("series").exists() {
            self.init_locked()?;
        }
        if existing.is_none() {
            self.register_scalar_series_locked(key, timeframe_ms)?;
        }
        let mut completed = Vec::new();
        for (month, rows) in &months {
            self.append_scalar_chunk_locked(
                key,
                timeframe_ms,
                rows,
                DEFAULT_SCALAR_ZMAP_BLOCK_SIZE,
            )
            .map_err(|error| monthly_write_error(error, &completed))?;
            completed.push(month.clone());
        }
        Ok(RangeWriteSummary {
            timeframe_ms,
            registered: existing.is_none(),
            requested_record_count: records.len(),
            written_record_count: records.len(),
            month_batches_written: completed.len(),
        })
    }

    pub fn append_scalar_chunk(
        &mut self,
        key: &ScalarSeriesKey,
        timeframe_ms: i64,
        records: &[ScalarRecord],
        block_size: usize,
    ) -> Result<()> {
        if timeframe_ms <= 0 || block_size == 0 {
            return Err(FastKError::InvalidInput(
                "timeframe and zone-map block size must be positive".into(),
            ));
        }
        validate_month_records(records)?;
        let _lock = self.begin_mutation()?;
        self.append_scalar_chunk_locked(key, timeframe_ms, records, block_size)
    }

    fn append_scalar_chunk_locked(
        &mut self,
        key: &ScalarSeriesKey,
        timeframe_ms: i64,
        records: &[ScalarRecord],
        block_size: usize,
    ) -> Result<()> {
        let month = path::month_key(records[0].ts)?;
        let mut combined = match self.load_scalar_meta(key) {
            Ok(meta) => {
                if meta.timeframe_ms != timeframe_ms {
                    return Err(FastKError::InvalidInput(
                        "scalar timeframe does not match existing series".into(),
                    ));
                }
                let mut rows = Vec::new();
                for index in meta.chunk_indices_for_month(&month) {
                    let chunk = &meta.chunks[index];
                    if chunk.state.is_readable() {
                        rows.extend(self.get_scalar_range(key, chunk.start_ts, chunk.end_ts)?);
                    }
                }
                rows
            }
            Err(FastKError::NotFound(_)) => Vec::new(),
            Err(error) => return Err(error),
        };
        if combined.last().is_some_and(|last| last.ts >= records[0].ts) {
            return Err(FastKError::InvalidInput(
                "scalar append overlaps existing month".into(),
            ));
        }
        combined.extend_from_slice(records);
        self.put_scalar_chunk_locked(key, timeframe_ms, &combined, block_size)
    }
}

pub(super) fn validate_configuration(
    timeframe_ms: i64,
    price_scale: i64,
    volume_scale: i64,
) -> Result<()> {
    if timeframe_ms <= 0 || price_scale <= 0 || volume_scale <= 0 {
        return Err(FastKError::InvalidInput(
            "timeframe and scales must be positive".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_month_records<R: FixedRecord>(records: &[R]) -> Result<()> {
    R::validate_strict_order(records)?;
    if path::month_key(records[0].ts())? != path::month_key(records.last().unwrap().ts())? {
        return Err(FastKError::InvalidInput(
            "records span multiple UTC months".into(),
        ));
    }
    Ok(())
}

fn month_batches<R: FixedRecord>(mut records: &[R]) -> Result<Vec<(String, &[R])>> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    R::validate_strict_order(records)?;
    let mut result = Vec::new();
    while !records.is_empty() {
        let month = path::month_key(records[0].ts())?;
        let end = path::next_month_start(records[0].ts())?;
        let count = records.partition_point(|record| record.ts() < end);
        result.push((month, &records[..count]));
        records = &records[count..];
    }
    Ok(result)
}

fn preflight_overlap<R: FixedRecord>(
    meta: Option<&SeriesMeta>,
    months: &[(String, &[R])],
    command: &str,
) -> Result<()> {
    if let Some(meta) = meta {
        for (month, rows) in months {
            let end = meta
                .chunk_indices_for_month(month)
                .iter()
                .map(|index| &meta.chunks[*index])
                .filter(|chunk| chunk.state.is_readable())
                .map(|chunk| chunk.end_ts)
                .max();
            if let Some(end) = end {
                if rows[0].ts() <= end {
                    return Err(FastKError::InvalidInput(format!(
                        "{command} would overlap existing month coverage for {month}: {} <= {end}",
                        rows[0].ts()
                    )));
                }
            }
        }
    }
    Ok(())
}

fn monthly_write_error(error: FastKError, completed: &[String]) -> FastKError {
    FastKError::InvalidData(format!("monthly write failed; completed_months={completed:?}; failed month may require recovery: {error}"))
}
