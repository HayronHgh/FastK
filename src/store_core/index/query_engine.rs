use crate::error::{FastKError, Result};
use crate::index::{vix, zmap, ValueIndexEntry, ZoneMapEntry};
use crate::types::{ScalarPredicate, ScalarRecord, ScalarSeriesKey};

/// Minimal predicate query engine for scalar series.
#[derive(Debug, Default, Clone, Copy)]
pub struct PredicateQueryEngine;

impl PredicateQueryEngine {
    /// Creates a new stateless query engine.
    pub fn new() -> Self {
        Self
    }

    /// Finds matching timestamps using zone-map pruning plus record scan.
    pub fn find_timestamps_via_zmap(
        &self,
        series_key: &ScalarSeriesKey,
        records: &[ScalarRecord],
        entries: &[ZoneMapEntry],
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        validate_series_key(series_key)?;
        zmap::find_timestamps(entries, records, predicate, start_ts, end_ts)
    }

    /// Finds matching timestamps using the value index.
    pub fn find_timestamps_via_vix(
        &self,
        series_key: &ScalarSeriesKey,
        entries: &[ValueIndexEntry],
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        validate_series_key(series_key)?;
        vix::find_timestamps(entries, predicate, start_ts, end_ts)
    }
}

fn validate_series_key(series_key: &ScalarSeriesKey) -> Result<()> {
    if series_key.symbol.trim().is_empty()
        || series_key.category.trim().is_empty()
        || series_key.name.trim().is_empty()
    {
        return Err(FastKError::InvalidInput(
            "scalar series key fields must not be empty".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::index::query_engine::PredicateQueryEngine;
    use crate::index::{vix, zmap, ValueIndexEntry};
    use crate::types::{CompareOp, ScalarPredicate, ScalarRecord, ScalarSeriesKey};

    #[test]
    fn zmap_build_read_and_query_returns_sorted_unique_timestamps() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let index_path = temp_dir.path().join("sample.zmap");
        let records = sample_records();

        let mut entries = zmap::build_entries(&records, 2).expect("zmap build should succeed");
        entries.push(entries[1]);
        zmap::write_entries(&index_path, &entries).expect("zmap write should succeed");

        let loaded = zmap::read_entries(&index_path).expect("zmap read should succeed");
        assert_eq!(loaded, entries);

        let engine = PredicateQueryEngine::new();
        let predicate = ScalarPredicate {
            op: CompareOp::Between,
            value: 20,
            value2: Some(40),
        };

        let result = engine
            .find_timestamps_via_zmap(
                &sample_key(),
                &records,
                &loaded,
                &predicate,
                1_706_745_600_000,
                1_706_746_000_000,
            )
            .expect("zmap query should succeed");

        assert_eq!(
            result,
            vec![1_706_745_720_000, 1_706_745_780_000, 1_706_745_840_000]
        );
    }

    #[test]
    fn vix_build_read_and_query_returns_sorted_unique_timestamps() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let index_path = temp_dir.path().join("sample.vix");
        let records = sample_records();

        let mut entries = vix::build_entries(&records);
        entries.push(ValueIndexEntry {
            value: 40,
            ts: 1_706_745_840_000,
        });
        entries.sort_by_key(|entry| (entry.value, entry.ts));
        vix::write_entries(&index_path, &entries).expect("vix write should succeed");

        let loaded = vix::read_entries(&index_path).expect("vix read should succeed");
        assert_eq!(loaded, entries);

        let engine = PredicateQueryEngine::new();
        let predicate = ScalarPredicate {
            op: CompareOp::Gte,
            value: 30,
            value2: None,
        };

        let result = engine
            .find_timestamps_via_vix(
                &sample_key(),
                &loaded,
                &predicate,
                1_706_745_600_000,
                1_706_746_000_000,
            )
            .expect("vix query should succeed");

        assert_eq!(
            result,
            vec![1_706_745_780_000, 1_706_745_840_000, 1_706_745_900_000]
        );
    }

    fn sample_key() -> ScalarSeriesKey {
        ScalarSeriesKey {
            symbol: "BTCUSDT".to_string(),
            category: "indicator".to_string(),
            name: "rsi14".to_string(),
        }
    }

    fn sample_records() -> Vec<ScalarRecord> {
        vec![
            ScalarRecord {
                ts: 1_706_745_600_000,
                value: 10,
            },
            ScalarRecord {
                ts: 1_706_745_660_000,
                value: 15,
            },
            ScalarRecord {
                ts: 1_706_745_720_000,
                value: 20,
            },
            ScalarRecord {
                ts: 1_706_745_780_000,
                value: 30,
            },
            ScalarRecord {
                ts: 1_706_745_840_000,
                value: 40,
            },
            ScalarRecord {
                ts: 1_706_745_900_000,
                value: 50,
            },
        ]
    }
}
