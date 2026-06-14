use std::time::Duration;

use serde::Serialize;

use crate::metrics::{MetricsLevel, StoreMetricsSnapshot};

/// Aggregated runtime metrics emitted by benchmark workloads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct BenchmarkMetrics {
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
    pub chunk_header_cache_hit_rate: f64,
    pub chunk_file_cache_hit_rate: f64,
    pub sidecar_cache_hit_rate: f64,
    pub manifest_cache_hit_rate: f64,
}

impl BenchmarkMetrics {
    pub fn from_snapshot(snapshot: StoreMetricsSnapshot) -> Self {
        Self {
            metrics_level: snapshot.metrics_level,
            bytes_read: snapshot.bytes_read,
            logical_bytes_written: snapshot.logical_bytes_written,
            chunk_header_cache_hits: snapshot.chunk_header_cache_hits,
            chunk_header_cache_misses: snapshot.chunk_header_cache_misses,
            chunk_file_cache_hits: snapshot.chunk_file_cache_hits,
            chunk_file_cache_misses: snapshot.chunk_file_cache_misses,
            sidecar_cache_hits: snapshot.sidecar_cache_hits,
            sidecar_cache_misses: snapshot.sidecar_cache_misses,
            manifest_cache_hits: snapshot.manifest_cache_hits,
            manifest_cache_misses: snapshot.manifest_cache_misses,
            manifest_load_ns: snapshot.manifest_load_ns,
            manifest_file_read_ns: snapshot.manifest_file_read_ns,
            manifest_decode_ns: snapshot.manifest_decode_ns,
            manifest_chunk_materialize_ns: snapshot.manifest_chunk_materialize_ns,
            manifest_sidecar_materialize_ns: snapshot.manifest_sidecar_materialize_ns,
            store_bootstrap_ns: snapshot.store_bootstrap_ns,
            session_attach_ns: snapshot.session_attach_ns,
            session_prewarm_ns: snapshot.session_prewarm_ns,
            chunk_lookup_ns: snapshot.chunk_lookup_ns,
            chunk_header_load_ns: snapshot.chunk_header_load_ns,
            chunk_file_open_ns: snapshot.chunk_file_open_ns,
            sparse_index_load_ns: snapshot.sparse_index_load_ns,
            point_record_read_ns: snapshot.point_record_read_ns,
            point_local_search_ns: snapshot.point_local_search_ns,
            point_decode_ns: snapshot.point_decode_ns,
            sidecar_load_ns: snapshot.sidecar_load_ns,
            chunk_header_cache_hit_rate: snapshot.chunk_header_cache_hit_rate(),
            chunk_file_cache_hit_rate: snapshot.chunk_file_cache_hit_rate(),
            sidecar_cache_hit_rate: snapshot.sidecar_cache_hit_rate(),
            manifest_cache_hit_rate: snapshot.manifest_cache_hit_rate(),
        }
    }
}

/// Mutable accumulator for combining metrics across repeated benchmark runs.
#[derive(Debug)]
pub struct MetricsAccumulator {
    metrics_level: MetricsLevel,
    bytes_read: u64,
    logical_bytes_written: u64,
    chunk_header_cache_hits: u64,
    chunk_header_cache_misses: u64,
    chunk_file_cache_hits: u64,
    chunk_file_cache_misses: u64,
    sidecar_cache_hits: u64,
    sidecar_cache_misses: u64,
    manifest_cache_hits: u64,
    manifest_cache_misses: u64,
    manifest_load_ns: u64,
    manifest_file_read_ns: u64,
    manifest_decode_ns: u64,
    manifest_chunk_materialize_ns: u64,
    manifest_sidecar_materialize_ns: u64,
    store_bootstrap_ns: u64,
    session_attach_ns: u64,
    session_prewarm_ns: u64,
    chunk_lookup_ns: u64,
    chunk_header_load_ns: u64,
    chunk_file_open_ns: u64,
    sparse_index_load_ns: u64,
    point_record_read_ns: u64,
    point_local_search_ns: u64,
    point_decode_ns: u64,
    sidecar_load_ns: u64,
}

impl Default for MetricsAccumulator {
    fn default() -> Self {
        Self {
            metrics_level: MetricsLevel::Off,
            bytes_read: 0,
            logical_bytes_written: 0,
            chunk_header_cache_hits: 0,
            chunk_header_cache_misses: 0,
            chunk_file_cache_hits: 0,
            chunk_file_cache_misses: 0,
            sidecar_cache_hits: 0,
            sidecar_cache_misses: 0,
            manifest_cache_hits: 0,
            manifest_cache_misses: 0,
            manifest_load_ns: 0,
            manifest_file_read_ns: 0,
            manifest_decode_ns: 0,
            manifest_chunk_materialize_ns: 0,
            manifest_sidecar_materialize_ns: 0,
            store_bootstrap_ns: 0,
            session_attach_ns: 0,
            session_prewarm_ns: 0,
            chunk_lookup_ns: 0,
            chunk_header_load_ns: 0,
            chunk_file_open_ns: 0,
            sparse_index_load_ns: 0,
            point_record_read_ns: 0,
            point_local_search_ns: 0,
            point_decode_ns: 0,
            sidecar_load_ns: 0,
        }
    }
}

impl MetricsAccumulator {
    pub fn add_snapshot(&mut self, snapshot: StoreMetricsSnapshot) {
        self.metrics_level = self.metrics_level.max(snapshot.metrics_level);
        self.bytes_read = self.bytes_read.saturating_add(snapshot.bytes_read);
        self.logical_bytes_written = self
            .logical_bytes_written
            .saturating_add(snapshot.logical_bytes_written);
        self.chunk_header_cache_hits = self
            .chunk_header_cache_hits
            .saturating_add(snapshot.chunk_header_cache_hits);
        self.chunk_header_cache_misses = self
            .chunk_header_cache_misses
            .saturating_add(snapshot.chunk_header_cache_misses);
        self.chunk_file_cache_hits = self
            .chunk_file_cache_hits
            .saturating_add(snapshot.chunk_file_cache_hits);
        self.chunk_file_cache_misses = self
            .chunk_file_cache_misses
            .saturating_add(snapshot.chunk_file_cache_misses);
        self.sidecar_cache_hits = self
            .sidecar_cache_hits
            .saturating_add(snapshot.sidecar_cache_hits);
        self.sidecar_cache_misses = self
            .sidecar_cache_misses
            .saturating_add(snapshot.sidecar_cache_misses);
        self.manifest_cache_hits = self
            .manifest_cache_hits
            .saturating_add(snapshot.manifest_cache_hits);
        self.manifest_cache_misses = self
            .manifest_cache_misses
            .saturating_add(snapshot.manifest_cache_misses);
        self.manifest_load_ns = self
            .manifest_load_ns
            .saturating_add(snapshot.manifest_load_ns);
        self.manifest_file_read_ns = self
            .manifest_file_read_ns
            .saturating_add(snapshot.manifest_file_read_ns);
        self.manifest_decode_ns = self
            .manifest_decode_ns
            .saturating_add(snapshot.manifest_decode_ns);
        self.manifest_chunk_materialize_ns = self
            .manifest_chunk_materialize_ns
            .saturating_add(snapshot.manifest_chunk_materialize_ns);
        self.manifest_sidecar_materialize_ns = self
            .manifest_sidecar_materialize_ns
            .saturating_add(snapshot.manifest_sidecar_materialize_ns);
        self.store_bootstrap_ns = self
            .store_bootstrap_ns
            .saturating_add(snapshot.store_bootstrap_ns);
        self.session_attach_ns = self
            .session_attach_ns
            .saturating_add(snapshot.session_attach_ns);
        self.session_prewarm_ns = self
            .session_prewarm_ns
            .saturating_add(snapshot.session_prewarm_ns);
        self.chunk_lookup_ns = self
            .chunk_lookup_ns
            .saturating_add(snapshot.chunk_lookup_ns);
        self.chunk_header_load_ns = self
            .chunk_header_load_ns
            .saturating_add(snapshot.chunk_header_load_ns);
        self.chunk_file_open_ns = self
            .chunk_file_open_ns
            .saturating_add(snapshot.chunk_file_open_ns);
        self.sparse_index_load_ns = self
            .sparse_index_load_ns
            .saturating_add(snapshot.sparse_index_load_ns);
        self.point_record_read_ns = self
            .point_record_read_ns
            .saturating_add(snapshot.point_record_read_ns);
        self.point_local_search_ns = self
            .point_local_search_ns
            .saturating_add(snapshot.point_local_search_ns);
        self.point_decode_ns = self
            .point_decode_ns
            .saturating_add(snapshot.point_decode_ns);
        self.sidecar_load_ns = self
            .sidecar_load_ns
            .saturating_add(snapshot.sidecar_load_ns);
    }

    pub fn finish(&self) -> BenchmarkMetrics {
        BenchmarkMetrics {
            metrics_level: self.metrics_level,
            bytes_read: self.bytes_read,
            logical_bytes_written: self.logical_bytes_written,
            chunk_header_cache_hits: self.chunk_header_cache_hits,
            chunk_header_cache_misses: self.chunk_header_cache_misses,
            chunk_file_cache_hits: self.chunk_file_cache_hits,
            chunk_file_cache_misses: self.chunk_file_cache_misses,
            sidecar_cache_hits: self.sidecar_cache_hits,
            sidecar_cache_misses: self.sidecar_cache_misses,
            manifest_cache_hits: self.manifest_cache_hits,
            manifest_cache_misses: self.manifest_cache_misses,
            manifest_load_ns: self.manifest_load_ns,
            manifest_file_read_ns: self.manifest_file_read_ns,
            manifest_decode_ns: self.manifest_decode_ns,
            manifest_chunk_materialize_ns: self.manifest_chunk_materialize_ns,
            manifest_sidecar_materialize_ns: self.manifest_sidecar_materialize_ns,
            store_bootstrap_ns: self.store_bootstrap_ns,
            session_attach_ns: self.session_attach_ns,
            session_prewarm_ns: self.session_prewarm_ns,
            chunk_lookup_ns: self.chunk_lookup_ns,
            chunk_header_load_ns: self.chunk_header_load_ns,
            chunk_file_open_ns: self.chunk_file_open_ns,
            sparse_index_load_ns: self.sparse_index_load_ns,
            point_record_read_ns: self.point_record_read_ns,
            point_local_search_ns: self.point_local_search_ns,
            point_decode_ns: self.point_decode_ns,
            sidecar_load_ns: self.sidecar_load_ns,
            chunk_header_cache_hit_rate: hit_rate(
                self.chunk_header_cache_hits,
                self.chunk_header_cache_misses,
            ),
            chunk_file_cache_hit_rate: hit_rate(
                self.chunk_file_cache_hits,
                self.chunk_file_cache_misses,
            ),
            sidecar_cache_hit_rate: hit_rate(self.sidecar_cache_hits, self.sidecar_cache_misses),
            manifest_cache_hit_rate: hit_rate(self.manifest_cache_hits, self.manifest_cache_misses),
        }
    }
}

/// Latency and throughput summary for repeated benchmark samples.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct LatencySummary {
    pub sample_count: usize,
    pub total_seconds: f64,
    pub avg_seconds: f64,
    pub min_seconds: f64,
    pub p50_seconds: f64,
    pub p95_seconds: f64,
    pub p99_seconds: f64,
    pub max_seconds: f64,
    pub total_ops: usize,
    pub ops_per_second: f64,
}

impl LatencySummary {
    pub fn from_durations(samples: &[Duration], total_ops: usize) -> Self {
        if samples.is_empty() {
            return Self {
                sample_count: 0,
                total_ops,
                ..Self::default()
            };
        }

        let mut secs: Vec<f64> = samples.iter().map(Duration::as_secs_f64).collect();
        secs.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
        let total_seconds: f64 = secs.iter().sum();
        let sample_count = secs.len();
        let avg_seconds = total_seconds / sample_count as f64;
        let min_seconds = secs[0];
        let max_seconds = secs[sample_count - 1];

        Self {
            sample_count,
            total_seconds,
            avg_seconds,
            min_seconds,
            p50_seconds: percentile(&secs, 0.50),
            p95_seconds: percentile(&secs, 0.95),
            p99_seconds: percentile(&secs, 0.99),
            max_seconds,
            total_ops,
            ops_per_second: if total_seconds > 0.0 {
                total_ops as f64 / total_seconds
            } else {
                0.0
            },
        }
    }
}

fn percentile(sorted_samples: &[f64], percentile: f64) -> f64 {
    if sorted_samples.is_empty() {
        return 0.0;
    }
    let clamped = percentile.clamp(0.0, 1.0);
    let rank = ((sorted_samples.len() - 1) as f64 * clamped).round() as usize;
    sorted_samples[rank]
}

fn hit_rate(hits: u64, misses: u64) -> f64 {
    let total = hits.saturating_add(misses);
    if total == 0 {
        0.0
    } else {
        hits as f64 / total as f64
    }
}

/// Benchmark temperature mode used by acceptance runners.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Temperature {
    Warm,
    ApproxCold,
    StricterColdish,
}

/// Static description of one workload in the acceptance matrix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkloadDescriptor {
    pub name: String,
    pub temperature: Temperature,
    pub scenario: String,
}

/// Builds a compact acceptance matrix covering point, range, scan, latest, write, append, and merge.
pub fn build_acceptance_matrix(short_range_sizes: &[usize]) -> Vec<WorkloadDescriptor> {
    let mut out = vec![
        WorkloadDescriptor {
            name: "write_initial".to_string(),
            temperature: Temperature::Warm,
            scenario: "multi_series_multi_month".to_string(),
        },
        WorkloadDescriptor {
            name: "append_active_month".to_string(),
            temperature: Temperature::Warm,
            scenario: "single_series_single_month".to_string(),
        },
        WorkloadDescriptor {
            name: "merge_active_month".to_string(),
            temperature: Temperature::Warm,
            scenario: "single_series_single_month".to_string(),
        },
        WorkloadDescriptor {
            name: "session_attach_single".to_string(),
            temperature: Temperature::ApproxCold,
            scenario: "single_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "session_attach_multi".to_string(),
            temperature: Temperature::ApproxCold,
            scenario: "multi_chunk_multi_series".to_string(),
        },
        WorkloadDescriptor {
            name: "get_at_hit".to_string(),
            temperature: Temperature::Warm,
            scenario: "single_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "get_at_hit".to_string(),
            temperature: Temperature::ApproxCold,
            scenario: "single_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "get_at_hit".to_string(),
            temperature: Temperature::StricterColdish,
            scenario: "single_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "get_at_miss".to_string(),
            temperature: Temperature::Warm,
            scenario: "multi_chunk_multi_series".to_string(),
        },
        WorkloadDescriptor {
            name: "get_at_miss".to_string(),
            temperature: Temperature::ApproxCold,
            scenario: "multi_chunk_multi_series".to_string(),
        },
        WorkloadDescriptor {
            name: "get_at_miss".to_string(),
            temperature: Temperature::StricterColdish,
            scenario: "multi_chunk_multi_series".to_string(),
        },
        WorkloadDescriptor {
            name: "get_range_medium".to_string(),
            temperature: Temperature::Warm,
            scenario: "multi_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "get_range_medium".to_string(),
            temperature: Temperature::ApproxCold,
            scenario: "multi_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "get_range_medium".to_string(),
            temperature: Temperature::StricterColdish,
            scenario: "multi_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "full_scan".to_string(),
            temperature: Temperature::Warm,
            scenario: "multi_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "full_scan".to_string(),
            temperature: Temperature::ApproxCold,
            scenario: "multi_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "full_scan".to_string(),
            temperature: Temperature::StricterColdish,
            scenario: "multi_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "latest_n".to_string(),
            temperature: Temperature::Warm,
            scenario: "multi_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "latest_n".to_string(),
            temperature: Temperature::ApproxCold,
            scenario: "multi_chunk_single_series".to_string(),
        },
        WorkloadDescriptor {
            name: "latest_n".to_string(),
            temperature: Temperature::StricterColdish,
            scenario: "multi_chunk_single_series".to_string(),
        },
    ];

    for size in short_range_sizes {
        for temperature in [
            Temperature::Warm,
            Temperature::ApproxCold,
            Temperature::StricterColdish,
        ] {
            out.push(WorkloadDescriptor {
                name: format!("get_range_short_{size}"),
                temperature,
                scenario: "single_chunk_single_series".to_string(),
            });
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::benchmark::{
        build_acceptance_matrix, LatencySummary, MetricsAccumulator, Temperature,
    };
    use crate::metrics::{MetricsLevel, StoreMetricsSnapshot};

    #[test]
    fn latency_summary_reports_expected_percentiles() {
        let samples = vec![
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_millis(3),
            Duration::from_millis(4),
            Duration::from_millis(5),
        ];

        let summary = LatencySummary::from_durations(&samples, 10);
        assert_eq!(summary.sample_count, 5);
        assert_eq!(summary.min_seconds, 0.001);
        assert_eq!(summary.p50_seconds, 0.003);
        assert_eq!(summary.p95_seconds, 0.005);
        assert_eq!(summary.p99_seconds, 0.005);
        assert_eq!(summary.max_seconds, 0.005);
        assert_eq!(summary.total_ops, 10);
    }

    #[test]
    fn metrics_accumulator_sums_snapshots() {
        let mut accumulator = MetricsAccumulator::default();
        accumulator.add_snapshot(StoreMetricsSnapshot {
            metrics_level: MetricsLevel::Detailed,
            bytes_read: 10,
            logical_bytes_written: 20,
            chunk_header_cache_hits: 3,
            chunk_header_cache_misses: 1,
            chunk_file_cache_hits: 4,
            chunk_file_cache_misses: 2,
            sidecar_cache_hits: 5,
            sidecar_cache_misses: 5,
            manifest_cache_hits: 2,
            manifest_cache_misses: 1,
            manifest_load_ns: 7,
            manifest_file_read_ns: 8,
            manifest_decode_ns: 9,
            manifest_chunk_materialize_ns: 10,
            manifest_sidecar_materialize_ns: 11,
            store_bootstrap_ns: 12,
            session_attach_ns: 13,
            session_prewarm_ns: 14,
            chunk_lookup_ns: 11,
            chunk_header_load_ns: 13,
            chunk_file_open_ns: 17,
            sparse_index_load_ns: 19,
            point_record_read_ns: 23,
            point_local_search_ns: 29,
            point_decode_ns: 31,
            sidecar_load_ns: 37,
        });
        accumulator.add_snapshot(StoreMetricsSnapshot {
            metrics_level: MetricsLevel::Basic,
            bytes_read: 7,
            logical_bytes_written: 9,
            chunk_header_cache_hits: 1,
            chunk_header_cache_misses: 1,
            chunk_file_cache_hits: 2,
            chunk_file_cache_misses: 0,
            sidecar_cache_hits: 1,
            sidecar_cache_misses: 3,
            manifest_cache_hits: 1,
            manifest_cache_misses: 1,
            manifest_load_ns: 41,
            manifest_file_read_ns: 43,
            manifest_decode_ns: 47,
            manifest_chunk_materialize_ns: 53,
            manifest_sidecar_materialize_ns: 59,
            store_bootstrap_ns: 61,
            session_attach_ns: 67,
            session_prewarm_ns: 71,
            chunk_lookup_ns: 43,
            chunk_header_load_ns: 47,
            chunk_file_open_ns: 53,
            sparse_index_load_ns: 59,
            point_record_read_ns: 61,
            point_local_search_ns: 67,
            point_decode_ns: 71,
            sidecar_load_ns: 73,
        });

        let combined = accumulator.finish();
        assert_eq!(combined.bytes_read, 17);
        assert_eq!(combined.logical_bytes_written, 29);
        assert_eq!(combined.chunk_header_cache_hits, 4);
        assert_eq!(combined.chunk_header_cache_misses, 2);
        assert_eq!(combined.chunk_file_cache_hits, 6);
        assert_eq!(combined.chunk_file_cache_misses, 2);
        assert_eq!(combined.sidecar_cache_hits, 6);
        assert_eq!(combined.sidecar_cache_misses, 8);
        assert_eq!(combined.manifest_cache_hits, 3);
        assert_eq!(combined.manifest_cache_misses, 2);
        assert_eq!(combined.manifest_load_ns, 48);
        assert_eq!(combined.manifest_file_read_ns, 51);
        assert_eq!(combined.manifest_decode_ns, 56);
        assert_eq!(combined.manifest_chunk_materialize_ns, 63);
        assert_eq!(combined.manifest_sidecar_materialize_ns, 70);
        assert_eq!(combined.store_bootstrap_ns, 73);
        assert_eq!(combined.session_attach_ns, 80);
        assert_eq!(combined.session_prewarm_ns, 85);
        assert_eq!(combined.chunk_lookup_ns, 54);
        assert_eq!(combined.sidecar_load_ns, 110);
        assert_eq!(combined.metrics_level, MetricsLevel::Detailed);
        assert!(combined.chunk_header_cache_hit_rate > 0.0);
    }

    #[test]
    fn metrics_accumulator_preserves_off_when_only_off_snapshots_are_added() {
        let mut accumulator = MetricsAccumulator::default();
        accumulator.add_snapshot(StoreMetricsSnapshot {
            metrics_level: MetricsLevel::Off,
            bytes_read: 0,
            logical_bytes_written: 0,
            chunk_header_cache_hits: 0,
            chunk_header_cache_misses: 0,
            chunk_file_cache_hits: 0,
            chunk_file_cache_misses: 0,
            sidecar_cache_hits: 0,
            sidecar_cache_misses: 0,
            manifest_cache_hits: 0,
            manifest_cache_misses: 0,
            manifest_load_ns: 0,
            manifest_file_read_ns: 0,
            manifest_decode_ns: 0,
            manifest_chunk_materialize_ns: 0,
            manifest_sidecar_materialize_ns: 0,
            store_bootstrap_ns: 0,
            session_attach_ns: 0,
            session_prewarm_ns: 0,
            chunk_lookup_ns: 0,
            chunk_header_load_ns: 0,
            chunk_file_open_ns: 0,
            sparse_index_load_ns: 0,
            point_record_read_ns: 0,
            point_local_search_ns: 0,
            point_decode_ns: 0,
            sidecar_load_ns: 0,
        });

        let combined = accumulator.finish();
        assert_eq!(combined.metrics_level, MetricsLevel::Off);
        assert_eq!(combined.bytes_read, 0);
        assert_eq!(combined.manifest_load_ns, 0);
    }

    #[test]
    fn acceptance_matrix_contains_warm_and_cold_workloads() {
        let workloads = build_acceptance_matrix(&[16, 64, 256]);
        assert!(workloads
            .iter()
            .any(|workload| workload.temperature == Temperature::Warm));
        assert!(workloads
            .iter()
            .any(|workload| workload.temperature == Temperature::ApproxCold));
        assert!(workloads
            .iter()
            .any(|workload| workload.temperature == Temperature::StricterColdish));
        assert!(workloads
            .iter()
            .any(|workload| workload.name == "get_range_short_256"));
    }
}
