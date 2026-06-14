use crate::types::{ChunkMeta, ChunkState, SeriesMeta};

/// Configurable thresholds for deciding when month deltas should be compacted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPolicy {
    pub delta_chunk_threshold: usize,
    pub delta_total_bytes_threshold: u64,
    pub append_count_threshold: usize,
    pub auto_merge: bool,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            delta_chunk_threshold: 4,
            delta_total_bytes_threshold: 4 * 1024 * 1024,
            append_count_threshold: 4,
            auto_merge: false,
        }
    }
}

/// Decision produced by evaluating a single month under a compaction policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionDecision {
    pub month_key: String,
    pub total_chunks: usize,
    pub delta_chunk_count: usize,
    pub delta_total_bytes: u64,
    pub append_count: usize,
    pub should_merge: bool,
    pub reasons: Vec<String>,
}

impl CompactionPolicy {
    pub fn evaluate_month(&self, meta: &SeriesMeta, month_key: &str) -> Option<CompactionDecision> {
        let month_chunks: Vec<&ChunkMeta> = meta
            .chunks
            .iter()
            .filter(|chunk| chunk.month_key == month_key)
            .collect();
        if month_chunks.is_empty() {
            return None;
        }

        let delta_chunk_count = month_chunks
            .iter()
            .filter(|chunk| chunk.state == ChunkState::Active)
            .count();
        let delta_total_bytes = month_chunks
            .iter()
            .filter(|chunk| chunk.state == ChunkState::Active)
            .map(|chunk| chunk.count.saturating_mul(meta.record_size as u64))
            .sum();
        let append_count = month_chunks
            .iter()
            .map(|chunk| chunk.generation as usize)
            .max()
            .unwrap_or(1)
            .saturating_sub(1);
        let mut reasons = Vec::new();

        if delta_chunk_count >= self.delta_chunk_threshold {
            reasons.push(format!(
                "delta chunk count {} reached threshold {}",
                delta_chunk_count, self.delta_chunk_threshold
            ));
        }
        if delta_total_bytes >= self.delta_total_bytes_threshold {
            reasons.push(format!(
                "delta bytes {} reached threshold {}",
                delta_total_bytes, self.delta_total_bytes_threshold
            ));
        }
        if append_count >= self.append_count_threshold {
            reasons.push(format!(
                "append count {} reached threshold {}",
                append_count, self.append_count_threshold
            ));
        }

        Some(CompactionDecision {
            month_key: month_key.to_string(),
            total_chunks: month_chunks.len(),
            delta_chunk_count,
            delta_total_bytes,
            append_count,
            should_merge: !reasons.is_empty(),
            reasons,
        })
    }

    pub fn evaluate_all(&self, meta: &SeriesMeta) -> Vec<CompactionDecision> {
        let mut months = Vec::new();
        for chunk in &meta.chunks {
            if months
                .last()
                .map(|month: &String| month == &chunk.month_key)
                .unwrap_or(false)
            {
                continue;
            }
            months.push(chunk.month_key.clone());
        }
        months
            .into_iter()
            .filter_map(|month_key| self.evaluate_month(meta, &month_key))
            .collect()
    }
}

pub fn can_transition(from: ChunkState, to: ChunkState) -> bool {
    matches!(
        (from, to),
        (ChunkState::Active, ChunkState::Merging)
            | (ChunkState::Active, ChunkState::Sealed)
            | (ChunkState::Active, ChunkState::Replaced)
            | (ChunkState::Sealed, ChunkState::Merging)
            | (ChunkState::Sealed, ChunkState::Replaced)
            | (ChunkState::Merging, ChunkState::Active)
            | (ChunkState::Merging, ChunkState::Sealed)
            | (ChunkState::Merging, ChunkState::Replaced)
    )
}

pub fn merge_output_state(chunks: &[ChunkMeta]) -> ChunkState {
    if chunks.iter().any(|chunk| chunk.state == ChunkState::Active) {
        ChunkState::Active
    } else {
        ChunkState::Sealed
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::compaction::{can_transition, CompactionPolicy};
    use crate::types::{ChunkMeta, ChunkState, RecordType, SeriesMeta};

    #[test]
    fn delta_count_threshold_triggers_merge() {
        let meta = sample_meta(4, 1);
        let decision = CompactionPolicy::default()
            .evaluate_month(&meta, "2024-02")
            .expect("decision should exist");
        assert!(decision.should_merge);
        assert_eq!(decision.delta_chunk_count, 4);
    }

    #[test]
    fn delta_bytes_threshold_triggers_merge() {
        let policy = CompactionPolicy {
            delta_chunk_threshold: 10,
            delta_total_bytes_threshold: 1,
            append_count_threshold: 10,
            auto_merge: false,
        };
        let meta = sample_meta(1, 10);
        let decision = policy
            .evaluate_month(&meta, "2024-02")
            .expect("decision should exist");
        assert!(decision.should_merge);
    }

    #[test]
    fn state_transition_rules_are_explicit() {
        assert!(can_transition(ChunkState::Active, ChunkState::Merging));
        assert!(can_transition(ChunkState::Sealed, ChunkState::Replaced));
        assert!(!can_transition(ChunkState::Replaced, ChunkState::Active));
    }

    fn sample_meta(active_chunks: usize, rows_per_chunk: u64) -> SeriesMeta {
        let mut chunks = vec![sample_chunk(1, ChunkState::Sealed, rows_per_chunk)];
        for offset in 0..active_chunks {
            chunks.push(sample_chunk(
                (offset + 2) as u64,
                ChunkState::Active,
                rows_per_chunk,
            ));
        }

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
            series_id: 1,
            manifest_seq: 1,
            created_at: 1,
            updated_at: 1,
            flags: 0,
            active_chunk_id: Some(2),
            chunks,
        }
    }

    fn sample_chunk(chunk_id: u64, state: ChunkState, count: u64) -> ChunkMeta {
        ChunkMeta {
            chunk_id,
            month_key: "2024-02".to_string(),
            start_ts: 100 + chunk_id as i64,
            end_ts: 199 + chunk_id as i64,
            count,
            state,
            layout_version: 2,
            header_len: 128,
            sparse_index_every: 128,
            sparse_index_offset: 512,
            sparse_index_len: 1,
            chunk_checksum: 1,
            generation: chunk_id as u32,
            relative_path: format!("chunks/{chunk_id}.chunk"),
            sidecars: Vec::new(),
        }
    }
}
