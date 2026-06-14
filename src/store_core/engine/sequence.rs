use crate::types::{BboRecord, BookDeltaRecord, FixedRecord, RecordType, TradeRecord};

/// Storage-level sequence scan summary for one fixed-record series.
///
/// This report only describes what is present in FastK storage. It does not repair gaps, apply
/// exchange-specific policy, or decide whether a dataset is suitable for research or trading.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SequenceScanReport {
    pub symbol: String,
    pub category: String,
    pub name: String,
    pub record_type: RecordType,
    pub sequence_field: String,
    pub start_ts: i64,
    pub end_ts: i64,
    pub scanned_chunk_count: usize,
    pub scanned_record_count: u64,
    pub first_sequence: Option<i64>,
    pub last_sequence: Option<i64>,
    pub gaps: Vec<SequenceGap>,
    pub duplicates: Vec<SequenceDuplicate>,
    pub violations: Vec<SequenceViolation>,
}

impl SequenceScanReport {
    pub(crate) fn new<R: SequencedRecord>(
        symbol: &str,
        category: &str,
        name: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Self {
        Self {
            symbol: symbol.to_string(),
            category: category.to_string(),
            name: name.to_string(),
            record_type: R::RECORD_TYPE,
            sequence_field: R::SEQUENCE_FIELD.to_string(),
            start_ts,
            end_ts,
            scanned_chunk_count: 0,
            scanned_record_count: 0,
            first_sequence: None,
            last_sequence: None,
            gaps: Vec::new(),
            duplicates: Vec::new(),
            violations: Vec::new(),
        }
    }

    pub fn is_clean(&self) -> bool {
        self.gaps.is_empty() && self.duplicates.is_empty() && self.violations.is_empty()
    }

    pub fn gap_count(&self) -> usize {
        self.gaps.len()
    }

    pub fn duplicate_count(&self) -> usize {
        self.duplicates.len()
    }

    pub fn violation_count(&self) -> usize {
        self.violations.len()
    }
}

/// Missing contiguous sequence range observed between adjacent stored records.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SequenceGap {
    pub previous_sequence: i64,
    pub expected_sequence: i64,
    pub next_sequence: i64,
    pub missing_count: u64,
    pub previous_ts: i64,
    pub next_ts: i64,
    pub previous_record_ordinal: u64,
    pub record_ordinal: u64,
}

/// Duplicate sequence observed in adjacent stored records.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SequenceDuplicate {
    pub sequence: i64,
    pub first_ts: i64,
    pub duplicate_ts: i64,
    pub first_record_ordinal: u64,
    pub duplicate_record_ordinal: u64,
}

/// Non-monotonic sequence observed in adjacent stored records.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SequenceViolation {
    pub previous_sequence: i64,
    pub next_sequence: i64,
    pub previous_ts: i64,
    pub next_ts: i64,
    pub previous_record_ordinal: u64,
    pub record_ordinal: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SequenceObservation {
    pub ts: i64,
    pub sequence: i64,
    pub ordinal: u64,
}

pub(crate) trait SequencedRecord: FixedRecord {
    const SEQUENCE_FIELD: &'static str;

    fn sequence_value(&self) -> i64;
}

impl SequencedRecord for BboRecord {
    const SEQUENCE_FIELD: &'static str = "sequence";

    fn sequence_value(&self) -> i64 {
        self.sequence
    }
}

impl SequencedRecord for BookDeltaRecord {
    const SEQUENCE_FIELD: &'static str = "sequence";

    fn sequence_value(&self) -> i64 {
        self.sequence
    }
}

impl SequencedRecord for TradeRecord {
    const SEQUENCE_FIELD: &'static str = "trade_id";

    fn sequence_value(&self) -> i64 {
        self.trade_id
    }
}

pub(crate) fn observe_sequence(
    report: &mut SequenceScanReport,
    previous: Option<SequenceObservation>,
    current: SequenceObservation,
) {
    report.scanned_record_count += 1;
    report.first_sequence.get_or_insert(current.sequence);
    report.last_sequence = Some(current.sequence);

    if let Some(prev) = previous {
        if current.sequence == prev.sequence {
            report.duplicates.push(SequenceDuplicate {
                sequence: current.sequence,
                first_ts: prev.ts,
                duplicate_ts: current.ts,
                first_record_ordinal: prev.ordinal,
                duplicate_record_ordinal: current.ordinal,
            });
        } else if current.sequence < prev.sequence {
            report.violations.push(SequenceViolation {
                previous_sequence: prev.sequence,
                next_sequence: current.sequence,
                previous_ts: prev.ts,
                next_ts: current.ts,
                previous_record_ordinal: prev.ordinal,
                record_ordinal: current.ordinal,
            });
        } else {
            let delta = current.sequence as i128 - prev.sequence as i128;
            if delta > 1 {
                report.gaps.push(SequenceGap {
                    previous_sequence: prev.sequence,
                    expected_sequence: prev.sequence.saturating_add(1),
                    next_sequence: current.sequence,
                    missing_count: (delta - 1) as u64,
                    previous_ts: prev.ts,
                    next_ts: current.ts,
                    previous_record_ordinal: prev.ordinal,
                    record_ordinal: current.ordinal,
                });
            }
        }
    }
}
