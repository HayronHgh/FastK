use crate::error::{FastKError, Result};

/// Supported scalar comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompareOp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    Between,
}

/// Legacy predicate applied to scalar values.
///
/// This struct is retained as the compatibility predicate used by the original
/// zmap/vix APIs. New storage-level predicate queries use [`ScalarPredicateExpr`]
/// so set predicates can be represented without changing this public layout.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScalarPredicate {
    pub op: CompareOp,
    pub value: i64,
    pub value2: Option<i64>,
}

impl ScalarPredicate {
    /// Returns true when a candidate value satisfies the predicate.
    pub fn matches(&self, candidate: i64) -> Result<bool> {
        Ok(match self.op {
            CompareOp::Gt => candidate > self.value,
            CompareOp::Gte => candidate >= self.value,
            CompareOp::Lt => candidate < self.value,
            CompareOp::Lte => candidate <= self.value,
            CompareOp::Eq => candidate == self.value,
            CompareOp::Between => {
                let (lower, upper) = self.bounds()?;
                candidate >= lower && candidate <= upper
            }
        })
    }

    /// Returns inclusive bounds for a `Between` predicate.
    pub fn bounds(&self) -> Result<(i64, i64)> {
        let upper = self.value2.ok_or_else(|| {
            FastKError::InvalidInput("between predicate requires value2".to_string())
        })?;
        Ok((self.value.min(upper), self.value.max(upper)))
    }

    /// Returns true when the predicate can be accelerated by continuous range pruning.
    pub fn is_continuous_range_like(&self) -> bool {
        matches!(
            self.op,
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte | CompareOp::Between
        )
    }

    /// Returns true when the predicate can be accelerated by exact value lookup.
    pub fn is_discrete_lookup_like(&self) -> bool {
        self.op == CompareOp::Eq
    }
}

/// Full storage-level predicate expression for `ScalarRecord.value`.
///
/// FastK treats every value as a caller-normalized integer. It does not attach
/// indicator, feature, factor, signal, strategy, or trading meaning to the value.
///
/// Use this type for new predicate queries. [`ScalarPredicate`] remains the
/// legacy compatibility shape for older zmap/vix helpers.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ScalarPredicateExpr {
    Eq(i64),
    Ne(i64),
    Gt(i64),
    Gte(i64),
    Lt(i64),
    Lte(i64),
    Between { min: i64, max: i64, inclusive: bool },
    InSet(Vec<i64>),
    NotInSet(Vec<i64>),
}

impl ScalarPredicateExpr {
    /// Returns true when a candidate value satisfies the predicate.
    pub fn matches(&self, value: i64) -> bool {
        match self {
            Self::Eq(target) => value == *target,
            Self::Ne(target) => value != *target,
            Self::Gt(target) => value > *target,
            Self::Gte(target) => value >= *target,
            Self::Lt(target) => value < *target,
            Self::Lte(target) => value <= *target,
            Self::Between {
                min,
                max,
                inclusive,
            } => {
                if *inclusive {
                    value >= *min && value <= *max
                } else {
                    value > *min && value < *max
                }
            }
            Self::InSet(values) => values.contains(&value),
            Self::NotInSet(values) => !values.contains(&value),
        }
    }

    /// Validates predicate parameters that cannot be expressed safely.
    pub fn validate(&self) -> Result<()> {
        if let Self::Between { min, max, .. } = self {
            if min > max {
                return Err(FastKError::InvalidInput(format!(
                    "between predicate min {min} must be <= max {max}"
                )));
            }
        }
        Ok(())
    }

    /// Returns true when the predicate is usually suited to zmap range pruning.
    pub fn is_continuous_range_like(&self) -> bool {
        matches!(
            self,
            Self::Gt(_) | Self::Gte(_) | Self::Lt(_) | Self::Lte(_) | Self::Between { .. }
        )
    }

    /// Returns true when the predicate is usually suited to vix exact lookup.
    pub fn is_discrete_lookup_like(&self) -> bool {
        matches!(self, Self::Eq(_) | Self::InSet(_))
    }

    /// Returns true when the predicate is guaranteed to match no values.
    pub fn is_impossible(&self) -> bool {
        matches!(self, Self::InSet(values) if values.is_empty())
    }

    pub(crate) fn may_match_value_range(&self, min_value: i64, max_value: i64) -> bool {
        match self {
            Self::Eq(target) => min_value <= *target && max_value >= *target,
            Self::Ne(target) => !(min_value == *target && max_value == *target),
            Self::Gt(target) => max_value > *target,
            Self::Gte(target) => max_value >= *target,
            Self::Lt(target) => min_value < *target,
            Self::Lte(target) => min_value <= *target,
            Self::Between {
                min,
                max,
                inclusive,
            } => {
                if *inclusive {
                    max_value >= *min && min_value <= *max
                } else {
                    max_value > *min && min_value < *max
                }
            }
            Self::InSet(values) => values
                .iter()
                .any(|value| min_value <= *value && max_value >= *value),
            Self::NotInSet(values) => {
                values.is_empty() || !(min_value == max_value && values.contains(&min_value))
            }
        }
    }
}

impl TryFrom<&ScalarPredicate> for ScalarPredicateExpr {
    type Error = FastKError;

    fn try_from(value: &ScalarPredicate) -> Result<Self> {
        Ok(match value.op {
            CompareOp::Gt => Self::Gt(value.value),
            CompareOp::Gte => Self::Gte(value.value),
            CompareOp::Lt => Self::Lt(value.value),
            CompareOp::Lte => Self::Lte(value.value),
            CompareOp::Eq => Self::Eq(value.value),
            CompareOp::Between => {
                let max = value.value2.ok_or_else(|| {
                    FastKError::InvalidInput("between predicate requires value2".to_string())
                })?;
                Self::Between {
                    min: value.value.min(max),
                    max: value.value.max(max),
                    inclusive: true,
                }
            }
        })
    }
}

/// Logical key for a scalar time series.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ScalarSeriesKey {
    pub symbol: String,
    pub category: String,
    pub name: String,
}

/// Storage-level scalar predicate query input.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScalarPredicateQuery {
    pub key: ScalarSeriesKey,
    pub start_ts: i64,
    pub end_ts: i64,
    pub predicate: ScalarPredicateExpr,
    pub return_values: bool,
}

/// One matched scalar predicate row.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScalarPredicateMatch {
    pub ts: i64,
    pub value: Option<i64>,
}

/// Scalar predicate query result with storage-level execution stats.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScalarPredicateQueryResult {
    pub matches: Vec<ScalarPredicateMatch>,
    pub stats: ScalarPredicateQueryStats,
}

/// Storage-level index family used by a predicate query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ScalarIndexKind {
    ZoneMap,
    ValueIndex,
    ZoneMapAndValueIndex,
    FullScan,
}

/// Storage-level predicate execution counters.
///
/// `Eq` and `InSet` are usually best suited to [`ScalarIndexKind::ValueIndex`].
/// `Gt`, `Gte`, `Lt`, `Lte`, and `Between` are usually best suited to
/// [`ScalarIndexKind::ZoneMap`]. `Ne` and `NotInSet` commonly require a raw
/// scan; callers should inspect `fallback_scan`, `index_used`, and
/// `rows_checked` before assuming a predicate was index-accelerated.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScalarPredicateQueryStats {
    pub chunks_considered: u64,
    pub chunks_scanned: u64,
    pub chunks_pruned: u64,
    pub blocks_considered: u64,
    pub blocks_scanned: u64,
    pub blocks_pruned: u64,
    pub rows_checked: u64,
    pub rows_matched: u64,
    pub index_used: Option<ScalarIndexKind>,
    pub fallback_scan: bool,
}

#[cfg(test)]
mod tests {
    use super::{CompareOp, ScalarPredicate, ScalarPredicateExpr};

    #[test]
    fn scalar_predicate_expr_matches_all_supported_operators() {
        assert!(ScalarPredicateExpr::Eq(10).matches(10));
        assert!(!ScalarPredicateExpr::Eq(10).matches(11));
        assert!(ScalarPredicateExpr::Ne(10).matches(11));
        assert!(ScalarPredicateExpr::Gt(10).matches(11));
        assert!(ScalarPredicateExpr::Gte(10).matches(10));
        assert!(ScalarPredicateExpr::Lt(10).matches(9));
        assert!(ScalarPredicateExpr::Lte(10).matches(10));
        assert!(ScalarPredicateExpr::Between {
            min: 10,
            max: 20,
            inclusive: true,
        }
        .matches(10));
        assert!(!ScalarPredicateExpr::Between {
            min: 10,
            max: 20,
            inclusive: false,
        }
        .matches(10));
        assert!(ScalarPredicateExpr::InSet(vec![1, 3, 5]).matches(3));
        assert!(!ScalarPredicateExpr::InSet(Vec::new()).matches(3));
        assert!(ScalarPredicateExpr::NotInSet(vec![1, 3, 5]).matches(4));
        assert!(ScalarPredicateExpr::NotInSet(Vec::new()).matches(4));
    }

    #[test]
    fn scalar_predicate_expr_rejects_invalid_between_bounds() {
        let err = ScalarPredicateExpr::Between {
            min: 20,
            max: 10,
            inclusive: true,
        }
        .validate()
        .expect_err("min > max should be invalid");
        assert!(matches!(err, crate::FastKError::InvalidInput(_)));
    }

    #[test]
    fn legacy_scalar_predicate_still_matches_and_classifies() {
        let predicate = ScalarPredicate {
            op: CompareOp::Between,
            value: 10,
            value2: Some(20),
        };
        assert!(predicate.matches(15).expect("legacy match should work"));
        assert!(predicate.is_continuous_range_like());
        assert!(!predicate.is_discrete_lookup_like());

        let eq = ScalarPredicate {
            op: CompareOp::Eq,
            value: 10,
            value2: None,
        };
        assert!(eq.is_discrete_lookup_like());
    }
}
