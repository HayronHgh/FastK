use crate::error::{FastKError, Result};

use crate::types::{FixedRecord, RecordType};

/// Fixed-width OHLCV record stored in chunk files.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, bytemuck::NoUninit)]
pub struct KlineRecord {
    pub ts: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: i64,
}

impl FixedRecord for KlineRecord {
    const BYTE_SIZE: usize = KlineRecord::BYTE_SIZE;
    const SCHEMA_ID: u32 = crate::chunk::header::KLINE_SCHEMA_ID;
    const RECORD_TYPE: RecordType = RecordType::Kline;

    fn ts(&self) -> i64 {
        self.ts
    }

    fn encode_le(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_le_bytes());
    }

    fn decode_le(bytes: &[u8]) -> Result<Self> {
        let raw: [u8; Self::BYTE_SIZE] = bytes
            .try_into()
            .map_err(|_| FastKError::InvalidData("invalid kline record bytes".to_string()))?;
        Ok(Self::from_le_bytes(raw))
    }
}

impl KlineRecord {
    pub const BYTE_SIZE: usize = 48;

    /// Encodes the record to a little-endian fixed-width buffer.
    pub fn to_le_bytes(&self) -> [u8; Self::BYTE_SIZE] {
        let mut buf = [0u8; Self::BYTE_SIZE];
        let mut offset = 0usize;

        for value in [
            self.ts,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
        ] {
            buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            offset += 8;
        }

        buf
    }

    /// Decodes a record from a fixed-width little-endian buffer.
    pub fn from_le_bytes(bytes: [u8; Self::BYTE_SIZE]) -> Self {
        let mut values = [0i64; 6];
        let mut offset = 0usize;

        for slot in values.iter_mut() {
            let mut tmp = [0u8; 8];
            tmp.copy_from_slice(&bytes[offset..offset + 8]);
            *slot = i64::from_le_bytes(tmp);
            offset += 8;
        }

        Self {
            ts: values[0],
            open: values[1],
            high: values[2],
            low: values[3],
            close: values[4],
            volume: values[5],
        }
    }

    /// Validates that a slice is strictly ordered by timestamp.
    pub fn validate_strict_order(records: &[Self]) -> Result<()> {
        if records.is_empty() {
            return Err(FastKError::InvalidInput(
                "kline chunk must contain at least one record".to_string(),
            ));
        }

        for window in records.windows(2) {
            if window[0].ts >= window[1].ts {
                return Err(FastKError::InvalidInput(format!(
                    "timestamps must be strictly increasing: {} >= {}",
                    window[0].ts, window[1].ts
                )));
            }
        }

        Ok(())
    }
}

/// Sentinel stored for an absent flow field; it is never a valid flow value.
///
/// Flow fields are non-negative when present, so the sentinel cannot collide with
/// data, and an absent value is never stored or reported as `0`.
pub const KLINE_FLOW_MISSING: i64 = i64::MIN;

/// Fixed-width kline record with the exchange order-flow fields (schema 3).
///
/// The first six words have exactly the [`KlineRecord`] layout. The extra words
/// carry quote volume, trade count, taker-buy base volume and taker-buy quote
/// volume as signed integers. `quote_volume` and `taker_buy_quote_volume` use the
/// series `quote_scale`; `taker_buy_volume` uses `volume_scale`; `trade_count` is
/// an unscaled count. [`KLINE_FLOW_MISSING`] marks an absent value; use the
/// `*_opt` accessors to read flow fields as `Option<i64>`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, bytemuck::NoUninit)]
pub struct KlineFlowRecord {
    pub ts: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: i64,
    pub quote_volume: i64,
    pub trade_count: i64,
    pub taker_buy_volume: i64,
    pub taker_buy_quote_volume: i64,
}

impl Default for KlineFlowRecord {
    /// Zero OHLCV words with every flow field absent.
    fn default() -> Self {
        Self::from_ohlcv(KlineRecord::default())
    }
}

impl FixedRecord for KlineFlowRecord {
    const BYTE_SIZE: usize = KlineFlowRecord::BYTE_SIZE;
    const SCHEMA_ID: u32 = crate::chunk::header::KLINE_FLOW_SCHEMA_ID;
    const RECORD_TYPE: RecordType = RecordType::Kline;

    fn ts(&self) -> i64 {
        self.ts
    }

    fn encode_le(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_le_bytes());
    }

    fn decode_le(bytes: &[u8]) -> Result<Self> {
        let raw: [u8; Self::BYTE_SIZE] = bytes
            .try_into()
            .map_err(|_| FastKError::InvalidData("invalid kline flow record bytes".to_string()))?;
        Ok(Self::from_le_bytes(raw))
    }
}

impl KlineFlowRecord {
    pub const BYTE_SIZE: usize = 80;

    /// Builds a record from OHLCV words and optional flow values.
    pub fn new(
        base: KlineRecord,
        quote_volume: Option<i64>,
        trade_count: Option<i64>,
        taker_buy_volume: Option<i64>,
        taker_buy_quote_volume: Option<i64>,
    ) -> Self {
        let word = |value: Option<i64>| value.unwrap_or(KLINE_FLOW_MISSING);
        Self {
            ts: base.ts,
            open: base.open,
            high: base.high,
            low: base.low,
            close: base.close,
            volume: base.volume,
            quote_volume: word(quote_volume),
            trade_count: word(trade_count),
            taker_buy_volume: word(taker_buy_volume),
            taker_buy_quote_volume: word(taker_buy_quote_volume),
        }
    }

    /// Lifts an OHLCV record; every flow field is absent, never zero.
    pub fn from_ohlcv(base: KlineRecord) -> Self {
        Self::new(base, None, None, None, None)
    }

    /// Returns the OHLCV prefix.
    pub fn ohlcv(&self) -> KlineRecord {
        KlineRecord {
            ts: self.ts,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
        }
    }

    pub fn quote_volume_opt(&self) -> Option<i64> {
        present(self.quote_volume)
    }

    pub fn trade_count_opt(&self) -> Option<i64> {
        present(self.trade_count)
    }

    pub fn taker_buy_volume_opt(&self) -> Option<i64> {
        present(self.taker_buy_volume)
    }

    pub fn taker_buy_quote_volume_opt(&self) -> Option<i64> {
        present(self.taker_buy_quote_volume)
    }

    /// Validates value ranges before a write.
    ///
    /// Volume and present flow values must be non-negative, taker-buy volume must
    /// not exceed volume, and taker-buy quote volume must not exceed quote volume
    /// (which therefore must be present).
    pub fn validate_values(&self) -> Result<()> {
        let invalid = |message: String| {
            Err(FastKError::InvalidInput(format!(
                "invalid kline flow record at ts {}: {message}",
                self.ts
            )))
        };
        if self.volume < 0 {
            return invalid(format!("volume must be non-negative, got {}", self.volume));
        }
        for (name, value) in [
            ("quote_volume", self.quote_volume),
            ("trade_count", self.trade_count),
            ("taker_buy_volume", self.taker_buy_volume),
            ("taker_buy_quote_volume", self.taker_buy_quote_volume),
        ] {
            if value < 0 && value != KLINE_FLOW_MISSING {
                return invalid(format!("{name} must be non-negative, got {value}"));
            }
        }
        if let Some(taker) = self.taker_buy_volume_opt() {
            if taker > self.volume {
                return invalid(format!(
                    "taker_buy_volume {taker} exceeds volume {}",
                    self.volume
                ));
            }
        }
        if let Some(taker_quote) = self.taker_buy_quote_volume_opt() {
            match self.quote_volume_opt() {
                None => {
                    return invalid(
                        "taker_buy_quote_volume requires quote_volume to be present".into(),
                    )
                }
                Some(quote) if taker_quote > quote => {
                    return invalid(format!(
                        "taker_buy_quote_volume {taker_quote} exceeds quote_volume {quote}"
                    ))
                }
                Some(_) => {}
            }
        }
        Ok(())
    }

    /// Validates every record's values (ordering is validated separately).
    pub fn validate_all(records: &[Self]) -> Result<()> {
        records.iter().try_for_each(Self::validate_values)
    }

    /// Encodes the record to a little-endian fixed-width buffer.
    pub fn to_le_bytes(&self) -> [u8; Self::BYTE_SIZE] {
        let mut buf = [0u8; Self::BYTE_SIZE];
        for (slot, value) in buf.chunks_exact_mut(8).zip(self.words()) {
            slot.copy_from_slice(&value.to_le_bytes());
        }
        buf
    }

    /// Decodes a record from a fixed-width little-endian buffer.
    pub fn from_le_bytes(bytes: [u8; Self::BYTE_SIZE]) -> Self {
        let mut values = [0i64; 10];
        for (slot, chunk) in values.iter_mut().zip(bytes.chunks_exact(8)) {
            *slot = i64::from_le_bytes(chunk.try_into().expect("8-byte word"));
        }
        Self {
            ts: values[0],
            open: values[1],
            high: values[2],
            low: values[3],
            close: values[4],
            volume: values[5],
            quote_volume: values[6],
            trade_count: values[7],
            taker_buy_volume: values[8],
            taker_buy_quote_volume: values[9],
        }
    }

    fn words(&self) -> [i64; 10] {
        [
            self.ts,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.quote_volume,
            self.trade_count,
            self.taker_buy_volume,
            self.taker_buy_quote_volume,
        ]
    }
}

fn present(value: i64) -> Option<i64> {
    (value != KLINE_FLOW_MISSING).then_some(value)
}
