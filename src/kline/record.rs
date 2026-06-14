use crate::error::{FastKError, Result};

use crate::types::{FixedRecord, RecordType};

/// Fixed-width OHLCV record stored in chunk files.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
