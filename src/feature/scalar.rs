use crate::error::{FastKError, Result};

use crate::types::{FixedRecord, RecordType};

/// Fixed-width scalar record used by the predicate query layer.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalarRecord {
    pub ts: i64,
    pub value: i64,
}

impl FixedRecord for ScalarRecord {
    const BYTE_SIZE: usize = ScalarRecord::BYTE_SIZE;
    const SCHEMA_ID: u32 = crate::chunk::header::SCALAR_SCHEMA_ID;
    const RECORD_TYPE: RecordType = RecordType::Scalar;

    fn ts(&self) -> i64 {
        self.ts
    }

    fn encode_le(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_le_bytes());
    }

    fn decode_le(bytes: &[u8]) -> Result<Self> {
        let raw: [u8; Self::BYTE_SIZE] = bytes
            .try_into()
            .map_err(|_| FastKError::InvalidData("invalid scalar record bytes".to_string()))?;
        Ok(Self::from_le_bytes(raw))
    }
}

impl ScalarRecord {
    pub const BYTE_SIZE: usize = 16;

    /// Encodes the record to a little-endian fixed-width buffer.
    pub fn to_le_bytes(&self) -> [u8; Self::BYTE_SIZE] {
        let mut buf = [0u8; Self::BYTE_SIZE];
        buf[..8].copy_from_slice(&self.ts.to_le_bytes());
        buf[8..16].copy_from_slice(&self.value.to_le_bytes());
        buf
    }

    /// Decodes a record from a fixed-width little-endian buffer.
    pub fn from_le_bytes(bytes: [u8; Self::BYTE_SIZE]) -> Self {
        let mut ts_buf = [0u8; 8];
        let mut value_buf = [0u8; 8];
        ts_buf.copy_from_slice(&bytes[..8]);
        value_buf.copy_from_slice(&bytes[8..16]);
        Self {
            ts: i64::from_le_bytes(ts_buf),
            value: i64::from_le_bytes(value_buf),
        }
    }

    /// Validates that scalar records are strictly ordered by timestamp.
    pub fn validate_strict_order(records: &[Self]) -> Result<()> {
        if records.is_empty() {
            return Err(FastKError::InvalidInput(
                "scalar records must not be empty".to_string(),
            ));
        }

        for window in records.windows(2) {
            if window[0].ts >= window[1].ts {
                return Err(FastKError::InvalidInput(format!(
                    "scalar timestamps must be strictly increasing: {} >= {}",
                    window[0].ts, window[1].ts
                )));
            }
        }

        Ok(())
    }
}
