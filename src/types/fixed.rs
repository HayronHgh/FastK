use crate::error::{FastKError, Result};

use super::RecordType;

/// Internal/public contract for fixed-width timestamped records stored in FastK chunks.
pub trait FixedRecord: Copy + Sized {
    const BYTE_SIZE: usize;
    const SCHEMA_ID: u32;
    const RECORD_TYPE: RecordType;
    const ALLOW_EQUAL_TIMESTAMPS: bool = false;

    fn ts(&self) -> i64;
    fn encode_le(&self, out: &mut Vec<u8>);
    fn decode_le(bytes: &[u8]) -> Result<Self>;

    fn validate_strict_order(records: &[Self]) -> Result<()> {
        if records.is_empty() {
            return Err(FastKError::InvalidInput(
                "fixed-record chunk must contain at least one record".to_string(),
            ));
        }
        for window in records.windows(2) {
            let violates_order = if Self::ALLOW_EQUAL_TIMESTAMPS {
                window[0].ts() > window[1].ts()
            } else {
                window[0].ts() >= window[1].ts()
            };
            if violates_order {
                return Err(FastKError::InvalidInput(format!(
                    "timestamps must be ordered: {} before {}",
                    window[0].ts(),
                    window[1].ts()
                )));
            }
        }
        Ok(())
    }
}

pub(crate) fn read_i64(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    let end = offset.saturating_add(8);
    let raw: [u8; 8] = bytes
        .get(*offset..end)
        .ok_or_else(|| FastKError::InvalidData("fixed record ended unexpectedly".to_string()))?
        .try_into()
        .map_err(|_| FastKError::InvalidData("invalid i64 field".to_string()))?;
    *offset = end;
    Ok(i64::from_le_bytes(raw))
}

pub(crate) fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset.saturating_add(4);
    let raw: [u8; 4] = bytes
        .get(*offset..end)
        .ok_or_else(|| FastKError::InvalidData("fixed record ended unexpectedly".to_string()))?
        .try_into()
        .map_err(|_| FastKError::InvalidData("invalid u32 field".to_string()))?;
    *offset = end;
    Ok(u32::from_le_bytes(raw))
}
