use crate::chunk::header::BOOK_DELTA_SCHEMA_ID;
use crate::error::{FastKError, Result};

use super::{read_i64, read_u32, FixedRecord, RecordType};

/// Fixed-width L2 order book delta record.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookDeltaRecord {
    pub ts: i64,
    pub recv_ts: i64,
    pub sequence: i64,
    pub price: i64,
    pub qty: i64,
    pub side: i8,
    pub action: i8,
    pub level: i16,
    pub flags: u32,
}

impl BookDeltaRecord {
    pub const BYTE_SIZE: usize = 48;

    pub fn to_le_bytes(&self) -> [u8; Self::BYTE_SIZE] {
        let mut out = Vec::with_capacity(Self::BYTE_SIZE);
        self.encode_le(&mut out);
        out.try_into()
            .expect("book delta record encoder must emit BYTE_SIZE bytes")
    }

    pub fn from_le_bytes(bytes: [u8; Self::BYTE_SIZE]) -> Self {
        Self::decode_le(&bytes).expect("fixed-size book delta bytes should decode")
    }

    pub fn validate_strict_order(records: &[Self]) -> Result<()> {
        <Self as FixedRecord>::validate_strict_order(records)
    }
}

impl FixedRecord for BookDeltaRecord {
    const BYTE_SIZE: usize = BookDeltaRecord::BYTE_SIZE;
    const SCHEMA_ID: u32 = BOOK_DELTA_SCHEMA_ID;
    const RECORD_TYPE: RecordType = RecordType::BookDelta;
    const ALLOW_EQUAL_TIMESTAMPS: bool = true;

    fn ts(&self) -> i64 {
        self.ts
    }

    fn encode_le(&self, out: &mut Vec<u8>) {
        for value in [self.ts, self.recv_ts, self.sequence, self.price, self.qty] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out.push(self.side as u8);
        out.push(self.action as u8);
        out.extend_from_slice(&self.level.to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
    }

    fn decode_le(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::BYTE_SIZE {
            return Err(FastKError::InvalidData(format!(
                "unexpected book delta record byte length: {}",
                bytes.len()
            )));
        }
        let mut offset = 0usize;
        let ts = read_i64(bytes, &mut offset)?;
        let recv_ts = read_i64(bytes, &mut offset)?;
        let sequence = read_i64(bytes, &mut offset)?;
        let price = read_i64(bytes, &mut offset)?;
        let qty = read_i64(bytes, &mut offset)?;
        let side = bytes[offset] as i8;
        offset += 1;
        let action = bytes[offset] as i8;
        offset += 1;
        let level = i16::from_le_bytes(
            bytes[offset..offset + 2]
                .try_into()
                .map_err(|_| FastKError::InvalidData("invalid i16 field".to_string()))?,
        );
        offset += 2;
        let flags = read_u32(bytes, &mut offset)?;
        Ok(Self {
            ts,
            recv_ts,
            sequence,
            price,
            qty,
            side,
            action,
            level,
            flags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::BookDeltaRecord;

    #[test]
    fn book_delta_record_binary_roundtrip_preserves_fields() {
        let record = BookDeltaRecord {
            ts: 100,
            recv_ts: 101,
            sequence: 900,
            price: 123_456,
            qty: 7,
            side: -1,
            action: 2,
            level: 5,
            flags: 99,
        };

        assert_eq!(
            std::mem::size_of::<BookDeltaRecord>(),
            BookDeltaRecord::BYTE_SIZE
        );
        assert_eq!(BookDeltaRecord::from_le_bytes(record.to_le_bytes()), record);
    }
}
