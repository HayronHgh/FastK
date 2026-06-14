use crate::chunk::header::BBO_SCHEMA_ID;
use crate::error::{FastKError, Result};

use super::{read_i64, FixedRecord, RecordType};

/// Fixed-width best bid/offer record.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BboRecord {
    pub ts: i64,
    pub recv_ts: i64,
    pub bid_price: i64,
    pub bid_qty: i64,
    pub ask_price: i64,
    pub ask_qty: i64,
    pub sequence: i64,
}

impl BboRecord {
    pub const BYTE_SIZE: usize = 56;

    pub fn to_le_bytes(&self) -> [u8; Self::BYTE_SIZE] {
        let mut out = Vec::with_capacity(Self::BYTE_SIZE);
        self.encode_le(&mut out);
        out.try_into()
            .expect("bbo record encoder must emit BYTE_SIZE bytes")
    }

    pub fn from_le_bytes(bytes: [u8; Self::BYTE_SIZE]) -> Self {
        Self::decode_le(&bytes).expect("fixed-size bbo bytes should decode")
    }

    pub fn validate_strict_order(records: &[Self]) -> Result<()> {
        <Self as FixedRecord>::validate_strict_order(records)
    }
}

impl FixedRecord for BboRecord {
    const BYTE_SIZE: usize = BboRecord::BYTE_SIZE;
    const SCHEMA_ID: u32 = BBO_SCHEMA_ID;
    const RECORD_TYPE: RecordType = RecordType::Bbo;

    fn ts(&self) -> i64 {
        self.ts
    }

    fn encode_le(&self, out: &mut Vec<u8>) {
        for value in [
            self.ts,
            self.recv_ts,
            self.bid_price,
            self.bid_qty,
            self.ask_price,
            self.ask_qty,
            self.sequence,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }

    fn decode_le(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::BYTE_SIZE {
            return Err(FastKError::InvalidData(format!(
                "unexpected bbo record byte length: {}",
                bytes.len()
            )));
        }
        let mut offset = 0usize;
        Ok(Self {
            ts: read_i64(bytes, &mut offset)?,
            recv_ts: read_i64(bytes, &mut offset)?,
            bid_price: read_i64(bytes, &mut offset)?,
            bid_qty: read_i64(bytes, &mut offset)?,
            ask_price: read_i64(bytes, &mut offset)?,
            ask_qty: read_i64(bytes, &mut offset)?,
            sequence: read_i64(bytes, &mut offset)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::BboRecord;

    #[test]
    fn bbo_record_binary_roundtrip_preserves_fields() {
        let record = BboRecord {
            ts: 100,
            recv_ts: 101,
            bid_price: 123,
            bid_qty: 10,
            ask_price: 124,
            ask_qty: 11,
            sequence: 77,
        };

        assert_eq!(std::mem::size_of::<BboRecord>(), BboRecord::BYTE_SIZE);
        assert_eq!(BboRecord::from_le_bytes(record.to_le_bytes()), record);
    }
}
