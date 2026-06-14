use std::io::Read;

use crate::error::{FastKError, Result};

pub const CHUNK_MAGIC: [u8; 8] = *b"FASTK001";
pub const CHUNK_VERSION_V1: u32 = 1;
pub const CHUNK_VERSION_V2: u32 = 2;
pub const CHUNK_VERSION_CURRENT: u32 = CHUNK_VERSION_V2;
pub const KLINE_SCHEMA_ID: u32 = 1;
pub const SCALAR_SCHEMA_ID: u32 = 2;
pub const TRADE_SCHEMA_ID: u32 = 10;
pub const BBO_SCHEMA_ID: u32 = 11;
pub const BOOK_DELTA_SCHEMA_ID: u32 = 12;
pub const CHUNK_FLAG_HAS_SPARSE_INDEX: u64 = 1 << 0;

/// Fixed chunk header written ahead of all records and sparse index side data.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkHeader {
    pub magic: [u8; 8],
    pub version: u32,
    pub header_size: u32,
    pub record_size: u32,
    pub schema_id: u32,
    pub series_id: u64,
    pub chunk_id: u64,
    pub generation: u64,
    pub timeframe_ms: i64,
    pub start_ts: i64,
    pub end_ts: i64,
    pub count: u64,
    pub flags: u64,
    pub index_offset: u64,
    pub index_len: u64,
    pub sparse_index_every: u64,
}

impl ChunkHeader {
    pub const BYTE_SIZE_V1: usize = 88;
    pub const BYTE_SIZE_V2: usize = 128;

    pub fn to_le_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.header_size as usize];
        let mut offset = 0usize;

        buf[..8].copy_from_slice(&self.magic);
        offset += 8;

        let words = [
            self.version as u64,
            self.header_size as u64,
            self.record_size as u64,
            self.schema_id as u64,
            self.series_id,
            self.chunk_id,
            self.generation,
            self.timeframe_ms as u64,
            self.start_ts as u64,
            self.end_ts as u64,
            self.count,
            self.flags,
            self.index_offset,
            self.index_len,
            self.sparse_index_every,
        ];

        for value in words {
            if offset + 8 > buf.len() {
                break;
            }
            buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            offset += 8;
        }

        buf
    }

    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let mut prefix = [0u8; 24];
        reader.read_exact(&mut prefix)?;

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&prefix[..8]);
        let version = read_word(&prefix[8..16])? as u32;
        let header_size = read_word(&prefix[16..24])? as usize;

        if magic != CHUNK_MAGIC {
            return Err(FastKError::InvalidData("invalid chunk magic".to_string()));
        }
        let expected = match version {
            CHUNK_VERSION_V1 => Self::BYTE_SIZE_V1,
            CHUNK_VERSION_V2 => Self::BYTE_SIZE_V2,
            other => {
                return Err(FastKError::InvalidData(format!(
                    "unsupported chunk version: {other}",
                )))
            }
        };
        if header_size != expected {
            return Err(FastKError::InvalidData(format!(
                "unexpected header size {header_size} for version {version}",
            )));
        }

        let mut bytes = vec![0u8; header_size];
        bytes[..24].copy_from_slice(&prefix);
        reader.read_exact(&mut bytes[24..])?;
        Self::from_le_bytes(&bytes)
    }

    pub fn from_le_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::BYTE_SIZE_V1 && bytes.len() != Self::BYTE_SIZE_V2 {
            return Err(FastKError::InvalidData(format!(
                "unexpected chunk header byte length: {}",
                bytes.len()
            )));
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&bytes[..8]);
        let version = read_word(&bytes[8..16])? as u32;
        let header_size = read_word(&bytes[16..24])? as u32;

        let mut offset = 24usize;
        let mut next = |buf: &[u8]| -> Result<u64> {
            let value = read_word(&buf[offset..offset + 8])?;
            offset += 8;
            Ok(value)
        };

        let record_size = next(bytes)? as u32;
        let schema_id = next(bytes)? as u32;
        let series_id = next(bytes)?;
        let (chunk_id, generation) = if version >= CHUNK_VERSION_V2 {
            (next(bytes)?, next(bytes)?)
        } else {
            (0, 0)
        };
        let timeframe_ms = next(bytes)? as i64;
        let start_ts = next(bytes)? as i64;
        let end_ts = next(bytes)? as i64;
        let count = next(bytes)?;
        let flags = next(bytes)?;
        let (index_offset, index_len, sparse_index_every) = if version >= CHUNK_VERSION_V2 {
            (next(bytes)?, next(bytes)?, next(bytes)?)
        } else {
            (header_size as u64, 0, 0)
        };

        let header = Self {
            magic,
            version,
            header_size,
            record_size,
            schema_id,
            series_id,
            chunk_id,
            generation,
            timeframe_ms,
            start_ts,
            end_ts,
            count,
            flags,
            index_offset,
            index_len,
            sparse_index_every,
        };

        header.validate()?;
        Ok(header)
    }

    pub fn validate(&self) -> Result<()> {
        if self.magic != CHUNK_MAGIC {
            return Err(FastKError::InvalidData("invalid chunk magic".to_string()));
        }
        match self.version {
            CHUNK_VERSION_V1 => {
                if self.header_size as usize != Self::BYTE_SIZE_V1 {
                    return Err(FastKError::InvalidData(format!(
                        "unexpected header size: {}",
                        self.header_size
                    )));
                }
            }
            CHUNK_VERSION_V2 => {
                if self.header_size as usize != Self::BYTE_SIZE_V2 {
                    return Err(FastKError::InvalidData(format!(
                        "unexpected header size: {}",
                        self.header_size
                    )));
                }
            }
            other => {
                return Err(FastKError::InvalidData(format!(
                    "unsupported chunk version: {other}",
                )))
            }
        }
        if self.count > 0 && self.start_ts > self.end_ts {
            return Err(FastKError::InvalidData(format!(
                "chunk start_ts {} is after end_ts {}",
                self.start_ts, self.end_ts
            )));
        }
        if self.version >= CHUNK_VERSION_V2
            && (self.flags & CHUNK_FLAG_HAS_SPARSE_INDEX) != 0
            && self.sparse_index_every == 0
        {
            return Err(FastKError::InvalidData(
                "chunk declares sparse index but sparse_index_every=0".to_string(),
            ));
        }
        Ok(())
    }
}

fn read_word(bytes: &[u8]) -> Result<u64> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| FastKError::InvalidData("invalid 8-byte word".to_string()))?;
    Ok(u64::from_le_bytes(array))
}
