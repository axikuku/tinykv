use bytes::BufMut;
use prost::{encode_length_delimiter, length_delimiter_len};

use crate::error::Result;

#[derive(Clone, Copy)]
pub enum RecordType {
    UnexpectCommand = 0,
    Normal = 1,
    Remove = 2,
}

impl From<u8> for RecordType {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Normal,
            2 => Self::Remove,
            _ => Self::UnexpectCommand,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RecordPos {
    pub(crate) gen: u32,
    pub(crate) offset: u64,
}

pub(crate) struct Record {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: RecordType,
}

impl Record {
    pub(crate) fn new_set(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            record_type: RecordType::Normal,
        }
    }

    pub(crate) fn new_remove(key: Vec<u8>) -> Self {
        Self {
            key,
            value: Vec::new(),
            record_type: RecordType::Remove,
        }
    }
    /// | type | key size | value size | key  | value | crc |
    /// | ---- | -------- | ---------- | ---- | ----- | --- |
    /// | 1    | 1 ~ 5    | 1 ~ 5      | dyn  | dyn   | 4   |
    ///
    /// 序列化为大端字符序列
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        // 为 buf header 部分预留可能的最大值
        // header_max = type + max(key size) + max(value size)
        let mut buf = Vec::with_capacity(self.encoded_len());
        buf.put_u8(self.record_type as u8);

        // 计算并存储key size和value size
        encode_length_delimiter(self.key.len(), &mut buf)?;
        encode_length_delimiter(self.value.len(), &mut buf)?;
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // 计算并存储CRC校验值
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);
        // self.crc = Some(crc);

        Ok(buf)
    }

    /// 获取目标`Record`的crc校验值
    pub(crate) fn target_crc(&mut self) -> Result<u32> {
        // 为 buf header 部分预留可能的最大值
        // header_max = type + max(key size) + max(value size)
        let mut buf = Vec::with_capacity(self.encoded_len());
        buf.put_u8(self.record_type as u8);

        // 计算并存储key size和value size
        encode_length_delimiter(self.key.len(), &mut buf)?;
        encode_length_delimiter(self.value.len(), &mut buf)?;
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // 计算并存储CRC校验值
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        Ok(hasher.finalize())
    }

    /// `Record`在磁盘中的实际长度
    fn encoded_len(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
    }
}

pub(crate) struct ReadRecordHeaderBuf {
    pub(crate) record_type: RecordType,
    pub(crate) key_size: usize,
    pub(crate) value_size: usize,
}

impl ReadRecordHeaderBuf {
    /// | type | key size | value size |
    /// | ---- | -------- | ---------- |
    /// | 1    | 1 ~ 5    | 1 ~ 5      |
    ///
    /// `Record`的header部分在磁盘中的长度
    pub(crate) fn get_header_len(&self) -> usize {
        length_delimiter_len(self.key_size) + length_delimiter_len(self.value_size) + 1
    }

    /// `Record`在磁盘中的实际长度
    pub(crate) fn encoded_len(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key_size)
            + length_delimiter_len(self.value_size)
            + self.key_size
            + self.value_size
            + 4
    }
}
