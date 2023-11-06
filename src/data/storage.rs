use super::record::{ReadRecordHeaderBuf, Record, RecordType};
use crate::{KvError, Result};
use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::decode_length_delimiter;
use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[cfg(target_os = "windows")]
use std::os::windows::prelude::FileExt;

const STORAGE_SUFFIX: &str = "storage";
const STORAGE_SUFFIX_WITH_DOT: &str = ".storage";

pub(crate) struct Storage {
    pub(crate) gen: u32,
    offset: Arc<AtomicU64>,
    fd: Arc<RwLock<File>>,
}

impl Storage {
    // 打开一个已存在的`Storage`
    pub(crate) fn open<P: AsRef<Path>>(gen_path: P) -> Result<Self> {
        let gen = is_storage_file(gen_path.as_ref())?;
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(gen_path)?;

        Ok(Self {
            gen,
            offset: Arc::new(AtomicU64::new(0)),
            fd: Arc::new(RwLock::new(fd)),
        })
    }

    /// 打开或初始化一个`Storgae`
    pub(crate) fn new<P: AsRef<Path>>(dir_path: &P, gen: u32) -> Result<Self> {
        let gen_path = dir_path.as_ref().join(storage_name_from_gen(gen));
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(gen_path)?;
        Ok(Self {
            gen,
            offset: Arc::new(AtomicU64::new(0)),
            fd: Arc::new(RwLock::new(fd)),
        })
    }

    /// 读取正确crc校验值的`Record`
    pub(crate) fn read_record(&self, offset: u64) -> Result<Record> {
        let header_buf = self.read_record_head_buf(offset)?;
        let header_len = header_buf.get_header_len();

        // 计算剩余部分的偏移量并读取
        let mut kv_buf = BytesMut::zeroed(header_buf.key_size + header_buf.value_size + 4);
        self.fd
            .read()
            .seek_read(&mut kv_buf, offset + header_len as u64)?;

        let mut target_record = Record {
            key: kv_buf.get(..header_buf.key_size).unwrap().to_vec(),
            // key: x.to_vec(),
            value: kv_buf
                .get(header_buf.key_size..kv_buf.len() - 4)
                .unwrap()
                .to_vec(),
            record_type: header_buf.record_type,
        };

        // 移动游标至最后4字节
        kv_buf.advance(header_buf.key_size + header_buf.value_size);
        let crc = kv_buf.get_u32();

        // 计算并验证crc正确性
        let target_crc = target_record.target_crc()?;
        if target_crc != crc {
            Err(KvError::InvalidCrc)
        } else {
            Ok(target_record)
        }
    }

    // 仅用于从storage中读取key，但未验证crc正确性
    pub(crate) fn read_key_from_header(
        &self,
        offset: u64,
        header_buf: &ReadRecordHeaderBuf,
    ) -> Result<Vec<u8>> {
        let header_len = header_buf.get_header_len();

        // 计算并获取key
        let mut key_buf = BytesMut::zeroed(header_buf.key_size);
        self.fd
            .read()
            .seek_read(&mut key_buf, offset + header_len as u64)?;

        Ok(key_buf.into())
    }

    /// 读取`Record`中的header部分，包括recory type，key size，value size
    pub(crate) fn read_record_head_buf(&self, offset: u64) -> Result<ReadRecordHeaderBuf> {
        // record type + max key size + max value size
        let mut header_buf = BytesMut::zeroed(1 + 5 + 5);
        self.fd.read().seek_read(&mut header_buf, offset)?;

        // 获取Record类型
        let record_type = header_buf.get_u8().into();
        if let RecordType::UnexpectCommand = record_type {
            return Err(KvError::ReadEOF);
        }

        // 获取key size和value size
        let key_size = decode_length_delimiter(&mut header_buf)?;
        let value_size = decode_length_delimiter(&mut header_buf)?;
        if key_size == 0 {
            // key的长度不允许为0
            return Err(KvError::InvalidKey);
        }

        Ok(ReadRecordHeaderBuf {
            record_type,
            key_size,
            value_size,
        })
    }

    /// 将buf写入至当前的`Storage`文件中
    pub(crate) fn write(&self, buf: &[u8]) -> Result<usize> {
        let len = self.fd.write().write(buf)?;

        self.offset.fetch_add(len as u64, Ordering::SeqCst);
        Ok(len)
    }

    /// 将当前`Storage`的数据同步
    pub(crate) fn sync(&self) -> Result<()> {
        self.fd.read().sync_all().map_err(KvError::Io)
    }

    /// 获取当前`Storage`的数据偏移
    pub(crate) fn get_offset(&self) -> u64 {
        self.offset.load(Ordering::Relaxed)
    }

    /// 设置当前`Storage`的数据偏移
    pub(crate) fn set_offset(&self, offset: u64) {
        self.offset.store(offset, Ordering::Relaxed);
    }
}

#[inline]
fn is_storage_file<P: AsRef<Path>>(gen_path: P) -> Result<u32> {
    if !gen_path.as_ref().is_file()
        || gen_path.as_ref().extension() != Some(STORAGE_SUFFIX.as_ref())
    {
        return Err(KvError::InvalidPath);
    }

    let Some(Ok(gen)) = gen_path
        .as_ref()
        .file_name()
        .and_then(OsStr::to_str)
        .map(|s| s.trim_end_matches(STORAGE_SUFFIX_WITH_DOT))
        .map(str::parse::<u32>)
    else {
        return Err(KvError::InvalidPath);
    };

    Ok(gen)
}

#[inline]
pub(crate) fn storage_name_from_gen(gen: u32) -> String {
    format!("{:09}{}", gen, STORAGE_SUFFIX_WITH_DOT)
}
