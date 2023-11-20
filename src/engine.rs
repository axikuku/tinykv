use std::{collections::HashMap, fs, path::Path, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    config::Config,
    data::{
        record::{Record, RecordPos, RecordType},
        storage::{storage_name_from_gen, Storage},
    },
    error::{KvError, Result},
    index::{new_index, Index, IndexType},
};

pub struct Engine {
    pub(crate) config: Config,
    pub(crate) active_storage: Arc<RwLock<Storage>>,
    pub(crate) older_storages: Arc<RwLock<HashMap<u32, Storage>>>,
    pub(crate) index: Box<dyn Index>,
}

impl Engine {
    /// 根据配置信息创建一个 Engine 实体
    pub fn new(config: Config) -> Result<Self> {
        if !config.dir_path.is_dir() {
            std::fs::create_dir_all(&config.dir_path)?;
        }
        // 获取目标目录下storage的集合
        let mut storages = load_storages_sorted(&config.dir_path)?;
        let index = build_index_from_storage(&mut storages, config.index_type)?;

        // gen最大的文件即就是活跃文件
        // 若集合为空，则初始化新的storage作为活跃文件
        let active_storage = match storages.pop() {
            Some(s) => s,
            None => Storage::init_zero(&config.dir_path)?,
        };

        let older_storages = storages
            .into_iter()
            .map(|s| (s.gen, s))
            .collect::<HashMap<_, _>>();

        Ok(Self {
            index,
            active_storage: Arc::new(RwLock::new(active_storage)),
            older_storages: Arc::new(RwLock::new(older_storages)),
            config,
        })
    }

    /// 存储 key，value 数据，其中 key 不为空
    pub fn set<B: Into<Vec<u8>>>(&self, key: B, value: B) -> Result<()> {
        let key = key.into();
        if key.is_empty() {
            return Err(KvError::InvalidKey);
        }
        let value = value.into();
        let record = Record::new_set(key, value);

        // 写入记录
        let pos = self.append_record(&record)?;

        // 更新索引
        self.index.put(record.key, pos);
        Ok(())
    }

    /// 根据 key 获取对应的数据
    pub fn get<B: Into<Vec<u8>>>(&self, key: B) -> Result<Bytes> {
        let key = key.into();
        if key.is_empty() {
            return Err(KvError::InvalidKey);
        }

        let Some(pos) = self.index.get(&key) else {
            // key在索引中不存在
            return Err(KvError::InvalidKey);
        };

        self.read_value_from_pos(&pos)
    }

    /// 根据 key 删除对应的数据
    pub fn delete<B: Into<Vec<u8>>>(&self, key: B) -> Result<()> {
        let key = key.into();
        if key.is_empty() {
            return Err(KvError::InvalidKey);
        }

        // 先在索引中查找是否存在key
        if self.index.get(&key).is_none() {
            return Err(KvError::InvalidKey);
        }

        let record = Record::new_remove(key);

        // 写入记录
        self.append_record(&record)?;

        // 更新索引
        self.index.delete(&record.key);
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.active_storage.read().sync()
    }

    pub(crate) fn read_value_from_pos(&self, pos: &RecordPos) -> Result<Bytes> {
        let active_storage = self.active_storage.read();
        if active_storage.gen == pos.gen {
            // 若key在活跃文件中
            return Ok(active_storage.read_record(pos.offset)?.value.into());
        }

        // 若key在旧文件中
        let older_storages = self.older_storages.read();
        match older_storages.get(&pos.gen) {
            Some(storage) => Ok(storage.read_record(pos.offset)?.value.into()),
            None => Err(KvError::InvalidKey),
        }
    }

    /// 追加写数据到活跃文件中
    pub(crate) fn append_record(&self, record: &Record) -> Result<RecordPos> {
        let record_data = record.encode()?;

        let mut active_storage = self.active_storage.write();
        let offset = active_storage.get_offset();

        // 判断`Storage`文件是否达到阈值
        if offset + record_data.len() as u64 > self.config.storage_size {
            // 先持久化数据
            active_storage.sync()?;

            let old_gen = active_storage.gen;
            // 初始化新的活跃文件
            let file_name = self
                .config
                .dir_path
                .join(storage_name_from_gen(old_gen + 1));
            *active_storage = Storage::new(file_name.as_path())?;

            // 将旧的活跃文件放入map中
            let old_gen_path = self.config.dir_path.join(storage_name_from_gen(old_gen));
            let older_storage = Storage::new(old_gen_path.as_path())?;

            let mut older_storages = self.older_storages.write();
            older_storages.insert(older_storage.gen, older_storage);
        }

        // 写入记录
        active_storage.write(&record_data)?;

        // 写时持久化
        if self.config.sync_write {
            active_storage.sync()?;
        }

        Ok(RecordPos {
            gen: active_storage.gen,
            offset,
        })
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        match self.active_storage.write().sync() {
            Ok(_) => {}
            Err(e) => tracing::warn!("{}", e),
        }
    }
}

/// 从指定目录中读取已排序的`Storage`
fn load_storages_sorted(dir_path: &Path) -> Result<Vec<Storage>> {
    let mut storages = fs::read_dir(dir_path)?
        .flat_map(|entry| -> Result<_> { Ok(entry?.path()) })
        .filter_map(|gen_path| Storage::new(gen_path.as_path()).ok())
        .collect::<Vec<Storage>>();
    storages.sort_by_key(|s| s.gen);
    Ok(storages)
}

/// 从`Storage`集合中构建索引
fn build_index_from_storage(
    storages: &mut Vec<Storage>,
    index_type: IndexType,
) -> Result<Box<dyn Index>> {
    let index = new_index(index_type);
    if storages.is_empty() {
        return Ok(Box::new(index));
    }

    for storage in storages.iter_mut() {
        let mut offset = 0;
        loop {
            let record = match storage.read_record_head_buf(offset) {
                Ok(r) => r,
                Err(e) => {
                    if let KvError::ReadEOF = e {
                        break;
                    }
                    return Err(e);
                }
            };
            let record_size = record.encoded_len();

            // 构建索引
            let key = storage.read_key_from_header(offset, &record)?;
            let record_mate = RecordPos {
                gen: storage.gen,
                offset,
            };

            match record.record_type {
                RecordType::UnexpectCommand => break,
                RecordType::Normal => index.put(key, record_mate),
                RecordType::Remove => index.delete(key.as_slice()),
            };
            offset += record_size as u64;
        }
        // 设置数据偏移
        storage.set_offset(offset);
    }

    Ok(Box::new(index))
}
