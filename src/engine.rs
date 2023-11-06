use crate::{
    config::Config,
    data::{
        record::{Record, RecordMeta, RecordType},
        storage::{storage_name_from_gen, Storage},
    },
    index::{self, new_index, Index},
    KvError, Result,
};
use parking_lot::RwLock;
use std::{collections::HashMap, fs, path::Path, sync::Arc};

pub struct Engine {
    pub(crate) config: Config,
    pub(crate) active_storage: Arc<RwLock<Storage>>,
    pub(crate) older_storages: Arc<RwLock<HashMap<u32, Storage>>>,
    pub(crate) index: Box<dyn index::Index>,
}

impl Engine {
    /// 用于构建`Engine`的默认配置
    /// - path: $tmp
    /// - storage size: 64MB
    /// - index type: BTree
    pub fn builder() -> Config {
        Config::default()
    }

    pub(crate) fn open(config: Config) -> Result<Self> {
        if !config.path.is_dir() {
            std::fs::create_dir_all(&config.path)?;
        }
        // 获取目标目录下storage的集合
        let mut storages = load_storages_sorted(&config.path)?;
        let index = build_index_from_storage(&mut storages, config.index_type)?;

        // gen最大的文件即就是活跃文件
        // 若集合为空，则初始化新的storage作为活跃文件
        let active_storage = match storages.pop() {
            Some(s) => s,
            None => Storage::new(&config.path, 0)?,
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
        self.index.set(record.key, pos);
        Ok(())
    }

    pub fn get<B: Into<Vec<u8>>>(&self, key: B) -> Result<Vec<u8>> {
        let key = key.into();
        if key.is_empty() {
            return Err(KvError::InvalidKey);
        }

        let Some(meta) = self.index.get(&key) else {
            // key在索引中不存在
            return Err(KvError::InvalidKey);
        };

        let active_storage = self.active_storage.read();
        if active_storage.gen == meta.gen {
            // 若key在活跃文件中
            return Ok(active_storage.read_record(meta.offset)?.value);
        }

        // 若key在旧文件中
        let older_storages = self.older_storages.read();
        match older_storages.get(&meta.gen) {
            Some(storage) => Ok(storage.read_record(meta.offset)?.value),
            None => Err(KvError::InvalidKey),
        }
    }

    pub fn remove<B: Into<Vec<u8>>>(&mut self, key: B) -> Result<()> {
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
        self.index.remove(&record.key);
        Ok(())
    }

    fn append_record(&self, record: &Record) -> Result<RecordMeta> {
        let record_data = record.encode()?;

        let mut active_storage = self.active_storage.write();
        let offset = active_storage.get_offset();

        // 判断`Storage`文件是否达到阈值
        if offset + record_data.len() as u64 > self.config.storage_size {
            // 先持久化数据
            active_storage.sync()?;

            let old_gen = active_storage.gen;
            // 初始化新的活跃文件
            *active_storage = Storage::new(&self.config.path, old_gen + 1)?;

            // 将旧的活跃文件放入map中
            let old_gen_path = self.config.path.join(storage_name_from_gen(old_gen));
            let older_storage = Storage::open(old_gen_path)?;

            let mut older_storages = self.older_storages.write();
            older_storages.insert(older_storage.gen, older_storage);
        }

        // 写入记录
        active_storage.write(&record_data)?;
        Ok(RecordMeta {
            gen: active_storage.gen,
            offset,
        })
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        match self.active_storage.write().sync() {
            Ok(_) => {}
            Err(e) => eprint!("{}", e),
        }
    }
}

/// 从指定目录中读取已排序的`Storage`
fn load_storages_sorted<P: AsRef<Path>>(dir_path: P) -> Result<Vec<Storage>> {
    let mut storages = fs::read_dir(dir_path)?
        .flat_map(|entry| -> Result<_> { Ok(entry?.path()) })
        .filter_map(|gen_path| Storage::open(gen_path).ok())
        .collect::<Vec<Storage>>();
    storages.sort_by_key(|s| s.gen);
    Ok(storages)
}

/// 从`Storage`集合中构建索引
fn build_index_from_storage(
    storages: &mut Vec<Storage>,
    index_type: index::IndexType,
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
            let record_mate = RecordMeta {
                gen: storage.gen,
                offset,
            };

            match record.record_type {
                RecordType::UnexpectCommand => break,
                RecordType::Set => index.set(key, record_mate),
                RecordType::Remove => index.remove(key.as_slice()),
            };
            offset += record_size as u64;
        }
        // 设置数据偏移
        storage.set_offset(offset);
    }

    Ok(Box::new(index))
}
