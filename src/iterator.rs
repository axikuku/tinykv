use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{config::IteratorConfig, data::record::RecordPos, error::Result, Engine};

pub(crate) trait IndexIterator: Sync + Send {
    fn rewind(&mut self);

    fn seek(&mut self, key: Vec<u8>);

    fn next(&mut self) -> Option<(&Vec<u8>, &RecordPos)>;
}

pub struct Iterator<'a> {
    engine: &'a Engine,
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>,
}

impl Iterator<'_> {
    pub fn rewind(&self) {
        self.index_iter.write().rewind();
    }

    pub fn seek(&self, key: Vec<u8>) {
        self.index_iter.write().seek(key)
    }

    pub fn next(&self) -> Option<(Vec<u8>, Bytes)> {
        if let Some((key, pos)) = self.index_iter.write().next() {
            return self
                .engine
                .read_value_from_pos(pos)
                .map(|value| (key.clone(), value))
                .ok();
        }
        None
    }
}

impl Engine {
    /// 获取迭代器
    pub fn iter(&self, config: IteratorConfig) -> Iterator {
        Iterator {
            engine: self,
            index_iter: Arc::new(RwLock::new(self.index.iterator(config))),
        }
    }

    /// 对数据库中当中的所有数据执行函数操作，函数返回 false 时终止
    pub fn fold<F>(&self, f: F) -> Result<()>
    where
        Self: Sized,
        F: Fn(Vec<u8>, Bytes) -> bool,
    {
        let iter = self.iter(IteratorConfig::default());
        while let Some((key, value)) = iter.next() {
            if !f(key, value) {
                break;
            }
        }
        Ok(())
    }
}
