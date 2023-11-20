use std::{collections::BTreeMap, sync::Arc};

use parking_lot::RwLock;

use crate::{config::IteratorConfig, data::record::RecordPos, iterator::IndexIterator};

use super::Index;

pub(crate) struct BTree {
    map: Arc<RwLock<BTreeMap<Vec<u8>, RecordPos>>>,
}

impl BTree {
    pub(crate) fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Index for BTree {
    fn put(&self, key: Vec<u8>, value: RecordPos) {
        let mut guard = self.map.write();
        guard.insert(key, value);
    }

    fn get(&self, key: &[u8]) -> Option<RecordPos> {
        let guard = self.map.read();
        guard.get(key).copied()
    }

    fn delete(&self, key: &[u8]) {
        let mut guard = self.map.write();
        guard.remove(key);
    }

    fn iterator(&self, config: IteratorConfig) -> Box<dyn IndexIterator> {
        let read_guard = self.map.read();
        let mut items = read_guard
            .iter()
            .map(|map| (map.0.clone(), *map.1))
            .collect::<Vec<_>>();

        if config.reverse {
            items.reverse()
        }
        Box::new(BTreeIterator {
            items,
            current_index: 0,
            config,
        })
    }
}

pub struct BTreeIterator {
    items: Vec<(Vec<u8>, RecordPos)>,
    current_index: usize,
    config: IteratorConfig,
}

impl IndexIterator for BTreeIterator {
    fn rewind(&mut self) {
        self.current_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.current_index = match self.items.binary_search_by(|(res, _)| {
            if self.config.reverse {
                res.cmp(&key).reverse()
            } else {
                res.cmp(&key)
            }
        }) {
            Ok(value) => value,
            Err(index) => index,
        };
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &RecordPos)> {
        if self.current_index >= self.items.len() {
            return None;
        }

        while let Some(item) = self.items.get(self.current_index) {
            self.current_index += 1;
            let prefix = &self.config.prefix;
            if prefix.is_empty() || item.0.starts_with(prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
    }
}
