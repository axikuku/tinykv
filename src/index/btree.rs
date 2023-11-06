use super::Index;
use crate::data::record::RecordMeta;
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct BTree {
    map: Arc<RwLock<BTreeMap<Vec<u8>, RecordMeta>>>,
}

impl BTree {
    pub(crate) fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Index for BTree {
    fn set(&self, key: Vec<u8>, value: RecordMeta) {
        let mut guard = self.map.write();
        guard.insert(key, value);
    }

    fn get(&self, key: &[u8]) -> Option<RecordMeta> {
        let guard = self.map.read();
        guard.get(key).copied()
    }

    fn remove(&self, key: &[u8]) {
        let mut guard = self.map.write();
        guard.remove(key);
    }
}
