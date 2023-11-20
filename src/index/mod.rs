mod btree;

use crate::{config::IteratorConfig, data::record::RecordPos, iterator::IndexIterator};

pub(crate) trait Index: Sync + Send {
    fn put(&self, key: Vec<u8>, value: RecordPos);

    fn get(&self, key: &[u8]) -> Option<RecordPos>;

    fn delete(&self, key: &[u8]);

    fn iterator(&self, config: IteratorConfig) -> Box<dyn IndexIterator>;
}

#[derive(Clone, Copy)]
pub enum IndexType {
    BTree,
}

pub(crate) fn new_index(index_type: IndexType) -> impl Index {
    match index_type {
        IndexType::BTree => btree::BTree::new(),
    }
}
