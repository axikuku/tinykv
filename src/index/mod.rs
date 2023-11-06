use crate::data::record::RecordMeta;

mod btree;

pub(crate) trait Index: Sync + Send {
    fn set(&self, key: Vec<u8>, value: RecordMeta);

    fn get(&self, key: &[u8]) -> Option<RecordMeta>;

    fn remove(&self, key: &[u8]);
}

#[derive(Clone, Copy)]
pub enum IndexType {
    Btree,
}

pub(crate) fn new_index(index_type: IndexType) -> impl Index {
    match index_type {
        IndexType::Btree => btree::BTree::new(),
    }
}
