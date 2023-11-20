use std::{env::temp_dir, path::PathBuf};

use crate::index::IndexType;

pub struct Config {
    pub dir_path: PathBuf,
    pub storage_size: u64,
    pub index_type: IndexType,
    pub sync_write: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            dir_path: temp_dir(),
            storage_size: 1024 * 1024 * 64, // 64MB
            index_type: IndexType::BTree,
            sync_write: false,
        }
    }
}

#[derive(Default)]
pub struct IteratorConfig {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

pub struct BatchConfig {
    pub max_batch_num: usize,
    pub sycn_write: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_num: 100,
            sycn_write: true,
        }
    }
}
