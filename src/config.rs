use std::{env::temp_dir, path::PathBuf};

use crate::{engine::Engine, index::IndexType, Result};

pub struct Config {
    pub(crate) path: PathBuf,
    pub(crate) storage_size: u64,
    pub(crate) index_type: IndexType,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: temp_dir(),
            storage_size: 1024 * 1024 * 64, // 64MB
            index_type: IndexType::Btree,
        }
    }
}

impl Config {
    pub fn set_path(mut self, path: PathBuf) -> Self {
        self.path = path;
        self
    }

    pub fn set_storage_size(mut self, size: u64) -> Self {
        self.storage_size = size;
        self
    }

    pub fn set_index_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    pub fn build(self) -> Result<Engine> {
        Engine::open(self)
    }
}
