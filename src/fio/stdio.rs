use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::Arc,
};

use parking_lot::RwLock;

use crate::error::{KvError, Result};

use super::FileIO;

pub(crate) struct StdIO {
    fd: Arc<RwLock<File>>,
}

impl StdIO {
    pub(crate) fn new(file_path: &Path) -> Result<Self> {
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_path)?;
        Ok(Self {
            fd: Arc::new(RwLock::new(fd)),
        })
    }
}

impl FileIO for StdIO {
    #[cfg(target_os = "windows")]
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::os::windows::prelude::FileExt;
        self.fd.read().seek_read(buf, offset).map_err(KvError::Io)
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        self.fd.write().write(buf).map_err(KvError::Io)
    }

    fn sync(&self) -> Result<()> {
        self.fd.write().sync_all().map_err(KvError::Io)
    }
}
