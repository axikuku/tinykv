mod stdio;

use std::path::Path;

use crate::error::Result;

pub(crate) trait FileIO: Sync + Send {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    fn write(&self, buf: &[u8]) -> Result<usize>;

    fn sync(&self) -> Result<()>;
}

pub(crate) fn new_file_io(file_path: &Path) -> Result<impl FileIO> {
    stdio::StdIO::new(file_path)
}
