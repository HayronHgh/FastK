use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::mem::MaybeUninit;
use std::path::Path;
use std::sync::Arc;

/// A reader-local cursor over a shared synchronous file. Every read supplies an
/// explicit offset; no read relies on the shared OS file position.
#[derive(Clone)]
pub(crate) struct ChunkFile {
    file: Arc<File>,
    offset: u64,
}

#[derive(Clone)]
pub(crate) struct ReadSpan {
    pub file: ChunkFile,
    pub offset: u64,
    pub rows: usize,
}

impl ChunkFile {
    pub(super) fn new(file: Arc<File>) -> Self {
        Self { file, offset: 0 }
    }

    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self::new(Arc::new(File::open(path)?)))
    }

    /// Only successful completion makes the entire destination initialized.
    pub fn read_exact_uninit(&mut self, mut bytes: &mut [MaybeUninit<u8>]) -> io::Result<()> {
        while !bytes.is_empty() {
            match self.read_uninit(bytes) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(count) => bytes = &mut bytes[count..],
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn read_uninit(&mut self, bytes: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        #[cfg(unix)]
        let count = rustix::io::pread(&*self.file, bytes, self.offset)
            .map(|(initialized, _)| initialized.len())
            .map_err(io::Error::from)?;
        #[cfg(windows)]
        let count = {
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::{
                Foundation::ERROR_HANDLE_EOF,
                Storage::FileSystem::ReadFile,
                System::IO::{OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0},
            };
            let mut overlapped = OVERLAPPED {
                Anonymous: OVERLAPPED_0 {
                    Anonymous: OVERLAPPED_0_0 {
                        Offset: self.offset as u32,
                        OffsetHigh: (self.offset >> 32) as u32,
                    },
                },
                ..Default::default()
            };
            let mut count = 0;
            // Files are opened synchronously by this module/cache. ReadFile completes
            // before returning, uses the explicit offset, and initializes count bytes.
            let success = unsafe {
                ReadFile(
                    self.file.as_raw_handle(),
                    bytes.as_mut_ptr().cast(),
                    bytes.len().min(u32::MAX as usize) as u32,
                    &mut count,
                    &mut overlapped,
                )
            };
            if success == 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(ERROR_HANDLE_EOF as i32) {
                    return Err(error);
                }
            }
            count as usize
        };
        self.offset = self
            .offset
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "file offset overflow"))?;
        Ok(count)
    }
}

impl Read for ChunkFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        // Initialized u8 values are also valid MaybeUninit<u8>. Bytes stay
        // initialized on partial reads/errors; no uninitialized u8 reference is formed.
        let output =
            unsafe { std::slice::from_raw_parts_mut(bytes.as_mut_ptr().cast(), bytes.len()) };
        self.read_uninit(output)
    }
}

impl Seek for ChunkFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let offset = match position {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::Current(delta) => self.offset.checked_add_signed(delta),
            SeekFrom::End(delta) => self.file.metadata()?.len().checked_add_signed(delta),
        };
        self.offset = offset
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid file offset"))?;
        Ok(self.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_offsets_clone_eof_and_invalid_seek_are_independent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bytes");
        std::fs::write(&path, b"0123456789").unwrap();
        let mut first = ChunkFile::open(&path).unwrap();
        first.seek(SeekFrom::Start(2)).unwrap();
        let mut second = first.clone();
        second.seek(SeekFrom::End(-2)).unwrap();
        let mut bytes = [0; 2];
        first.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"23");
        second.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"89");
        assert_eq!(second.read(&mut bytes).unwrap(), 0);
        assert!(first.seek(SeekFrom::Current(-5)).is_err());
        first.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"45");
        let mut output = [MaybeUninit::uninit(); 3];
        first.read_exact_uninit(&mut output).unwrap();
        assert_eq!(output.map(|byte| unsafe { byte.assume_init() }), *b"678");
        assert_eq!(
            first.read_exact_uninit(&mut output).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
}
