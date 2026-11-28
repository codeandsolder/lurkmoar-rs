use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"LURKMR01";
pub const HEADER_BYTES: u64 = 8;
pub const FRAME_HEADER_BYTES: u64 = 4;
const LOCK_NAME: &str = ".lurkmoar.lock";
const STAGING_NAME: &str = ".lurkmoar.tmp";

#[derive(Debug)]
pub struct Store {
    directory: PathBuf,
    // Advisory OS lock: released even after a process crash. Never unlink the
    // lock file, because another process could otherwise lock a different inode.
    _lock: File,
}

#[derive(Debug)]
pub struct Entry {
    pub path: PathBuf,
    pub timestamp_ms: u64,
    sequence: u64,
    pub bytes: u64,
}

impl Store {
    pub fn open(directory: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&directory)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join(LOCK_NAME))?;
        lock.try_lock().map_err(io::Error::other)?;
        // A crash before atomic publication leaves only the staging file.
        match fs::remove_file(directory.join(STAGING_NAME)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Ok(Self {
            directory,
            _lock: lock,
        })
    }

    pub fn files(&self) -> io::Result<Vec<Entry>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let path = entry.path();
            if path
                .extension()
                .is_none_or(|extension| extension != "spool")
            {
                continue;
            }
            if !entry.file_type()?.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "spool entry is not a regular file",
                ));
            }
            let (timestamp_ms, sequence) = file_key(&path)?;
            files.push(Entry {
                path,
                timestamp_ms,
                sequence,
                bytes: entry.metadata()?.len(),
            });
        }
        // Compare numeric suffixes, not their lexical ordering (2 before 10).
        files.sort_unstable_by_key(|entry| (entry.timestamp_ms, entry.sequence));
        Ok(files)
    }

    pub fn write(&self, timestamp_ms: u64, contents: &[u8]) -> io::Result<()> {
        let files = self.files()?;
        let (timestamp_ms, sequence) = match files.last() {
            Some(last) if last.timestamp_ms >= timestamp_ms => (
                last.timestamp_ms,
                last.sequence
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("spool sequence exhausted"))?,
            ),
            _ => (timestamp_ms, 0),
        };
        let path = self
            .directory
            .join(format!("{timestamp_ms:020}-{sequence:020}.spool"));
        let staging = self.directory.join(STAGING_NAME);
        let result = (|| {
            let mut file = File::create(&staging)?;
            file.write_all(contents)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&staging, &path)?;
            #[cfg(unix)]
            File::open(&self.directory)?.sync_all()?;
            Ok(())
        })();
        if result.is_err()
            && let Err(error) = fs::remove_file(&staging)
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(%error, "lurkmoar: failed to remove spool staging file");
        }
        result
    }
}

fn file_key(path: &Path) -> io::Result<(u64, u64)> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid spool filename: {}", path.display()),
        )
    };
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(invalid)?;
    let (timestamp, sequence) = stem.split_once('-').unwrap_or((stem, "0"));
    Ok((
        timestamp.parse().map_err(|_| invalid())?,
        sequence.parse().map_err(|_| invalid())?,
    ))
}

pub fn encode<'a>(batches: impl IntoIterator<Item = &'a [u8]>) -> io::Result<Vec<u8>> {
    let mut out = Vec::from(MAGIC.as_slice());
    for batch in batches {
        let len = u32::try_from(batch.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "spool frame exceeds u32 length",
            )
        })?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(batch);
    }
    Ok(out)
}

/// A malformed trailing frame does not erase the valid prefix of a file.
pub struct Frames<'a> {
    remaining: &'a [u8],
}

pub fn decode(contents: &[u8]) -> io::Result<Frames<'_>> {
    let remaining = contents
        .strip_prefix(MAGIC)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad spool magic"))?;
    Ok(Frames { remaining })
}

impl<'a> Iterator for Frames<'a> {
    type Item = io::Result<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }
        // Clear first so a malformed frame produces exactly one error.
        let remaining = std::mem::take(&mut self.remaining);
        Some((|| {
            let (len, rest) = remaining.split_first_chunk::<4>().ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "truncated frame length")
            })?;
            let len = usize::try_from(u32::from_le_bytes(*len))
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame too large"))?;
            let (batch, rest) = rest
                .split_at_checked(len)
                .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated frame"))?;
            self.remaining = rest;
            Ok(batch)
        })())
    }
}

#[cfg(test)]
mod tests {
    use super::{decode, encode, file_key};
    use std::path::Path;

    #[test]
    fn round_trip_preserves_frames() -> Result<(), Box<dyn std::error::Error>> {
        let first = b"one".as_slice();
        let second = b"two-two".as_slice();
        let encoded = encode([first, second])?;
        assert_eq!(
            decode(&encoded)?.collect::<Result<Vec<_>, _>>()?,
            vec![first, second]
        );
        Ok(())
    }

    #[test]
    fn truncated_frame_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut encoded = encode([b"payload".as_slice()])?;
        let _ = encoded.pop();
        assert!(decode(&encoded)?.collect::<Result<Vec<_>, _>>().is_err());
        Ok(())
    }

    #[test]
    fn filename_order_uses_numeric_suffixes() -> Result<(), Box<dyn std::error::Error>> {
        assert!(
            file_key(Path::new("00000000000000000001-2.spool"))?
                < file_key(Path::new("00000000000000000001-10.spool"))?
        );
        assert_eq!(file_key(Path::new("00000000000000000001.spool"))?, (1, 0));
        assert!(file_key(Path::new("garbage.spool")).is_err());
        Ok(())
    }
}
