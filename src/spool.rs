use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"LURKMR01";

pub fn encode<'a>(batches: impl IntoIterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    for batch in batches {
        let Ok(len) = u32::try_from(batch.len()) else {
            continue;
        };
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(batch);
    }
    out
}

pub fn decode(contents: &[u8]) -> io::Result<Vec<&[u8]>> {
    if !contents.starts_with(MAGIC) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad spool magic",
        ));
    }
    let mut offset = MAGIC.len();
    let mut batches = Vec::new();
    while offset < contents.len() {
        if contents.len() - offset < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated frame length",
            ));
        }
        let len_bytes: [u8; 4] = contents[offset..offset + 4]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad frame length"))?;
        offset += 4;
        let len = usize::try_from(u32::from_le_bytes(len_bytes))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame too large"))?;
        if contents.len() - offset < len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated frame",
            ));
        }
        batches.push(&contents[offset..offset + len]);
        offset += len;
    }
    Ok(batches)
}

pub fn files(directory: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "spool")
        })
        .collect::<Vec<_>>();
    files.sort();
    Ok(files)
}

pub fn size(directory: &Path) -> io::Result<u64> {
    files(directory)?
        .into_iter()
        .try_fold(0_u64, |total, path| {
            Ok(total.saturating_add(fs::metadata(path)?.len()))
        })
}

pub fn write(directory: &Path, timestamp_ms: u64, contents: &[u8]) -> io::Result<PathBuf> {
    let mut suffix = 0_u32;
    loop {
        let filename = if suffix == 0 {
            format!("{timestamp_ms:020}.spool")
        } else {
            format!("{timestamp_ms:020}-{suffix}.spool")
        };
        let path = directory.join(filename);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                file.write_all(contents)?;
                return Ok(path);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                suffix = suffix.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{decode, encode};

    #[test]
    fn round_trip_preserves_frames() -> Result<(), Box<dyn std::error::Error>> {
        let first = b"one".as_slice();
        let second = b"two-two".as_slice();
        let encoded = encode([first, second]);
        assert_eq!(decode(&encoded)?, vec![first, second]);
        Ok(())
    }

    #[test]
    fn truncated_frame_is_rejected() {
        let mut encoded = encode([b"payload".as_slice()]);
        let _ = encoded.pop();
        assert!(decode(&encoded).is_err());
    }
}
