//! Fixed-size chunking and reassembly.
//!
//! Per design §4, files are split into `chunk_size` pieces; even a file
//! smaller than `chunk_size` produces a one-chunk manifest so the model is
//! uniform. Empty files produce zero chunks (the only case that breaks the
//! "always at least one chunk" rule — the manifest's `chunks` is empty).
//!
//! Reading is buffered to avoid one allocation per byte, but each chunk is
//! materialized in memory before it's hashed/written. That's fine for v1
//! (default chunk size = 8 MB); switching to streaming hashing is future work
//! if memory pressure becomes an issue on tiny hosts.

use std::io::{Read, Write};

use crate::Result;

/// Iterator-style chunker. Reads from an `R` and yields exactly
/// `chunk_size`-byte chunks until the input is exhausted; the final chunk may
/// be shorter (down to one byte) but never empty unless the input itself is
/// empty.
pub struct Chunker<R> {
    reader: R,
    chunk_size: usize,
    done: bool,
}

impl<R: Read> Chunker<R> {
    pub fn new(reader: R, chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "chunk_size must be > 0");
        Self {
            reader,
            chunk_size,
            done: false,
        }
    }

    /// Pull the next chunk from the input. Returns `Ok(None)` when the reader
    /// is fully consumed.
    pub fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        if self.done {
            return Ok(None);
        }
        let mut buf = vec![0u8; self.chunk_size];
        let mut filled = 0;
        while filled < self.chunk_size {
            let n = self.reader.read(&mut buf[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        if filled == 0 {
            self.done = true;
            return Ok(None);
        }
        if filled < self.chunk_size {
            self.done = true;
            buf.truncate(filled);
        }
        Ok(Some(buf))
    }
}

/// Reassemble a stream of chunk bodies into the original file. Caller is
/// responsible for ordering chunks correctly (i.e. matching the manifest's
/// `chunks` order).
pub fn reassemble<W: Write>(
    mut writer: W,
    chunks: impl IntoIterator<Item = Vec<u8>>,
) -> Result<()> {
    for chunk in chunks {
        writer.write_all(&chunk)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn collect_all(input: &[u8], chunk_size: usize) -> Vec<Vec<u8>> {
        let mut c = Chunker::new(Cursor::new(input), chunk_size);
        let mut out = Vec::new();
        while let Some(chunk) = c.next_chunk().unwrap() {
            out.push(chunk);
        }
        out
    }

    #[test]
    fn empty_file_yields_no_chunks() {
        let chunks = collect_all(&[], 8);
        assert!(chunks.is_empty());
    }

    #[test]
    fn exact_multiple_yields_full_chunks() {
        // 3 chunks of exactly 4 bytes each.
        let chunks = collect_all(b"AAAABBBBCCCC", 4);
        assert_eq!(
            chunks,
            vec![b"AAAA".to_vec(), b"BBBB".to_vec(), b"CCCC".to_vec()]
        );
    }

    #[test]
    fn one_byte_under_yields_two_chunks() {
        // 7 bytes, chunk_size 4 → [4, 3]
        let chunks = collect_all(b"AAAABBB", 4);
        assert_eq!(chunks, vec![b"AAAA".to_vec(), b"BBB".to_vec()]);
    }

    #[test]
    fn one_byte_over_yields_two_chunks() {
        // 5 bytes, chunk_size 4 → [4, 1]
        let chunks = collect_all(b"AAAAB", 4);
        assert_eq!(chunks, vec![b"AAAA".to_vec(), b"B".to_vec()]);
    }

    #[test]
    fn smaller_than_chunk_yields_one_chunk() {
        let chunks = collect_all(b"ab", 8);
        assert_eq!(chunks, vec![b"ab".to_vec()]);
    }

    #[test]
    fn reassemble_round_trips() {
        let input: Vec<u8> = (0..1000u32).flat_map(|i| (i as u8).to_le_bytes()).collect();
        let chunks = collect_all(&input, 17); // odd size to exercise tail
        let mut out = Vec::new();
        reassemble(&mut out, chunks).unwrap();
        assert_eq!(out, input);
    }

    #[test]
    fn reassemble_empty() {
        let mut out = Vec::new();
        reassemble(&mut out, Vec::<Vec<u8>>::new()).unwrap();
        assert!(out.is_empty());
    }
}
