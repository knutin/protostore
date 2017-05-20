use std::io;
use std::path::{Path, PathBuf};

use std::slice::from_raw_parts;
use std::mem::transmute;
use memmap;


pub struct TableOfContents {
    uuids: Vec<[u8; 16]>,
    offsets: Vec<u64>,
    lens: Vec<u16>,
    _files: Vec<memmap::Mmap>,
}


impl TableOfContents {
    pub fn from_path(path: &Path) -> Result<TableOfContents, io::Error> {

        let mut uuids_path = PathBuf::from(path);
        let mut offsets_path = PathBuf::from(path);
        let mut lens_path = PathBuf::from(path);

        uuids_path.push("protostore.toc.uuids");
        offsets_path.push("protostore.toc.offsets");
        lens_path.push("protostore.toc.lengths");


        let uuids_meta = uuids_path.metadata()?;
        let num_entries = uuids_meta.len() / 16;


        let uuid_mmap = memmap::Mmap::open_path(uuids_path, memmap::Protection::Read)?;
        let offsets_mmap = memmap::Mmap::open_path(offsets_path, memmap::Protection::Read)?;
        let lens_mmap = memmap::Mmap::open_path(lens_path, memmap::Protection::Read)?;


        let uuids = unsafe {
            let slice = uuid_mmap.as_slice();
            let uuid: &[[u8; 16]] = transmute(slice);
            from_raw_parts(uuid.as_ptr(), num_entries as usize).to_vec()
        };

        let offsets = unsafe {
            let slice = offsets_mmap.as_slice();
            let offsets: &[u64] = transmute(slice);
            from_raw_parts(offsets.as_ptr(), num_entries as usize).to_vec()
        };

        let lens = unsafe {
            let slice = lens_mmap.as_slice();
            let lens: &[u16] = transmute(slice);
            from_raw_parts(lens.as_ptr(), num_entries as usize).to_vec()
        };

        let _files = vec![uuid_mmap, offsets_mmap, lens_mmap];

        Ok(TableOfContents {
               uuids,
               offsets,
               lens,
               _files,
           })
    }


    pub fn offset_and_len(&self, uuid: &[u8; 16]) -> Option<(u64, u16)> {
        match self.uuids.binary_search(uuid) {
            Ok(index) => Some((self.offsets[index], self.lens[index])),
            Err(_) => None,
        }
    }

    pub fn max_len(&self) -> usize {
        *self.lens.iter().max().unwrap() as usize
    }
}


#[cfg(test)]
mod tests {
    extern crate tempdir;

    use self::tempdir::TempDir;
    use std::path::PathBuf;
    use std::fs::OpenOptions;
    use std::io::Write;

    use byteorder::{ByteOrder, LittleEndian};

    use toc::TableOfContents;

    #[test]
    fn open() {
        let tmp = TempDir::new("toc").unwrap();
        let path = tmp.into_path();

        let mut uuids_path = PathBuf::from(path.clone());
        let mut offsets_path = PathBuf::from(path.clone());
        let mut lens_path = PathBuf::from(path.clone());
        uuids_path.push("protostore.toc.uuids");
        offsets_path.push("protostore.toc.offsets");
        lens_path.push("protostore.toc.lengths");

        let uuids: Vec<[u8; 16]> = vec![[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                                        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
                                        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5]];

        let offsets: Vec<u64> = vec![0, 4, 8];
        let lens: Vec<u16> = vec![4, 4, 4];


        let mut opts = OpenOptions::new();
        opts.write(true).create(true).truncate(true);

        {
            let mut uuids_file = opts.open(uuids_path).unwrap();
            let mut offsets_file = opts.open(offsets_path).unwrap();
            let mut lens_file = opts.open(lens_path).unwrap();

            for uuid in &uuids {
                uuids_file.write_all(uuid).unwrap();
            }

            for offset in &offsets {
                let mut encoded_offset = [0; 8];
                LittleEndian::write_u64(&mut encoded_offset, *offset);
                offsets_file.write_all(&encoded_offset).unwrap();
            }

            for len in &lens {
                let mut encoded_len = [0; 2];
                LittleEndian::write_u16(&mut encoded_len, *len);
                lens_file.write_all(&encoded_len).unwrap();
            }
        }


        let toc = TableOfContents::from_path(path.as_path());
        assert!(toc.is_ok());
        let toc = toc.unwrap();

        assert!(toc.has_uuid(&uuids[0]));
        assert!(!toc.has_uuid(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4]));

        assert_eq!(Some((4, 4)), toc.offset_and_len(&uuids[1]));
        assert_eq!(None,
                   toc.offset_and_len(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4]));
    }
}
