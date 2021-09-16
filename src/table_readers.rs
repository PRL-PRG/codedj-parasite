use std::io;
use std::io::{Seek, SeekFrom, Read, Write, BufWriter, BufReader};
use std::fs;
use std::fs::{File, OpenOptions};

use memmap::{Mmap};

use byteorder::*;

use crate::serialization::*;
use crate::savepoints::*;

use crate::table_writer::*;

/** An indexer of append only table's contents. 
 
    The indexer facilitates the random access to the stored contents by keeping for each id the last offset at which the id's value is set. Indexed readers only work up to a given savepoint limit. 

    Note that to be usable, the indexer assumes continous ids to be present in the underlying table, otherwise there will be large holes in the index file making it very inefficient.

    The index file contains some extra bookkeeping, so overall has the following structure:

    8     size of the indexer (equivalent to largest indexed id + 1)
    8 * N the indexer itself 
    8     number of unique ids in the file
    8     savepoint limit used for the index
    X     serialized string that is the filename of the table from which the index was created
 */
struct IndexedReader<RECORD : TableRecord> {
    capacity : usize, 
    valid_entries : usize, 
    index : Mmap,
    table : Mmap,
    why_oh_why : std::marker::PhantomData<RECORD>
}

pub(crate) fn record_table_index_path<RECORD: TableRecord>(root : & str) -> String { format!("{}/{}.index", root, RECORD::TABLE_NAME) }


impl<RECORD : TableRecord> IndexedReader<RECORD> {

    /** Creates an indexed reader for given table and savepoint. 
     
        If the index file already exists, makes sure that it corresponds to provided savepoint and if so uses it. If no index is found, new index file is generated first. If an index file exists, but does not conform to the specified source table or savepoint size, an error is generated. 
     */
    //pub fn get_or_create(index_root : & str, table_root : & str, savepoint : & Savepoint) -> io::Result<IndexedReader> {
    //    unimplemented!();
    //}

    /** Returns the capacity of the indexer, that is the number of  */
    pub fn capacity(& self) -> usize { self.capacity }

    pub fn valid_entries(& self) -> usize { self.valid_entries }

    pub fn has(& mut self, id : & RECORD::Id) -> bool {
        if self.get_offset_for(id) == u64::MAX { false } else { true }
    }

    pub fn get(& mut self, id : & RECORD::Id) -> Option<RECORD::Value> {
        let offset = self.get_offset_for(id);
        if offset != Self::EMPTY {
            return Some(self.read_table(offset as usize));
        } else {
            return None;
        }
    }

    const EMPTY : u64 = u64::MAX;

    fn savepoint_limit(& self) -> u64 {
        return self.read_index(self.capacity * 8 + 16);
    }

    fn table_filename(& self) -> String {
        let mut buf = self.index.get(8 * (self.capacity + 3)..).unwrap();
        return String::just_read_from(& mut buf).unwrap();
    }

    fn get_offset_for(& self, id : & RECORD::Id) -> u64 {
        if id.to_number() as usize >= self.capacity {
            return Self::EMPTY;
        } else {
            return self.read_index((id.to_number() * 8 + 8) as usize);
        }
    }

    fn read_index(& self, offset : usize) -> u64 {
        return self.index.get(offset..(offset + 8)).unwrap().read_u64::<LittleEndian>().unwrap();
    }

    fn read_table(& self, offset : usize) -> RECORD::Value {
        unimplemented!();
    }

    /** Builds the index file for the table.
     */
    fn create(table_root : & str, index_root : & str, savepoint : & Savepoint) -> io::Result<usize> {
        let index_filename = record_table_index_path::<RECORD>(index_root);
        let mut index = Vec::<u64>::new();
        let mut iter = TableIterator::<RECORD>::for_savepoint(table_root, savepoint);
        let mut unique_ids = 0;
        loop {
            let offset = iter.offset();
            if let Some((id, _)) = iter.next() {
                let idx = id.to_number() as usize;
                while index.len() <= idx {
                    index.push(Self::EMPTY);
                }
                if index[idx] == u64::MAX {
                    unique_ids += 1;
                }
                index[idx] = offset;
            } else {
                break;
            }
        }
        // we have the index built, time to save it
        let mut f = BufWriter::new(OpenOptions::new().
                    write(true).
                    create(true).
                    open(& index_filename)?);
        f.write_u64::<LittleEndian>(index.len() as u64)?;
        for offset in index {
            f.write_u64::<LittleEndian>(offset)?;
        }
        f.write_u64::<LittleEndian>(unique_ids as u64)?;
        f.write_u64::<LittleEndian>(iter.savepoint_limit())?;
        String::just_write_to(& mut f, & record_table_path::<RECORD>(table_root))?;
        return Ok(unique_ids);
    }

}
