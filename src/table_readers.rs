use std::io;
use std::io::{BufWriter};
use std::fs;
use std::fs::{File, OpenOptions};

use log::*;
use memmap::{Mmap};

use byteorder::*;

use crate::serialization::*;
use crate::savepoints::*;

use crate::table_writers::*;

/** An indexer of append only table's contents. 
 
    The indexer facilitates the random access to the stored contents by keeping for each id the last offset at which the id's value is set. Indexed readers only work up to a given savepoint limit. 

    Note that to be usable, the indexer assumes continous ids to be present in the underlying table, otherwise there will be large holes in the index file making it very inefficient.

    The index file is simply an array of offsets, where offset at given position corresponds to the offset of the latest record for given id. 

    Extra metadata about the index file is kept in a separate file - `index.info`, which consists of the following:

    8     size of the indexer (equivalent to largest indexed id + 1)
    8     number of unique ids in the file
    8     savepoint limit used for the index
    X     serialized string that is the filename of the table from which the index was created
 */
struct IndexedReader<RECORD : TableRecord> {
    f_index : File,
    index : Mmap,
    f_table : File,
    table : Mmap,
    capacity : usize, 
    valid_entries : usize, 
    savepoint_limit : u64,
    why_oh_why : std::marker::PhantomData<RECORD>
}

pub(crate) fn record_table_index_path<RECORD: TableRecord>(root : & str) -> String { format!("{}/{}.index", root, RECORD::TABLE_NAME) }

pub(crate) fn record_table_index_info_path<RECORD: TableRecord>(root : & str) -> String { format!("{}/{}.index.info", root, RECORD::TABLE_NAME) }


impl<RECORD : TableRecord> IndexedReader<RECORD> {

    /** Creates an indexed reader for given table and savepoint. 
     
        If the index file already exists, makes sure that it corresponds to provided savepoint and if so uses it. If no index is found, new index file is generated first. If an index file exists, but does not conform to the specified source table or savepoint size, an error is generated. 
     */
    pub fn get_or_create(index_root : & str, table_root : & str, savepoint : & Savepoint) -> io::Result<IndexedReader<RECORD>> {
        let table_filename = record_table_path::<RECORD>(table_root);
        let index_filename = record_table_index_path::<RECORD>(index_root);
        if ! fs::metadata(& index_filename).map(|x| x.is_file()).unwrap_or(false) {
            info!("Generating index for table {} (table root: {}, index root: {})", RECORD::TABLE_NAME, table_root, index_root);
            let valid_values = Self::create(table_root, index_root, savepoint)?;
            info!("Index generated, valid values: {}", valid_values);
        }
        let f_table = OpenOptions::new().
            read(true).
            open(& table_filename)?;
        let f_index = OpenOptions::new().
            read(true).
            open(& index_filename)?;
        let table = unsafe { Mmap::map(& f_table)? }; // oh boy:(
        let index = unsafe { Mmap::map(& f_index)? };
        // create the indexer, then initialize & verify its contents
        let mut result = IndexedReader{
            f_index, 
            index, 
            f_table, 
            table,
            capacity : 0,
            valid_entries : 0,
            savepoint_limit : 0,
            why_oh_why : std::marker::PhantomData{},
        };
        let savepoint_limit = savepoint.get_size_for::<RECORD>();
        result.initialize_and_verify(index_root, & table_filename, savepoint_limit)?;
        info!("Index table for {} loaded, capacity {}, valid entries {}, savepoint limit {}", RECORD::TABLE_NAME, result.capacity, result.valid_entries, savepoint_limit);
        return Ok(result);
    }

    /** Returns the capacity of the indexer, that is the actual size of the index vector including holes. 
     
        In other words this is the highest stored id - 1.   
     */
    pub fn capacity(& self) -> usize { self.capacity }

    /** Returns the number of valid entries in the indexer. 
     
        This is the number of ids for which a valid offset is defined (and therefore a value). 
     */
    pub fn valid_entries(& self) -> usize { self.valid_entries }

    /** Returns true if the table has a value for given id. 
     */
    pub fn has(& mut self, id : & RECORD::Id) -> bool {
        if self.get_offset_for(id) == u64::MAX { false } else { true }
    }

    /** Returns value for given id. 
     
        If the id is outside of the specified range, or contains an empty offset, returns None. 
     */
    pub fn get(& mut self, id : & RECORD::Id) -> Option<RECORD::Value> {
        let offset = self.get_offset_for(id);
        if offset != Self::EMPTY {
            return Some(self.read_table(offset as usize));
        } else {
            return None;
        }
    }

    /** Maximum possible index is internally used to denote empty value, i.e. the id does not have valid entry in the table.  
     */
    const EMPTY : u64 = u64::MAX;

    /** Initializes the capacity and valid entries, which we cache in the indexed reader and verifies that the index file is what we expect it to be. 
     */
    fn initialize_and_verify(& mut self, index_root : & str, expected_table_filename : & str, expected_savepoint_limit : u64) -> io::Result<()> {
        let info_filename = record_table_index_info_path::<RECORD>(index_root);
        let mut f_info = OpenOptions::new().
            read(true).
            open(& info_filename)?;
        let capacity = u64::just_read_from(& mut f_info)? as usize;
        let valid_entries = u64::just_read_from(& mut f_info)? as usize;
        let savepoint_limit = u64::just_read_from(& mut f_info)?;
        let table_filename = String::just_read_from(& mut f_info)?;

        if table_filename != expected_table_filename {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Index table origin differs: expected {}, found {}", expected_table_filename, table_filename)));            
        }
        if savepoint_limit != expected_savepoint_limit {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Savepoint limit differs: expected {}, found {}", expected_savepoint_limit, savepoint_limit)));            
        }
        self.capacity = capacity;
        self.valid_entries = valid_entries;
        self.savepoint_limit = savepoint_limit;
        return Ok(());
    }

    fn savepoint_limit(& self) -> u64 { self.savepoint_limit }

    fn get_offset_for(& self, id : & RECORD::Id) -> u64 {
        if id.to_number() as usize >= self.capacity {
            return Self::EMPTY;
        } else {
            let offset = (id.to_number() as usize) * 8;
            return self.index.get(offset..).unwrap().read_u64::<LittleEndian>().unwrap();
        }
    }

    /** Reads a value from the table at given offset. 
     
        It is the responsibility of the indexer to make sure the offset is valid. 
     */
    fn read_table(& self, offset : usize) -> RECORD::Value {
        let mut buf = self.table.get(offset..).unwrap();
        return RECORD::Value::just_read_from(& mut buf).unwrap();
    }

    /** Builds the index file for the table.
     
        Calculates the indices and stores them in the index file together with the extra metdata (see indexer struct info for more details).
     */
    fn create(table_root : & str, index_root : & str, savepoint : & Savepoint) -> io::Result<usize> {
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
        // save the info file first to silence borrow checker
        {
            let info_filename = record_table_index_info_path::<RECORD>(index_root);
            let mut f = BufWriter::new(OpenOptions::new().
                        write(true).
                        create(true).
                        open(& info_filename)?);
            f.write_u64::<LittleEndian>(index.len() as u64)?;
            f.write_u64::<LittleEndian>(unique_ids as u64)?;
            f.write_u64::<LittleEndian>(iter.savepoint_limit())?;
            String::just_write_to(& mut f, & record_table_path::<RECORD>(table_root))?;
        }
        // we have the index built, time to save it
        {
            let index_filename = record_table_index_path::<RECORD>(index_root);
            let mut f = BufWriter::new(OpenOptions::new().
                        write(true).
                        create(true).
                        open(& index_filename)?);
            for offset in index {
                f.write_u64::<LittleEndian>(offset)?;
            }
        }
        return Ok(unique_ids);
    }

}

/** Linked reader provides indexed access to a table with links preserved between records for the same id. 
 
    This will be likely done via double index - first index is id to offset to second index, second index is just offsets to values in some order. 

 */
struct LinkedReader<RECORD : TableRecord> {
    why_oh_why : std::marker::PhantomData<RECORD>,
}
