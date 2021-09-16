use std::io;
use std::io::{BufWriter};
use std::fs;
use std::fs::{File, OpenOptions};

use log::*;
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
    f_index : File,
    index : Mmap,
    f_table : File,
    table : Mmap,
    why_oh_why : std::marker::PhantomData<RECORD>
}

pub(crate) fn record_table_index_path<RECORD: TableRecord>(root : & str) -> String { format!("{}/{}.index", root, RECORD::TABLE_NAME) }


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
            capacity : 0, 
            valid_entries : 0,
            f_index, 
            index, 
            f_table, 
            table,
            why_oh_why : std::marker::PhantomData{},
        };
        let savepoint_limit = savepoint.get_size_for::<RECORD>();
        result.initialize_and_verify(& table_filename, 0)?;
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
    fn initialize_and_verify(& mut self, expected_table_filename : & str, expected_savepoint_limit : u64) -> io::Result<()> {
        if self.table_filename() != expected_table_filename {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Index table origin differs: expected {}, found {}", expected_table_filename, self.table_filename())));            
        }
        if self.savepoint_limit() != expected_savepoint_limit {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Savepoint limit differs: expected {}, found {}", expected_savepoint_limit, self.savepoint_limit())));            
        }
        self.capacity = self.read_index(0) as usize;
        self.valid_entries = self.read_index(1 + self.capacity) as usize;
        return Ok(());
    }

    fn savepoint_limit(& self) -> u64 {
        return self.read_index(self.capacity + 2);
    }

    fn table_filename(& self) -> String {
        let mut buf = self.index.get((self.capacity + 3)..).unwrap();
        return String::just_read_from(& mut buf).unwrap();
    }

    fn get_offset_for(& self, id : & RECORD::Id) -> u64 {
        if id.to_number() as usize >= self.capacity {
            return Self::EMPTY;
        } else {
            return self.read_index((id.to_number() + 1) as usize);
        }
    }

    /** Reads the index-th index stored in the indexer. 
     
        Note that first index is effectively the capacity of the indexer (so for actual IDs we need +1) and the actual index file is followed by two more u64s, the number of valid entries in the index file and the savepoint limit. All these values are readable using this method.
     */
    fn read_index(& self, mut offset : usize) -> u64 {
        offset *= 8; // we store u64s
        return self.index.get(offset..).unwrap().read_u64::<LittleEndian>().unwrap();
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
