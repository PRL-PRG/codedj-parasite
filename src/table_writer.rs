use std::io;
use std::io::{Seek, SeekFrom, Read, Write, BufWriter, BufReader};
use std::fs;
use std::fs::{File, OpenOptions};

use byteorder::*;

use crate::serialization::*;
use crate::savepoints::*;


/** A simple trait for IDs. 
 
    Bare minimum is required from an ID, i.e. they must be able to convert themselves to and from u64, which is how they are stored in the datastore. Since they can convert to u64s, any id is trivially serializable. 
 */
pub trait Id : Copy + Clone + Eq + PartialEq {
    fn to_number(& self) -> u64;
    fn from_number(id : u64) -> Self;
}

/** Serializable implementation for any ID. IDs trivially serialize as the u64 numbers they convert to/from.
 */
impl<T : Id> Serializable for T {
    type Item = T;
    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        *offset += 8;
        return Ok(T::from_number(f.read_u64::<LittleEndian>()?));
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        *offset += 8;
        return f.write_u64::<LittleEndian>(item.to_number());
    }
}

/** Since IDs serialize as u64s, they also have fixed size of 8.
 */
impl<T : Id> FixedSize for T {
    fn size_of() -> usize { 8 }
}

/** A record in a datastore. 
 
    A record specifies the type of its ID, the type of its value and importantly, the filename under which it is to be stored in the datastore. This architecture allows new tables to be added to the datastore easily with almost no overhead (hashmap lookup on the table name). Name clashes can also be checked at runtime. 
 */
pub trait TableRecord {
    type Id : Id;
    type Value : Serializable<Item = Self::Value>;
    const TABLE_NAME : &'static str;

}

/** Append only table.
 
    The table consists of a binary file containing the records appended one after another in the order the append() method was called. Each record consists of the id followed by the actual record information. The table *must* be flushed from time to time as the flush makes sure that the all pending writes in the backing buffer are propagated to the file and creates a corresponding checkpoint that verifies the integrity of the file up to the stored size. 

    NOTE that checkpoints provide guarantees of integrity for a single table at a time. If a table fails the checkpoint verification it can't revert back on its own as other tables that have passed the checkpoint verification might still point to the code from this table that would get reverted. In case of failed table verification, the entire datastore must be reverted to the latest savepoint. 
 */
pub struct TableWriter<RECORD : TableRecord> {
    filename : String,
    f : BufWriter<File>,
    offset : u64,
    why_oh_why : std::marker::PhantomData<RECORD>,
}

/** Given a record and a root folder returns the path of a file in which the table should live. 
 */
pub(crate) fn record_table_path<RECORD: TableRecord>(root : & str) -> String { format!("{}/{}", root, RECORD::TABLE_NAME) }

impl<RECORD : TableRecord> TableWriter<RECORD> {
    /** Opens existing table in given datastore root, or if it does not exist, creates new table.
     */
    pub fn open_or_create(root : & str) -> TableWriter<RECORD> {
        let filename = record_table_path::<RECORD>(root);
        let mut f = OpenOptions::new().
                    write(true).
                    create(true).
                    open(& filename).unwrap();
        // seek towards the end because (a) Rust won't do it for us and (b) determine the offset
        let offset = f.seek(SeekFrom::End(0)).unwrap();
        // create the append only table and return it
        return TableWriter{
            filename,
            f : BufWriter::new(f),
            offset, 
            why_oh_why : std::marker::PhantomData{}
        };
    }

    /** Appends the given key-value pair to the table and returns the offset at which it has been written. 
     */
    pub fn append(& mut self, id : RECORD::Id, value : & RECORD::Value) -> u64 {
        let result = self.offset;
        RECORD::Id::write_to(& mut self.f, & id, & mut self.offset).unwrap();
        RECORD::Value::write_to(& mut self.f, value, & mut self.offset).unwrap();
        return result;
    }

    /** Flushes the writer to disk. 
     
        The flush also serves as a barrier that verifies the expected and actual size of the table on disk and stores the number so that unexpected shutdowns can be handled more gracefully.  
     */
    pub fn flush(& mut self) -> Result<u64,std::io::Error> {
        self.f.flush().unwrap();
        let actual_size = fs::metadata(& self.filename)?.len();
        if actual_size == self.offset {
            { 
                let mut f = OpenOptions::new().
                                write(true).
                                create(true).
                                truncate(true).
                                open(self.checkpoint_filename())?;
                f.write_u64::<LittleEndian>(actual_size)?; // write stuff twice so that corrupted writes are more likely to be caught
                f.write_u64::<LittleEndian>(actual_size)?;
            } // close the file
            return Ok(actual_size);
        } else {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Checkpoint size differs. Expected {}, but {} found", self.offset, actual_size)));
        }
    }

    /** Verifies the integrity of the table. 
     
        This is a rather simple check that makes sure that the actual size of the table is equivalent to the last saved checkpoint. If this were not the case, it means that some data has been written after last checkpoint for which we have no guarantee of integrity and we must roll back to nearest savepoint. 
     */
    pub fn verify(& mut self) -> Result<u64, std::io::Error> {
        if let Ok(mut f) = OpenOptions::new().read(true).open(self.checkpoint_filename()) {
            let actual_size = f.read_u64::<LittleEndian>()?;
            let actual_size_alt = f.read_u64::<LittleEndian>()?;
            if actual_size != actual_size_alt {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Checkpoint size differs from backup. {} vs {} ", actual_size, actual_size_alt)));
            }
            if self.offset != actual_size {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Checkpoint size differs. Expected {}, but {} found", self.offset, actual_size)));
            }
        } else {
            if self.offset != 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Checkpoint size differs. Expected {}, but no checkpoint found", self.offset)));
            }
        }
        return Ok(self.offset);
    }

    /** Adds information about the table to given savepoint. 
     
        This first flushes and if successful, adds the size under the table name as dictated by the record.
     */
    pub fn add_to_savepoint(& mut self, savepoint : & mut Savepoint) -> Result<(),std::io::Error> {
        let savepoint_size = self.flush()?;
        return savepoint.set_size_for::<RECORD>(savepoint_size);
    }

    /** Reverts the table to given savepoint. 
     
        Truncates the underlying file to the size specified by the savepoint record. If the file is not part of the savepoint, it is expected to be empty and will be truncated to zero. Updates the checkpoint to reflect the change and returns the new size.       
     */
    pub fn revert_to_savepoint(& mut self, savepoint : & Savepoint) -> Result<u64, std::io::Error> {
        // if there is our record in the savepoint revert to the stored size, otherwise revert to empty file
        let len = savepoint.get_size_for::<RECORD>();
        let mut actual_size = fs::metadata(& self.filename)?.len();
        // this is really bad, so fail badly
        if actual_size < len {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Savepoint is larger ({}) than current file size ({}) for file {}", len, actual_size, self.filename)));
        }
        // if the size is the same, we haven't changed since the savepoint, do nothing:)
        if actual_size == len {
            return Ok(len);
        }
        // close the buffer so that we can operate on the file
        drop(& mut self.f);
        let mut f = OpenOptions::new().
            write(true).
            create(true).
            open(& self.filename)?;
        f.set_len(len)?;
        drop(& mut self.f); // we might not have to do this, but let's be super cautious
        // reopen the file, wrap it in buffer and update ourselves
        f = OpenOptions::new().
            write(true).
            create(true).
            open(& self.filename)?;
        self.offset = f.seek(SeekFrom::End(0))?;
        self.f = BufWriter::new(f);
        // update the checkpoint size to the savepoint and check that it is the size we expect
        actual_size = self.flush()?;
        if actual_size != len {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Setting filesize to {} failed for file {}, with actual size {}", len, self.filename, actual_size)));
        } 
        return Ok(actual_size);
    }

    /** Returns the filename in which the checkpoint size is stored. 
     */
    fn checkpoint_filename(& self) -> String {
        return format!("{}.checkpoint", self.filename);
    }
}

/** An iterator into the append only table. 
 
    The iterator simply returns *all* entries in the table in the order they were written to it. If the underlying table supports updates to ids, it is the responsibility of the iterator client to make sure that only the latest value for any given id will be used, unless interested in history. The iterator can either go over entire table, or up to a given savepoint. 
    
    In practice, the raw table iterator is not expected to be used by general users, who should always use the datastore view which provides the indexed interators on demand. 

    TODO that safest would be to only go to given checkpoint, but as the table is only ever used internally, I do not think we should bother as the checkpoints are really only for data integrity and end users should only ever see savepoints. 
 */
pub struct TableIterator<RECORD : TableRecord> {
    f : BufReader<File>,
    offset : u64, 
    savepoint_limit : u64,
    why_oh_why : std::marker::PhantomData<RECORD>
}

impl<RECORD : TableRecord> Iterator for TableIterator<RECORD> {
    type Item = (RECORD::Id, RECORD::Value);

    fn next(& mut self) -> Option<Self::Item> {
        if self.offset >= self.savepoint_limit {
            return None;
        }
        let id = RECORD::Id::read_from(& mut self.f, & mut self.offset).unwrap();
        let value = RECORD::Value::read_from(& mut self.f, & mut self.offset).unwrap();
        return Some((id, value));
    }
}

impl<RECORD : TableRecord> TableIterator<RECORD> {
    /** Returns table iterator that would iterate over the entire table. 
     
        Note that this is really dagnerous if the table is written to at the same time as the iterator may fail on the latest record if incomplete at the time of reading. 
     */
    pub (crate) fn for_all(root : & str) -> TableIterator<RECORD> {
        let filename = record_table_path::<RECORD>(root);
        let f = OpenOptions::new().
                    read(true).
                    open(& filename).unwrap();
        // seek towards the end because (a) Rust won't do it for us and (b) determine the offset
        // create the append only table and return it
        return TableIterator{
            f : BufReader::new(f),
            offset : 0,
            savepoint_limit : u64::MAX, 
            why_oh_why : std::marker::PhantomData{}
        };
    }

    /** Returns table iterator that will traverse the table up to the given savepoint. 
     
        This is safe even in the presence of continuous updates to the table. 
     */
    pub fn for_savepoint(root : & str, savepoint : & Savepoint) -> TableIterator<RECORD> {
        let mut iter = Self::for_all(root);
        iter.savepoint_limit = savepoint.get_size_for::<RECORD>();
        return iter;
    }

    pub(crate) fn offset(& self) -> u64 { self.offset }
    pub(crate) fn savepoint_limit(& self) -> u64 { self.savepoint_limit }

}

