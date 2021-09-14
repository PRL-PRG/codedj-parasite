use std::io;
use std::io::{Seek, SeekFrom, Read, Write, BufWriter};
use std::fs;
use std::fs::{File, OpenOptions};
use std::collections::HashMap;

use byteorder::*;


use crate::serialization::*;

/** A simple trait for IDs. 
 
    Bare minimum is required from an ID, i.e. they must be able to convert themselves to and from u64, which is how they are stored in the datastore. Since they can convert to u64s, any id is trivially serializable. 
 */
pub trait Id : Copy + Clone + Eq + PartialEq + std::fmt::Debug {
    fn to_number(& self) -> u64;
    fn from_number(id : u64) -> Self;
}

/** A record in a datastore. 
 
    A record specifies the type of its ID, the type of its value and importantly, the filename under which it is to be stored in the datastore. This architecture allows new tables to be added to the datastore easily with almost no overhead (hashmap lookup on the table name). Name clashes can also be checked at runtime. 
 */
pub trait TableRecord {
    type Id : Id;
    type Value : Serializable<Item = Self::Value>;
    const TABLE_NAME : &'static str;
}

/** Datastore's savepoint.
 
    The savepoint simply contains the actual sizes of all tables within a datastore. 
 */
pub struct Savepoint {
    sizes : HashMap<String, u64>,
} 

/** Append only table.
 
    The table consists of a binary file containing the records appended one after another in the order the append() method was called. Each record consists of the id followed by the actual record information. The table *must* be flushed from time to time as the flush makes sure that the all pending writes in the backing buffer are propagated to the file and creates a corresponding checkpoint that verifies the integrity of the file up to the stored size. 

    NOTE that checkpoints provide guarantees of integrity for a single table at a time. If a table fails the checkpoint verification it can't revert back on its own as other tables that have passed the checkpoint verification might still point to the code from this table that would get reverted. In case of failed table verification, the entire datastore must be reverted to the latest savepoint. 
 */
struct TableWriter<RECORD : TableRecord> {
    filename : String,
    f : BufWriter<File>,
    offset : u64,
    why_oh_why : std::marker::PhantomData<RECORD>,
}

impl<RECORD : TableRecord> TableWriter<RECORD> {
    /** Opens existing table in given datastore root, or if it does not exist, creates new table.
     */
    pub fn open_or_create(root : & str) -> Result<TableWriter<RECORD>, io::Error> {
        let filename = format!("{}/{}", root, RECORD::TABLE_NAME);
        let mut f = OpenOptions::new().
                    write(true).
                    create(true).
                    open(& filename)?;
        // seek towards the end because (a) Rust won't do it for us and (b) determine the offset
        let offset = f.seek(SeekFrom::End(0))?;
        // create the append only table and return it
        return Ok(TableWriter{
            filename,
            f : BufWriter::new(f),
            offset, 
            why_oh_why : std::marker::PhantomData{}
        });
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

    pub fn add_to_savepoint(& mut self, savepoint : & mut Savepoint) -> Result<(),std::io::Error> {
        let savepoint_size = self.flush()?;
        let savepoint_name = String::from(RECORD::TABLE_NAME);
        if savepoint.sizes.contains_key(& savepoint_name) {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Savepoint already defined for {}", savepoint_name)));
        } else {
            savepoint.sizes.insert(savepoint_name, savepoint_size);
            return Ok(());
        }
    }

    /** Returns the filename in which the checkpoint size is stored. 
     */
    fn checkpoint_filename(& self) -> String {
        return format!("{}.checkpoint", self.filename);
    }

}


/** Datastore implementation.
 
    The datastore is a simple collection of append only tables that store information about entities. The datastore does not care about what entities it stores. All tables in the datastore are lock protected. 
 */
struct Datastore {


}







// ------------------------------------------------------------------------------------------------

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
