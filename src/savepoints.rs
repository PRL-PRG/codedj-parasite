use std::collections::{HashMap};
use std::io;
use std::io::{Read, Write};

use crate::serialization::*;
use crate::table_writer::*;


/** Datastore's savepoint.
 
    The savepoint simply contains the actual sizes of all tables within a datastore. And a name and a time at which it was taken. The guarantee of a savepoint is that the whole datastore is internally consistent up to it. Therefore, non-modidying access to datastore using the datastore view class are only provided up to a specified savepoint, ensuring data consistency. 

    Furthermore, when data inconsistency on a table level is detected (via the table checkpoint mechanism), the whole datastore must be reverted to latest savepoint. 
 */
pub struct Savepoint {
    name : String, 
    time : i64,
    sizes : HashMap<String, u64>,
} 

impl Savepoint {
    pub fn new(name : String) -> Savepoint {
        return Savepoint{
            name, 
            time : crate::now(),
            sizes : HashMap::new(),
        }
    }

    pub fn name(& self) -> & str { self.name.as_str() }

    pub fn time(& self) -> i64 { self.time }

    pub(crate) fn get_size_for<RECORD: TableRecord>(& self) -> u64 {
        return self.sizes.get(& String::from(RECORD::TABLE_NAME)).map(|x| *x).unwrap_or(0);        
    }

    pub(crate) fn set_size_for<RECORD: TableRecord>(& mut self, savepoint_size : u64) -> io::Result<()> {
        let savepoint_name = String::from(RECORD::TABLE_NAME);
        if self.sizes.contains_key(& savepoint_name) {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Savepoint already defined for {}", savepoint_name)));
        } else {
            self.sizes.insert(savepoint_name, savepoint_size);
            return Ok(());
        }

    }
}

/** Serializable implementation for savepoints. 
 */
impl Serializable for Savepoint {
    type Item = Savepoint;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Savepoint> {
        let name = String::read_from(f, offset)?;
        let time = i64::read_from(f, offset)?;
        let sizes = HashMap::<String, u64>::read_from(f, offset)?;
        return Ok(Savepoint{name, time, sizes});
    }

    fn write_to(f : & mut dyn Write, item : & Savepoint, offset : & mut u64) -> io::Result<()> {
        String::write_to(f, & item.name, offset)?;
        i64::write_to(f, & item.time, offset)?;
        HashMap::<String, u64>::write_to(f, & item.sizes, offset)?;
        return Ok(());
    }
}

