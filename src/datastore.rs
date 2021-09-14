/*
use std::io::{Seek, SeekFrom, Read, Write, BufWriter};
use std::fs::{File, OpenOptions};
use std::collections::HashMap;

use byteorder::*;
*/

use std::io;
use std::fs;
use std::sync::Mutex;

use crate::serialization::*;
use crate::tables::*;
use crate::records::*;


struct Projects { }

struct Commits { } impl TableRecord for Commits {
    type Id = CommitId;
    type Value = Commit;
    const TABLE_NAME : &'static str = "commits";

}

struct CommitHashes { } impl TableRecord for CommitHashes { 
    type Id = CommitId; 
    type Value = SHA; 
    const TABLE_NAME : &'static str = "commit-hashes";
}

struct Users { }


struct Savepoints {} impl TableRecord for Savepoints {
    type Id = FakeId; 
    type Value = Savepoint; 
    const TABLE_NAME: &'static str = "savepoints"; 
}



/** Datastore implementation.

    The datastore is extremely simple and very generic structure. It is basically only a collection of append only tables and basic maintenance infrastucture around them. 
 */
struct Datastore {
    folder : String,



    commits : Mutex<TableWriter<Commits>>,
    commit_hashes : Mutex<TableWriter<CommitHashes>>,

    /** The savepoints specified for the datastore. 
     */
    savepoints : Mutex<TableWriter<Savepoints>>,



}

impl Datastore {
    pub fn open_or_create(folder : & str) -> io::Result<Datastore> {
        // create the folder if it does not exist
        fs::create_dir_all(folder)?;
        // create or open the datastore
        let result = Datastore{
            folder: folder.to_owned(),
            commits : Mutex::new(TableWriter::open_or_create(folder)),
            commit_hashes : Mutex::new(TableWriter::open_or_create(folder)),
            savepoints : Mutex::new(TableWriter::open_or_create(folder)),
        };
        // verify the datastore's consistency
        result.commits().verify()?;
        result.commit_hashes().verify()?;

        result.savepoints.lock().unwrap().verify()?;

        // if all is ok, return the valid datastore
        return Ok(result);
    }

    pub fn commits<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<Commits>> {
        return self.commits.lock().unwrap();
    }

    pub fn commit_hashes<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<CommitHashes>> {
        return self.commit_hashes.lock().unwrap();
    }

}













/** Fake id used for savepoints and other id-less structures so that we can store them in a table. 
 */
#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash)]
struct FakeId { id : u64 }

impl Id for FakeId {
    fn to_number(&self) -> u64 { self.id }
    fn from_number(id : u64) -> Self { FakeId{id} }
}





