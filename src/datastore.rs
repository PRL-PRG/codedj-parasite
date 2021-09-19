use std::io;
use std::fs;
use std::sync::Mutex;

use crate::folder_lock::*;
use crate::savepoints::*;
use crate::table_writer::*;
use crate::records::*;


pub struct Projects {} impl TableRecord for Projects {
    type Id = ProjectId;
    type Value = Project;
    const TABLE_NAME : &'static str = "projects";
}

pub struct ProjectHeads {} impl TableRecord for ProjectHeads {
    type Id = ProjectId;
    type Value = Heads;
    const TABLE_NAME : &'static str = "project-heads";
}

pub struct Commits {} impl TableRecord for Commits {
    type Id = CommitId;
    type Value = Commit;
    const TABLE_NAME : &'static str = "commits";
}

pub struct CommitHashes {} impl TableRecord for CommitHashes { 
    type Id = CommitId; 
    type Value = SHA; 
    const TABLE_NAME : &'static str = "commit-hashes";
}

pub struct Paths {} impl TableRecord for Paths {
    type Id = PathId;
    type Value = String;
    const TABLE_NAME : &'static str = "paths";
}

pub struct Contents {} impl TableRecord for Contents {
    type Id = ContentsId;
    type Value = FileContents;
    const TABLE_NAME : &'static str = "contents";
}

pub struct Users {} impl TableRecord for Users {
    type Id = UserId;
    type Value = String;
    const TABLE_NAME : &'static str = "users";
}


struct Savepoints {} impl TableRecord for Savepoints {
    type Id = FakeId; 
    type Value = Savepoint; 
    const TABLE_NAME: &'static str = "savepoints"; 
}



/** Datastore implementation.

    The datastore is extremely simple and very generic structure. It is basically only a collection of append only tables and basic maintenance infrastucture around them. 
 */
pub struct Datastore {
    folder_lock : FolderLock,

    /** Projects that are currently available in the datastore. 
     */
    projects : Mutex<TableWriter<Projects>>,
    // project heads
    // project log

    commits : Mutex<TableWriter<Commits>>,
    commit_hashes : Mutex<TableWriter<CommitHashes>>,

    // users
    // contents

    /** The savepoints specified for the datastore. 
     */
    savepoints : Mutex<TableWriter<Savepoints>>,

}

impl Datastore {
    pub fn open_or_create(folder : String) -> io::Result<Datastore> {
        // create the folder if it does not exist
        fs::create_dir_all(& folder)?;
        let folder_lock = FolderLock::lock(folder)?;
        // create or open the datastore
        let result = Datastore{
            projects : Mutex::new(TableWriter::open_or_create(folder_lock.folder())),
            commits : Mutex::new(TableWriter::open_or_create(folder_lock.folder())),
            commit_hashes : Mutex::new(TableWriter::open_or_create(folder_lock.folder())),
            savepoints : Mutex::new(TableWriter::open_or_create(folder_lock.folder())),
            folder_lock,
        };
        // verify the datastore's consistency
        result.projects().verify()?;
        result.commits().verify()?;
        result.commit_hashes().verify()?;

        result.savepoints.lock().unwrap().verify()?;

        // if all is ok, return the valid datastore
        return Ok(result);
    }

    /** Creates new savepoint with given name and adds to it all tables in the datastore. 
     
        Note that in order to make sure that the savepoint is really consistent, locks to *all* tables are obtained first, which is likely to deadlock if any active workers are working on the datastore at the sametime. 

     */
    pub fn create_savepoint(& self, name : String) -> io::Result<()> {
        let mut savepoint = Savepoint::new(name);
        let mut lcommits = self.commits();
        let mut lcommit_hashes = self.commit_hashes();
        let mut lsavepoints = self.savepoints.lock().unwrap();

        lcommits.add_to_savepoint(& mut savepoint)?;
        lcommit_hashes.add_to_savepoint(& mut savepoint)?;
        lsavepoints.add_to_savepoint(& mut savepoint)?;

        lsavepoints.append(FakeId::ID, & savepoint);
        return Ok(());
    }

    /** Returns the closest savepoint that was taken *before* the specified time.
     
        If no such savepoint exists, returns none. 
     */
    pub fn get_closest_savepoint(& self, time : i64) -> Option<Savepoint> {
        let _g = self.savepoints.lock().unwrap(); // let no-one interfere as we are iterating over the entire file
        return TableIterator::<Savepoints>::for_all(& self.folder_lock.folder())
            .filter(|(_id, sp)| sp.time() <= time)
            .map(|(_id, sp)| sp)
            .last();
    }

    /** Returns the last savepoint with given name. 
     
        We are returning last savepoint because we don't really check for savepoint name collision and the raionale is that if there are savepoints with the same name, then the latest savepoint will preserve more data. 
     */
    pub fn get_savepoint_by_name(& self, name : & str) -> Option<Savepoint> {
        let _g = self.savepoints.lock().unwrap(); // let no-one interfere as we are iterating over the entire file
        return TableIterator::<Savepoints>::for_all(& self.folder_lock.folder())
            .filter(|(_id, sp)| sp.name() == name)
            .map(|(_id, sp)| sp)
            .last();
    }

    /** Returns the latest savepoint, if any.
     */
    pub fn get_latest_savepoint(& self) -> Option<Savepoint> {
        let _g = self.savepoints.lock().unwrap(); // let no-one interfere as we are iterating over the entire file
        return TableIterator::<Savepoints>::for_all(& self.folder_lock.folder())
            .map(|(_id, sp)| sp)
            .last();
    }

    /** Reverts to given savepoint. 
     
        Acquires locks to all tables and reverts them to given savepoint. May deadlock if someone else is using the datastore as well. 
     */
    pub fn revert_to_savepoint(& self, savepoint : & Savepoint) -> io::Result<()> {
        let mut lcommits = self.commits();
        let mut lcommit_hashes = self.commit_hashes();
        let mut lsavepoints = self.savepoints.lock().unwrap();

        lcommits.revert_to_savepoint(& savepoint)?;
        lcommit_hashes.revert_to_savepoint(& savepoint)?;
        lsavepoints.revert_to_savepoint(& savepoint)?;

        return Ok(());
    }

    /** Returns the locked projects table. 
     */
    pub fn projects<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<Projects>> {
        return self.projects.lock().unwrap();
    }

    /** Returns the locked commits table. 
     */
    pub fn commits<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<Commits>> {
        return self.commits.lock().unwrap();
    }

    /** Returns the locked commit_hashes table.
     */
    pub fn commit_hashes<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<CommitHashes>> {
        return self.commit_hashes.lock().unwrap();
    }

}












/** Fake id used for savepoints and other id-less structures so that we can store them in a table. 
 */
#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash)]
pub(crate) struct FakeId { id : u64 }

impl FakeId {
    pub (crate) const ID : FakeId = FakeId{ id : 0 };
}

impl Id for FakeId {
    fn to_number(&self) -> u64 { self.id }
    fn from_number(id : u64) -> Self { FakeId{id} }
}





