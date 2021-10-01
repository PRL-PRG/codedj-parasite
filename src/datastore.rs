use std::io;
use std::fs;
use std::sync::Mutex;

use crate::folder_lock::*;
use crate::savepoints::*;
use crate::table_writers::*;
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

pub struct ProjectLogs {} impl TableRecord for ProjectLogs {
    type Id = ProjectId;
    type Value = ProjectLog;
    const TABLE_NAME : &'static str = "project-logs";
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
    type Value = User;
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
     
        A project points to its kind (Git, GitHub) and url. A project may also be deleted, which means we keep its copy in the store, but it has been removed from upstream (such as projects deleted, or becoming private on GitHub). Finally a project can be tombstoned, which means the project is no longer part of the datastore (although we still need to keep the record and contents of the project in the datastore for historical accuracy, it will not be returned in searches).
     */
    projects : Mutex<TableWriter<Projects>>,

    /** Project heads. 
     
        Every successful update of a project that actually changed its contents will result in a new project heads entry for the project.
     */
    project_heads : Mutex<TableWriter<ProjectHeads>>,
    
    /** Project logs. 
     
        Everytime a project is moved to/from the datastore, renamed, or updated a log entry is added to the log table. Any errors during its update are also logged. 
     */
    project_logs : Mutex<TableWriter<ProjectLogs>>,

    /** All commits belonging to projects in the datastore. 
     
        Note that contrary to previous versions, project has is part of the commit record and does not reside in a separate table.
     */
    commits : Mutex<TableWriter<Commits>>,

    /** Paths to ids mapping. 
     
        A path is a string. 
     */
    paths : Mutex<TableWriter<Paths>>,
    
    /** Contents of files. 
     
        Compressed contents of a file, deduplicated by the SHA hash of the uncompressed contents. 
     */
    contents : Mutex<TableWriter<Contents>>,

    /** Users.  
     
        Users are deduplicated based on their email. 

        TODO have more than string here, like a full user description, etc. 
     */
    users : Mutex<TableWriter<Users>>,

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
            project_heads : Mutex::new(TableWriter::open_or_create(folder_lock.folder())),
            project_logs : Mutex::new(TableWriter::open_or_create(folder_lock.folder())),
            commits : Mutex::new(TableWriter::open_or_create(folder_lock.folder())),
            paths : Mutex::new(TableWriter::open_or_create(folder_lock.folder())),
            contents : Mutex::new(TableWriter::open_or_create(folder_lock.folder())),
            users : Mutex::new(TableWriter::open_or_create(folder_lock.folder())),

            savepoints : Mutex::new(TableWriter::open_or_create(folder_lock.folder())),
            folder_lock,
        };
        // verify the datastore's consistency
        result.projects().verify()?;
        result.project_heads().verify()?;
        result.commits().verify()?;
        result.paths().verify()?;
        result.contents().verify()?;
        result.users().verify()?;

        result.savepoints.lock().unwrap().verify()?;

        // if all is ok, return the valid datastore
        return Ok(result);
    }

    /** Creates new savepoint with given name and adds to it all tables in the datastore. 
     
        Note that in order to make sure that the savepoint is really consistent, locks to *all* tables are obtained first, which is likely to deadlock if any active workers are working on the datastore at the sametime. 

     */
    pub fn create_savepoint(& self, name : String) -> io::Result<()> {
        let mut savepoint = Savepoint::new(name);
        let mut lprojects = self.projects();
        let mut lproject_heads = self.project_heads();
        let mut lproject_logs = self.project_logs();
        let mut lcommits = self.commits();
        let mut lpaths = self.paths();
        let mut lcontents = self.contents();
        let mut lusers = self.users();
        let mut lsavepoints = self.savepoints.lock().unwrap();

        lprojects.add_to_savepoint(& mut savepoint)?;
        lproject_heads.add_to_savepoint(& mut savepoint)?;
        lproject_logs.add_to_savepoint(& mut savepoint)?;
        lcommits.add_to_savepoint(& mut savepoint)?;
        lpaths.add_to_savepoint(& mut savepoint)?;
        lcontents.add_to_savepoint(& mut savepoint)?;
        lusers.add_to_savepoint(& mut savepoint)?;
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
        let mut lprojects = self.projects();
        let mut lproject_heads = self.project_heads();
        let mut lproject_logs = self.project_logs();
        let mut lcommits = self.commits();
        let mut lpaths = self.paths();
        let mut lcontents = self.contents();
        let mut lusers = self.users();
        let mut lsavepoints = self.savepoints.lock().unwrap();

        lprojects.revert_to_savepoint(& savepoint)?;
        lproject_heads.revert_to_savepoint(& savepoint)?;
        lproject_logs.revert_to_savepoint(& savepoint)?;
        lcommits.revert_to_savepoint(& savepoint)?;
        lpaths.revert_to_savepoint(& savepoint)?;
        lcontents.revert_to_savepoint(& savepoint)?;
        lusers.revert_to_savepoint(& savepoint)?;
        lsavepoints.revert_to_savepoint(& savepoint)?;

        return Ok(());
    }

    /** Returns the locked projects table. 
     */
    pub fn projects<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<Projects>> {
        return self.projects.lock().unwrap();
    }

    /** Returns the locked project heads records. 
     */
    pub fn project_heads<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<ProjectHeads>> {
        return self.project_heads.lock().unwrap();
    }

    /** returns the locked project logs table. 
     */
    pub fn project_logs<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<ProjectLogs>> {
        return self.project_logs.lock().unwrap();
    }

    /** Returns the locked commits table. 
     */
    pub fn commits<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<Commits>> {
        return self.commits.lock().unwrap();
    }

    /** Returns the locked paths table. 
     */
    pub fn paths<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<Paths>> {
        return self.paths.lock().unwrap();
    }

    /** Returns the locked contents table. 
     */
    pub fn contents<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<Contents>> {
        return self.contents.lock().unwrap();
    }

    /** returns the locked users table. 
     */
    pub fn users<'a>(&'a self) -> std::sync::MutexGuard<'a, TableWriter<Users>> {
        return self.users.lock().unwrap();
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





