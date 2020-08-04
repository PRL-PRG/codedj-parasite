use std::collections::{HashMap, HashSet, BinaryHeap};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

pub enum Source {
    GHTorrent,
    GitHub,
}

/** User information
 
    Users may come from different platforms and therefore there can be two different users with same name and email from different platforms (i.e. Github & bitbucket). 

    TODO Alternatively, we can say that email identifies an user.
 */
pub struct User {
    // id of the user
    id : u64,
    // email for the user
    email : String,
    // name of the user
    name : String, 
    // source
    source : Source,
}

/** Snapshot information.
 
    The snapshot information consists of the actual value of the snapshot (given as path) and metadata that can be associated with the snapshot (such as size, lines of code, detected language, etc.).
    
    Snapshots are source agnostic. 
 */
pub struct Snapshot {
    // snapshot id and its hash
    id : u64,
    hash : String,
    // file path to the snapshot
    path : Option<String>, 
    // metadata
    metadata : HashMap<String, String>,
}

pub struct FilePath {
    // path id
    id : u64,
    // the actual path
    path : String,
}

/** Commit information.
  
    The commit consists of its id, information about its parents, origin, changes made and its source (ghtorrent, github, etc). 
    
    If changes are empty, it means the commit has not yet been analyzed in detail. 

    TODO should commits have metadata? 
 */
pub struct Commit {
    // commit id and its hash
    id : u64, 
    hash : String,
    // id of parents
    parents : Vec<u64>,
    // committer id and time
    committer_id : u64,
    committer_time : u64,
    // author id and time
    author_id : u64,
    author_time : u64,
    // changes (path -> snapshot)
    changes : Option<HashMap<u64, u64>>,
    // source the commit has been obtained from
    source : Source,
}

/** The project record.
 
    Projects can again come from different sources. 
    
 */
pub struct Project {
    // id of the project
    id : u64,
    // url of the project (latest used)
    url : String,
    // time at which the project was updated last (i.e. time for which its data are valid)
    last_update: u64,
    // head refs of the project at the last update time
    heads : Option<HashMap<String, u64>>,
    // project metadata
    metadata : HashMap<String, String>,
    // source the project data comes from    
    source : Source,
}

/** Basic access to the DejaCode Downloader database.
 
    The API is tailored to reasonably fast random access to items identified by their IDs so that it can, in theory proceed in parallel (disk permits).  
 */
pub struct DCD {
     root_ : String, 
     num_projects_ : u64,
}

impl DCD {

    pub fn new(rootFolder : & str) -> Result<DCD, std::io::Error> {
        let mut dcd = DCD{
            root_ : String::from(rootFolder),
            num_projects_ : DCD::get_num_projects(rootFolder),
        };

        return Ok(dcd);
    }

    pub fn num_projects(& self) -> u64 {
        return self.num_projects_;
    }
    
    pub fn get_user(& self, id : u64) -> Option<User> {
        return None;
    }

    pub fn get_snapshot(& self, id : u64) -> Option<Snapshot> {
        return None;
    }

    pub fn get_file_path(& self, id : u64) -> Option<FilePath> {
        return None;
    }

    pub fn get_commit(& self, id : u64) -> Option<Commit> {
        return None;
    }

    pub fn get_project(& self, id : u64) -> Option<Project> {
        return None;
    }

    fn get_num_projects(rootFolder : & str) -> u64 {
        let filename = format!{"{}/projects.csv", rootFolder};
        if Path::new(& filename).exists() {
            if let Ok(mut reader) = csv::Reader::from_path(& filename) {
                if let Some(Ok(record)) = reader.records().next() {
                    if record.len() == 1 {
                        if let Ok(nextId) = record[0].parse::<u64>() {
                            return nextId - 1;
                        }
                    }
                }
            }
        } 
        return 0;
    }


}