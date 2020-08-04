mod project;
mod commit;

use std::collections::{HashMap, HashSet, BinaryHeap};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use crate::project::*;
use crate::commit::*;

/** Returns current time in milliseconds.
 */
fn now() -> u64 {
    use std::time::SystemTime;
    return SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("Invalid time detected").as_secs();
}

fn pretty_time(mut seconds : u64) -> String {
    let d = seconds / (24 * 3600);
    seconds = seconds % (24 * 3600);
    let h = seconds / 3600;
    seconds = seconds % 3600;
    let m = seconds / 60;
    seconds = seconds % 60;
    if d > 0 {
        return format!("{}d {}h {}m {}s", d, h, m, seconds);
    } else if h > 0 {
        return format!("{}h {}m {}s", h, m, seconds);
    } else if m > 0 {
        return format!("{}m {}s", m, seconds);
    } else {
        return format!("{}s", seconds);
    }
}



pub enum Source {
    GHTorrent,
    GitHub,
}

impl Source {

    fn from_string(s : & str) -> Source {
        if *s == *"GHTorrent" {
            return Source::GHTorrent;
        } else if *s == *"GitHub" {
            return Source::GitHub;
        } else {
            panic!("Invalid source detected: {}", s);
        }
    }
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


/** Path in the file */
pub struct FilePath {
    // path id
    id : u64,
    // the actual path
    path : String,
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