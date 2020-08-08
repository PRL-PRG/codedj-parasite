use std::collections::{HashMap, HashSet, BinaryHeap};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

// TODO how can I make these package only?
// this should be package-only
//pub mod downloader_state;
//pub mod ghtorrent;
//pub mod project;
//pub mod commit;
//pub mod user;
pub mod db_manager;
pub mod record;
pub mod helpers;


/** Different ids for the entities the database contains.
 */
pub type UserId = u64;
pub type SnapshotId = u64;
pub type BlobId = u64;
pub type PathId = u64;
pub type CommitId = u64;
pub type ProjectId = u64;

/** Source of the information from the downloader. 
 
    For now we only support GHTorrent and GitHub. In the future we might add more. While the downloader exports this, it should not really matter for the users in most cases, other than reliability - stuff coming from GitHub is more reliable than GhTorrent.   
 */
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Source {
    NA,
    GHTorrent,
    GitHub,
}

/** Project is the main gateway to the database. 

    Each project comes with 
    
 */
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Project {
    // id of the project
    pub id : ProjectId,
    // url of the project (latest used)
    pub url : String,
    // time at which the project was updated last (i.e. time for which its data are valid)
    pub last_update: u64,
    // metadata information for the project
    pub metadata : HashMap<String,String>,
    // head refs of the project at the last update time
    pub heads : Vec<(String, CommitId)>,
    // source the project data comes from    
    pub source : Source,
}

/** Single commit information. 
 
    The basic information is required for all commits in the database. Some commits will optionally also return their commit message and changes.
    
    TODO message should I think be bytes, not string because of non-utf-8 garbage. 
 */
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    // commit id
    pub id : CommitId, 
    // id of parents
    pub parents : Vec<CommitId>,
    // committer id and time
    pub committer_id : u64,
    pub committer_time : u64,
    // author id and time
    pub author_id : u64,
    pub author_time : u64,
    // commit message
    pub message: Option<String>,
    // changes made by the commit 
    pub changes: Option<HashMap<PathId, SnapshotId>>,
    // source the commit has been obtained from
    pub source : Source,
}

/** User information. 
 
    Users are unique based on their email.
 */
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct User {
    // id of the user
    pub id : UserId,
    // email for the user
    pub email : String,
    // name of the user
    pub name : String, 
    // source of the user information
    pub source : Source,
}

/** Snapshot is an unique particular file contents. 
 
    Each snapshot can have metadata and optionally be associated with downloaded contents, which can be retrieved using the blob api.  
 */
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    // id of the snapshot
    pub id : SnapshotId,
    // contents id, None if the contents was not downloaded
    pub contents : Option<BlobId>,
    // metadata for the snapshot
    pub metadata : HashMap<String, String>,
}

/** Actual contents identified by its  hash. 
    
    TODO the string here should also likely be bytes.
 */
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blob {
    // id of the blob
    pub id : BlobId, 
    // hash of the contents
    pub hash : git2::Oid,
    // the contents
    pub contents : String,
}

/** File path
 */
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilePath {
    // path id
    id : u64,
    // the actual path
    path : String,
}

/** A trait for tests. */
pub trait Database {
    fn num_projects(& self) -> u64;
    fn get_user(& self, id : UserId) -> Option<& User>;
    fn get_snapshot(& self, id : BlobId) -> Option<Snapshot>;
    fn get_file_path(& self, id : PathId) -> Option<FilePath>;
    fn get_commit(& self, id : CommitId) -> Option<Commit>;
    fn get_project(& self, id : ProjectId) -> Option<Project>;
}

/** The dejacode downloader interface.
 
    Notes on how things are stored (so far only the following things are):

    # Projects

    Each project lives in its own folder and in order to return its representation, notably the log file, metadata file and heads. These must be loaded and analyzed for the project to be constructed. As such projects are not cached and each request will be the disk access. 

    # Commits

    Commit information lives in the following global files:

    - commit_hashes.csv (SHA1 to commit id)
    - commits.csv (time, id, author, committer, author time, committer time, source)
    - commit_parents.csv (time, commit id, parent id)

    Inside the commits and commit parents files each record has a time and newer records *completely* override the information provided in the old records (i.e. first the commit is filled in via ghTorrent, but later when reanalyzed from github directly the information can be updated). The timestamps will allow us to reconstruct the database to any particular time in the past precisely. 

    Commits from the above files are preloaded when DCD is initialized. 
    
    TODO in the future there will also be commit_changes.csv and commit_messages and commit_messages_index.csv for getting changes and messages. 

    # Users

    Similarly to commits, users live in a few global files:

    - user_emails.csv (email to user id)
    - users.csv (time, id, name, source)

    the users file is also timed. 

    TODO in the future perhaps more files, such as metadata. 


 */
pub struct DCD {
    // the root folder
    root_ : String, 
}

impl DCD {

    pub fn new(root : String) -> DCD {
        let mut result = DCD{
            root_ : root,
        };




        return result;
    }

    fn load_commits(& mut self) {
        println!("Loading commit records...");
        // loads the records for the projects and commits...
    }

}

impl Source {

    /** Creates source from string.
     */
    pub fn from_str(s : & str) -> Source {
        if *s == *"NA" {
            return Source::NA;
        } else if *s == *"GHT" {
            return Source::GHTorrent;
        } else if *s == *"GH" {
            return Source::GitHub;
        } else {
            panic!("Invalid source detected: {}", s);
        }
    }
}



impl std::fmt::Display for Source {
    fn fmt(& self, f : & mut std::fmt::Formatter) -> std::fmt::Result {
        match & self {
            Source::NA => {
                return write!(f, "NA");
            },
            Source::GHTorrent => {
                return write!(f, "GHT");
            },
            Source::GitHub => {
                return write!(f, "GH");
            }
        }
    }
}


/*











/** Basic access to the DejaCode Downloader database.
 
    The API is tailored to reasonably fast random access to items identified by their IDs so that it can, in theory proceed in parallel (disk permits).
 */
pub struct DCDx {
     root_ : String, 
     num_projects_ : u64,
     users_ : Vec<User>,

}

impl DCDx {

    pub fn from(root_folder : & str) -> Result<DCD, std::io::Error> {
        let mut dcd = DCD{
            root_ : String::from(root_folder),
            num_projects_ : DCD::get_num_projects(root_folder),
            users_ : Vec::new(),
        };

        return Ok(dcd);
    }

    /** Loads the global table of users. This must run before any user details are obtained from the downloader. 
     */
    pub fn load_users(& mut self) {
        let mut reader = csv::Reader::from_path(format!("{}/projects.csv", self.root_)).unwrap();
        for x in reader.records() {
            if let Ok(record) = x {
            }
        }
    }

    /** Creates empty uninitialized downloader with given root folder. 
     */
    fn new(root_folder : & str) -> DCD {
        return DCD{
            root_ : String::from(root_folder),
            num_projects_ : 0,
            users_ : Vec::new(),
        };
    }

    /** Returns the root folder from which the downloader operates. 
     */
    pub fn rootFolder(& self) -> & str {
        return & self.root_;
    }

    // private

    fn get_num_projects(root_folder : & str) -> u64 {
        let filename = format!{"{}/projects.csv", root_folder};
        if Path::new(& filename).exists() {
            if let Ok(mut reader) = csv::Reader::from_path(& filename) {
                if let Some(Ok(record)) = reader.records().next() {
                    if record.len() == 1 {
                        if let Ok(next_id) = record[0].parse::<u64>() {
                            return next_id - 1;
                        }
                    }
                }
            }
        } 
        return 0;
    }

    fn get_project_root(& self, id : ProjectId) -> String {
        return format!("{}/projects/{}/{}", self.root_, id % 1000, id);
    }

    fn get_users_file(& self) -> String {
        return format!("{}/users.csv", self.root_);
    }

}

impl Database for DCD {

    /** Returns the number of projects the downloader contains.
     */
    fn num_projects(& self) -> u64 {
        return self.num_projects_;
    }
    
    /** Users reside in one large file that needs to be loaded first. 
     */
    fn get_user(& self, id : UserId) -> Option<& User> {
        return self.users_.get(id as usize);
    }

    fn get_snapshot(& self, id : BlobId) -> Option<Snapshot> {
        return None;
    }

    fn get_file_path(& self, id : PathId) -> Option<FilePath> {
        return None;
    }

    /** Commits reside in their own files, so random access is simple.
     */
    fn get_commit(& self, id : CommitId) -> Option<Commit> {
        return None;
    }

    /** Projects reside in their own files, so random access is simple.
     */
    fn get_project(& self, id : ProjectId) -> Option<Project> {
        /*
        if let Ok(project) = std::panic::catch_unwind(||{
            return Project::new(id, & self.get_project_root(id));
        }) {
            return Some(project);
        } else {
            return None;
        }
        */
        return None;
    }

}
*/