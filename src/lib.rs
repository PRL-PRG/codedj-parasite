use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::prelude::*;

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

    TODO zap last_update
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
    pub committer_id : UserId,
    pub committer_time : u64,
    // author id and time
    pub author_id : UserId,
    pub author_time : u64,
    // commit message
    pub message: Option<String>,
    // changes made by the commit 
    pub changes: Option<HashMap<PathId, SnapshotId>>,
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
    id : PathId,
    // the actual path
    path : String,
}

/** A trait for tests. */
pub trait Database {
    fn num_projects(& self) -> u64;
    fn num_commits(& self) -> u64;
    fn num_users(& self) -> u64;

    fn get_project(& self, id : ProjectId) -> Option<Project>;
    fn get_commit(& self, id : CommitId) -> Option<Commit>;
    fn get_user(& self, id : UserId) -> Option<& User>;
    //fn get_snapshot(& self, id : BlobId) -> Option<Snapshot>;
    //fn get_file_path(& self, id : PathId) -> Option<FilePath>;
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
    root_ : String,
    num_projects_ : u64,
    users_ : Vec<User>,
    commit_ids_ : HashMap<git2::Oid, CommitId>,
    commits_ : Vec<CommitBase>,
}

impl DCD {

    pub fn new(root : String) -> DCD {
        println!("Loading dejacode database...");
        let num_projects = db_manager::DatabaseManager::get_num_projects(& root);
        println!("    {} projects", num_projects);
        let users = Self::get_users(& root);
        println!("    {} users", users.len());
        let commit_ids = db_manager::DatabaseManager::get_commit_ids(& root);
        println!("    {} commits", commit_ids.len());
        let commits = Self::get_commits(& root, commit_ids.len());
        println!("    {} commit records", commit_ids.len());

        let result = DCD{
            root_ : root, 
            num_projects_ : num_projects,
            users_ : users,
            commit_ids_ : commit_ids,
            commits_ : commits,
        };
        return result;
    }


    fn get_users(root : & str) -> Vec<User> {
        let mut result = Vec::<User>::new();
        // first load the immutable email to id mapping
        {
            let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/user_ids.csv", root)).unwrap();
            for x in reader.records() {
                let record = x.unwrap();
                let email = String::from(& record[0]);
                let id = record[1].parse::<u64>().unwrap() as UserId;
                assert_eq!(id as usize, result.len());
                result.push(User{
                    id,
                    email,
                    name : String::new()
                });
            }
        }
        // now load the records
        {
            let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/user_records.csv", root)).unwrap();
            for x in reader.records() {
                let record = x.unwrap();
                let id = record[1].parse::<u64>().unwrap();
                let name = String::from(& record[2]);
                result[id as usize].name = name;
            }
        }
        return result;
    }

    fn get_commits(root : & str, num_commits : usize) -> Vec<CommitBase> {
        let mut result = Vec::<CommitBase>::with_capacity(num_commits);
        for _ in 0..num_commits {
            result.push(CommitBase{
                parents : Vec::new(),
                committer_id : 0,
                committer_time : 0,
                author_id : 0,
                author_time : 0,
            })
        }
        {
            let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/commit_records.csv", root)).unwrap();
            for x in reader.records() {
                let record = x.unwrap();
                let id = record[1].parse::<usize>().unwrap();
                let ref mut commit = result[id];
                commit.committer_id = record[2].parse::<u64>().unwrap() as UserId;
                commit.committer_time = record[3].parse::<u64>().unwrap();
                commit.author_id = record[4].parse::<u64>().unwrap() as UserId;
                commit.author_time = record[5].parse::<u64>().unwrap();
            }
        }
        // and now load the parents
        let mut parents_update_times = Vec::<u64>::with_capacity(num_commits);
        for _ in 0..num_commits {
            parents_update_times.push(0);
        }
        {
            let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/commit_parents.csv", root)).unwrap();
            for x in reader.records() {
                let record = x.unwrap();
                let t = record[0].parse::<u64>().unwrap();
                let id = record[1].parse::<usize>().unwrap();
                // clear the parent records if the update time differs
                if t != parents_update_times[id] {
                    parents_update_times[id] = t;
                    result[id].parents.clear();
                }
                result[id].parents.push(record[2].parse::<u64>().unwrap() as CommitId);
            }
        }
        return result;
    }

}

impl Database for DCD {

    /** Returns the number of projects the downloader contains.
     */
    fn num_projects(& self) -> u64 {
        return self.num_projects_;
    }

    fn num_commits(& self) -> u64 {
        return self.commit_ids_.len() as u64;
    }

    fn num_users(& self) -> u64 {
        return self.users_.len() as u64;
    }
    
    fn get_project(& self, id : ProjectId) -> Option<Project> {
        if let Ok(project) = std::panic::catch_unwind(||{
            return Project::from_log(id, & db_manager::DatabaseManager::get_project_log_file(& self.root_, id), & self);
        }) {
            return Some(project);
        } else {
            return None;
        }
    }

    fn get_commit(& self, id : CommitId) -> Option<Commit> {
        if let Some(base) = self.commits_.get(id as usize) {
            let result = Commit::new(id, base);
            // TODO check lazily for message and changes
            return Some(result);
        } else {
            return None;
        }
    }

    fn get_user(& self, id : UserId) -> Option<&User> {
        return self.users_.get(id as usize);
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


impl Project {
    /** Constructs the project information from given log file. 
     */
    fn from_log(id : ProjectId, log_file : & str, dcd : & DCD) -> Project {
        let mut result = Project{
            id, 
            url : String::new(),
            last_update : 0,
            metadata : HashMap::new(),
            heads : Vec::new(),
        };
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(log_file).unwrap();
        let mut clear_heads = false;
        for x in reader.records() {
            match record::ProjectLogEntry::from_csv(x.unwrap()) {
                record::ProjectLogEntry::Init{ time : _, source : _, url } => {
                    result.url = url;
                },
                record::ProjectLogEntry::UpdateStart{ time : _, source : _ } => {
                    clear_heads = true;
                },
                record::ProjectLogEntry::Update{ time, source : _ } => {
                    result.last_update = time;
                },
                record::ProjectLogEntry::NoChange{ time, source : _} => {
                    result.last_update = time;
                },
                record::ProjectLogEntry::Metadata{ time : _, source : _, key, value } => {
                    result.metadata.insert(key, value);
                },
                record::ProjectLogEntry::Head{ time : _, source : _, name, hash} => {
                    if clear_heads {
                        result.heads.clear();
                        clear_heads = false;
                    } 
                    result.heads.push((name, dcd.commit_ids_[& hash]));
                }
            }
        }
        return result;
    }

}

impl Commit {
    fn new(id : CommitId, base : & CommitBase) -> Commit {
        return Commit{
            id : id, 
            parents : base.parents.clone(),
            committer_id : base.committer_id,
            committer_time : base.committer_time,
            author_id : base.author_id,
            author_time : base.author_time,
            message : None, 
            changes : None,
        };
    } 
}


/** Smaller struct for containing the non-lazy elements of the commit. 
 */
struct CommitBase {
    // id of parents
    pub parents : Vec<CommitId>,
    // committer id and time
    pub committer_id : u64,
    pub committer_time : u64,
    // author id and time
    pub author_id : u64,
    pub author_time : u64,
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