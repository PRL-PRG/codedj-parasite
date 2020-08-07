// this should be package-only
pub mod downloader_state;
pub mod ghtorrent;



pub mod project;
pub mod commit;
pub mod user;
mod helpers;
// this will go away in the future, it contains all stuff that haven't been merged in the new multi-source API yet
pub mod undecided;

use std::collections::{HashMap, HashSet, BinaryHeap};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use crate::project::*;
use crate::commit::*;
use crate::user::*;
use crate::helpers::*;
use crate::undecided::*;

/** Source of the information from the downloader. 
 
    For now we only support GHTorrent and GitHub. In the future we might add more. While the downloader exports this, it should not really matter for the users in most cases, other than reliability - stuff coming from GitHub is more reliable than GhTorrent.   
 */
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Source {
    NA,
    GHTorrent,
    GitHub,
}

impl Source {

    /** Creates source from string.
     */
    pub fn from_string(s : & str) -> Source {
        if (*s == *"NA") {
            return Source::NA;
        } else if *s == *"GHTorrent" {
            return Source::GHTorrent;
        } else if *s == *"GitHub" {
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
                return write!(f, "GhTorrent");
            },
            Source::GitHub => {
                return write!(f, "GitHub");
            }
        }
    }
}

type UserId = u64;
type BlobId = u64;
type PathId = u64;
type CommitId = u64;
type ProjectId = u64;

pub trait Database {
    fn num_projects(& self) -> u64;
    fn get_user(& self, id : UserId) -> Option<& User>;
    fn get_snapshot(& self, id : BlobId) -> Option<Snapshot>;
    fn get_file_path(& self, id : PathId) -> Option<FilePath>;
    fn get_commit(& self, id : CommitId) -> Option<Commit>;
    fn get_project(& self, id : ProjectId) -> Option<Project>;
    // TODO get commit changes and get commit message functions
}


/** Basic access to the DejaCode Downloader database.
 
    The API is tailored to reasonably fast random access to items identified by their IDs so that it can, in theory proceed in parallel (disk permits).
 */
pub struct DCD {
     root_ : String, 
     num_projects_ : u64,
     users_ : Vec<User>,

}

impl DCD {

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
        if let Ok(project) = std::panic::catch_unwind(||{
            return Project::new(id, & self.get_project_root(id));
        }) {
            return Some(project);
        } else {
            return None;
        }
    }

}