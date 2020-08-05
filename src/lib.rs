// this should be package-only
pub mod downloader_state;
pub mod ghtorrent;



pub mod project;
pub mod commit;
mod helpers;
// this will go away in the future, it contains all stuff that haven't been merged in the new multi-source API yet
pub mod undecided;

use std::collections::{HashMap, HashSet, BinaryHeap};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use crate::project::*;
use crate::commit::*;
use crate::helpers::*;
use crate::undecided::*;



/** Source of the information from the downloader. 
 
    For now we only support GHTorrent and GitHub. In the future we might add more. While the downloader exports this, it should not really matter for the users in most cases, other than reliability - stuff coming from GitHub is more reliable than GhTorrent.   
 */
#[derive(Clone)]
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

trait Database {
    fn num_projects(& self) -> u64;
    fn get_user(& self, id : u64) -> Option<User>;
    fn get_snapshot(& self, id : u64) -> Option<Snapshot>;
    fn get_file_path(& self, id : u64) -> Option<FilePath>;
    fn get_commit(& self, id : u64) -> Option<Commit>;
    fn get_project(& self, id : u64) -> Option<Project>;
}


/** Basic access to the DejaCode Downloader database.
 
    The API is tailored to reasonably fast random access to items identified by their IDs so that it can, in theory proceed in parallel (disk permits).
 */
pub struct DCD {
     root_ : String, 
     num_projects_ : u64,

}

impl DCD {

    pub fn from(root_folder : & str) -> Result<DCD, std::io::Error> {
        let mut dcd = DCD{
            root_ : String::from(root_folder),
            num_projects_ : DCD::get_num_projects(root_folder),
        };

        return Ok(dcd);
    }

    /** Creates empty uninitialized downloader with given root folder. 
     */
    fn new(root_folder : & str) -> DCD {
        return DCD{
            root_ : String::from(root_folder),
            num_projects_ : 0,
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

    fn get_project_root(& self, id : u64) -> String {
        return format!("{}/projects/{}/{}", self.root_, id % 1000, id);
    }

}

impl Database for DCD {

    /** Returns the number of projects the downloader contains.
     */
    fn num_projects(& self) -> u64 {
        return self.num_projects_;
    }
    
    fn get_user(& self, id : u64) -> Option<User> {
        return None;
    }

    fn get_snapshot(& self, id : u64) -> Option<Snapshot> {
        return None;
    }

    fn get_file_path(& self, id : u64) -> Option<FilePath> {
        return None;
    }

    /** Commits reside in their own files, so random access is simple.
     */
    fn get_commit(& self, id : u64) -> Option<Commit> {
        return None;
    }

    /** Projects reside in their own files, so random access is simple.
     */
    fn get_project(& self, id : u64) -> Option<Project> {
        return None;
    }

}