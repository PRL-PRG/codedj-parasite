mod project;
mod commit;
mod helpers;
// this will go away in the future, it contains all stuff that haven't been merged in the new multi-source API yet
mod undecided;

use std::collections::{HashMap, HashSet, BinaryHeap};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use crate::project::*;
use crate::commit::*;
use crate::helpers::*;
use crate::undecided::*;




#[derive(Clone)]
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