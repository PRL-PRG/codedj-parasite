use std::collections::{HashMap, HashSet, BinaryHeap};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use crate::Source;

/** Commit information.
  
    The commit consists of its id, information about its parents and its source (ghtorrent, github, etc). Commit messages and actual changes of the commit are to be obtained differently.
    
 */
pub struct Commit {
    // commit id and its hash
    id : u64, 
    hash : git2::Oid,
    // id of parents
    parents : Vec<u64>,
    // committer id and time
    committer_id : u64,
    committer_time : u64,
    // author id and time
    author_id : u64,
    author_time : u64,
    // source the commit has been obtained from
    source : Source,
}

impl Commit {

    pub(crate) fn write_to_csv(& self, f : & mut File) {
        //writeln!(f, "{},{}")
    }

    
}
