use std::collections::{HashMap, HashSet, BinaryHeap};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use crate::Source;
use crate::*;

/** Commit information.
  
    The commit consists of its id, information about its parents and its source (ghtorrent, github, etc). Commit messages and actual changes of the commit are to be obtained differently.
    
 */
#[derive(Clone)]
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
    // source the commit has been obtained from
    pub source : Source,
}

impl Commit {

    pub(crate) fn new(id : CommitId, source : Source) -> Commit {
        return Commit {
            id : id, 
            parents : Vec::new(),
            committer_id : 0,
            committer_time : 0,
            author_id : 0,
            author_time : 0,
            source : source,
        };
    }
    
}
