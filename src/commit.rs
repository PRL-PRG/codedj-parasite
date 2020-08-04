use std::collections::{HashMap, HashSet, BinaryHeap};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use crate::Source;

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

impl Commit {

    
}
