use std::collections::{HashMap, HashSet, BinaryHeap};

enum Source {
    GHTorrent,
    GitHub,
}


/** User information
 
    Users may come from different platforms and therefore there can be two different users with same name and email from different platforms (i.e. Github & bitbucket). 

    TODO Alternatively, we can say that email identifies an user.
 */
struct User {
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
struct Snapshot {
    // snapshot id and its hash
    id : u64,
    hash : String,
    // file path to the snapshot
    path : Option<String>, 
    // metadata
    metadata : HashMap<String, String>,
}

struct Path {
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
struct Commit {
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
struct Project {
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

pub fn foobar() -> u64 {
    return 78;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
