use crate::*;

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

