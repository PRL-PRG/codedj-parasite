use std::collections::{HashMap};

use crate::tables::*;

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash)]
pub struct ProjectId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash)]
pub struct CommitId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash)]
pub struct ContentsId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash)]
pub struct PathId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash)]
pub struct NameId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash)]
pub struct TreeId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash)]
pub struct UserId { id : u64 }

/** Throughout parasite, SHA hashes are used to identify entities.
 
    Nicely, this is also what git does internally;-D
 */
pub type SHA = git2::Oid;

/** List of supported substores.
 
    New substores can be added 
 */
pub enum SubstoreKind {

}

/** List of supported file contents. 
 
    TODO do we really want this?
 */
/*pub enum ContentsKind {


}
*/


/** Project information. 
 
    Different project sources are possible, such as plain git, or Github and this enum lists them all. The implementation then specializes the basic interface to the supported kinds. 

    TODO This should allow relatively easy extension of the infrastructure by supporting other project sources, such as software heritage, etc. But deduplication using these stores might be a bit more involved. For now therefore only git & github are supported and deduplicated based on the clone url. 
 */
pub enum Project {

}

/** Project heads. 
 
    When updated, multiple branches of the project may exist and each successful update stores the most recent project heads. A head has a unique name within the project and maps to commit id that corresponds to the state of the head at its current state. 

    Compared to V3 we no longer store the hash as well. It is not necessary and is an issue the updater should actually deal with rather than storing more in the database. Furthermore it would tie us to git as a provider as other sources may not use commit hashes to begin with so there would be nothing to fit in. 

    TODO ^- maybe revisit the above? 
 */
pub type ProjectHeads = HashMap<String, CommitId>;


/** Commit information. 
 
    Commits are identified by their hash and therefore immutable. All information can thus be stored in the same table. 
 */
pub struct Commit {

}

/** A single change made by a commit.
  
    A change consists of path id and an optional contents id. If the contents id is none, it means the file was deleted by the commit. 

    Internally, deletion is represented as the largest commit id (2^64-1) as this is the least likely to be used... 
 */
pub struct Change {
    pub path : PathId,
    pub contents : Option<ContentsId> 
}

/** A tree.  
 
    Like commits, trees are immutable. A tree is a representation of a folder's contents and is made up by a map from filenames to contents id for files and a map from names to tree ids for folders (each folder itself is a tree). 
*/
pub struct Tree {
    pub files : HashMap<NameId, ContentsId>,
    pub folders : HashMap<NameId, TreeId>,
}

/** The contents of a file. 
 
    Stored as a byte vector.
 */
pub type Contents = Vec<u8>;



// Implementation ----------------------------------------------------------------------------------


impl Id for ProjectId {
    fn to_number(&self) -> u64 { self.id }
    fn from_number(id : u64) -> Self { ProjectId{id} }
}

impl Id for CommitId {
    fn to_number(&self) -> u64 { self.id }
    fn from_number(id : u64) -> Self { CommitId{id} }
}

impl Id for ContentsId {
    fn to_number(&self) -> u64 { self.id }
    fn from_number(id : u64) -> Self { ContentsId{id} }
}

impl Id for PathId {
    fn to_number(&self) -> u64 { self.id }
    fn from_number(id : u64) -> Self { PathId{id} }
}

impl Id for NameId {
    fn to_number(&self) -> u64 { self.id }
    fn from_number(id : u64) -> Self { NameId{id} }
}

impl Id for TreeId {
    fn to_number(&self) -> u64 { self.id }
    fn from_number(id : u64) -> Self { TreeId{id} }
}

impl Id for UserId {
    fn to_number(&self) -> u64 { self.id }
    fn from_number(id : u64) -> Self { UserId{id} }
}

