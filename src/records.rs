use std::io;
use std::io::{Read, Write};
use std::collections::{HashMap};

use byteorder::*;

use crate::serialization::*;
use crate::table_writer::*;

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

/** Project information. 
 
    Different project sources are possible, such as plain git, or Github and this enum lists them all. The implementation then specializes the basic interface to the supported kinds. 

    TODO This should allow relatively easy extension of the infrastructure by supporting other project sources, such as software heritage, etc. But deduplication using these stores might be a bit more involved. For now therefore only git & github are supported and deduplicated based on the clone url. 
 */
#[derive(Eq, PartialEq, Hash)]
pub enum Project {
    /** Tombstone project means that a project of this id existed in the datastore previously, but has been moved, to other datastore. 
     */
    Tombstone{},
    /** Indicates a project that has been deleted from upstream. Such project still remains in the datastore. 
     */
    Deleted{url : String },
    /** A generic git project. Upstream is a git clone url. 
     */
    Git{url : String },
    /** A Github project, upstream is a github hosted repository. 
     */
    GitHub{ user : String, repo : String },
}


/** Project heads. 
 
    When updated, multiple branches of the project may exist and each successful update stores the most recent project heads. A head has a unique name within the project and maps to commit id that corresponds to the state of the head at its current state. 

    Compared to V3 we no longer store the hash as well. It is not necessary and is an issue the updater should actually deal with rather than storing more in the database. Furthermore it would tie us to git as a provider as other sources may not use commit hashes to begin with so there would be nothing to fit in. 

    TODO ^- maybe revisit the above? 
 */
pub type Heads = HashMap<String, CommitId>;


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
pub type FileContents = Vec<u8>;


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

impl Project {
    const TOMBSTONE : u8 = 0;
    const DELETED : u8 = 1;
    const GIT : u8 = 2;
    const GITHUB : u8 = 3;

    /** Attempts to construct a project from given string. 
     
        The string is assumed to be an url. Only https is supported. 

        TODO is the https only a problem?
     */
    pub fn from_string(url : & str) -> Option<Project> {
        if url.starts_with("https://github.com/") {
            if url.ends_with(".git") {
                return Self::from_github_user_and_repo(& url[19..(url.len() - 4)]);
            } else {
                return Self::from_github_user_and_repo(& url[19..]);
            };
        } else if url.starts_with("https://api.github.com/repos/") {
            return Self::from_github_user_and_repo(& url[29..]);
        } else if url.ends_with(".git") && url.starts_with("https://") {
            return Some(Project::Git{ url : url[8..(url.len() - 4)].to_owned() });
        } 
        return None;
    }

    /** Returns the clone url of the project.
     
        Note that this function panics is called on tombstone project as these are not expected to be ever returned to user.
     */
    pub fn clone_url(& self) -> String {
        match self {
            Self::Deleted{url} => format!("https://{}.git", url),
            Self::Git{url} => format!("https://{}.git", url),
            Self::GitHub{user, repo} => format!("https://github.com/{}/{}.git", user, repo),
            _ => panic!("Tombstoned projects do not have clone urls!")
        }
    }

    /** Just a helper that creates a GitHub project from given user and repo part of url.
     */
    fn from_github_user_and_repo(u_and_r : & str) -> Option<Project> {
        // an ugly hack to move the strings from the vector when reconstructing
        let mut user_and_repo : Vec<Option<String>> = u_and_r.split("/").map(|x| Some(x.to_owned())).collect();
        if user_and_repo.len() == 2 {
            return Some(Project::GitHub{user : user_and_repo[0].take().unwrap(), repo : user_and_repo[1].take().unwrap()});
        } else {
            return None;
        }
    }
}

impl Serializable for Project {
    type Item = Project;

    fn read_from(f : & mut dyn Read, offset: & mut u64) -> io::Result<Project> {
        let kind = f.read_u8()?;
        *offset += 1;
        match kind {
            Self::TOMBSTONE => {
                return Ok(Project::Tombstone{});

            },
            Self::DELETED => {
                let url = String::read_from(f, offset)?;
                return Ok(Project::Deleted{url});
            },
            Self::GIT => {
                let url = String::read_from(f, offset)?;
                return Ok(Project::Git{url});
            },
            Self::GITHUB => {
                let user = String::read_from(f, offset)?;
                let repo = String::read_from(f, offset)?;
                return Ok(Project::GitHub{user, repo});
            },
            _ => { unreachable!() }
        }
    }

    fn write_to(f : & mut dyn Write, item : & Project, offset : & mut u64) -> io::Result<()> {
        match item {
            Self::Tombstone{} => {
                f.write_u8(Self::TOMBSTONE)?;
                *offset += 1;
            },
            Self::Deleted{url} => {
                f.write_u8(Self::DELETED)?;
                *offset += 1;
                String::write_to(f, url, offset)?;
            },
            Self::Git{url} => {
                f.write_u8(Self::GIT)?;
                *offset += 1;
                String::write_to(f, url, offset)?;
            },
            Self::GitHub{user, repo} => {
                f.write_u8(Self::GITHUB)?;
                *offset += 1;
                String::write_to(f, user, offset)?;
                String::write_to(f, repo, offset)?;
            }
        }
        return Ok(());
    }
}

impl Serializable for Commit {
    type Item = Commit;

    fn read_from(_f : & mut dyn Read, _offset : & mut u64) -> io::Result<Self::Item> {
        unimplemented!();
    }

    fn write_to(_f : & mut dyn Write, _item : & Self::Item, _offset : & mut u64) -> io::Result<()> {
        unimplemented!();
    }

}

impl Serializable for SHA {
    type Item = SHA;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        let mut buffer = vec![0; 20];
        f.read(& mut buffer)?;
        *offset += 20;
        match git2::Oid::from_bytes(& buffer) {
            Ok(hash) => Ok(hash),
            Err(_) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Cannot convert to SHA hash")))
        }
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        f.write(item.as_bytes())?;
        *offset += 20;
        return Ok(());
    }

}


