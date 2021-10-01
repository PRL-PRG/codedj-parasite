use std::io;
use std::io::{Read, Write};
use std::collections::{HashMap};

use byteorder::*;

use crate::serialization::*;
use crate::table_writers::*;

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash, Debug)]
pub struct ProjectId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash, Debug)]
pub struct CommitId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash, Debug)]
pub struct ContentsId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash, Debug)]
pub struct PathId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash, Debug)]
pub struct NameId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash, Debug)]
pub struct TreeId { id : u64 }

#[derive(Copy, Clone, Eq, PartialEq, std::hash::Hash, Debug)]
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


/** Log of project update record inside a datastore. 
 */
pub enum ProjectLog {
    /** An error occured while trying to update the project. This is usually followed by the Deleted message, but does not always have to be (some errors might be internal)
     */
    Error{time : i64, msg : String },
    /** A project has been deleted upstream. 
     */
    Deleted{time : i64},
    /** The project has moved into the datastore. We keep a link to the previous datastore the project belonged to in case we ever need it. 
     */
    New{time : i64, old_datastore : String },
    /** The project has been tombstoned, i.e. moved to different datastore. We keep which datastore in case it is ever needed.
     */
    Tombstone{ time : i64, new_datastore : String },
    /** The project has been renamed. We keep the old *and* the new urls here as quick references. 
     */
    Rename{time : i64, old : Project, new : Project },
    /** The project has been checked for updates, but changes have been found since last check. 
     */
    NoChange{time : i64},
    /** The project has been updated successfully. 
     */
    Ok{time : i64},
}

/** Commit information. 
 
    Commits are identified by their hash and therefore immutable. All information can thus be stored in the same table. 
 */
pub struct Commit {
    pub hash : SHA,
    pub committer : UserId,
    pub committer_time : i64,
    pub author : UserId,
    pub author_time : i64,
    pub message : String,
    pub tree : TreeId,
    pub parents : Vec<CommitId>,
    pub changes : Vec<Change>,
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

/** Information about a user. 
 */
pub struct User {
    pub email : String,
}

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

impl ProjectLog {
    const ERROR : u8 = 0;
    const DELETED : u8 = 1;
    const NEW : u8 = 2;
    const TOMBSTONE : u8 = 3;
    const RENAME : u8 = 4;
    const NO_CHANGE : u8 = 5;
    const OK : u8 = 6;
}

impl Serializable for ProjectLog {
    type Item = ProjectLog;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<ProjectLog> {
        let kind = f.read_u8()?; 
        *offset += 1;
        match kind {
            Self::ERROR => {
                let time = i64::read_from(f, offset)?;
                let msg = String::read_from(f, offset)?;
                return Ok(Self::Error{time, msg});
            },
            Self::DELETED => {
                let time = i64::read_from(f, offset)?;
                return Ok(Self::Deleted{time});
            },
            Self::NEW => {
                let time = i64::read_from(f, offset)?;
                let old_datastore = String::read_from(f, offset)?;
                return Ok(Self::New{time, old_datastore});

            },
            Self::TOMBSTONE => {
                let time = i64::read_from(f, offset)?;
                let new_datastore = String::read_from(f, offset)?;
                return Ok(Self::Tombstone{time, new_datastore});
            },
            Self::RENAME => {
                let time = i64::read_from(f, offset)?;
                let old = Project::read_from(f, offset)?;
                let new = Project::read_from(f, offset)?;
                return Ok(Self::Rename{time, old, new});

            },
            Self::NO_CHANGE => {
                let time = i64::read_from(f, offset)?;
                return Ok(Self::NoChange{time});
            },
            Self::OK => {
                let time = i64::read_from(f, offset)?;
                return Ok(Self::Ok{time});
            },
            _ => unreachable!()
        }
    }

    fn write_to(f : & mut dyn Write, item : & ProjectLog, offset : & mut u64) -> io::Result<()> {
        match item {
            Self::Error{time, msg } => {
                f.write_u8(Self::ERROR)?;
                *offset += 1;
                i64::write_to(f, time, offset)?;
                String::write_to(f, msg, offset)?;
            },
            Self::Deleted{time} => {
                f.write_u8(Self::DELETED)?;
                *offset += 1;
                i64::write_to(f, time, offset)?;
            }
            Self::New{time, old_datastore} => {
                f.write_u8(Self::NEW)?;
                *offset += 1;
                i64::write_to(f, time, offset)?;
                String::write_to(f, old_datastore, offset)?;
            },
            Self::Tombstone{time, new_datastore} => {
                f.write_u8(Self::TOMBSTONE)?;
                *offset += 1;
                i64::write_to(f, time, offset)?;
                String::write_to(f, new_datastore, offset)?;
            },
            Self::Rename{time, old, new} => {
                f.write_u8(Self::RENAME)?;
                *offset += 1;
                i64::write_to(f, time, offset)?;
                Project::write_to(f, old, offset)?;
                Project::write_to(f, new, offset)?;
            },
            Self::NoChange{time} => {
                f.write_u8(Self::NO_CHANGE)?;
                *offset += 1;
                i64::write_to(f, time, offset)?;
            },
            Self::Ok{time} => {
                f.write_u8(Self::OK)?;
                *offset += 1;
                i64::write_to(f, time, offset)?;
            }
        }
        return Ok(());
    }
}

impl Serializable for Commit {
    type Item = Commit;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        let hash = SHA::read_from(f, offset)?;
        let committer = UserId::read_from(f, offset)?;
        let committer_time = i64::read_from(f, offset)?;
        let author = UserId::read_from(f, offset)?;
        let author_time = i64::read_from(f, offset)?;
        let message = String::read_from(f, offset)?;
        let tree = TreeId::read_from(f, offset)?;
        let parents = Vec::<CommitId>::read_from(f, offset)?;
        let changes = Vec::<Change>::read_from(f, offset)?;
        return Ok(Commit{
            hash, 
            committer, 
            committer_time, 
            author,
            author_time,
            message,
            tree,
            parents,
            changes,
        });
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        SHA::write_to(f, & item.hash, offset)?;
        UserId::write_to(f, & item.committer, offset)?;
        i64::write_to(f, & item.committer_time, offset)?;
        UserId::write_to(f, & item.author, offset)?;
        i64::write_to(f, & item.author_time, offset)?;
        String::write_to(f, & item.message, offset)?;
        TreeId::write_to(f, & item.tree, offset)?;
        Vec::<CommitId>::write_to(f, & item.parents, offset)?;
        Vec::<Change>::write_to(f, & item.changes, offset)?;
        return Ok(());
    }
}

impl Change {
    const DELETED : ContentsId = ContentsId{id : u64::MAX};
}

impl Serializable for Change {
    type Item = Change;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        let path = PathId::read_from(f, offset)?;
        let contents = ContentsId::read_from(f, offset)?;
        if contents == Self::DELETED {
            return Ok(Change{path, contents : None});
        } else {
            return Ok(Change{path, contents : Some(contents)});
        }
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        PathId::write_to(f, & item.path, offset)?;
        match item.contents {
            Some(id) => ContentsId::write_to(f, & id, offset),
            None => ContentsId::write_to(f, & Change::DELETED, offset),
        }
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

impl Serializable for User {
    type Item = User;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        let email = String::read_from(f, offset)?;
        return Ok(User{email});
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        return String::write_to(f, & item.email, offset);
    }

}


