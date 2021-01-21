use std::fs::{File};
use std::io::{Read, Write};
use std::collections::*;
use byteorder::*;
use flate2::*;
use git2;
use num_derive::*;

use crate::db::*;
use crate::datastore::*;
use crate::helpers;
use std::fmt::Display;

#[derive(std::fmt::Debug, std::cmp::PartialEq, std::cmp::Eq, std::hash::Hash, std::marker::Copy, std::clone::Clone)]
pub struct ProjectId {
    id : u64,
}

impl std::convert::From<u64> for ProjectId {
    fn from(id : u64) -> ProjectId {
        return ProjectId{id};
    }
}

impl std::convert::From<ProjectId> for u64 {
    fn from(value : ProjectId) -> u64 {
        return value.id;
    }
}

impl Id for ProjectId {}

impl std::fmt::Display for ProjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return write!(f, "{}", self.id);
    }
}

#[derive(std::fmt::Debug, std::cmp::PartialEq, std::cmp::Eq, std::hash::Hash, std::marker::Copy, std::clone::Clone)]
pub struct CommitId {
    id : u64,
}

impl CommitId {
    pub const INVALID : CommitId = CommitId{id : 0};
}

impl std::convert::From<u64> for CommitId {
    fn from(id : u64) -> CommitId {
        return CommitId{id};
    }
}

impl std::convert::From<CommitId> for u64 {
    fn from(value : CommitId) -> u64 {
        return value.id;
    }
}

impl Id for CommitId {}

impl std::fmt::Display for CommitId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return write!(f, "{}", self.id);
    }
}

#[derive(std::fmt::Debug, std::cmp::PartialEq, std::cmp::Eq, std::hash::Hash, std::marker::Copy, std::clone::Clone)]
pub struct HashId {
    id : u64,
}

impl HashId {
    pub const DELETED : HashId = HashId{id : 0};
}

impl std::convert::From<u64> for HashId {
    fn from(id : u64) -> HashId {
        return HashId{id};
    }
}

impl std::convert::From<HashId> for u64 {
    fn from(value : HashId) -> u64 {
        return value.id;
    }
}

impl Id for HashId {}

impl std::fmt::Display for HashId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return write!(f, "{}", self.id);
    }
}


#[derive(std::fmt::Debug, std::cmp::PartialEq, std::cmp::Eq, std::hash::Hash, std::marker::Copy, std::clone::Clone)]
pub struct PathId {
    id : u64,
}

impl PathId {
    pub const EMPTY : PathId = PathId{id : 0};
}

impl std::convert::From<u64> for PathId {
    fn from(id : u64) -> PathId {
        return PathId{id};
    }
}

impl std::convert::From<PathId> for u64 {
    fn from(value : PathId) -> u64 {
        return value.id;
    }
}

impl Id for PathId {}

impl std::fmt::Display for PathId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return write!(f, "{}", self.id);
    }
}

#[derive(std::fmt::Debug, std::cmp::PartialEq, std::cmp::Eq, std::hash::Hash, std::marker::Copy, std::clone::Clone)]
pub struct UserId {
    id : u64,
}

impl UserId {
    pub const INVALID : UserId = UserId{id : 0};
}

impl std::convert::From<u64> for UserId {
    fn from(id : u64) -> UserId {
        return UserId{id};
    }
}

impl std::convert::From<UserId> for u64 {
    fn from(value : UserId) -> u64 {
        return value.id;
    }
}

impl Id for UserId {}

impl std::fmt::Display for UserId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return write!(f, "{}", self.id);
    }
}


/** Datastore kinds. 
 
    Up to 1024 datastore kinds are supported. This limitation exists because the datastore kind id is part of the unique identifiers
 */
#[repr(u16)]
#[derive(Clone, Copy, Debug, std::cmp::PartialEq, std::cmp::Eq, std::hash::Hash, FromPrimitive)]
pub enum StoreKind {
    Generic,
    SmallProjects,
    C,
    Cpp,
    CSharp,
    Clojure,
    CoffeeScript,
    Erlang,
    Go,
    Haskell,
    Html,
    Java,
    JavaScript,
    ObjectiveC,
    Perl,
    Php,
    Python,
    Ruby,
    Scala,
    Shell,
    TypeScript,

    Unspecified // sentinel to denote number of store kinds
}

impl StoreKind {
    /** Returns true if the store kind is a valid store value. 
     */
    pub fn is_specified(& self) -> bool {
        match self {
            StoreKind::Unspecified => return false,
            _ => return true
        };
    }

    /** Gets the store kind based on the string given. 
     
        Supports both long and short names. Is case insensitive.
     */
    pub fn from_string(name : & str) -> Option<StoreKind> {
        match name.to_lowercase().as_str() {
            "small" | "smallprojects" => Some(StoreKind::SmallProjects),
            "c" => Some(StoreKind::C),
            "cpp" | "c++" => Some(StoreKind::Cpp),
            "csharp" | "cs" | "c#" => Some(StoreKind::CSharp),
            "clojure" => Some(StoreKind::Clojure),
            "coffeescript" => Some(StoreKind::CoffeeScript),
            "erlang" => Some(StoreKind::Erlang),
            "go" => Some(StoreKind::Go),
            "haskell" => Some(StoreKind::Haskell),
            "html" => Some(StoreKind::Html),
            "java" => Some(StoreKind::Java),
            "javascript" | "js" => Some(StoreKind::JavaScript),
            "objectivec" | "objc" | "objective-c" => Some(StoreKind::ObjectiveC),
            "perl" => Some(StoreKind::Perl),
            "php" => Some(StoreKind::Php), 
            "python" => Some(StoreKind::Python),
            "ruby" => Some(StoreKind::Ruby),
            "scala" => Some(StoreKind::Scala),
            "shell" => Some(StoreKind::Shell),
            "typescript" | "ts" => Some(StoreKind::TypeScript),
            _ => None
        }
    }
}

impl Display for StoreKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StoreKind::Generic => write!(f, "Generic"),
            StoreKind::SmallProjects => write!(f, "Small"),
            StoreKind::C => write!(f, "C"),
            StoreKind::Cpp => write!(f, "C++"),
            StoreKind::CSharp => write!(f, "C#"),
            StoreKind::Clojure => write!(f, "Clojure"),
            StoreKind::CoffeeScript => write!(f, "CoffeeScript"),
            StoreKind::Erlang => write!(f, "Erlang"),
            StoreKind::Go => write!(f, "Go"),
            StoreKind::Haskell => write!(f, "Haskell"),
            StoreKind::Html => write!(f, "HTML"),
            StoreKind::Java => write!(f, "Java"),
            StoreKind::JavaScript => write!(f, "JavaScript"),
            StoreKind::ObjectiveC => write!(f, "ObjectiveC"),
            StoreKind::Perl => write!(f, "Perl"),
            StoreKind::Php => write!(f, "PHP"),
            StoreKind::Python => write!(f, "Python"),
            StoreKind::Ruby => write!(f, "Ruby"),
            StoreKind::Scala => write!(f, "Scala"),
            StoreKind::Shell => write!(f, "Shell"),
            StoreKind::TypeScript => write!(f, "TypeScript"),
            StoreKind::Unspecified => write!(f, "Unspecified"),
        }
    }
}

impl SplitKind for StoreKind {
    const COUNT : u64 = StoreKind::Unspecified as u64;

    const EMPTY : StoreKind = StoreKind::Unspecified;

    fn to_number(& self) -> u64 {
        return *self as u64;
    }

    fn from_number(value : u64) -> StoreKind {
        return num::FromPrimitive::from_u64(value).unwrap();
    }

}

impl Serializable for StoreKind {
    type Item = StoreKind;
    fn serialize(f : & mut File, value : & StoreKind) {
        f.write_u16::<LittleEndian>(value.to_number() as u16).unwrap();
    }

    fn deserialize(f : & mut File) -> StoreKind {
        return StoreKind::from_number(f.read_u16::<LittleEndian>().unwrap() as u64);
    }

    fn verify(f : & mut File) -> Result<StoreKind, std::io::Error> {
        let index = u16::verify(f)? as u64;
        if index >= Self::COUNT {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid store kind index"));
        } else {
            return Ok(StoreKind::from_number(index));
        }
    }
}

impl FixedSizeSerializable for StoreKind {
    const SIZE : u64 = 2;
}

/** Project description. 
 
    Each project has its type and unique string identifier. This is to save memory by not storing any common prefixes or suffixes of the clone urls that projects of the same kind would inevitable have. The following project kinds are supported:

    ProjectKind::Git : the id is the full git url to clone the project. Only https is supported. 
    
    ProjectKind::Github : the id is the username and repo name.
 */
#[derive(Clone,Debug, std::cmp::PartialEq, std::cmp::Eq, std::hash::Hash)]
pub enum ProjectUrl{
    Git{url : String},
    GitHub{user_and_repo : String},
}

impl ProjectUrl {

    pub fn clone_url(& self) -> String {
        match self {
            ProjectUrl::Git{url} => {
                return format!("https://{}.git", url);
            },
            ProjectUrl::GitHub{user_and_repo} => {
                return format!("https://github.com/{}.git", user_and_repo);                
            }
        }
    }

    pub fn name(& self) -> String {
        match self {
            ProjectUrl::Git{url} => {
                return url.clone();
            },
            ProjectUrl::GitHub{user_and_repo} => {
                return user_and_repo.clone();                
            }
        }
    }

    pub fn from_url(url : & str) -> Option<ProjectUrl> {
        if url.starts_with("https://github.com/") {
            if url.ends_with(".git") {
                return Some(ProjectUrl::GitHub{ user_and_repo : url[19..(url.len() - 4)].to_owned() });
            } else {
                return Some(ProjectUrl::GitHub{ user_and_repo : url[19..].to_owned() });
            }
        } else if url.starts_with("https://api.github.com/repos/") {
            return Some(ProjectUrl::GitHub{ user_and_repo : url[29..].to_owned() });
        } else if url.ends_with(".git") && url.starts_with("https://") {
            return Some(ProjectUrl::Git{ url : url[8..(url.len() - 4)].to_owned() });
        } else {
            return None;
        }
    }

    /** Determines whether the given project url matches the provided one. 
     
        
     */
    pub fn matches_url(& self, mut url : & str) -> bool {
        match self {
            ProjectUrl::Git{url : git_url} => {
                if url.ends_with(".git") {
                    url = & url[0..url.len()-4];
                }
                if url.starts_with("https://") {
                    url = & url[8..url.len()];
                } else if url.starts_with("http://") {
                    url = & url[7..url.len()];
                }
                return git_url == url;
            },
            ProjectUrl::GitHub{user_and_repo} => {
                if url.ends_with(".git") {
                    url = & url[0..url.len()-4];
                }
                if url.starts_with("https://github.com/") {
                    url = & url[19..url.len()];
                } else if url.starts_with("http://github.com/") {
                    url = & url[18..url.len()];
                } else if url.starts_with("https://api.github.com/repos/") {
                    url = & url[29..url.len()];
                }
                return user_and_repo == url;
            }
        }
    }

    /* A helper function that given the project and a commit hash returns the commit hash formatted as a terminal link, if the project supports it. 

       Currently only github projects will return a link. Terminals that do not support the link feature will still show the hash properly. 
    */
    pub fn get_commit_terminal_link(& self, commit_hash : SHA) -> String {
        match self {
            ProjectUrl::Git{url : _ } => 
                return format!("{}", commit_hash),
            ProjectUrl::GitHub{user_and_repo} => 
                return format!("\x1b]8;;https://github.com/{}/commit/{}\x07{}\x1b]8;;\x07", user_and_repo, commit_hash, commit_hash),
        }
    }

    /* A helper function that given the project, commit hash, path and contents hash returns the path formatted as a terminal link, if the project supports it. 

       Currently only github projects will return a link. Terminals that do not support the link feature will still show the hash properly. 
    */
    pub fn get_change_terminal_link(& self, commit_hash : SHA, path : & str, contents_hash : SHA) -> String {
        if contents_hash == SHA::zero() {
            return path.to_owned();
        }
        match self {
            ProjectUrl::Git{url : _ } => 
                return path.to_owned(),
            ProjectUrl::GitHub{user_and_repo} => 
                return format!("\x1b]8;;https://github.com/{}/blob/{}/{}\x07{}\x1b]8;;\x07", user_and_repo, commit_hash, path, path),
        }
    }
}

impl Serializable for ProjectUrl {
    type Item = ProjectUrl;
    fn serialize(f : & mut File, value : & ProjectUrl) {
        match value {
            ProjectUrl::Git{url} => {
                u8::serialize(f, & 0);
                String::serialize(f, url);
            }
            ProjectUrl::GitHub{user_and_repo } => {
                u8::serialize(f, & 1);
                String::serialize(f, user_and_repo);
            }
        }
    }

    fn deserialize(f : & mut File) -> ProjectUrl {
        match u8::deserialize(f) {
            0 => {
                let url = String::deserialize(f);
                return ProjectUrl::Git{ url };
            },
            1 => {
                let user_and_repo = String::deserialize(f);
                return ProjectUrl::GitHub{ user_and_repo };
            },
            _ => panic!("Unknown project kind"),
        }
    }

    fn verify(f : & mut File) -> Result<ProjectUrl, std::io::Error> {
        match u8::verify(f)? {
            0 => {
                let url = String::verify(f)?;
                return Ok(ProjectUrl::Git{ url });
            },
            1 => {
                let user_and_repo = String::verify(f)?;
                return Ok(ProjectUrl::GitHub{ user_and_repo });
            },
            _ => return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid project kind id")),
        }
    }
}

/** Project update status. 
 
    Every time a repository is updated, an update status message is added to the projects update status so that the history of updates and repository lifetime can be reconstructed:

    # NoChange

    # Ok

    # Rename

    Issued when project url change is detected by the updater. Although project kind change is not expected during the rename, it may change as well. The `old_offset` argument is the old offset in the projects table that contains the old identification of the project.  

    # Tombstone

    # Error
 */
pub enum ProjectLog {
    NoChange{time : i64, version : u16}, // 0
    Ok{time : i64, version : u16},  // 1
    /** Project url changes. Although project kind change is not expected when issuing project renames, it is technically possible. 
     */
    Rename{time : i64, version : u16, old_offset : u64}, // 2
    ChangeStore{time : i64, version : u16, new_kind : StoreKind }, // 3
    Error{time : i64, version : u16, error : String }, // 255
}

impl ProjectLog {
    pub fn version(& self) -> u16 {
        match self {
            ProjectLog::NoChange{time : _, version } => return *version,
            ProjectLog::Ok{time : _, version} => return *version,
            ProjectLog::Rename{time : _, version, old_offset: _} => return *version,
            ProjectLog::ChangeStore{time : _, version, new_kind : _ } => return *version,
            ProjectLog::Error{time : _, version, error: _ } => return *version,
        }
    }

    pub fn time(& self) -> i64 {
        match self {
            ProjectLog::NoChange{time, version: _ } => return *time,
            ProjectLog::Ok{time, version : _} => return *time,
            ProjectLog::Rename{time, version : _, old_offset: _} => return *time,
            ProjectLog::ChangeStore{time, version : _, new_kind : _ } => return *time,
            ProjectLog::Error{time, version : _, error: _ } => return *time,
        }

    }
}

impl Serializable for ProjectLog {
    type Item = ProjectLog;
    fn serialize(f : & mut File, value : & ProjectLog) {
        match value {
            ProjectLog::NoChange{time , version } => {
                u8::serialize(f, & 0);
                i64::serialize(f, time);
                u16::serialize(f, version);
            },
            ProjectLog::Ok{time , version} =>  {
                u8::serialize(f, & 1);
                i64::serialize(f, time);
                u16::serialize(f, version);
            },
            ProjectLog::Rename{time , version, old_offset} =>  {
                u8::serialize(f, & 2);
                i64::serialize(f, time);
                u16::serialize(f, version);
                u64::serialize(f, old_offset);
            },
            ProjectLog::ChangeStore{time , version, new_kind } =>  {
                u8::serialize(f, & 3);
                i64::serialize(f, time);
                u16::serialize(f, version);
                StoreKind::serialize(f, new_kind);
            },
            ProjectLog::Error{time , version, error } =>  {
                u8::serialize(f, & 255);
                i64::serialize(f, time);
                u16::serialize(f, version);
                String::serialize(f, error);
            },
        }
    }

    fn deserialize(f : & mut File) -> ProjectLog {
        let kind = u8::deserialize(f);
        let time = i64::deserialize(f);
        let version = u16::deserialize(f);
        match kind {
            0 => {
                return ProjectLog::NoChange{time, version};
            },
            1 => {
                return ProjectLog::Ok{time, version};
            },
            2 => {
                return ProjectLog::Rename{time, version, old_offset : u64::deserialize(f)};
            },
            3 => {
                return ProjectLog::ChangeStore{time, version, new_kind : StoreKind::deserialize(f)};
            },
            255 => {
                return ProjectLog::Error{time, version, error : String::deserialize(f)};
            },
            _ => panic!("Unknown project update status kind"),
        }
    }

    fn verify(f : & mut File) -> Result<ProjectLog, std::io::Error> {
        let kind = u8::verify(f)?;
        match kind {
            0 | 1 | 2 | 3 | 255 => {
                let time = i64::verify(f)?;
                let version = u16::verify(f)?;
                match kind {
                    0 => {
                        return Ok(ProjectLog::NoChange{time, version});
                    },
                    1 => {
                        return Ok(ProjectLog::Ok{time, version});
                    },
                    2 => {
                        return Ok(ProjectLog::Rename{time, version, old_offset : u64::deserialize(f)});
                    },
                    3 => {
                        return Ok(ProjectLog::ChangeStore{time, version, new_kind : StoreKind::deserialize(f)});
                    },
                    255 => {
                        return Ok(ProjectLog::Error{time, version, error : String::deserialize(f)});
                    },
                    _ => unreachable!(),
                }
        
            },
            _ => return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid project update status id")),
        };
    }
}

impl std::fmt::Display for ProjectLog {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ProjectLog::NoChange{time , version } => {
                return write!(f, "{}: no change (v {})", helpers::pretty_timestamp(*time), version);
            },
            ProjectLog::Ok{time , version} =>  {
                return write!(f, "{}: ok (v {})", helpers::pretty_timestamp(*time), version);
            },
            ProjectLog::Rename{time , version, old_offset : _} =>  {
                return write!(f, "{}: project renamed (v {})", helpers::pretty_timestamp(*time), version);
            },
            ProjectLog::ChangeStore{time , version, new_kind } =>  {
                return write!(f, "{}: substore: {:?} (v {})", helpers::pretty_timestamp(*time), new_kind, version);
            },
            ProjectLog::Error{time , version, error } =>  {
                return write!(f, "{}: error: {} (v {})", helpers::pretty_timestamp(*time), error, version);
            },
        }
    }
}

/** Head references at any given repository update.
 
    The references are hashmap from branch names to the ids of the latest commits as of the time of cloning the project (fetching its heads to be precise). 
    
    For practical reasons, the heads keep both the id of the latest commit's hash as well as the hash itself. This is important so that the updater can compare the string hashes against the possibly new commits in new heads without having to consult the substore, while everyone else can use the commit ids directly.
 */
pub type ProjectHeads = HashMap<String, (CommitId, SHA)>;

impl Serializable for ProjectHeads {
    type Item = ProjectHeads;
    fn serialize(f : & mut File, value : & ProjectHeads) {
        u32::serialize(f, & (value.len() as u32));
        for (name, (id, hash)) in value {
            String::serialize(f, name);
            u64::serialize(f, & u64::from(*id));
            SHA::serialize(f, hash);
        }
    }

    fn deserialize(f : & mut File) -> ProjectHeads {
        let mut records = u32::deserialize(f);
        let mut result = ProjectHeads::new();
        while records > 0 {
            let name = String::deserialize(f);
            let id = CommitId::from(u64::deserialize(f));
            let hash = SHA::deserialize(f);
            result.insert(name, (id, hash));
            records -= 1;
        }
        return result;
    }

    fn verify(f : & mut File) -> Result<ProjectHeads, std::io::Error> {
        let mut records = u32::verify(f)?;
        if records as u64 > MAX_BUFFER_LENGTH {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid length of project heads"));
        }
        let mut result = ProjectHeads::new();
        while records > 0 {
            let name = String::verify(f)?;
            let id = CommitId::from(u64::verify(f)?);
            let hash = SHA::verify(f)?;
            result.insert(name, (id, hash));
            records -= 1;
        }
        return Ok(result);
    }
}

pub type SHA = git2::Oid;

impl Serializable for SHA {
    type Item = SHA;
    fn serialize(f : & mut File, value : & SHA) {
        f.write(value.as_bytes()).unwrap();
    }

    fn deserialize(f : & mut File) -> SHA {
        let mut buffer = vec![0; 20];
        f.read(& mut buffer).unwrap();
        return git2::Oid::from_bytes(& buffer).unwrap();
    }

    fn verify(f : & mut File) -> Result<SHA, std::io::Error> {
        let mut buffer = vec![0; 20];
        f.read(& mut buffer)?;
        match git2::Oid::from_bytes(& buffer) {
            Ok(oid) => return Ok(oid),
            Err(err) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", err))),
        }
    }
}

impl FixedSizeSerializable for SHA {
    const SIZE : u64 = 20;
}

/** Content kinds.
 
    Up to 1024 content kinds are supported. 
 */
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, std::hash::Hash, FromPrimitive)]
pub enum ContentsKind {
    Generic,
    SmallFiles,
    C,
    Cpp,
    CSharp,
    Clojure,
    CoffeeScript,
    Erlang,
    Go,
    Haskell,
    Html,
    Java,
    JavaScript,
    JSON,
    ObjectiveC,
    Perl,
    Php,
    Python,
    Readme,
    Ruby,
    Scala,
    Shell,
    TypeScript,

    Sentinel // sentinel to denote number of content kinds
}

impl ContentsKind {

    /** Determines a contents kind based on the path of the file.
     */
    pub fn from_path(path : & str) -> Option<ContentsKind> {
        let parts = path.split(".").collect::<Vec<& str>>();
        match parts[parts.len() - 1] {
            // generic files
            "README" => Some(ContentsKind::Readme),
            // C
            "c" | "h" => Some(ContentsKind::C),
            // C++ 
            "cpp" | "cc" | "cxx" | "hpp" | "C" => Some(ContentsKind::Cpp),
            // C#
            "cs" => Some(ContentsKind::CSharp),
            // Clojure
            "clj" | "cljs" | "cljc" | "edn" => Some(ContentsKind::Clojure),
            // CoffeeScript
            "coffee" | "litcoffee" => Some(ContentsKind::CoffeeScript),
            // Erlang
            "erl" | "hrl" => Some(ContentsKind::Erlang),
            // Go
            "go" => Some(ContentsKind::Go),
            // Haskell
            "hs" | "lhs" => Some(ContentsKind::Haskell),
            // HTML
            "html" | "htm" => Some(ContentsKind::Html),
            // Java
            "java" => Some(ContentsKind::Java),
            // JavaScript
            "js" | "mjs" => Some(ContentsKind::JavaScript),
            // Objective-C
            "m" | "mm" | "M" => Some(ContentsKind::ObjectiveC),
            // Perl
            "plx"| "pl" | "pm" | "xs" | "t" | "pod" => Some(ContentsKind::Perl),
            // PHP
            "php" | "phtml" | "php3" | "php4" | "php5" | "php7" | "phps" | "php-s" | "pht" | "phar" => Some(ContentsKind::Php),            
            // Python
            "py" | "pyi" | "pyc" | "pyd" | "pyo" | "pyw" | "pyz" => Some(ContentsKind::Python),
            // Ruby
            "rb" => Some(ContentsKind::Ruby),
            // Scala
            "scala" | "sc" => Some(ContentsKind::Scala),
            // Shell
            "sh" => Some(ContentsKind::Shell),
            // TypeScript
            "ts" | "tsx" => Some(ContentsKind::TypeScript),
            // JSON
            "json" => Some(ContentsKind::JSON),
            _ => None
        }
    }

    /** Determines the contents kind from the actual contents of the file. 
     
        For now, we only check if the file is really small, otherwise we keep the category as determined by its path.
     */
    pub fn from_contents(contents : & [u8], from_path : ContentsKind) -> Option<ContentsKind> {
        if contents.len() < Datastore::SMALL_FILE_THRESHOLD {
            return Some(ContentsKind::SmallFiles);
        } else {
            return Some(from_path);
        }
    }
}

impl SplitKind for ContentsKind {
    const COUNT : u64 = ContentsKind::Sentinel as u64;

    const EMPTY : ContentsKind = ContentsKind::Sentinel;

    fn to_number(& self) -> u64 {
        return *self as u64;
    }

    fn from_number(value : u64) -> ContentsKind {
        return num::FromPrimitive::from_u64(value).unwrap();
    }
}

impl Serializable for ContentsKind {
    type Item = ContentsKind;
    fn serialize(f : & mut File, value : & ContentsKind) {
        f.write_u16::<LittleEndian>(value.to_number() as u16).unwrap();
    }

    fn deserialize(f : & mut File) -> ContentsKind {
        return ContentsKind::from_number(f.read_u16::<LittleEndian>().unwrap() as u64);
    }

    fn verify(f : & mut File) -> Result<ContentsKind, std::io::Error> {
        let index = u16::verify(f)? as u64;
        if index >= Self::COUNT {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid contents kind index"));
        } else {
            return Ok(ContentsKind::from_number(index));
        }
    }
}

impl FixedSizeSerializable for ContentsKind {
    const SIZE : u64 = 2;
}

pub type PathString = String;

impl ReadOnly for PathString {
}

/** The contents of a file. 
 
    File contents are automatically compressed and decompressed during the serialization. 
 */

pub type FileContents = Vec<u8>;

impl ReadOnly for FileContents {
}

impl Serializable for FileContents {
    type Item = FileContents;
    fn serialize(f : & mut File, value : & FileContents) {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), Compression::best());
        enc.write_all(value).unwrap();
        let encoded = enc.finish().unwrap();
        f.write_u64::<LittleEndian>(encoded.len() as u64).unwrap();
        f.write(& encoded).unwrap();
    }

    fn deserialize(f : & mut File) -> FileContents {
        let len = f.read_u64::<LittleEndian>().unwrap() as usize;
        let mut encoded = vec![0; len];
        f.read(& mut encoded).unwrap();
        let mut dec = flate2::read::GzDecoder::new(&encoded[..]);
        let mut result = Vec::new();
        dec.read_to_end(& mut result).unwrap();    
        return result;
    }

    fn verify(f : & mut File) -> Result<FileContents, std::io::Error> {
        let len = u64::verify(f)?;
        if len > MAX_BUFFER_LENGTH {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Compressed file contents too large"));
        }
        let mut encoded = vec![0; len as usize];
        f.read(& mut encoded)?;
        let mut dec = flate2::read::GzDecoder::new(&encoded[..]);
        let mut result = Vec::new();
        dec.read_to_end(& mut result)?;    
        return Ok(result);
    }
}

/** Metadata values. 
 
    Metadata are encoded as simple key/value store. 
 */
pub struct Metadata {
    pub key : String, 
    pub value : String
}

impl Metadata {
    pub const GITHUB_METADATA : &'static str = "github_metadata";
}

impl Serializable for Metadata {
    type Item = Metadata;
    fn serialize(f : & mut File, value : & Metadata) {
        String::serialize(f, & value.key);
        String::serialize(f, & value.value);
    }

    fn deserialize(f : & mut File) -> Metadata {
        return Metadata {
            key : String::deserialize(f),
            value : String::deserialize(f),
        };
    }

    fn verify(f : & mut File) -> Result<Metadata, std::io::Error> {
        return Ok(Metadata{
            key : String::verify(f)?,
            value : String::verify(f)?,
        });
    }
}

pub struct CommitInfo {
    pub committer : UserId,
    pub committer_time : i64,
    pub author : UserId,
    pub author_time : i64,
    pub parents : Vec<CommitId>,
    pub changes : HashMap<PathId,HashId>,
    pub message : String,
}

impl CommitInfo {
    pub fn new() -> CommitInfo {
        return CommitInfo{
            committer : UserId::INVALID,
            committer_time : 0,
            author : UserId::INVALID,
            author_time : 0,
            parents : Vec::new(),
            changes : HashMap::new(),
            message : String::new(),
        };
    }
}

impl ReadOnly for CommitInfo {
}

impl Serializable for CommitInfo {
    type Item = CommitInfo;
    fn serialize(f : & mut File, value : & CommitInfo) {
        u64::serialize(f, & u64::from(value.committer));
        i64::serialize(f, & value.committer_time);
        u64::serialize(f, & u64::from(value.author));
        i64::serialize(f, & value.author_time);
        u16::serialize(f, & (value.parents.len() as u16));
        for parent in value.parents.iter() {
            u64::serialize(f, & u64::from(*parent));
        }
        u32::serialize(f, & (value.changes.len() as u32));
        for (path, hash) in value.changes.iter() {
            u64::serialize(f, & u64::from(*path));
            u64::serialize(f, & u64::from(*hash));
        }
        String::serialize(f, & value.message);
    }

    fn deserialize(f : & mut File) -> CommitInfo {
        let mut result = CommitInfo::new();
        result.committer = UserId::from(u64::deserialize(f));
        result.committer_time = i64::deserialize(f);
        result.author = UserId::from(u64::deserialize(f));
        result.author_time = i64::deserialize(f);
        let mut num_parents = u16::deserialize(f);
        while num_parents > 0 {
            result.parents.push(CommitId::from(u64::deserialize(f)));
            num_parents -= 1;
        }
        let mut num_changes = u32::deserialize(f);
        while num_changes > 0 {
            let path = PathId::from(u64::deserialize(f));
            let hash = HashId::from(u64::deserialize(f));
            result.changes.insert(path, hash);
            num_changes -= 1;
        }
        result.message = String::deserialize(f);
        return result;
    }

    fn verify(f : & mut File) -> Result<CommitInfo, std::io::Error> {
        let mut result = CommitInfo::new();
        result.committer = UserId::from(u64::verify(f)?);
        result.committer_time = i64::verify(f)?;
        result.author = UserId::from(u64::verify(f)?);
        result.author_time = i64::verify(f)?;
        let mut num_parents = u16::verify(f)?;
        if num_parents as u64 > MAX_BUFFER_LENGTH {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Too many commit parents"));
        }
        while num_parents > 0 {
            result.parents.push(CommitId::from(u64::verify(f)?));
            num_parents -= 1;
        }
        let mut num_changes = u32::verify(f)?;
        if num_changes as u64 > MAX_BUFFER_LENGTH {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Too many commit changes"));
        }
        while num_changes > 0 {
            let path = PathId::from(u64::verify(f)?);
            let hash = HashId::from(u64::verify(f)?);
            result.changes.insert(path, hash);
            num_changes -= 1;
        }
        result.message = String::verify(f)?;
        return Ok(result);
    }
}



