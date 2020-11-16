use std::fs::*;
use std::io::*;
use std::collections::*;
use byteorder::*;
use flate2::*;
use git2;
use num_derive::*;

use crate::db::*;
use crate::datastore::*;

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
            "csharp" | "cs" => Some(StoreKind::CSharp),
            "clojure" => Some(StoreKind::Clojure),
            "coffeescript" => Some(StoreKind::CoffeeScript),
            "erlang" => Some(StoreKind::Erlang),
            "go" => Some(StoreKind::Go),
            "haskell" => Some(StoreKind::Haskell),
            "html" => Some(StoreKind::Html),
            "java" => Some(StoreKind::Java),
            "javascript" | "js" => Some(StoreKind::JavaScript),
            "objectivec" | "objc" => Some(StoreKind::ObjectiveC),
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
    fn serialize(f : & mut File, value : & StoreKind) {
        f.write_u16::<LittleEndian>(value.to_number() as u16).unwrap();
    }

    fn deserialize(f : & mut File) -> StoreKind {
        return StoreKind::from_number(f.read_u16::<LittleEndian>().unwrap() as u64);
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
pub enum Project{
    Git{url : String},
    GitHub{user_and_repo : String},
}

impl Project {

    pub fn clone_url(& self) -> String {
        match self {
            Project::Git{url} => {
                return format!("https://{}.git", url);
            },
            Project::GitHub{user_and_repo} => {
                return format!("https://github.com/{}.git", user_and_repo);                
            }
        }
    }

    pub fn name(& self) -> String {
        match self {
            Project::Git{url} => {
                return url.clone();
            },
            Project::GitHub{user_and_repo} => {
                return user_and_repo.clone();                
            }
        }
    }

    pub fn from_url(url : & str) -> Option<Project> {
        if url.starts_with("https://github.com/") {
            if url.ends_with(".git") {
                return Some(Project::GitHub{ user_and_repo : url[19..(url.len() - 4)].to_owned() });
            } else {
                return Some(Project::GitHub{ user_and_repo : url[19..].to_owned() });
            }
        } else if url.starts_with("https://api.github.com/repos/") {
            return Some(Project::GitHub{ user_and_repo : url[29..].to_owned() });
        } else if url.ends_with(".git") && url.starts_with("https://") {
            return Some(Project::Git{ url : url[8..(url.len() - 4)].to_owned() });
        } else {
            return None;
        }
    }
}

impl Serializable for Project {
    fn serialize(f : & mut File, value : & Project) {
        match value {
            Project::Git{url} => {
                u8::serialize(f, & 0);
                String::serialize(f, url);
            }
            Project::GitHub{user_and_repo } => {
                u8::serialize(f, & 1);
                String::serialize(f, user_and_repo);
            }
        }
    }

    fn deserialize(f : & mut File) -> Project {
        match u8::deserialize(f) {
            0 => {
                let url = String::deserialize(f);
                return Project::Git{ url };
            },
            1 => {
                let user_and_repo = String::deserialize(f);
                return Project::GitHub{ user_and_repo };
            },
            _ => panic!("Unknown project kind"),
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
pub enum ProjectUpdateStatus {
    NoChange{time : i64, version : u16}, // 0
    Ok{time : i64, version : u16},  // 1
    /** Project url changes. Although project kind change is not expected when issuing project renames, it is technically possible. 
     */
    Rename{time : i64, version : u16, old_offset : u64}, // 2
    ChangeStore{time : i64, version : u16, new_kind : StoreKind }, // 3
    Error{time : i64, version : u16, error : String }, // 255
}

impl ProjectUpdateStatus {
    pub fn version(& self) -> u16 {
        match self {
            ProjectUpdateStatus::NoChange{time : _, version } => return *version,
            ProjectUpdateStatus::Ok{time : _, version} => return *version,
            ProjectUpdateStatus::Rename{time : _, version, old_offset: _} => return *version,
            ProjectUpdateStatus::ChangeStore{time : _, version, new_kind : _ } => return *version,
            ProjectUpdateStatus::Error{time : _, version, error: _ } => return *version,
        }
    }

    pub fn time(& self) -> i64 {
        match self {
            ProjectUpdateStatus::NoChange{time, version: _ } => return *time,
            ProjectUpdateStatus::Ok{time, version : _} => return *time,
            ProjectUpdateStatus::Rename{time, version : _, old_offset: _} => return *time,
            ProjectUpdateStatus::ChangeStore{time, version : _, new_kind : _ } => return *time,
            ProjectUpdateStatus::Error{time, version : _, error: _ } => return *time,
        }

    }
}

impl Serializable for ProjectUpdateStatus {
    fn serialize(f : & mut File, value : & ProjectUpdateStatus) {
        match value {
            ProjectUpdateStatus::NoChange{time , version } => {
                u8::serialize(f, & 0);
                i64::serialize(f, time);
                u16::serialize(f, version);
            },
            ProjectUpdateStatus::Ok{time , version} =>  {
                u8::serialize(f, & 1);
                i64::serialize(f, time);
                u16::serialize(f, version);
            },
            ProjectUpdateStatus::Rename{time , version, old_offset} =>  {
                u8::serialize(f, & 2);
                i64::serialize(f, time);
                u16::serialize(f, version);
                u64::serialize(f, old_offset);
            },
            ProjectUpdateStatus::ChangeStore{time , version, new_kind } =>  {
                u8::serialize(f, & 3);
                i64::serialize(f, time);
                u16::serialize(f, version);
                StoreKind::serialize(f, new_kind);
            },
            ProjectUpdateStatus::Error{time , version, error } =>  {
                u8::serialize(f, & 255);
                i64::serialize(f, time);
                u16::serialize(f, version);
                String::serialize(f, error);
            },
        }
    }

    fn deserialize(f : & mut File) -> ProjectUpdateStatus {
        let kind = u8::deserialize(f);
        let time = i64::deserialize(f);
        let version = u16::deserialize(f);
        match kind {
            0 => {
                return ProjectUpdateStatus::NoChange{time, version};
            },
            1 => {
                return ProjectUpdateStatus::Ok{time, version};
            },
            2 => {
                return ProjectUpdateStatus::Rename{time, version, old_offset : u64::deserialize(f)};
            },
            3 => {
                return ProjectUpdateStatus::ChangeStore{time, version, new_kind : StoreKind::deserialize(f)};
            },
            255 => {
                return ProjectUpdateStatus::Error{time, version, error : String::deserialize(f)};
            },
            _ => panic!("Unknown project update status kind"),
        }
    }
}

/** Head references at any given repository update.
 
    The references are hashmap from branch names to the ids of the latest commits as of the time of cloning the project (fetching its heads to be precise). 
    
    For practical reasons, the heads keep both the id of the latest commit's hash as well as the hash itself. This is important so that the updater can compare the string hashes against the possibly new commits in new heads without having to consult the substore, while everyone else can use the commit ids directly.
 */
pub type ProjectHeads = HashMap<String, (u64, Hash)>;

impl Serializable for ProjectHeads {
    fn serialize(f : & mut File, value : & ProjectHeads) {
        u32::serialize(f, & (value.len() as u32));
        for (name, (id, hash)) in value {
            String::serialize(f, name);
            u64::serialize(f, id);
            Hash::serialize(f, hash);
        }
    }

    fn deserialize(f : & mut File) -> ProjectHeads {
        let mut records = u32::deserialize(f);
        let mut result = ProjectHeads::new();
        while records > 0 {
            let name = String::deserialize(f);
            let id = u64::deserialize(f);
            let hash = Hash::deserialize(f);
            result.insert(name, (id, hash));
            records -= 1;
        }
        return result;
    }
}

/*
impl IDPrefix for StoreKind {
    fn prefix(id : u64) -> StoreKind {
        return StoreKind::SmallProjects;
    }

    fn sequential_part(id : u64) -> u64 {
        return id;
    }

    fn augment(& self, sequential_part : u64) -> u64 {
        return sequential_part;
    }
} */

/** Hash type for SHA-1 hashes used throughout the downloader. 
 
    Based on the Oid from git2, since we are already using that crate anyways. 
 */

pub type Hash = git2::Oid;

impl Serializable for Hash {
    fn serialize(f : & mut File, value : & Hash) {
        f.write(value.as_bytes()).unwrap();
    }

    fn deserialize(f : & mut File) -> Hash {
        let mut buffer = vec![0; 20];
        f.read(& mut buffer).unwrap();
        return git2::Oid::from_bytes(& buffer).unwrap();
    }
}

impl FixedSizeSerializable for Hash {
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
    fn serialize(f : & mut File, value : & ContentsKind) {
        f.write_u16::<LittleEndian>(value.to_number() as u16).unwrap();
    }

    fn deserialize(f : & mut File) -> ContentsKind {
        return ContentsKind::from_number(f.read_u16::<LittleEndian>().unwrap() as u64);
    }
}

impl FixedSizeSerializable for ContentsKind {
    const SIZE : u64 = 2;
}

/** The contents of a file. 
 
    File contents are automatically compressed and decompressed during the serialization. 
 */

pub type FileContents = Vec<u8>;

impl Serializable for FileContents {
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
}

pub struct CommitInfo {
    pub committer : u64,
    pub committer_time : i64,
    pub author : u64,
    pub author_time : i64,
    pub parents : Vec<u64>,
    pub changes : HashMap<u64,u64>,
    pub message : String,
}

impl CommitInfo {
    pub fn new() -> CommitInfo {
        return CommitInfo{
            committer : 0,
            committer_time : 0,
            author : 0,
            author_time : 0,
            parents : Vec::new(),
            changes : HashMap::new(),
            message : String::new(),
        };
    }
}

impl Serializable for CommitInfo {
    fn serialize(f : & mut File, value : & CommitInfo) {
        u64::serialize(f, & value.committer);
        i64::serialize(f, & value.committer_time);
        u64::serialize(f, & value.author);
        i64::serialize(f, & value.author_time);
        u16::serialize(f, & (value.parents.len() as u16));
        for parent in value.parents.iter() {
            u64::serialize(f, parent);
        }
        u32::serialize(f, & (value.changes.len() as u32));
        for (path, hash) in value.changes.iter() {
            u64::serialize(f, path);
            u64::serialize(f, hash);
        }
        String::serialize(f, & value.message);
    }

    fn deserialize(f : & mut File) -> CommitInfo {
        let mut result = CommitInfo::new();
        result.committer = u64::deserialize(f);
        result.committer_time = i64::deserialize(f);
        result.author = u64::deserialize(f);
        result.author_time = i64::deserialize(f);
        let mut num_parents = u16::deserialize(f);
        while num_parents > 0 {
            result.parents.push(u64::deserialize(f));
            num_parents -= 1;
        }
        let mut num_changes = u32::deserialize(f);
        while num_changes > 0 {
            let path = u64::deserialize(f);
            let hash = u64::deserialize(f);
            result.changes.insert(path, hash);
            num_changes -= 1;
        }
        result.message = String::deserialize(f);
        return result;
    }
}



