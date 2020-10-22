use std::fs::{File, OpenOptions};
use std::io::*;
use std::hash::*;
use std::collections::*;
use byteorder::*;
use flate2::*;
use git2;
use num::*;
use num_derive::*;

use crate::db3::*;

/** Datastore kinds. 
 
    Up to 1024 datastore kinds are supported. This limitation exists because the datastore kind id is part of the unique identifiers
 */
#[repr(u16)]
#[derive(Clone, Copy, Debug, std::cmp::PartialEq, std::cmp::Eq, std::hash::Hash, FromPrimitive)]
pub enum StoreKind {
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

    Sentinel // sentinel to denote number of store kinds
}

impl SplitKind for StoreKind {
    const COUNT : u64 = StoreKind::Sentinel as u64;

    const EMPTY : StoreKind = StoreKind::Sentinel;

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
pub struct Project {
    kind : ProjectKind, 
    id : String,
    store_kind : StoreKind,
}

#[repr(u8)]
#[derive(Clone, Debug, std::cmp::PartialEq, std::cmp::Eq, std::hash::Hash)]
pub enum ProjectKind {
    Git,
    Github,
}

impl Project {
    pub const GIT : u8 = 0;
    pub const GITHUB : u8 = 1;

    pub fn url(& self) -> String {
        match self.kind {
            ProjectKind::Git => return self.id.clone(),
            ProjectKind::Github => return format!("https://github.com/{}.git", self.id.clone()),
        }
    }
}

impl Serializable for Project {
    fn serialize(f : & mut File, value : & Project) {
        //f.write(value.as_bytes()).unwrap();
    }

    fn deserialize(f : & mut File) -> Project {
        unimplemented!();
        //let mut buffer = vec![0; 20];
        //f.read(& mut buffer).unwrap();
        //return git2::Oid::from_bytes(& buffer).unwrap();
    }
}

pub enum ProjectUpdateStatus {
    NoChange{time : i64, version : u16}, // 0
    Ok{time : i64, version : u16},  // 1
    Tombstone{time : i64, version : u16, new_kind : StoreKind }, // 254
    Error{time : i64, version : u16, error : String }, // 255
}

impl Serializable for ProjectUpdateStatus {
    fn serialize(f : & mut File, value : & ProjectUpdateStatus) {
        unimplemented!();
    }

    fn deserialize(f : & mut File) -> ProjectUpdateStatus {
        unimplemented!();
    }

}


/** Head references at any given repository update.
 
    The references are hashmap from branch names to the ids of the latest commits as of the time of cloning the project (fetching its heads to be precise). 
 */
pub type ProjectHeads = HashMap<String, u64>;

impl Serializable for ProjectHeads {
    fn serialize(f : & mut File, value : & ProjectHeads) {
        unimplemented!();
    }

    fn deserialize(f : & mut File) -> ProjectHeads {
        unimplemented!();
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
    ObjectiveC,
    Perl,
    Php,
    Python,
    Ruby,
    Scala,
    Shell,
    TypeScript,

    Sentinel // sentinel to denote number of content kinds
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




