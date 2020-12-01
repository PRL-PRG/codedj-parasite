use std::fs::*;
use std::io::prelude::*;
use std::io::*;
use std::collections::hash_map::*;
use byteorder::*;
use flate2::*;
//use std::fmt::Debug;

use crate::*;
use crate::db::*;

/** Project heads
 
    For each project we store the latest project heads so that these can be compared against projects already used. 
 */
pub type Heads = HashMap<String, u64>;

impl FileWriter<Heads> for Heads {
    fn read(f : & mut File) -> Heads {
        let mut result = Heads::new();
        let records = f.read_u32::<LittleEndian>().unwrap() as usize;
        while result.len() < records {
            let commit_id = f.read_u64::<LittleEndian>().unwrap();
            let name = String::read(f);
            result.insert(name, commit_id);
        }
        return result;
    }

    fn write(f : & mut File, value : & Heads) {
        f.write_u32::<LittleEndian>(value.len() as u32).unwrap();
        for (name, commit_id) in value {
            u64::write(f, commit_id);
            String::write(f, name);
        }
    }
}

/** Basic commit information. 
 */
#[derive(Debug)]
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

impl FileWriter<CommitInfo> for CommitInfo {
    fn read(f : & mut File) -> CommitInfo {
        let mut result = CommitInfo::new();
        result.committer = f.read_u64::<LittleEndian>().unwrap();
        result.committer_time = f.read_i64::<LittleEndian>().unwrap();
        result.author = f.read_u64::<LittleEndian>().unwrap();
        result.author_time = f.read_i64::<LittleEndian>().unwrap();
        let num_parents = f.read_u32::<LittleEndian>().unwrap() as usize;
        while num_parents > result.parents.len() {
            result.parents.push(f.read_u64::<LittleEndian>().unwrap());
        }
        let num_changes = f.read_u32::<LittleEndian>().unwrap() as usize;
        while num_changes > result.changes.len() {
            let path = f.read_u64::<LittleEndian>().unwrap();
            let hash = f.read_u64::<LittleEndian>().unwrap();
            result.changes.insert(path, hash);
        }
        result.message = String::read(f);
        return result;
    }

    fn write(f : & mut File, value : & CommitInfo) {
        f.write_u64::<LittleEndian>(value.committer).unwrap();
        f.write_i64::<LittleEndian>(value.committer_time).unwrap();
        f.write_u64::<LittleEndian>(value.author).unwrap();
        f.write_i64::<LittleEndian>(value.author_time).unwrap();
        f.write_u32::<LittleEndian>(value.parents.len() as u32).unwrap();
        for id in value.parents.iter() {
            f.write_u64::<LittleEndian>(*id).unwrap();
        }
        f.write_u32::<LittleEndian>(value.changes.len() as u32).unwrap();
        for (path, hash) in value.changes.iter() {
            f.write_u64::<LittleEndian>(*path).unwrap();
            f.write_u64::<LittleEndian>(*hash).unwrap();
        }
        String::write(f, & value.message);
    }
}

/** Data about contents of a file. 
 
    This is just a dumb array of bytes. Compressed and decompressed on write/read.
 */
pub type ContentsData = Vec<u8>;

impl FileWriter<ContentsData> for ContentsData {
    fn read(f : & mut File) -> ContentsData {
        let len = f.read_u64::<LittleEndian>().unwrap() as usize;
        if len > 10 * 1024 * 1024 * 1024 {
            println!("errorneous length: {}", len);
            return Vec::new();
        }
        let mut encoded = vec![0; len];
        f.read(& mut encoded).unwrap();
        let mut dec = flate2::read::GzDecoder::new(&encoded[..]);
        let mut result = Vec::new();
        match dec.read_to_end(& mut result) {
            Ok(_) => return result,
            Err(_) => return Vec::new(),
        }
    }

    fn write(f : & mut File, value : & ContentsData) {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), Compression::best());
        enc.write_all(value).unwrap();
        let encoded = enc.finish().unwrap();
        f.write_u64::<LittleEndian>(encoded.len() as u64).unwrap();
        f.write(& encoded).unwrap();
    }
}

/** Update Log Entry. 
 */
pub enum UpdateLog {
    NoChange{time : i64, version : u16}, // = 0
    Ok{time : i64, version : u16}, // = 1
    Error{time : i64, version : u16, error : String}, // = 255
}

#[allow(dead_code)]
impl UpdateLog {
    pub fn time(& self) -> i64 {
        match self {
            UpdateLog::NoChange{time, version : _} => return *time, 
            UpdateLog::Ok{time, version : _} => return *time, 
            UpdateLog::Error{time, version : _, error : _} => return *time, 
        }
    }

    pub fn version(& self) -> u16 {
        match self {
            UpdateLog::NoChange{time : _, version} => return *version, 
            UpdateLog::Ok{time : _, version} => return *version, 
            UpdateLog::Error{time : _, version, error : _} => return *version, 
        }
    }

    pub fn is_ok(& self) -> bool {
        match self {
            UpdateLog::Error{time : _, version : _, error : _} => return false,
            _ => return true
        }
    }
}

impl FileWriter<UpdateLog> for UpdateLog {
    fn read(f : & mut File) -> UpdateLog {
        let kind = f.read_u8().unwrap();
        let time = f.read_i64::<LittleEndian>().unwrap();
        let version = f.read_u16::<LittleEndian>().unwrap();
        match kind {
            0 => {
                return UpdateLog::NoChange{time, version};
            },
            1 => {
                return UpdateLog::Ok{time, version};
            },
            255 => {
                let error = String::read(f);
                return UpdateLog::Error{time, version, error};
            }
            _ => panic!("Invalid log kind")
        }
    }   

    fn write(f : & mut File, value : & UpdateLog) {
        match value {
            UpdateLog::NoChange{time, version} => {
                f.write_u8(0).unwrap();
                f.write_i64::<LittleEndian>(*time).unwrap();
                f.write_u16::<LittleEndian>(*version).unwrap();
            },
            UpdateLog::Ok{time, version} => {
                f.write_u8(1).unwrap();
                f.write_i64::<LittleEndian>(*time).unwrap();
                f.write_u16::<LittleEndian>(*version).unwrap();
            },
            UpdateLog::Error{time, version, error} => {
                f.write_u8(255).unwrap();
                f.write_i64::<LittleEndian>(*time).unwrap();
                f.write_u16::<LittleEndian>(*version).unwrap();
                String::write(f, error);
            }
        }
    }
}

/** Metadata Entry 
 */
pub struct Metadata {
    pub key : String, 
    pub value : String
}

pub trait MetadataReader {
    fn read_metadata(& mut self, id : u64) -> HashMap<String, String>;
    fn get_metadata(& mut self, id : u64, key : & str) -> Option<String>;
}

impl FileWriter<Metadata> for Metadata {
    fn read(f : & mut File) -> Metadata {
        let key = String::read(f);
        let value = String::read(f);
        return Metadata{key, value};
    }

    fn write(f : & mut File, value : & Metadata) {
        String::write(f, & value.key);
        String::write(f, & value.value);
    }
}

/** Savepoint is simply a hashmap from file name to its size. 
 */
pub struct Savepoint {
    time : i64, 
    file_sizes : HashMap<String, u64>
}

#[allow(dead_code)]
impl Savepoint {

    pub fn new() -> Savepoint {
        return Savepoint{
            time : helpers::now(), 
            file_sizes : HashMap::new(),
        }
    }

    pub fn time(& self) -> i64 {
        return self.time;
    }

    pub fn add_entry(& mut self, fname : & str, f : & mut File) {
        f.flush().unwrap();
        let old = f.seek(SeekFrom::Current(0)).unwrap();
        let size = f.seek(SeekFrom::End(0)).unwrap();
        f.seek(SeekFrom::Start(old)).unwrap();
        self.file_sizes.insert(fname.to_owned(), size);
    }

    pub fn limit_for(& self, fname : & str) -> u64 {
        if let Some(limit) = self.file_sizes.get(fname) {
            return *limit;
        } else {
            // if not found, it means the file did not exist when the savepoint was taken so stop immediately
            return 0;
        }
    }

}

impl FileWriter<Savepoint> for Savepoint {
    fn read(f : & mut File) -> Savepoint {
        let time = f.read_i64::<LittleEndian>().unwrap();
        let mut result = Savepoint{time, file_sizes : HashMap::new()};
        let entries = f.read_u16::<LittleEndian>().unwrap() as usize;
        while result.file_sizes.len() < entries {
            let fname = String::read(f);
            let size = f.read_u64::<LittleEndian>().unwrap();
            result.file_sizes.insert(fname, size);
        }
        return result;
    }

    fn write(f : & mut File, value : & Savepoint) {
        f.write_i64::<LittleEndian>(value.time).unwrap();
        f.write_u16::<LittleEndian>(value.file_sizes.len() as u16).unwrap();
        for (fname, size) in value.file_sizes.iter() {
            String::write(f, fname);
            f.write_u64::<LittleEndian>(*size).unwrap();
        }
    }
}
