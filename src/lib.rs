mod datastore;
mod records;
mod db;
mod helpers;


// v2 from here:

use std::sync::*;
use std::io::*;
use std::str;
use byteorder::*;
use db::*;
use datastore::*;
use records::*;

type Commit = records::CommitInfo;

pub struct DatastoreView {
    ds : Datastore,
    sp : Savepoint,

}

impl DatastoreView {
    pub fn new(root : & str, time : i64) -> DatastoreView {
        let ds = Datastore::from(root);
        let sp = ds.get_savepoint(time);
        return DatastoreView{ds, sp};
    }

    pub fn commit_hashes(& self) -> HashMappingIterator {
        let mut g = self.ds.commits.lock().unwrap();
        g.writer.f.seek(SeekFrom::Start(0)).unwrap();
        return HashMappingIterator{
            g,
            limit : self.sp.limit_for("commits"),
            id : 0,
            buffer : vec![0; 20]            
        };
    }

    pub fn hashes(& self) -> HashMappingIterator {
        let mut g = self.ds.hashes.lock().unwrap();
        g.writer.f.seek(SeekFrom::Start(0)).unwrap();
        return HashMappingIterator{
            g,
            limit : self.sp.limit_for("hashes"),
            id : 0,
            buffer : vec![0; 20]            
        };
    }

    pub fn users(& self) -> StringMappingIterator {
        let mut g = self.ds.users.lock().unwrap();
        g.writer.f.seek(SeekFrom::Start(0)).unwrap();
        return StringMappingIterator{
            g,
            limit : self.sp.limit_for("users"),
            id : 0,
        };
    }

    pub fn paths(& self) -> StringMappingIterator {
        let mut g = self.ds.paths.lock().unwrap();
        g.writer.f.seek(SeekFrom::Start(0)).unwrap();
        return StringMappingIterator{
            g,
            limit : self.sp.limit_for("paths"),
            id : 0,
        };
    }

    pub fn commits(& self) -> CommitsIterator {
        let mut g = self.ds.commits_info.lock().unwrap();
        g.f.seek(SeekFrom::Start(0)).unwrap();
        return CommitsIterator{g, limit : self.sp.limit_for("commits_info")};
    }
}

/** Iterator into hashed mappings. 
 */
pub struct HashMappingIterator<'a> {
    g : MutexGuard<'a, DirectMapping<git2::Oid>>,
    limit : u64, 
    id : u64,
    buffer : Vec<u8>,
}


impl<'a> Iterator for HashMappingIterator<'a> {
    type Item = (u64, git2::Oid);

    fn next(& mut self) -> Option<Self::Item> {
        let offset = self.g.writer.f.seek(SeekFrom::Current(0)).unwrap();
        if offset >= self.limit {
            return None;
        }
        // TODO this is a bit ugly, would be nice if this was part of the API? 
        if let Ok(20) = self.g.writer.f.read(& mut self.buffer) {
            let id = self.id;
            self.id += 1;
            return Some((id, git2::Oid::from_bytes(& self.buffer).unwrap()));
        } else {
            return None;
        }
    }
}

/** Iterator into string mappings.
 */

pub struct StringMappingIterator<'a> {
    g : MutexGuard<'a, Mapping<String>>,
    limit : u64, 
    id : u64,
}

impl<'a> Iterator for StringMappingIterator<'a> {
    type Item = (u64, String);

    fn next(& mut self) -> Option<Self::Item> {
        let offset = self.g.writer.f.seek(SeekFrom::Current(0)).unwrap();
        if offset >= self.limit {
            return None;
        }
        // TODO this is a bit ugly, would be nice if this was part of the API? 
        if let Ok(len) = self.g.writer.f.read_u32::<LittleEndian>() {
            let mut buf = vec![0; len as usize];
            if self.g.writer.f.read(& mut buf).unwrap() as u32 != len {
                panic!("Corrupted binary format");
            }
            let id = self.id;
            self.id += 1;
            return Some((id, String::from_utf8(buf).unwrap()));
        } else {
            return None;
        }
    }
}



/** Iterator into commits information. 
 */
pub struct CommitsIterator<'a> {
    g : MutexGuard<'a, PropertyStore<CommitInfo>>,
    limit : u64,
}

impl<'a> Iterator for CommitsIterator<'a> {
    type Item = (u64, Commit);

    fn next(& mut self) -> Option<Self::Item> {
        let offset = self.g.f.seek(SeekFrom::Current(0)).unwrap();
        if offset >= self.limit {
            return None;
        }
        if let Ok(id) = self.g.f.read_u64::<LittleEndian>() {
            let value = Commit::read(& mut self.g.f);
            return Some((id, value));
        } else {
            return None;
        }
    }
}
