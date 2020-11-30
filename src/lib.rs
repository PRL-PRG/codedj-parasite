#[allow(dead_code)]
mod datastore;
mod records;
#[allow(dead_code)]
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

pub type Commit = records::CommitInfo;

/** View into the datastore at a particular time. 
 
    The datastore view is created with 
 */
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

    // patched  
    pub fn project_urls(& self) -> PropertyStoreIterator<String> {
        let g = self.ds.project_urls.lock().unwrap();
        return PropertyStoreIterator{g, limit : self.sp.limit_for("project_urls"), id : 0};
    }

    // patched
    pub fn project_heads(& self) -> PropertyStoreIterator<Heads> {
        let g = self.ds.project_heads.lock().unwrap();
        return PropertyStoreIterator{g, limit : self.sp.limit_for("project_heads"), id : 0};
    }

    pub fn projects_metadata(& self) -> LinkedPropertyStoreIterator<Metadata> {
        let mut g = self.ds.projects_metadata.lock().unwrap();
        g.f.seek(SeekFrom::Start(0)).unwrap();
        return LinkedPropertyStoreIterator{g, limit : self.sp.limit_for("projects_metadata")};
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

    // patched
    pub fn commits(& self) -> PropertyStoreIterator<Commit> {
        let g = self.ds.commits_info.lock().unwrap();
        return PropertyStoreIterator{g, limit : self.sp.limit_for("commits_info"), id : 0};
    }

    /** Mapping from changes id in commits to contents data id in contents data.
     */
    pub fn contents(& self) -> IdMappingIterator {
        let mut g = self.ds.contents.lock().unwrap();
        g.writer.f.seek(SeekFrom::Start(0)).unwrap();
        return IdMappingIterator{
            g,
            limit : self.sp.limit_for("contents"),
            id : 0,
        };
    }

    // patched
    pub fn contents_data(& self) -> PropertyStoreIterator<ContentsData> {
        let g = self.ds.contents_data.lock().unwrap();
        return PropertyStoreIterator{g, limit : self.sp.limit_for("contents_data"), id : 0};
    }

    /** returns snapshot of given id if one exists. 
     */
    pub fn content_data(& self, id : u64) -> Option<ContentsData> {
        let mut g = self.ds.contents_data.lock().unwrap();
        return g.get(id);        
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

pub struct IdMappingIterator<'a> {
    g : MutexGuard<'a, DirectMapping<u64>>,
    limit : u64, 
    id : u64
}


impl<'a> Iterator for IdMappingIterator<'a> {
    type Item = (u64, u64);

    fn next(& mut self) -> Option<Self::Item> {
        let offset = self.g.writer.f.seek(SeekFrom::Current(0)).unwrap();
        if offset >= self.limit {
            return None;
        }
        if let Ok(value) = self.g.writer.f.read_u64::<LittleEndian>() {
            let id = self.id;
            self.id += 1;
            return Some((id, value));
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
        // first id, then offset
        self.g.writer.f.read_u64::<LittleEndian>().unwrap();
        // TODO this is a bit ugly, would be nice if this was part of the API? 
        if let Ok(len) = self.g.writer.f.read_u32::<LittleEndian>() {
            let mut buf = vec![0; len as usize];
            if self.g.writer.f.read(& mut buf).unwrap() as u32 != len {
                panic!("Corrupted binary format");
            }
            let id = self.id;
            self.id += 1;
            match String::from_utf8(buf) {
                Ok(str) => {
                    return Some((id, str));
                },
                _ => {
                    println!("Non-UTF8 string detected, id: {}, offset: {}, invalid utf8, length: {}", id, offset, len);
                    return Some((id, String::new()));
                }
            }
        } else {
            return None;
        }
    }
}

pub struct PropertyStoreIterator<'a, T : FileWriter<T>> {
    g : MutexGuard<'a, PropertyStore<T>>,
    limit : u64,
    id : u64
}

impl<'a, T : FileWriter<T>> Iterator for PropertyStoreIterator<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<Self::Item> {
        while self.id < self.g.indexer.size {
            if let Some(offset) = self.g.indexer.get(self.id) {
                if offset >= self.limit {
                    return None;
                }
                self.g.f.seek(SeekFrom::Start(offset)).unwrap();
                let check_id = self.g.f.read_u64::<LittleEndian>().unwrap();
                if self.id == check_id {
                    let id = self.id;
                    self.id += 1;
                    return Some((id, T::read(& mut self.g.f)));
                }
            }
            self.id += 1;
        }
        return None;
    }
}

pub struct LinkedPropertyStoreIterator<'a, T : FileWriter<T>> {
    g : MutexGuard<'a, LinkedPropertyStore<T>>,
    limit : u64,
}

impl<'a, T : FileWriter<T>> Iterator for LinkedPropertyStoreIterator<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<Self::Item> {
        let offset = self.g.f.seek(SeekFrom::Current(0)).unwrap();
        if offset >= self.limit {
            return None;
        }
        if let Ok(id) = self.g.f.read_u64::<LittleEndian>() {
            let value = T::read(& mut self.g.f);
            // read and skip the previous record offset
            self.g.f.read_u64::<LittleEndian>().unwrap();
            return Some((id, value));
        } else {
            return None;
        }
    }
}

/*
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
*/