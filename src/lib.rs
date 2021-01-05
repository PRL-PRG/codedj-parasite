use std::hash::Hash;

mod helpers;

#[allow(dead_code)]
mod db;
#[allow(dead_code)]
mod records;
#[allow(dead_code)]
mod datastore;
#[allow(dead_code)]
mod updater;
#[allow(dead_code)]
mod task_add_projects;
mod task_update_repo;
mod task_update_substore;
mod task_load_substore;
mod task_drop_substore;
mod task_verify_substore;
mod github;

pub type Savepoint = db::Savepoint;
pub type StoreKind = records::StoreKind;
pub type SHA = records::Hash;
pub type CommitId = records::CommitId;
pub type HashId = records::HashId;
pub type PathId = records::PathId;
pub type UserId = records::UserId;
pub type Metadata = records::Metadata;
pub type CommitInfo = records::CommitInfo;
pub type FileContents = records::FileContents;
pub type ContentsKind = records::ContentsKind;



/** Datastore view is similar to datastore, but allows only read access. 
 
    Furthermore when accessing, savepoints can be selected. 
 
 
 */
pub struct DatastoreView {
    ds : datastore::Datastore, 
}

impl DatastoreView {

    /** Creates new datastore view from given path. 
     */
    pub fn new(root : & str) -> DatastoreView {
        return DatastoreView{
            ds : datastore::Datastore::new(root, false),
        };
    }

    /** Returns the threshold for projects to belong to the small projects substore.
     
        This number now stands at 10 commits, i.e. any project that has less than 10 commits will belong to the small projects substore regardless of its language. 

        NOTE while this is an arbitrary number, changing it is possible, but is not easy and there is no code written for this that would reshuffle the datastore accordingly. 
     */
    pub fn small_project_commits_threshold(& self) -> usize {
        return datastore::Datastore::SMALL_PROJECT_THRESHOLD;
    }

    /** Returns the threshold for a file to go in the small files category.
    
        This number now stands at 100 bytes, i.e. any file below 100 bytes will go in the small files category.

        NOTE while this is an arbitrary number, changing it is fairly comples as the whole file contents storage would have to be reshuffled. 
     */
    pub fn small_file_threshold(& self) -> usize {
        return datastore::Datastore::SMALL_FILE_THRESHOLD;
    }

    /* Savepoints 
    
       The savepoint API allows to gain view access to the underlying linked store that contains all savepoints for the dataset as well as helper functions to locate savepoint by its name and a nearest savepoint to any given time. 
     */

    pub fn savepoints(& self) -> SavepointsView {
        let guard = self.ds.savepoints.lock().unwrap();
        return SavepointsView{ guard };
    }

    pub fn latest(& self) -> Savepoint {
        return self.ds.create_savepoint("latest".to_owned(), false);
    }

    pub fn get_nearest_savepoint(& self, timestamp : i64) -> Option<Savepoint> {
        let mut guard = self.ds.savepoints.lock().unwrap();
        let mut result = None;
        for (_, sp) in guard.iter() {
            if sp.time() > timestamp {
                break;
            }
            result = Some(sp);
        }
        return result;
    }

    pub fn get_savepoint(& self, name : & str) -> Option<Savepoint> {
        let mut guard = self.ds.savepoints.lock().unwrap();
        return guard.iter()
            .find(|(_, sp)| sp.name() == name)
            .map(|(_, sp)| sp);
    }

    /* Projects

       
     */


    /* Substores
     */

    //pub fn substores(& self) -> Iter {
    //
    //}

     pub fn get_substore(& self, substore : StoreKind) -> SubstoreView {
         return SubstoreView{
             ds : self, 
             ss : self.ds.substore(substore)
         };
     }

}

/** A view into a substore. 
 
    The datastore has several substores which store the actual information about the projects that currently belong to it, such as commits, file contents, users and paths. By design there is one substore per language (see TODO SOMEWHERE for how the language is calculated), while specific stores, such as those for small projects (below 10 commits).
 */
pub struct SubstoreView<'a> {
    ds : &'a DatastoreView,
    ss : &'a datastore::Substore,
}

impl<'a> SubstoreView<'a> {

    pub fn kind(& self) -> StoreKind {
        return self.ss.prefix;
    }

    /* Commits

       Iterators to known commits (SHA & CommitId) and to commit information and metadata is provided. Note that same id can be returned multiple times, which is not a mistake, but means that a value in the database was overriden at a later point. 
     */

    pub fn commits(& self) -> MappingView<SHA, CommitId> {
        let guard = self.ss.commits.lock().unwrap();
        return MappingView{ guard };
    }

    pub fn commits_info(& self) -> StoreView<CommitInfo, CommitId> {
        let guard = self.ss.commits_info.lock().unwrap();
        return StoreView{ guard };
    }

    pub fn commits_metadata(& self) -> LinkedStoreView<Metadata, CommitId> {
        let guard = self.ss.commits_metadata.lock().unwrap();
        return LinkedStoreView{ guard };
    }

    /* Hashes & file contents
     */
    pub fn hashes(& self) -> MappingView<SHA, HashId> {
        let guard = self.ss.hashes.lock().unwrap();
        return MappingView{ guard };
    }

    pub fn contents(& self) -> SplitStoreView<FileContents, ContentsKind, HashId> {
        let guard = self.ss.contents.lock().unwrap();
        return SplitStoreView{ guard };
    }

    pub fn contents_metadata(& self) -> LinkedStoreView<Metadata, HashId> {
        let guard = self.ss.contents_metadata.lock().unwrap();
        return LinkedStoreView{ guard };
    }

    /* Paths
     */
    pub fn paths(& self) -> MappingView<SHA, PathId> {
        let guard = self.ss.paths.lock().unwrap();
        return MappingView{ guard };
    }

    pub fn paths_stings(& self) -> StoreView<String, PathId> {
        let guard = self.ss.path_strings.lock().unwrap();
        return StoreView{ guard };
    }

    /* Users
     */
    pub fn users(& self) -> IndirectMappingView<String, UserId> {
        let guard = self.ss.users.lock().unwrap();
        return IndirectMappingView{ guard };
    }

    pub fn users_metadata(& self) -> LinkedStoreView<Metadata, UserId> {
        let guard = self.ss.users_metadata.lock().unwrap();
        return LinkedStoreView{ guard };
    }

}




pub struct StoreView<'a, T : db::Serializable<Item = T>, ID : db::Id = u64> {
    guard : std::sync::MutexGuard<'a, db::Store<T,ID>>,
}

impl<'a, T : db::Serializable<Item = T>, ID : db::Id > StoreView<'a, T, ID> {
    pub fn iter(& mut self, sp : & Savepoint) -> db::StoreIterAll<T,ID> {
        return self.guard.savepoint_iter_all(sp);
    }
}

pub struct LinkedStoreView<'a, T : db::Serializable<Item = T>, ID : db::Id> {
    guard : std::sync::MutexGuard<'a, db::LinkedStore<T,ID>>,
}

impl<'a, T : db::Serializable<Item = T>, ID : db::Id > LinkedStoreView<'a, T, ID> {
    pub fn iter(& mut self, sp : & Savepoint) -> db::LinkedStoreIterAll<T,ID> {
        return self.guard.savepoint_iter_all(sp);
    }
}

pub struct MappingView<'a, T : db::FixedSizeSerializable<Item = T> + Eq + Hash + Clone, ID : db::Id> {
    guard : std::sync::MutexGuard<'a, db::Mapping<T,ID>>,
}

impl<'a, T : db::FixedSizeSerializable<Item = T> + Eq + Hash + Clone, ID : db::Id> MappingView<'a, T, ID> {
    pub fn iter(& mut self, sp : & Savepoint) -> db::MappingIter<T, ID> {
        return self.guard.savepoint_iter(sp);
    }
}

pub struct IndirectMappingView<'a, T : db::Serializable<Item = T> + Eq + Hash + Clone, ID : db::Id> {
    guard : std::sync::MutexGuard<'a, db::IndirectMapping<T,ID>>,
}

impl<'a, T : db::Serializable<Item = T> + Eq + Hash + Clone, ID : db::Id> IndirectMappingView<'a, T, ID> {
    pub fn iter(& mut self, sp : & Savepoint) -> db::StoreIterAll<T, ID> {
        return self.guard.savepoint_iter(sp);
    }
}

pub struct SplitStoreView<'a, T : db::Serializable<Item = T>, KIND : db::SplitKind<Item = KIND>, ID : db::Id> {
    guard : std::sync::MutexGuard<'a, db::SplitStore<T,KIND, ID>>,
}

impl<'a, T : db::Serializable<Item = T>, KIND : db::SplitKind<Item = KIND>, ID : db::Id> SplitStoreView<'a, T, KIND, ID> {
    // TODO add the iterators here!!!!!
}

pub struct SavepointsView<'a> {
    guard : std::sync::MutexGuard<'a, db::LinkedStore<Savepoint,u64>>,
}

impl<'a> SavepointsView<'a> {
    pub fn iter(& mut self) -> db::LinkedStoreIterAll<Savepoint,u64> {
        return self.guard.iter_all();
    }
}





// v3 from here




/*


// v2 from here:

use std::sync::*;
use std::io::*;
use std::str;
use byteorder::*;
use db::*;
use datastore::*;
use records::*;

type Commit = records::CommitInfo;

/** View into the datastore at a particular time. 
 
    - projects 
    - project updates
    - project heads
    - project metadata
    
    - commit hashes
    - commit info
    - commits metadata

    - users
    - users_metadata

    - paths
    - paths metadata

    - hashes (hash to id)
    - contents (hash id to contents id)
    - contents data
    - contents metadata

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

    pub fn commits(& self) -> PropertyStoreIterator<Commit> {
        let mut g = self.ds.commits_info.lock().unwrap();
        g.f.seek(SeekFrom::Start(0)).unwrap();
        return PropertyStoreIterator{g, limit : self.sp.limit_for("commits_info")};
    }

    /*
    pub fn contents(& self) -> PropertyStoreIterator<ContentsData> {
        let mut g = self.ds.contents_data.lock().unwrap();
        g.f.seek(SeekFrom::Start(0)).unwrap();
        return PropertyStoreIterator{g, limit : self.sp.limit_for("contents_data")};
    }
    */

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

pub struct PropertyStoreIterator<'a, T : FileWriter<T>> {
    g : MutexGuard<'a, PropertyStore<T>>,
    limit : u64,
}

impl<'a, T : FileWriter<T>> Iterator for PropertyStoreIterator<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<Self::Item> {
        let offset = self.g.f.seek(SeekFrom::Current(0)).unwrap();
        if offset >= self.limit {
            return None;
        }
        if let Ok(id) = self.g.f.read_u64::<LittleEndian>() {
            let value = T::read(& mut self.g.f);
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

*/