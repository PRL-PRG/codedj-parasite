use std::hash::Hash;
use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Read, Write};

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

use crate::db::Indexable;

pub type Savepoint = db::Savepoint;
pub type StoreKind = records::StoreKind;
pub type SHA = records::Hash;
pub type ProjectId = records::ProjectId;
pub type ProjectUrl = records::Project;
pub type ProjectHeads = records::ProjectHeads;
pub type CommitId = records::CommitId;
pub type HashId = records::HashId;
pub type PathId = records::PathId;
pub type UserId = records::UserId;
pub type Metadata = records::Metadata;
pub type CommitInfo = records::CommitInfo;
pub type FileContents = records::FileContents;
pub type ContentsKind = records::ContentsKind;
pub type ProjectLog = records::ProjectUpdateStatus;

/** Datastore size broken up into actual database contents and the redundant indexing files. 
 */
pub struct DatastoreSize {
    pub contents : usize,
    pub indices : usize,
}

impl std::ops::Add<DatastoreSize> for DatastoreSize {
    type Output = DatastoreSize;

    fn add(self, rhs: DatastoreSize) -> DatastoreSize {
        return DatastoreSize{
            contents : self.contents + rhs.contents,
            indices : self.indices + rhs.indices,
        };
    }
}

impl std::fmt::Display for DatastoreSize {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "size,kind")?;
        writeln!(f, "{},contents", self.contents)?;
        writeln!(f, "{},indices", self.indices)?;
        return Ok(());
    }
}

trait DatastoreSizeGetter {
    fn datastore_size(& mut self) -> DatastoreSize;
}

impl<T : db::Indexable + db::Serializable<Item = T>, ID : db::Id> DatastoreSizeGetter for db::Indexer<T, ID> {
    fn datastore_size(& mut self) -> DatastoreSize {
        return DatastoreSize{
            contents : 0, 
            indices : self.len() * (T::SIZE as usize),
        };
    }
}

impl<T: db::Serializable<Item = T>, ID : db::Id> DatastoreSizeGetter for db::Store<T, ID> {
    fn datastore_size(& mut self) -> DatastoreSize {
        let contents = self.f.seek(SeekFrom::End(0)).unwrap() as usize;
        return self.indexer.datastore_size() + DatastoreSize{ contents, indices : 0 };
    }
}

impl<T: db::Serializable<Item = T>, ID : db::Id> DatastoreSizeGetter for db::LinkedStore<T, ID> {
    fn datastore_size(& mut self) -> DatastoreSize {
        let contents = self.f.seek(SeekFrom::End(0)).unwrap() as usize;
        return self.indexer.datastore_size() + DatastoreSize{ contents, indices : 0 };
    }
}

impl<T: db::FixedSizeSerializable<Item = T> + Eq + Hash + Clone, ID : db::Id> DatastoreSizeGetter for db::Mapping<T, ID> {
    fn datastore_size(& mut self) -> DatastoreSize {
        return DatastoreSize{ contents : self.len() * (T::SIZE as usize), indices : 0 };
    }
}

impl<T: db::Serializable<Item = T> + Eq + Hash + Clone, ID : db::Id> DatastoreSizeGetter for db::IndirectMapping<T, ID> {
    fn datastore_size(& mut self) -> DatastoreSize {
        return self.store.datastore_size();
    }
}

impl<T: db::Serializable<Item = T>, KIND : db::SplitKind<Item = KIND>, ID : db::Id> DatastoreSizeGetter for db::SplitStore<T, KIND, ID> {
    fn datastore_size(& mut self) -> DatastoreSize {
        let mut result = self.indexer.datastore_size();
        for f in self.files.iter_mut() {
            let contents = f.seek(SeekFrom::End(0)).unwrap() as usize;
            result = result + DatastoreSize{ contents, indices : 0 };
        }
        return result;
    }
}




/** Summary of the datastore in terms of stored elements. 
 */
pub struct Summary {
    pub projects : usize,
    pub commits : usize,
    pub paths : usize,
    pub users : usize,
    pub hashes : usize,
    pub contents : usize,
}

impl Summary {
    pub fn new() -> Summary {
        return Summary{ projects : 0, commits : 0, paths : 0, users : 0, hashes : 0, contents : 0 };
    }
}

impl std::ops::Add<Summary> for Summary {
    type Output = Summary;

    fn add(self, rhs: Summary) -> Summary {
        return Summary{
            projects : self.projects + rhs.projects,
            commits : self.commits + rhs.commits,
            paths : self.paths + rhs.paths,
            users : self.users + rhs.users,
            hashes : self.hashes + rhs.hashes,
            contents : self.contents + rhs.contents,
        };
    }
}

/** Simple formatter that writes the summary in a csv format.
 */
impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "size,kind")?;
        writeln!(f, "{},projects", self.projects)?;
        writeln!(f, "{},commits", self.commits)?;
        writeln!(f, "{},paths", self.paths)?;
        writeln!(f, "{},users", self.users)?;
        writeln!(f, "{},hashes", self.hashes)?;
        writeln!(f, "{},contents", self.contents)?;
        return Ok(());
    }
}



pub struct Project {

}

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

    /* Low-level project API

       Projects may change substores in their lifetime (and theoretically not just once), there may be errors in their updates and every update creates its own new set of project heads, combined with savepoints, which render any random access into their properties impossible. The low level API below is useful for getting the project specification parts in their entirety as they only provide views into the datastore records.

       Note however that this data has to be heavily preprocessed to become really usable. Consider using the higher level projects api described below in the function projects:

     */
    pub fn project_urls(& self) -> StoreView<ProjectUrl, ProjectId> {
        let guard = self.ds.projects.lock().unwrap();
        return StoreView{ guard };
    }

    pub fn project_substores(& self) -> StoreView<StoreKind, ProjectId> {
        let guard = self.ds.project_substores.lock().unwrap();
        return StoreView{ guard };
    }

    pub fn project_heads(& self) -> StoreView<ProjectHeads, ProjectId> {
        let guard = self.ds.project_heads.lock().unwrap();
        return StoreView{ guard };
    }

    pub fn project_metadata(& self) -> LinkedStoreView<Metadata, ProjectId> {
        let guard = self.ds.project_metadata.lock().unwrap();
        return LinkedStoreView{ guard };
    }

    pub fn project_log(& self) -> LinkedStoreView<ProjectLog, ProjectId> {
        let guard = self.ds.project_updates.lock().unwrap();
        return LinkedStoreView{ guard };
    }

    /** High-level project API
     
        This function loads all the parts that form a project and assembles them in memory to provide a useful view into the dataset. 
     */
    pub fn projects(& self, sp : & Savepoint) -> HashMap<ProjectId, Project> {
        return self.assemble_projects(sp, None);
    }

    /* Substores
     */

    pub fn substores(& self) -> SubstoreViewIterator {
        return SubstoreViewIterator{
            ds : self,
            i : self.ds.substores.iter(),
        };
    }

    pub fn get_substore(& self, substore : StoreKind) -> SubstoreView {
        return SubstoreView{
            ds : self, 
            ss : self.ds.substore(substore)
        };
    }



    fn assemble_projects(& self, sp : & Savepoint, substore : Option<StoreKind>) -> HashMap<ProjectId, Project> {
        unimplemented!();
    }


    /** A simple function that returns the summary of the dataset. 
     */
    pub fn summary(& self) -> Summary {
        println!("Calculating summary for entire datastore...");
        let mut result = Summary::new();
        result.projects = self.ds.projects.lock().unwrap().len();
        for ss in self.substores() {
            result = result + ss.summary();
        }
        return result;
    }

    pub fn projects_size(& self) -> DatastoreSize {
        let mut result = DatastoreSize{ contents : 0, indices : 0 };
        result = result + self.ds.projects.lock().unwrap().datastore_size();
        result = result + self.ds.project_substores.lock().unwrap().datastore_size();
        result = result + self.ds.project_updates.lock().unwrap().datastore_size();
        result = result + self.ds.project_heads.lock().unwrap().datastore_size();
        result = result + self.ds.project_metadata.lock().unwrap().datastore_size();
        return result;
    }

    pub fn savepoints_size(& self) -> DatastoreSize {
        return self.ds.savepoints.lock().unwrap().datastore_size();
    }

    pub fn commits_size(& self) -> DatastoreSize {
        let mut result = DatastoreSize{ contents : 0, indices : 0 };
        for ss in self.substores() {
            result = result + ss.commits_size();
        }
        return result;
    }

    pub fn contents_size(& self) -> DatastoreSize {
        let mut result = DatastoreSize{ contents : 0, indices : 0 };
        for ss in self.substores() {
            result = result + ss.contents_size();
        }
        return result;
    }

    pub fn paths_size(& self) -> DatastoreSize {
        let mut result = DatastoreSize{ contents : 0, indices : 0 };
        for ss in self.substores() {
            result = result + ss.paths_size();
        }
        return result;
    }

    pub fn users_size(& self) -> DatastoreSize {
        let mut result = DatastoreSize{ contents : 0, indices : 0 };
        for ss in self.substores() {
            result = result + ss.users_size();
        }
        return result;
    }

    pub fn datastore_size(& self) -> DatastoreSize {
        return self.projects_size() + self.savepoints_size() + self.commits_size() + self.contents_size() + self.paths_size() + self.users_size();
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

    /** Loads all projects belonging to the substore at given savepoint and returns their hashmap.
     
     */
    pub fn projects(& self, sp : & Savepoint) -> HashMap<ProjectId, Project> {
        return self.ds.assemble_projects(sp, Some(self.ss.prefix));
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

    pub fn summary(& self) -> Summary {
        println!("calculating summary for substore {:?}", self.kind());
        let mut result = Summary::new();
        result.commits = self.ss.commits.lock().unwrap().len();
        result.hashes = self.ss.hashes.lock().unwrap().len();
        result.paths = self.ss.paths.lock().unwrap().len();
        result.users = self.ss.users.lock().unwrap().len();
        // getting the actual contents saved is more complex as there is no way to determine how many we have unless we actually iterate over them
        result.contents = self.ss.contents.lock().unwrap().indexer.iter()
            .filter(|(_,index)| { index != & db::SplitOffset::<records::ContentsKind>::EMPTY })
            .count();
        return result;
    }

    pub fn commits_size(& self) -> DatastoreSize {
        return self.ss.commits.lock().unwrap().datastore_size() + self.ss.commits_info.lock().unwrap().datastore_size() + self.ss.commits_metadata.lock().unwrap().datastore_size();
    }

    pub fn contents_size(& self) -> DatastoreSize {
        return self.ss.hashes.lock().unwrap().datastore_size() + self.ss.contents.lock().unwrap().datastore_size() + self.ss.contents_metadata.lock().unwrap().datastore_size();
    }

    pub fn paths_size(& self) -> DatastoreSize {
        return self.ss.paths.lock().unwrap().datastore_size() + self.ss.path_strings.lock().unwrap().datastore_size();
    }

    pub fn users_size(& self) -> DatastoreSize {
        return self.ss.users.lock().unwrap().datastore_size() + self.ss.users_metadata.lock().unwrap().datastore_size();
    }

}


pub struct SubstoreViewIterator<'a> {
    ds : &'a DatastoreView,
    i : std::slice::Iter<'a, datastore::Substore>,
}

impl<'a> Iterator for SubstoreViewIterator<'a> {
    type Item = SubstoreView<'a>;

    fn next(& mut self) -> Option<Self::Item> {
        match self.i.next() {
            Some(x) => return Some(SubstoreView{ds : self.ds, ss : x}),
            None => return None,
        }
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

    pub fn get(& mut self, id : ID) -> Option<T> {
        return self.guard.get(id);
    }
}

pub struct SavepointsView<'a> {
    guard : std::sync::MutexGuard<'a, db::LinkedStore<Savepoint,u64>>,
}

impl<'a> SavepointsView<'a> {
    pub fn iter(& mut self) -> db::LinkedStoreIterAll<Savepoint,u64> {
        return self.guard.iter_all();
    }
}



