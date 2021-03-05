use std::collections::*;

#[macro_use]
extern crate lazy_static;


mod helpers;

#[allow(dead_code)]
mod db;
#[allow(dead_code)]
pub mod records;
#[allow(dead_code)]
mod datastore;
#[allow(dead_code)]
mod updater;
#[allow(dead_code)]
mod datastore_maintenance_tasks;
mod task_update_repo;
mod task_update_substore;
mod task_verify_substore;
mod github;
#[allow(dead_code)]
mod settings;
#[allow(dead_code)]
mod reporter;



//use crate::db::TableImplementation;

pub use db::Id;
pub use db::Table;
pub use db::TableOwningIterator;
pub use db::SplitTable;
pub use crate::records::*;

use crate::settings::SETTINGS;
use crate::datastore::*;



/** A simple, read-only view into the datastore. 
 
 */
pub struct DatastoreView {
    root : String
}


impl DatastoreView {
    /** Returns new datastore with given root.
     */
    pub fn from(root : & str) -> DatastoreView {
        // TODO check that there is a valid datastore on the path first
        return DatastoreView{
            root : root.to_owned()
        };
    } 

    pub fn project_urls(& self) -> impl Iterator<Item = (ProjectId, ProjectUrl)> {
        return db::Store::new(& self.root, & DatastoreView::table_filename(Datastore::PROJECTS), true).into_iter();
    }

    pub fn project_substores(& self) -> impl Iterator<Item = (ProjectId, StoreKind)> {
        return db::Store::new(& self.root, & DatastoreView::table_filename(Datastore::PROJECT_SUBSTORES), true).into_iter();
    }

    pub fn project_updates(& self) -> impl Iterator<Item = (ProjectId, ProjectLog)> {
        return db::LinkedStore::new(& self.root, & DatastoreView::table_filename(Datastore::PROJECT_UPDATES), true).into_iter();
    }

    pub fn project_heads(& self) -> impl Iterator<Item = (ProjectId, ProjectHeads)> {
        return db::Store::new(& self.root, & DatastoreView::table_filename(Datastore::PROJECT_HEADS), true).into_iter();
    }

    pub fn project_metadata(& self) -> impl Iterator<Item = (ProjectId, Metadata)> {
        return db::LinkedStore::new(& self.root, & DatastoreView::table_filename(Datastore::PROJECT_METADATA), true).into_iter();
    }

    pub fn savepoints(& self) -> impl Iterator<Item = db::Savepoint> {
        return db::LinkedStore::<db::Savepoint, u64>::new(& self.root, & DatastoreView::table_filename(Datastore::SAVEPOINTS), true).into_iter().map(|(_, sp)| sp);
    }

    /* Substore contents getters and iterators. 
     */
    pub fn commits(& self, substore : StoreKind) -> impl Table<Id = CommitId, Value = SHA> {
        return db::Mapping::new(& self.root, & DatastoreView::substore_table_filename(substore, Substore::COMMITS), true);
    }

    pub fn commits_info(& self, substore : StoreKind) -> impl Table<Id = CommitId, Value = CommitInfo> {
        return db::Store::new(& self.root, & DatastoreView::substore_table_filename(substore, Substore::COMMITS_INFO), true);
    }

    pub fn commits_metadata(& self, substore : StoreKind) -> impl Iterator<Item = (CommitId, Metadata)> {
        return db::LinkedStore::new(& self.root, & DatastoreView::substore_table_filename(substore, Substore::COMMITS_METADATA), true).into_iter();
    }

    pub fn hashes(& self, substore : StoreKind) -> impl Table<Id = HashId, Value = SHA> {
        return db::Mapping::new(& self.root, & DatastoreView::substore_table_filename(substore, Substore::HASHES), true);
    }

    pub fn contents(& self, substore : StoreKind) -> impl SplitTable<Id = HashId, Value = (ContentsKind, FileContents), Kind = ContentsKind, SplitIterator = db::SplitStorePart<FileContents, HashId>> {
        return db::SplitStore::<FileContents, ContentsKind, HashId>::new(& self.root, & DatastoreView::substore_table_filename(substore, Substore::CONTENTS),true);
    }

    pub fn contents_metadata(& self, substore : StoreKind) -> impl Iterator<Item = (HashId, Metadata)> {
        return db::LinkedStore::new(& self.root, & DatastoreView::substore_table_filename(substore, Substore::CONTENTS_METADATA), true).into_iter().into_iter();
    }

    pub fn paths(& self, substore : StoreKind) -> impl Table<Id = PathId, Value = SHA> {
        return db::Mapping::new(& self.root, & DatastoreView::substore_table_filename(substore, Substore::PATHS), true);
    }

    pub fn paths_strings(& self, substore : StoreKind) -> impl Table<Id = PathId, Value = PathString> {
        return db::Store::new(& self.root, & DatastoreView::substore_table_filename(substore, Substore::PATHS_STRINGS), true);
    }

    pub fn users(& self, substore : StoreKind) -> impl Table<Id = UserId, Value = String> {
        return db::IndirectMapping::new(& self.root, & DatastoreView::substore_table_filename(substore, Substore::USERS), true);
    }

    pub fn users_metadata(& self, substore : StoreKind) -> impl Iterator<Item = (UserId, Metadata)> {
        return db::LinkedStore::new(& self.root, & DatastoreView::substore_table_filename(substore, Substore::USERS_METADATA), true).into_iter();
    }

    fn table_filename(table : & str) -> String {
        return format!("{}", table);
    }

    fn substore_table_filename(kind : StoreKind, table : & str) -> String {
        return format!("{:?}/{:?}-{}", kind, kind, table);
    }
}

pub struct ProjectCommitsIterator<T : Table<Id = CommitId, Value = CommitInfo>> {
    commits : T,
    visited : HashSet<CommitId>,
    queue : Vec<CommitId>
}

impl<T : Table<Id = CommitId, Value = CommitInfo>> Iterator for ProjectCommitsIterator<T> {
    type Item = (CommitId, CommitInfo);

    fn next(& mut self) -> Option<(CommitId, CommitInfo)> {
        loop {
            if let Some(id) = self.queue.pop() {
                if self.visited.contains(&id) {
                    continue;
                }
                self.visited.insert(id);
                let cinfo = self.commits.get(id).unwrap(); // this would mean inconsistent data, so we panic
                // add parents to queue
                self.queue.extend(cinfo.parents.iter());
                return Some((id, cinfo));
            } else {
                return None;
            }
        }  
    }
}

impl<T : Table<Id = CommitId, Value = CommitInfo>> ProjectCommitsIterator<T> {
    pub fn new(heads : & ProjectHeads, commits : T) -> ProjectCommitsIterator<T> {
        return ProjectCommitsIterator {
            commits, 
            visited : HashSet::new(),
            queue : heads.iter().map(|(_, (id, _))| *id).collect()
        };
    }
}

/** Information about an assembled project. 
 */
pub struct Project {
    pub url : ProjectUrl, 
    pub substore : StoreKind,
    pub latest_status : ProjectLog,
    pub latest_valid_status : ProjectLog,
    pub heads : ProjectHeads,
}

impl Project {

    fn new(url : ProjectUrl, substore : StoreKind) -> Project {
        return Project{
            url,
            substore,
            latest_status : ProjectLog::Error{time : 0, version : datastore::Datastore::VERSION, error : "no_data".to_owned()},
            latest_valid_status : ProjectLog::Error{time : 0, version : datastore::Datastore::VERSION, error : "no_data".to_owned()},
            heads : ProjectHeads::new(),
        };
    }

    pub fn is_valid(& self) -> bool {
        match self.latest_status {
            ProjectLog::NoChange{time : _, version : _} => return true,
            ProjectLog::Ok{time : _, version : _} => return true,
            _ => return false,
        }

    }

    pub fn latest_valid_update_time(& self) -> Option<i64> {
        match self.latest_valid_status {
            ProjectLog::NoChange{time, version : _} => return Some(time),
            ProjectLog::Ok{time, version : _} => return Some(time),
            _ => return None,
        }
    }

    pub fn assemble(ds : & DatastoreView) -> HashMap<ProjectId, Project> {
        let mut projects = HashMap::<ProjectId, Project>::new();
        // we have to start with urls as these are the only ones guaranteed to exist
        LOG!("Loading latest project urls...");
        for (id, url) in ds.project_urls() {
            projects.insert(id, Project::new(url, StoreKind::Unspecified));
        }
        LOG!("    {} projects found", projects.len());
        LOG!("Loading project substores...");
        for (id, kind) in ds.project_substores() {
            projects.get_mut(&id).unwrap().substore = kind;
        }
        LOG!("Loading project state...");
        for (id, status) in ds.project_updates() {
            if let Some(p) = projects.get_mut(& id) {
                p.latest_status = status;
            }
        }
        LOG!("Loading project heads...");
        for (id, heads) in ds.project_heads() {
            if let Some(p) = projects.get_mut(& id) {
                p.heads = heads;
            }
        }
        return projects;
    }
}

/** A class that facilitates merging one datastore into another. 
 
    TODO datastoreviews into the merged datastore will be invalidated by the merge. Do we care? 

    Merges parts of the source datastore into the target datastore. The merge squishes any histories and source savepoints. 


    

 */
pub struct DatastoreMerger {
    target : DatastoreView,
    source : DatastoreView,


}

impl DatastoreMerger {

    /** Creates new datastore merger that can be used to merge substores from source into the target datastore. 
     */
    pub fn new(target : & str, source : & str) -> DatastoreMerger {
        return DatastoreMerger{
            target : DatastoreView::from(target),
            source : DatastoreView::from(source)
        };
    }

    
    /** Merges single substore from source datastore into selected substore in the target substore. 
     
        This may be the same substore, or multiple source substores can be joined in a single target substore by repeatedly calling the method for different source substores. 

        TODO add validator as argument too
    */
    pub fn merge_substore<T : MergeValidator>(& mut self, target_substore : StoreKind, source_substore : StoreKind, validator : T) {
        let mut context = MergeContext::new(& self.target, target_substore, source_substore, validator);
        self.merge_users(& mut context);
        self.merge_paths(& mut context);
        self.merge_hashes(& mut context);
        self.merge_contents(& mut context);
        self.merge_commits(& mut context);
        self.merge_projects(& mut context);
    }

    fn merge_users<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        let target_substore = context.target.substore(context.target_substore); 
        let mut users = target_substore.users.lock().unwrap();
        users.load();
        for (source_id, email) in self.source.users(context.source_substore) {
            if context.validator.valid_user(source_id) {
                let x = users.get_or_create_mapping(& email);
                context.users.insert(source_id, x);
                match x.1 {
                    true => context.users_count.new += 1,
                    false => context.users_count.existing += 1,
                }
            }
            context.users_count.total += 1;
        }
        users.clear();
        // merge users metadata
        let mut users_metadata = target_substore.users_metadata.lock().unwrap();
        for (source_id, mtd) in self.source.users_metadata(context.source_substore) {
            // only add the information *if* there is a new mapping 
            if let Some((target_id, true)) = context.users.get(& source_id) {
                users_metadata.set(*target_id, & mtd);
            }
        }
    }

    fn merge_paths<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        let target_substore = context.target.substore(context.target_substore); 
        let mut paths = target_substore.paths.lock().unwrap();
        paths.load();
        for (source_id, hash) in self.source.paths(context.source_substore) {
            if context.validator.valid_path(source_id) {
                let x = paths.get_or_create_mapping(& hash);
                context.paths.insert(source_id, x);
                match x.1 {
                    true => context.paths_count.new += 1,
                    false => context.paths_count.existing += 1,
                }
            }
            context.paths_count.total += 1;
        }
        paths.clear();
        // merge path strings
        let mut path_strings = target_substore.path_strings.lock().unwrap();
        for (source_id, path) in self.source.paths_strings(context.source_substore) {
            // only add the information *if* there is a new mapping 
            if let Some((target_id, true)) = context.paths.get(& source_id) {
                path_strings.set(*target_id, & path);
            }
        }
    }

    fn merge_hashes<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        let target_substore = context.target.substore(context.target_substore); 
        let mut hashes = target_substore.hashes.lock().unwrap();
        hashes.load();
        for (source_id, hash) in self.source.hashes(context.source_substore) {
            if context.validator.valid_hash(source_id) {
                let x = hashes.get_or_create_mapping(& hash);
                context.hashes.insert(source_id, x);
                match x.1 {
                    true => context.hashes_count.new += 1,
                    false => context.hashes_count.existing += 1,
                }
            }
            context.hashes_count.total += 1;
        }
        hashes.clear();
    }

    fn merge_contents<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        // add the contents if they have been selected *and* are new
        let target_substore = context.target.substore(context.target_substore); 
        let mut contents = target_substore.contents.lock().unwrap();
        // added contents
        let mut added_contents = HashMap::<HashId, HashId>::new();
        for (source_id, (contents_kind, raw_contents)) in self.source.contents(context.source_substore) {
            context.contents_count.total += 1;
            if context.validator.valid_contents(source_id) {
                match context.hashes.get(& source_id) {
                    Some((target_id, true)) => {
                        // it's a valid contents and a new hash, so it definitely does not exist in target
                        contents.set(*target_id, contents_kind, & raw_contents);
                        added_contents.insert(source_id, *target_id);
                        context.contents_count.new += 1;
                    },
                    Some((target_id, false)) => {
                        // it's a valid hash that already exists, we have to check first if the contents exists in target, and only add the contents if it does not
                        if ! contents.has(*target_id) {
                            contents.set(*target_id, contents_kind, & raw_contents);
                            added_contents.insert(source_id, *target_id);
                            context.contents_count.new += 1;
                        } else {
                            context.contents_count.existing += 1;

                        }
                    },
                    None => {
                        context.contents_count.existing += 1;
                        // this is an inconsistency, we said this is a valid contents id, but not a hash id, so at this point it can't be added
                        unimplemented!();
                    }
                }
            }
        }
        // merge contents metadata
        let mut contents_metadata = target_substore.contents_metadata.lock().unwrap();
        for (source_id, mtd) in self.source.contents_metadata(context.source_substore) {
            if let Some(target_id) = added_contents.get(& source_id) {
                contents_metadata.set(*target_id, & mtd);
            }
        }
    }

    fn merge_commits<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        let target_substore = context.target.substore(context.target_substore); 
        let mut commits = target_substore.commits.lock().unwrap();
        commits.load();
        for (source_id, hash) in self.source.commits(context.source_substore) {
            if context.validator.valid_commit(source_id) {
                let x = commits.get_or_create_mapping(& hash);
                context.commits.insert(source_id,x);
                match x.1 {
                    true => context.commits_count.new += 1,
                    false => context.commits_count.existing += 1,
                }

            }
            context.commits_count.total += 1;
        }
        commits.clear();
        // commits info for the new commits, where we need to update the ids where necessary
        let mut commits_info = target_substore.commits_info.lock().unwrap();
        for (source_id, mut cinfo) in self.source.commits_info(context.source_substore) {
            // only add the information *if* there was a new mapping 
            if let Some((target_id, true)) = context.commits.get(& source_id) {
                cinfo.committer = context.translate_user(cinfo.committer);
                cinfo.author = context.translate_user(cinfo.author);
                cinfo.parents = cinfo.parents.iter().map(|x| context.translate_commit(*x)).collect();
                cinfo.changes = cinfo.changes.iter().map(|x| context.translate_change((*x.0, *x.1))).collect();
                commits_info.set(*target_id, & cinfo);
            }
        }
        // merge commits metadata
        let mut commits_metadata = target_substore.commits_metadata.lock().unwrap();
        for (source_id, mtd) in self.source.commits_metadata(context.source_substore) {
            // only add the information *if* there is a new mapping 
            if let Some((target_id, true)) = context.commits.get(& source_id) {
                commits_metadata.set(*target_id, & mtd);
            }
        }
    }

    fn merge_projects<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        
    }

}

struct MergeCount {
    /** Total number of items found in the source datastore.
     */
    total : usize,
    /** Number of items in source datastore that were already present in the target. 
     */
    existing : usize,
    /** Number of items from the source that have been merged into the target datastore. 
     */
    new : usize,
}

impl MergeCount {
    pub fn new() -> MergeCount {
        return MergeCount {
            total : 0, 
            existing : 0,
            new : 0,
        };
    }
}

struct MergeContext<T : MergeValidator> {
    target : Datastore,
    target_substore : StoreKind,
    source_substore : StoreKind,
    validator : T,
    users : HashMap<UserId, (UserId, bool)>,
    paths : HashMap<PathId, (PathId, bool)>,
    hashes : HashMap<HashId, (HashId, bool)>,
    commits : HashMap<CommitId, (CommitId, bool)>,
    projects : HashMap<ProjectId, (ProjectId, bool)>,
    users_count : MergeCount,
    paths_count : MergeCount,
    hashes_count : MergeCount,
    contents_count : MergeCount,
    commits_count : MergeCount,
    projects_count : MergeCount,
}

impl<T : MergeValidator> MergeContext<T> {
    fn new(target : & DatastoreView, target_substore : StoreKind, source_substore : StoreKind, validator : T) -> MergeContext<T> {
        return MergeContext {
            target : Datastore::new(target.root.as_str(), false),
            target_substore,
            source_substore,
            validator,
            users : HashMap::new(),
            paths : HashMap::new(),
            hashes : HashMap::new(),
            commits : HashMap::new(),
            projects : HashMap::new(),
            users_count : MergeCount::new(),
            paths_count : MergeCount::new(),
            hashes_count : MergeCount::new(),
            contents_count : MergeCount::new(),
            commits_count : MergeCount::new(),
            projects_count : MergeCount::new(),
        };
    }

    fn translate_user(& self, src_id : UserId) -> UserId {
        if let Some((target_id, _)) = self.users.get(& src_id) {
            return *target_id;
        } else {
            // TODO report that the user does not exist as a warning
            return UserId::NONE;
        }
    }

    fn translate_commit(& self, src_id : CommitId) -> CommitId {
        if let Some((target_id, _)) = self.commits.get(& src_id) {
            return *target_id;
        } else {
            // TODO report that the user does not exist as a warning
            return CommitId::NONE;
        }
    }

    fn translate_path(& self, src_id : PathId) -> PathId {
        if let Some((target_id, _)) = self.paths.get(& src_id) {
            return *target_id;
        } else {
            // TODO report that the user does not exist as a warning
            return PathId::NONE;
        }
    }

    fn translate_hash(& self, src_id : HashId) -> HashId {
        if let Some((target_id, _)) = self.hashes.get(& src_id) {
            return *target_id;
        } else {
            // TODO report that the user does not exist as a warning
            return HashId::NONE;
        }
    }

    fn translate_change(& self, (path_id, hash_id) : (PathId, HashId)) -> (PathId, HashId) {
        return (
            self.translate_path(path_id),
            self.translate_hash(hash_id)
        );
    }

}

/** A simple trait that validates whether given ids from the source datastore are to be merged into the target. 
 */
pub trait MergeValidator {
    fn valid_project(& self, id : ProjectId) -> bool;
    fn valid_commit(& self, id : CommitId) -> bool;
    fn valid_hash(& self, id : HashId) -> bool;
    fn valid_contents(& self, id : HashId) -> bool;
    fn valid_path(& self, id : PathId) -> bool;
    fn valid_user(& self, id : UserId) -> bool;
}

/** A trivial validator that validates everything
 */
pub struct ValidateAll {
}

impl MergeValidator for ValidateAll {
    fn valid_project(& self, _id : ProjectId) -> bool {
        return true;
    }

    fn valid_commit(& self, _id : CommitId) -> bool {
        return true;
    }

    fn valid_hash(& self, _id : HashId) -> bool {
        return true;
    }

    fn valid_contents(& self, _id : HashId) -> bool {
        return true;
    }

    fn valid_path(& self, _id : PathId) -> bool {
        return true;
    }

    fn valid_user(& self, _id : UserId) -> bool {
        return true;
    }

}






/*
use crate::settings::SETTINGS;
use crate::db::Indexable;
//use crate::db::Id;
//use crate::datastore::Datastore;
pub use crate::records::*;

/* Exported types.
*/
pub type Savepoint = db::Savepoint;

/** The assembled project information. 
 */
pub struct Project {
    pub url : ProjectUrl, 
    pub substore : StoreKind,
    pub latest_status : ProjectLog,
    pub latest_valid_status : ProjectLog,
    pub heads : ProjectHeads,
}

impl Project {
    fn new(url : ProjectUrl, substore : StoreKind) -> Project {
        return Project{
            url,
            substore,
            latest_status : ProjectLog::Error{time : 0, version : datastore::Datastore::VERSION, error : "no_data".to_owned()},
            latest_valid_status : ProjectLog::Error{time : 0, version : datastore::Datastore::VERSION, error : "no_data".to_owned()},
            heads : ProjectHeads::new(),
        };
    }

    pub fn is_valid(& self) -> bool {
        match self.latest_status {
            ProjectLog::NoChange{time : _, version : _} => return true,
            ProjectLog::Ok{time : _, version : _} => return true,
            _ => return false,
        }

    }

    pub fn latest_valid_update_time(& self) -> Option<i64> {
        match self.latest_valid_status {
            ProjectLog::NoChange{time, version : _} => return Some(time),
            ProjectLog::Ok{time, version : _} => return Some(time),
            _ => return None,
        }
    }
}

/** Datastore view is similar to datastore, but allows only read access. 
 
    Furthermore when accessing, savepoints can be selected. 
 
 
 */
pub struct DatastoreView {
    ds : datastore::Datastore, 
}

impl DatastoreView {

    /** Creates new datastore view from given path. 
     
        The view cannot be updated, only read. 
     */
    pub fn from(root : & str) -> DatastoreView {
        return DatastoreView{
            ds : datastore::Datastore::new(root, true), // readonly
        };
    }

    /** Creates new appendable datastore view that allows new information to be added. 
     
        This is useful for merging. 
     */
    pub fn append(root : & str) -> DatastoreView {
        return DatastoreView{
            ds : datastore::Datastore::new(root, false), // not readonly
        };
    }

    pub fn version(& self) -> u16 {
        return datastore::Datastore::VERSION;
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

    pub fn current_savepoint(& self) -> Savepoint {
        return self.ds.create_savepoint("latest".to_owned());
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
    
    /** Returns a specified savepoint. 
     
        If no name is given, always succeeds and returns the current savepoint. Otherwise returns a savepoint with given name, or None if no such savepoint found. 
     */
    pub fn get_savepoint(& self, name : Option<& str>) -> Option<Savepoint> {
        match name {
            None => return Some(self.current_savepoint()),
            Some(savepoint_name) => {
                return self.ds.get_savepoint(savepoint_name);
            },
        }
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

    pub fn project_commits(& self, p : & Project) -> HashMap<CommitId, CommitInfo> {
        let ss = self.get_substore(p.substore);
        let mut commits = ss.commits_info();
        let mut result = HashMap::new();
        let mut q = Vec::<CommitId>::new();
        q.extend(p.heads.iter().map(|(_, (id, _))| id));
        while let Some(id) = q.pop() {
            match result.entry(id) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    let commit_info = commits.get(id).unwrap();
                    q.extend(commit_info.parents.iter());
                    e.insert(commit_info);
                },
                // if we already have the commit, do nothing
                _ => {},
            }
        }
        return result;
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
        let mut projects = HashMap::<ProjectId, Project>::new();
        // we have to start with urls as these are the only ones guaranteed to exist
        LOG!("Loading latest project urls...");
        for (id, url) in self.ds.projects.lock().unwrap().savepoint_iter_all(sp) {
            projects.insert(id, Project::new(url, StoreKind::Unspecified));
        }
        LOG!("    {} projects found", projects.len());
        LOG!("Loading project substores...");
        for (id, kind) in self.ds.project_substores.lock().unwrap().savepoint_iter_all(sp) {
            if let Some(expected) = substore {
                if expected != kind {
                    continue;
                } 
            }
            projects.get_mut(&id).unwrap().substore = kind;
        }
        if let Some(expected) = substore {
            projects.retain(|_, p| { p.substore == expected });
        }
        LOG!("    {} projects with matching store {:?} found", projects.len(), substore);
        LOG!("Loading project state...");
        for (id, status) in self.ds.project_updates.lock().unwrap().savepoint_iter_all(sp) {
            if let Some(p) = projects.get_mut(& id) {
                p.latest_status = status;
            }
        }
        LOG!("Loading project heads...");
        for (id, heads) in self.ds.project_heads.lock().unwrap().savepoint_iter_all(sp) {
            if let Some(p) = projects.get_mut(& id) {
                p.heads = heads;
            }
        }
        return projects;
    }


    /** A simple function that returns the summary of the dataset. 
     */
    pub fn summary(& self) -> Summary {
        LOG!("Calculating summary for entire datastore...");
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

    pub fn paths_strings(& self) -> StoreView<String, PathId> {
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

/*
/** Merges and filters on a datastore substore. 
 
    The idea is that you create this, initialize with a datastore where the results will be stored, datastore view from which the data will be taken and a savepoint. 

    Then you control which entities will be merged. 
 */
pub struct MergerAndFilter {
    target : Datastore,
    source : DatastoreView,
}

impl MergerAndFilter {
    /** Creates new merger and filter.
     
        It will merge the substores from source datastore and add them to generic substore in the target store. 
     */
    pub fn new(target_root : & str, source_root : & str) -> MergerAndFilter {
        return MergerAndFilter{
            target : Datastore::new(target_root, false),
            source : DatastoreView::new(source_root),
        };
    }



    /*
    pub fn get_substore(& self, substore : StoreKind) -> SubstoreMerger {
        return SubstoreMerger::new(self.source.get_substore(substore), & self.target);
    }
    */
}
*/

/*
/** Merger interface of a single substore. 

    
    
 */
pub struct SubstoreMerger<'a> {
    source : SubstoreView<'a>,
    target : &'a Datastore,
    projects : HashSet<ProjectId>,
    commits : HashSet<CommitId>,
}

/*
*/

impl<'a> SubstoreMerger<'a> {
    fn new(source : SubstoreView<'a>, target : &'a Datastore) -> SubstoreMerger<'a> {
        return SubstoreMerger{
            source,
            target
        };
    }
}
*/

/* Substore filter for merging. 
 
    Contains the substore view that can be queried in any way a normal substore is queried, but also provides an API for remembering what entities should be preserved during the merge & filter phase. 

    Note that while this seems pretty simple, the API is actually extremely low level and should be used very carefully, otherwise an invalid datastore can easily be created (say by adding a commit, but not adding its parent commit). The merge filter will report warnings if any such problem is detected, but (a) it does not guarantee to spot everything, and (b) these are just warnings and it is possible (but discouraged) to create datastores that are useful, but are not valid in parasite's sense. 

    Also note that the implementation can be optimized if needs be. For now, we keep all the mappings in memory and deduplicate immediately. If memory would ever become a problem, we can simply output all the ids in files without deduplication and then merge on a per id basis by first loading and deduplicating the filter ids file. 
 */
/*
pub struct SubstoreFilter<'a> {
    substore : SubstoreView<'a>,
    projects : HashSet<ProjectId>,
    commits : HashSet<CommitId>,
    hashes : HashSet<HashId>,
    contents : HashSet<HashId>,
}


impl<'a> SubstoreFilter<'a> {
}
*/



/** A helper class that iterates over all substores in a datastore and returns substore views to them. 
 */
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

/** A helper trait that defines a random access capability for a view. 
 
    Under the hood, the random access view utilizes the index files built by the updater and so is only available for ReadOnly records as these can never be updated once written, so the index must be valid even for older savepoints. 
 */
pub trait RandomAccessView<'a, T : db::ReadOnly, ID : db::Id> {
    fn get(& mut self, id : ID) -> Option<T>;
}


/** A guarded iterator that contains the guard, and an iterator itself.
 */
/*
pub struct GuardedIterator<T : Iterator, G> {
    i : T,
    guard : G,
}

impl<T : Iterator, G> Iterator for GuardedIterator<T,G> {
    type Item = T::Item;

    fn next(& mut self) -> Option<Self::Item> {
        return self.i.next();
    }

}
*/


/** Specifies iterator and how to obtain it */
/*
pub trait IteratorWrapper<T : Iterator> {

    fn iter(self, sp : & Savepoint) -> T;
}
*/

/** Holds the iterator, and its guard.
 */
/*
pub struct ConsumingIterator<T : IteratorWrapper> {
    guard : T,
    i : T::IteratorType
}

impl<T : IteratorWrapper> Iterator for ConsumingIterator<T> {
    //type Item = T::IteratorType::Item;
    type Item = <<T as IteratorWrapper>::IteratorType as Iterator>::Item;

    fn next(& mut self) -> Option<Self::Item> {
        return self.i.next();
    }
}
*/

/** A view into a Store. 
 
    Provides iterator for all elements within given savepoint and if the store holds ReadOnly records, provides random access as well. 
 */
pub struct StoreView<'a, T : db::Serializable<Item = T>, ID : db::Id = u64> {
    guard : std::sync::MutexGuard<'a, db::Store<T,ID>>,
}

impl<'a, T : db::Serializable<Item = T>, ID : db::Id> StoreView<'a, T, ID> {
    pub fn iter(& mut self, sp : & Savepoint) -> db::StoreIterAll<T,ID> {
        return self.guard.savepoint_iter_all(sp);
    } 

    /*
    pub fn giter(self, sp : & Savepoint) -> GuardedIterator<db::StoreIterAll<'a, T, ID>, std::sync::MutexGuard<'a, db::Store<T,ID>>> {
        let g = self.guard;
        let i = g.savepoint_iter_all(sp);
        let mut result = GuardedIterator{i, guard : g};
        result.next();
        return result;
    }
    */

    /*
    fn into_iter(mut self, sp : & Savepoint) -> ConsumingIterator<'a, Self> {
        let iter = self.guard.savepoint_iter_all(sp);
        return ConsumingIterator{
            guard : self, 
            i : iter
        };
    } */
}

/*
impl<'a, T : db::Serializable<Item = T>, ID : db::Id> IteratorWrapper for StoreView<'a, T, ID> {
    type IteratorType = db::StoreIterAll<'a, T, ID>;

    fn iter(&'a mut self, sp : & Savepoint) -> db::StoreIterAll<'a, T,ID> {
        return self.guard.savepoint_iter_all(sp);
    }

} 
*/


impl<'a, T : db::Serializable<Item = T> + db::ReadOnly, ID : db::Id> RandomAccessView<'a, T, ID> for StoreView<'a, T, ID> {
    fn get(& mut self, id : ID) -> Option<T> {
        unimplemented!();
        //return self.guard.get(id);
    }
}

/** A view into a LinkedStore. 
 
    Provides iterator for all elements within given savepoint and if the store holds ReadOnly records, provides random access as well. 
 */
pub struct LinkedStoreView<'a, T : db::Serializable<Item = T>, ID : db::Id> {
    guard : std::sync::MutexGuard<'a, db::LinkedStore<T,ID>>,
}

impl<'a, T : db::Serializable<Item = T>, ID : db::Id > LinkedStoreView<'a, T, ID> {
    pub fn iter(& mut self, sp : & Savepoint) -> db::LinkedStoreIterAll<T,ID> {
        return self.guard.savepoint_iter_all(sp);
    }
}

impl<'a, T : db::Serializable<Item = T> + db::ReadOnly, ID : db::Id> RandomAccessView<'a, T, ID> for LinkedStoreView<'a, T, ID> {
    fn get(& mut self, id : ID) -> Option<T> {
        return self.guard.get(id);
    }
}

/** Provides a view into a mapping. 

    Since mappings are always ReadOnly, provides random access as well.
 */
pub struct MappingView<'a, T : db::FixedSizeSerializable<Item = T> + Eq + Hash + Clone, ID : db::Id> {
    guard : std::sync::MutexGuard<'a, db::Mapping<T,ID>>,
}

impl<'a, T : db::FixedSizeSerializable<Item = T> + Eq + Hash + Clone, ID : db::Id> MappingView<'a, T, ID> {
    pub fn iter(& mut self, sp : & Savepoint) -> db::MappingIter<T, ID> {
        return self.guard.savepoint_iter(sp);
    }

    pub fn get(& mut self, id : ID) -> Option<T> {
        return self.guard.get_value(id);
    } 
}

/** Provides a view into a indirect mapping. 

    Since mappings are always ReadOnly, provides random access as well.
 */
pub struct IndirectMappingView<'a, T : db::Serializable<Item = T> + Eq + Hash + Clone, ID : db::Id> {
    guard : std::sync::MutexGuard<'a, db::IndirectMapping<T,ID>>,
}

impl<'a, T : db::Serializable<Item = T> + Eq + Hash + Clone, ID : db::Id> IndirectMappingView<'a, T, ID> {
    pub fn iter(& mut self, sp : & Savepoint) -> db::StoreIterAll<T, ID> {
        return self.guard.savepoint_iter(sp);
    }

    pub fn get(& mut self, id : ID) -> Option<T> {
        return self.guard.get_value(id);
    } 
}

/** Provides a view into a SplitStore. 
 
    Provides iterator for all elements within given savepoint and if the store holds ReadOnly records, provides random access as well. 

    TODO add iterator based on kind. 
    
 */
pub struct SplitStoreView<'a, T : db::Serializable<Item = T>, KIND : db::SplitKind<Item = KIND>, ID : db::Id> {
    guard : std::sync::MutexGuard<'a, db::SplitStore<T,KIND, ID>>,
}

impl<'a, T : db::Serializable<Item = T>, KIND : db::SplitKind<Item = KIND>, ID : db::Id> SplitStoreView<'a, T, KIND, ID> {

    pub fn iter(& mut self, sp : & Savepoint) -> db::SplitStoreIterAll<T, KIND, ID> {
        return self.guard.savepoint_iter(sp);
    }
}

impl<'a, T : db::Serializable<Item = T> + db::ReadOnly, KIND : db::SplitKind<Item = KIND>, ID : db::Id> RandomAccessView<'a, T, ID> for SplitStoreView<'a, T, KIND, ID> {
    fn get(& mut self, id : ID) -> Option<T> {
        return self.guard.get(id);
    }
}

/** Special case for iterator into savepoints as savepoints are not savepointed actually so the savepoint taking api of LinkedStore is useless.
 */
pub struct SavepointsView<'a> {
    guard : std::sync::MutexGuard<'a, db::LinkedStore<Savepoint,u64>>,
}

impl<'a> SavepointsView<'a> {
    pub fn iter(& mut self) -> db::LinkedStoreIterAll<Savepoint,u64> {
        return self.guard.iter_all();
    }
}

/* ====================================================================================================================
   Helper classes for various statistics about the datastore. 
   ====================================================================================================================
 */


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
        return write!(f, "{},{}", self.contents, self.indices);
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
            let contents = f.f.seek(SeekFrom::End(0)).unwrap() as usize;
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

/*

/* Datastore merging 

   The merging should work from datastore to datastore. Merging happens on a per substore basis, i.e. one must explicitly select source substore and target substore. While merging, filtering can be performed, i.e. not all data from source need to propagate to target. This allows the merging to perform multiple different operations:alloc
   
   - merging two datasets (i.e. from source substores to target substores, carry everything)
   - joining into single substore (i.e. from source to empty target, from N substores to 1 substore, optionally filter) 
   - or really just a simple filter (i.e. from substores to target substores, target starts empty, not everything is copied)


   target = DatastoreView::append("");
   source = DatastoreMerger::new("");
   

 */

impl<'a> SubstoreMerger<'a> {

    /** Merges projects. 
     
        This again is a bit more involved. We take all projects that are in source's current substore, but we only add those projects that are not in the target *at all*, i.e. if there is the same project in the target, but in a different substore, we will not add it. 

        To determine if two projects are equal, we use *latest* url in the source and compare it against *all* urls in targets. 

        NOTE: The above is not perfect, but is safe.
     */
    fn merge_projects(& mut self, sp : & Savepoint) {
        // first we need to create a mapping from project urls to project ids, including historic urls for the target datastore
        let mut urls = HashMap::<ProjectUrl, ProjectId>::new();
        for (id, url) in self.target.ds.ds.projects.lock().unwrap().iter_all() {
            urls.insert(url, id);
        }
        // now look at source projects and add ids of those that we have already seen in the target (even historically)
        for (src_id, url) in self.source.ds.project_urls().iter(sp) {
            if let Some(target_id) = urls.get(& url) {
                self.projects.entry(src_id).and_modify(|e| {
                    *e = (*target_id, false);
                });
            }
        }
        // in second pass, add the remaining projects
        let ref target = self.target.ds.ds;
        for (src_id, url) in self.source.ds.project_urls().iter(sp) {
            self.projects.entry(src_id).and_modify(|e| {
                if e.0 == ProjectId::NONE {
                    if let Some(target_id) = target.add_project(& url) {
                        *e = (target_id, true);
                        target.update_project(e.0, & url);
                    } else {
                        unreachable!();
                    }
                } else if e.1 {
                    target.update_project(e.0, & url);
                }
            });
        }
        // now all the project urls have been added and we know which projects information to include. Since merging happens on a substore basis, the substore information cannot be properly preserved in the merged datastore, instead, all added projects substore is set to the merge target
        let mut project_substores = self.target.ds.project_substores();
        for p in self.projects.iter() {
            if let (_, (target_id, true)) = p {
                project_substores.guard.set(*target_id, & self.target.kind());
            }
        }
        // For updates and changes, we only store the last one for each project 






        /*
        self.target.ds.ds.load_all_project_urls();
        let mut urls = self.target.ds.ds.project_urls.lock();
        // first look at all selected source projects and remove those whose urls in any given point it time were already analyzed in the target
        for (src_id, url) in self.source.ds.project_urls() {
            self.projects.entry(src_id).and_modify(|e| {
                *e = commits.guard.get_or_create(& hash);
            });
            
            if urls.contains(url)
                self.projects.
        }
        */
        


    }






}


*/

*/

