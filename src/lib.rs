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
    */
    pub fn merge_substore<T : MergeValidator>(& mut self, target_substore : StoreKind, source_substore : StoreKind, validator : T) {
        let mut context = MergeContext::new(& self.target, target_substore, source_substore, validator);
        self.merge_users(& mut context);
        self.merge_paths(& mut context);
        self.merge_hashes(& mut context);
        self.merge_contents(& mut context);
        self.merge_commits(& mut context);
        self.merge_projects(& mut context);
        println!("merging done.");
    }

    fn merge_users<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        println!("merging users...");
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
        println!("    total:    {}", context.users_count.total);
        println!("    existing: {}", context.users_count.existing);
        println!("    new:      {}", context.users_count.new);
        // merge users metadata
        println!("merging user metadata...");
        let mut users_metadata = target_substore.users_metadata.lock().unwrap();
        for (source_id, mtd) in self.source.users_metadata(context.source_substore) {
            // only add the information *if* there is a new mapping 
            if let Some((target_id, true)) = context.users.get(& source_id) {
                users_metadata.set(*target_id, & mtd);
            }
        }
    }

    fn merge_paths<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        println!("merging paths...");
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
        println!("    total:    {}", context.paths_count.total);
        println!("    existing: {}", context.paths_count.existing);
        println!("    new:      {}", context.paths_count.new);
        // merge path strings
        println!("merging path strings...");
        let mut path_strings = target_substore.path_strings.lock().unwrap();
        for (source_id, path) in self.source.paths_strings(context.source_substore) {
            // only add the information *if* there is a new mapping 
            if let Some((target_id, true)) = context.paths.get(& source_id) {
                path_strings.set(*target_id, & path);
            }
        }
    }

    fn merge_hashes<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        println!("mergingh hashes...");
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
        println!("    total:    {}", context.hashes_count.total);
        println!("    existing: {}", context.hashes_count.existing);
        println!("    new:      {}", context.hashes_count.new);
    }

    fn merge_contents<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        println!("merging contents...");
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
                        println!("Cannot add contents id {} as the hash not selected. Target will be inconsistent", source_id);
                    }
                }
            }
        }
        println!("    total:    {}", context.contents_count.total);
        println!("    existing: {}", context.contents_count.existing);
        println!("    new:      {}", context.contents_count.new);
        // merge contents metadata
        println!("merging contents metadata...");
        let mut contents_metadata = target_substore.contents_metadata.lock().unwrap();
        for (source_id, mtd) in self.source.contents_metadata(context.source_substore) {
            if let Some(target_id) = added_contents.get(& source_id) {
                contents_metadata.set(*target_id, & mtd);
            }
        }
    }

    fn merge_commits<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        println!("merging commits...");
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
        println!("    total:    {}", context.commits_count.total);
        println!("    existing: {}", context.commits_count.existing);
        println!("    new:      {}", context.commits_count.new);
        // commits info for the new commits, where we need to update the ids where necessary
        println!("merging commits info...");
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
        println!("meging commits metadata...");
        let mut commits_metadata = target_substore.commits_metadata.lock().unwrap();
        for (source_id, mtd) in self.source.commits_metadata(context.source_substore) {
            // only add the information *if* there is a new mapping 
            if let Some((target_id, true)) = context.commits.get(& source_id) {
                commits_metadata.set(*target_id, & mtd);
            }
        }
    }

    /** Merges projects from the source dataset to. 

        If a project exists in target, it's ignored. Projects that have multiple updates will only keep the latest update. 
     */
    fn merge_projects<T : MergeValidator>(& mut self, context : & mut MergeContext<T>) {
        println!("merging projects...");
        let mut projects = HashMap::<ProjectId, ProjectId>::new();
        // only add projects that have completely new urls, i.e. this is a two pass step. First we create list of all projects that have only new urls and then add these projects and all of their urls
        context.target.load_all_project_urls();
        {
            let mut new_projects = HashSet::<ProjectId>::new();
            let mut existing_projects = HashSet::<ProjectId>::new();
            {
                let target_urls = context.target.project_urls.lock().unwrap();
                for (project_id, url) in self.source.project_urls() {
                    if context.validator.valid_project(project_id) {
                        // if the url exists in target flag the project as existing
                        if target_urls.contains(& url) {
                            existing_projects.insert(project_id);
                            new_projects.remove(& project_id);
                        // otherwise if the project is not marked as existing, add it to new projects
                        } else if ! existing_projects.contains(& project_id) {
                            new_projects.insert(project_id);
                        }
                    }
                    context.projects_count.total += 1;
                }
            }
            context.projects_count.new = new_projects.len();
            context.projects_count.existing = existing_projects.len();
            for (project_id, url) in self.source.project_urls() {
                if new_projects.contains(& project_id) { 
                    if let Some(target_id) = projects.get(& project_id) {
                        context.target.update_project(* target_id, & url);
                    } else {
                        projects.insert(project_id, context.target.add_project(&url).unwrap());
                    }
                }
            }
        }
        println!("    total:    {}", context.projects_count.total);
        println!("    existing: {}", context.projects_count.existing);
        println!("    new:      {}", context.projects_count.new);
        println!("merging projects substore information...");
        // now do project substores - we only take the latest substore and we assume that the latest substore is the source substore. If not, the data is inconsistent and a warning should be reported. To do this we can actually seek reads
        {
            let mut latest_substore = HashMap::<ProjectId, StoreKind>::new();
            for (source_id, substore) in self.source.project_substores() {
                if projects.contains_key(& source_id) {
                    latest_substore.insert(source_id, substore);
                }
            }
            // if the latest substore is not the source substore, report error and remove the project, also remove projects that do not have substores defined, this will leave the project url in the target, so that it can be analyzed later
            projects.retain(|source_id, _| {
                if let Some(kind) = latest_substore.get(source_id) {
                    if *kind == context.source_substore {
                        return true;
                    } else {
                        println!("Selected project {} not part of source substore, but belongs to {:?}", source_id, kind);
                    }
                } else {
                    println!("Selected project {} does not have substore specified", source_id);
                }
                return false;
            });
            // write substore information for the surviving projects
            let mut target_substores = context.target.project_substores.lock().unwrap();
            for (_, target_id) in projects.iter() {
                target_substores.set(*target_id, & context.target_substore);
            }
        }
        println!("merging project update logs...");
        // project updates - only take the latest updates
        {
            let mut latest_update = HashMap::<ProjectId, ProjectLog>::new();
            for (source_id, log) in self.source.project_updates() {
                if projects.contains_key(& source_id) {
                    latest_update.insert(source_id, log);
                }
            }
            let mut target_updates = context.target.project_updates.lock().unwrap();
            for (source_id, log) in latest_update {
                target_updates.set(projects[& source_id], & log);
            }
        }
        println!("merging project heads...");
        // project heads - only take latest change as well
        {
            let mut latest_heads = HashMap::<ProjectId, ProjectHeads>::new();
            for (source_id, heads) in self.source.project_heads() {
                if projects.contains_key(& source_id) {
                    latest_heads.insert(source_id, heads);
                }
            }
            let mut target_heads = context.target.project_heads.lock().unwrap();
            for (source_id, heads) in latest_heads {
                let translated_heads : ProjectHeads = heads.iter().map(|(name, (commit_id, sha))|{
                    return (name.clone(),  (context.translate_commit(*commit_id), *sha));
                }).collect();
                target_heads.set(projects[&source_id], & translated_heads);
            }
        }
        println!("merging project metadata...");
        // and finally, merge metadata, since we do not know what is in metadata, we'll merge them all
        {
            let mut projects_metadata = context.target.project_metadata.lock().unwrap();
            for (source_id, mtd) in self.source.project_metadata() {
                // only add the information *if* there is a new mapping 
                if let Some(target_id) = projects.get(& source_id) {
                    projects_metadata.set(*target_id, & mtd);
                }
            }
        }
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
            println!("Required user id {} not selected. Target will not be consistent", src_id);
            return UserId::NONE;
        }
    }

    fn translate_commit(& self, src_id : CommitId) -> CommitId {
        if let Some((target_id, _)) = self.commits.get(& src_id) {
            return *target_id;
        } else {
            println!("Required commit id {} not selected. Target will not be consistent", src_id);
            return CommitId::NONE;
        }
    }

    fn translate_path(& self, src_id : PathId) -> PathId {
        if let Some((target_id, _)) = self.paths.get(& src_id) {
            return *target_id;
        } else {
            println!("Required path id {} not selected. Target will not be consistent", src_id);
            return PathId::NONE;
        }
    }

    fn translate_hash(& self, src_id : HashId) -> HashId {
        if let Some((target_id, _)) = self.hashes.get(& src_id) {
            return *target_id;
        } else {
            println!("Required hash id {} not selected. Target will not be consistent", src_id);
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

impl ValidateAll {
    pub fn new() -> ValidateAll {
        return ValidateAll{};
    }

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
