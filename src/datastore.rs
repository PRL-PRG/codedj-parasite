use std::collections::*;
use std::sync::*;
use std::io::*;
use std::path::Path;
use std::fs::{File, OpenOptions};
use sha1::{Sha1, Digest};

use crate::db::*;
use crate::records::*;
use crate::helpers;

/** The global datastore. 
 
    - all projects
    - split datastores 

    # API

    - file contents = sequential, rewrites not expected
    - file metadata = sequential, updates frequent

    - commits = sequential, updates not expected
    - commits metadata = sequential, updates not expected

    How to get projects for given substore and their latest 

    Provides access to the available savepoints. Once a savepoint is selected, provides iterators to the stored data based on the substore split, i.e. you specify the StoreKind and then can retrieve *all* information *in* the order it was stored up to the savepoint in a particular substore. 

    Random access is *only* provided to file contents. 

    TODO: ALL IDS FROM THE PUBLIC API MUST BE SUBSTORE-KIND AUGMENTED!!!!!!!!!!!!
 */
pub struct Datastore {
    /** Datastore root folder. 
     
        Contains global data, such as datastore savepoints and project mappings as well as subfolders for the substores.
     */
    root : String,

    /** Projects. 

        The datastore keeps a global map of projects known to it. Each provides information about its kind (Git, GitHub, etc.) and means of getting its data from external sources (as of now only git clone url is supported, but more is possible in the future).

        The datastore also keeps information about the projects in the global level as project ids are global. This is possible because a project can be at any given time part of only *one* substore. Storing the project information globally means that we don't pay for huge gaps between project ids in substores, but iterating over projects from given substore is slightly more complicated.

        The following information is kept per project:

        - which substore it belongs to (this can change over time)
        - the linked history of its updates with precise timestamps and update results
        - heads of all branches in the project
        - project metadata

     */
    pub (crate) projects : Mutex<Store<Project>>,
    project_substores : Mutex<Store<StoreKind>>,
    project_updates : Mutex<LinkedStore<ProjectUpdateStatus>>,
    project_heads : Mutex<Store<ProjectHeads>>,
    project_metadata : Mutex<LinkedStore<Metadata>>,

    /** Current and past urls for known projects so that when new projects are added we can check for ambiguity.
     */
    project_urls : Mutex<HashSet<Project>>,

    /** The substores. 
     
        Stored in vector since we know their ids from the IDSplit based on the SubstoreKind. Does not have to be protected as the substores protect their data with mutexes themselves and all substores are cratead when the datastore is instantiated. 
     */
    substores : Vec<Substore>,
}

impl Datastore {

    /** The version of the datastore. 
     
        Versions have backwards compatibility, but newer versions may add extra items, or metadata. When new version is executed, all projects & commits and other items are force updated to make sure that all data that should be obtained are obtained. 
     */
    pub const VERSION : u16 = 0;

    /** Creates the datastore from given root folder. 
     
        If the path does not exist, initializes an empty datastore. 
     */
    pub fn new(root : & str) -> Datastore {
        // make sure the paths exist
        let root_path = std::path::Path::new(root);
        if ! root_path.exists() {
            std::fs::create_dir_all(& root_path).unwrap();
        }
        println!("* Loading datastore in {}", root);
        // create the datastore
        let mut ds = Datastore{
            root : root.to_owned(),
            projects : Mutex::new(Store::new(root, "projects")),
            project_substores : Mutex::new(Store::new(root, "project-substores")),
            project_updates : Mutex::new(LinkedStore::new(root, "project-updates")),
            project_heads : Mutex::new(Store::new(root, "project-heads")),
            project_metadata : Mutex::new(LinkedStore::new(root, "project-metadata")),
            project_urls : Mutex::new(HashSet::new()),

            substores : Vec::new(),
        };
        // initialize the substores
        for store_kind in SplitKindIter::<StoreKind>::new() {
            ds.substores.push(Substore::new(
                & root_path.join(format!("{:?}", store_kind)),
                store_kind
            ));
        }
        return ds;
    }

    /** Returns the root folder of the datastore. 
     */
    pub fn root_folder(&self) -> & str {
        return & self.root;
    }

    // substores --------------------------------------------------------------------------------------------------------

    /** Returns the appropriate substore.
     */
    pub (crate) fn substore(& self, substore : StoreKind) -> & Substore {
        return self.substores.get(substore.to_number() as usize).unwrap();
    }

    // projects ---------------------------------------------------------------------------------------------------------

    pub fn num_projects(& self) -> usize {
        return self.projects.lock().unwrap().len();
    }

    /** Returns the information about given project. 
     */
    pub fn get_project(& self, id : u64) -> Option<Project> {
        return self.projects.lock().unwrap().get(id);
    }

    /** Updates the information about given project. 
     
        Updates the project info and adds the appropriate update record. 
     */
    pub (crate) fn update_project(& self, id : u64, project : & Project) {
        let old_offset;
        {
            let mut projects = self.projects.lock().unwrap();
            old_offset = projects.indexer.get(id).unwrap();
            self.projects.lock().unwrap().set(id, project);
        }
        self.project_updates.lock().unwrap().set(id, & ProjectUpdateStatus::Rename{
            time : helpers::now(),
            version : Self::VERSION,
            old_offset
        });
    }

    pub fn get_project_last_update(& self, id : u64) -> Option<ProjectUpdateStatus> {
        return self.project_updates.lock().unwrap().get(id);
    }

    pub fn get_project_substore(& self, id : u64) -> StoreKind {
        return self.project_substores.lock().unwrap().get(id).or(Some(StoreKind::Unspecified)).unwrap();
    }

    /** Returns the latest project heads for given project. 
     */
    pub fn get_project_heads(& self, id : u64) -> Option<ProjectHeads> {
        return self.project_heads.lock().unwrap().get(id);
    }

    /** Updates the project heads to given value. 
     */
    pub (crate) fn update_project_heads(& self, id : u64, heads : & ProjectHeads) {
        self.project_heads.lock().unwrap().set(id, heads);
    }

    /** Returns metadata value for given key and project, if one exists. 
     */
    pub fn get_project_metadata(& self, id : u64, key : & str) -> Option<String> {
        let mut metadata = self.project_metadata.lock().unwrap();
        for kv in metadata.iter_id(id) {
            if kv.key == key {
                return Some(kv.value);
            }
        }
        return None;
    }

    /** Updates metadata value for given key if the last stored value differs. 
     
        Returns true if the value was updated, false otherwise.
     */
    pub (crate) fn update_project_metadata_if_differ(& self, id : u64, key : String, value : String) -> bool {
        let mut metadata = self.project_metadata.lock().unwrap();
        for kv in metadata.iter_id(id) {
            if kv.key == key {
                if kv.value == value {
                    return false;
                } else {
                    break;
                }
            }
        }
        metadata.set(id, & Metadata{key, value });
        return true;
    }

    pub (crate) fn project_urls_loaded(& self) -> bool {
        if self.project_urls.lock().unwrap().len() > 0 {
            return true;
        }
        return self.projects.lock().unwrap().len() == 0;
    }

    pub (crate) fn load_project_urls(& self, mut reporter : impl FnMut(usize)) {
        let mut urls = self.project_urls.lock().unwrap();
        if urls.is_empty() {
            for (_, p) in self.projects.lock().unwrap().iter_all() {
                if urls.len() % 1000 == 0 {
                    reporter(urls.len());
                }
                urls.insert(p);
            }
        }
    }

    pub (crate) fn drop_project_urls(& self) {
        self.project_urls.lock().unwrap().clear();
    }

    /** Attempts to add a project to the datastore. 
     
        If the project does not exist, adds the project and returns its id. If the project already exists in the known urls, returns None. 
     */
    pub (crate) fn add_project(& self, project : & Project) -> Option<u64> {
        let mut urls = self.project_urls.lock().unwrap();
        let mut projects = self.projects.lock().unwrap();
        assert!(projects.len() == 0 || urls.len() != 0, "Load project urls first");
        if urls.insert(project.clone()) {
            let id = projects.len() as u64;
            projects.set(id, project);
            return Some(id);
        } else {
            return None;
        }
    }

    /** Returns the SHA-1 hash of given contents. 
     */
    pub (crate) fn hash_of(contents : & [u8]) -> Hash {
        let mut hasher = Sha1::new();
        hasher.update(contents);
        return Hash::from_bytes(& hasher.finalize()).unwrap();
    }
}

/** Contains information about a selected subset of projetcs from the datastore. 
 
 */
pub (crate) struct Substore {
    /** Root folder where the dataset is located. 
     */
    root : String,
    /** The prefix of the dataset. 
     */
    prefix : StoreKind,

    /** Determines whether the substore's mappings are loaded in memory and therefore new items can be added to it. 
     */
    loaded : atomic::AtomicBool,

    /** Commits stored in the dataset. 
     */
    commits : Mutex<Mapping<Hash>>,
    commits_info : Mutex<Store<CommitInfo>>,
    commits_metadata : Mutex<LinkedStore<Metadata>>,

    /** File hashes and their contents. 
     
        Every time a commit a file is changed, the hash of its contents is added to the hashes mapping. Some of these files may then have their contents stored and some won't.  

        We use a split store based on the ContentsKind of the file to be stored. The index is kept for *all* file hashes, i.e. there will be a lot of empty holes in it, but these holes will be relatively cheap (20 bytes per hole, 10 bytes for contents and 10 bytes for metadata) and on disk, where it bothers us less. 
     */
    hashes : Mutex<Mapping<Hash>>,
    contents : Mutex<SplitStore<FileContents, ContentsKind>>,
    contents_metadata : Mutex<LinkedStore<Metadata>>,

    /** Paths. 
     
        Path hash to ids is stored in a mapping at runtime, while path strings are stored separately in an indexable store on disk. 
     */
    paths : Mutex<Mapping<Hash>>,
    path_strings : Mutex<Store<String>>,

    /** Users.
     
        Users are mapped by their email. 
     */
    users : Mutex<IndirectMapping<String>>,
    users_metadata : Mutex<LinkedStore<Metadata>>,

}

impl Substore {

    pub fn new(root_path : & Path, kind : StoreKind) -> Substore {
        //if the path root path does not exist, create it
        if ! root_path.exists() {
            std::fs::create_dir_all(root_path).unwrap();
        }
        // and create the store
        let root = root_path.to_str().unwrap();
        println!("** Loading substore {:?}", kind);
        return Substore{
            root : root.to_owned(),
            prefix : kind,
            loaded : atomic::AtomicBool::new(false), 

            commits : Mutex::new(Mapping::new(root, "commits")),
            commits_info : Mutex::new(Store::new(root, "commits-info")),
            commits_metadata : Mutex::new(LinkedStore::new(root, "commits-metadata")),

            hashes : Mutex::new(Mapping::new(root, "hashes")),
            contents : Mutex::new(SplitStore::new(root, "contents")),
            contents_metadata : Mutex::new(LinkedStore::new(root, "contents-metadata")),

            paths : Mutex::new(Mapping::new(root, "paths")),
            path_strings : Mutex::new(Store::new(root, "path-strings")),

            users : Mutex::new(IndirectMapping::new(root, "users")),
            users_metadata : Mutex::new(LinkedStore::new(root, "users-metadata")),

        };
    }

    pub (crate) fn load(& self) {
        self.commits.lock().unwrap().load();
        self.hashes.lock().unwrap().load();
        self.paths.lock().unwrap().load();
        self.users.lock().unwrap().load();
        self.loaded.store(true, atomic::Ordering::SeqCst);
    }

    pub (crate) fn clear(& self) {
        self.loaded.store(false, atomic::Ordering::SeqCst);
        self.commits.lock().unwrap().clear();
        self.hashes.lock().unwrap().clear();
        self.paths.lock().unwrap().clear();
        self.users.lock().unwrap().clear();
    }

    pub (crate) fn is_loaded(& self) -> bool {
        return self.loaded.load(atomic::Ordering::SeqCst);
    }

    /** Returns and id of given commit. 
     
        The secord returned value determines whether the commit is new,  or already known.
     */
    pub (crate) fn get_or_create_commit_id(& self, hash : & Hash) -> (u64, bool) {
        return self.commits.lock().unwrap().get_or_create(hash);
    }

    pub (crate) fn add_commit_info_if_missing(& self, id : u64, commit_info : & CommitInfo) {
        let mut cinfo = self.commits_info.lock().unwrap();
        if ! cinfo.has(id) {
            cinfo.set(id, commit_info);
        }
    }

    pub (crate) fn get_or_create_user_id(& self, email : & String) -> (u64, bool) {
        return self.users.lock().unwrap().get_or_create(email);
    }

    /** Returns an id of given path. 
     
        Returns a tuple of the id and whether the path is new, or already existing one. 
     */
    pub (crate) fn get_or_create_path_id(& self, path : & String) -> (u64, bool) {
        let hash = Datastore::hash_of(path.as_bytes());
        let (id, is_new) = self.paths.lock().unwrap().get_or_create(& hash);
        if is_new {
            self.path_strings.lock().unwrap().set(id, path);
        }
        return (id, is_new);
    }

    pub (crate) fn convert_hashes_to_ids(& self, hashes : & Vec<Hash>) -> Vec<(u64, bool)> {
        let mut mapping = self.hashes.lock().unwrap();
        return hashes.iter().map(|hash| {
            return mapping.get_or_create(hash);
        }).collect();
    }

    pub (crate) fn convert_paths_to_ids(& self, paths : & Vec<String>) -> Vec<(u64, bool)> {
        let mut mapping = self.paths.lock().unwrap();
        let mut path_strings = self.path_strings.lock().unwrap();
        return paths.iter().map(|path| {
            let hash = Datastore::hash_of(path.as_bytes());
            let (id, is_new) = mapping.get_or_create(& hash);
            if is_new {
                path_strings.set(id, path);
            }
            return (id, is_new);
        }).collect();
    }

}
