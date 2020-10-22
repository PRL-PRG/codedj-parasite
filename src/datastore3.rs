use std::sync::*;
use std::io::*;
use std::path::Path;
use std::fs::{File, OpenOptions};
use sha1::{Sha1, Digest};

use crate::db3::*;
use crate::records3::*;


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

        The datastore keeps a global map of projects known to it. Each provides information about its kind (Git, GitHub, etc.), means of getting its data from external sources (as of now only git clone url is supported, but more is possible in the future) and the substore in which the actual data for the project are stored. Any of this information can be changed at any time, in which case new record is created.

        We need to keep indices to project metadata, changes and updates (and therefore the backing stores as well for better code reuse) in the global datastore, otherwise we'd pay for *big* holes in the per substore indices. 
        
     */
    projects : Mutex<Store<Project>>,
    project_updates : Mutex<SplitStore<ProjectUpdateStatus, StoreKind>>,
    project_heads : Mutex<SplitStore<ProjectHeads, StoreKind>>,
    project_metadata : Mutex<SplitLinkedStore<Metadata, StoreKind>>,

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
        let project_updates_path = root_path.join("project-updates");
        let project_heads_path = root_path.join("project-heads");
        let project_metadata_path = root_path.join("project-metadata");
        if ! root_path.exists() {
            std::fs::create_dir_all(& root_path).unwrap();
            std::fs::create_dir_all(& project_updates_path).unwrap();
            std::fs::create_dir_all(& project_heads_path).unwrap();
            std::fs::create_dir_all(& project_metadata_path).unwrap();
        }
        println!("* Loading datastore in {}", root);
        // create the datastore
        let mut ds = Datastore{
            root : root.to_owned(),
            projects : Mutex::new(Store::new(root, "projects")),
            project_updates : Mutex::new(SplitStore::new(project_updates_path.to_str().unwrap(), "project-updates")),
            project_heads : Mutex::new(SplitStore::new(project_heads_path.to_str().unwrap(), "project-heads")),
            project_metadata : Mutex::new(SplitLinkedStore::new(project_metadata_path.to_str().unwrap(), "project-metadata")),
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

    /** Commits stored in the dataset. 
     */
    commits : Mutex<Mapping<Hash>>,
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

            commits : Mutex::new(Mapping::new(root, "commits")),
            commits_metadata : Mutex::new(LinkedStore::new(root, "commits-metadata")),

            hashes : Mutex::new(Mapping::new(root, "hashes")),
            contents : Mutex::new(SplitStore::new(root, "contents")),
            contents_metadata : Mutex::new(LinkedStore::new(root, "contents-metadata")),

            paths : Mutex::new(Mapping::new(root, "paths")),
            path_strings : Mutex::new(Store::new(root, "path-strings")),

        };
    }

    pub (crate) fn load(& self) {
        self.commits.lock().unwrap().load();
        self.hashes.lock().unwrap().load();
        self.paths.lock().unwrap().load();
    }

    pub (crate) fn clear(& self) {
        self.commits.lock().unwrap().clear();
        self.hashes.lock().unwrap().clear();
        self.paths.lock().unwrap().clear();
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
}
