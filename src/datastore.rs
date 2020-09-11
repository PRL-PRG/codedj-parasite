use std::sync::*;
use std::fs::*;
use std::io::*;
use std::collections::hash_map::*;

use crate::db::*;

use crate::records::*;



/** The datastore implementation. 
 
 */
pub struct Datastore {
    pub (crate) root : String,

    /** The version of the datastore. 
     
        Versions have backwards compatibility, but newer versions may add extra items, or metadata. When new version is executed, all projects & commits and other items are force updated to make sure that all data that should be obtained are obtained. 
     */
    pub (crate) version : u16,

    /** Project URLs. 
       
        Contains both dead and live urls for the projects known. The latest (i.e. indexed) url for a project is its live url, all previous urls are its past urls currently considered dead.  
     */
    pub (crate) project_urls : Mutex<PropertyStore<String>>,
    pub (crate) project_last_updates : Mutex<PropertyStore<i64>>,
    pub (crate) project_heads : Mutex<PropertyStore<Heads>>,

    /** Mappings of the objects the datastore keeps track of. 
     */
    pub (crate) commits : Mutex<DirectMapping<git2::Oid>>,
    pub (crate) commits_info : Mutex<PropertyStore<CommitInfo>>,
    pub (crate) users : Mutex<Mapping<String>>,
    pub (crate) paths : Mutex<Mapping<String>>,
    pub (crate) hashes : Mutex<DirectMapping<git2::Oid>>,

    pub (crate) contents : Mutex<DirectMapping<u64>>,
    pub (crate) contents_data : Mutex<PropertyStore<ContentsData>>,


}

impl Datastore {

    /** Creates datastore in the specified directory. 
     */
    pub fn from(root : & str) -> Datastore {
        let result = Datastore {
            root : root.to_owned(),
            version : 0,
            project_urls : Mutex::new(PropertyStore::new(& format!("{}/project-urls.dat", root))),
            project_last_updates : Mutex::new(PropertyStore::new(& format!("{}/project-updates.dat", root))),
            project_heads : Mutex::new(PropertyStore::new(& format!("{}/project-heads.dat", root))),

            commits : Mutex::new(DirectMapping::new(& format!("{}/commits.dat", root))),
            commits_info : Mutex::new(PropertyStore::new(& format!("{}/commits-info.dat", root))),
            users : Mutex::new(Mapping::new(& format!("{}/users.dat", root))),
            paths : Mutex::new(Mapping::new(& format!("{}/paths.dat", root))),
            hashes : Mutex::new(DirectMapping::new(& format!("{}/hashes.dat", root))),

            contents : Mutex::new(DirectMapping::new(& format!("{}/contents.dat", root))),
            contents_data : Mutex::new(PropertyStore::new(& format!("{}/contents-data.dat", root))),
        };
        println!("Datastore loaded from {}:", root);
        println!("    projects: {}", result.project_urls.lock().unwrap().indices_len());
        println!("    commits:  {}", result.commits.lock().unwrap().len());
        println!("    users:    {}", result.users.lock().unwrap().len());
        println!("    paths:    {}", result.paths.lock().unwrap().len());
        println!("    hashes:   {}", result.hashes.lock().unwrap().len());
        println!("    contents: {}", result.contents.lock().unwrap().len());
        return result;
    }

    pub fn root(& self) -> & String {
        return & self.root;
    }

    /** Returns the number of projects in the datastore. 
     */ 
    pub fn num_projects(& self) -> usize {
        return self.project_urls.lock().unwrap().indices_len();
    }

    /** Fills the mappings of the datastore. 
     
        This is a separate function as the mappings are only needed for some processed and their loading actually takes considerable time. 
        
        TODO maybe this should go to the updater. 
     */
    pub fn fill_mappings(& mut self) {
        println!("Filling datastore mappings...");
        self.commits.lock().unwrap().fill();
        println!("    commits:  {}", self.commits.lock().unwrap().loaded_len());
        self.users.lock().unwrap ().fill();
        println!("    users:    {}", self.users.lock().unwrap().loaded_len());
        self.paths.lock().unwrap().fill();
        println!("    paths:    {}", self.paths.lock().unwrap().loaded_len());
        self.hashes.lock().unwrap().fill();
        println!("    hashes:   {}", self.hashes.lock().unwrap().loaded_len());
        self.contents.lock().unwrap().fill();
        println!("    contents: {}", self.contents.lock().unwrap().loaded_len());
    }

    /** Creates a savepoint. 
     
        Flushes all held buffers and remembers the actual sizes of files that exist in the database so that any information stored *after* the latest savepoint can be easily rolled back if needed.
     */
    pub fn savepoint(& self) {
        unimplemented!();
    }

    /** Adds new project to the store. 
     
        Returns the new project id and sets its last update time to 0 so that the project will be updated first. 
        
        NOTE: Does not check whether the added url already exists in the dataset, only makes sure that the mappings are preserved. 
     */
    pub fn add_project(& self, url : & String) -> u64 {
        let mut project_urls = self.project_urls.lock().unwrap();
        let mut project_last_updates = self.project_last_updates.lock().unwrap();
        let id = project_urls.indices_len() as u64;
        project_urls.set(id, url);
        project_last_updates.set(id, & 0);
        return id;
    }

    pub fn get_project_url(& self, id : u64) -> String {
        if let Some(url) = self.project_urls.lock().unwrap().get(id) {
            return url;
        } else {
            panic!(format!("Project {} does not have url", id));
        }
    }

    /** Returns the last observed heads for given project. 
     
        If heads were never sampled, returns empty heads. 
     */
    pub fn get_project_heads(& self, id : u64) -> Heads {
        if let Some(heads) = self.project_heads.lock().unwrap().get(id) {
            return heads;
        } else {
            return Heads::new();
        }
    }

    pub const DEAD_PROJECT_UPDATE_TIME : i64 = std::i64::MAX;

}
