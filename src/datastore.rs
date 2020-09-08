use std::sync::*;
use std::fs::*;
use std::io::*;
use std::collections::hash_map::*;
use byteorder::*;

use crate::*;
use crate::db::*;

/** The datastore implementation. 
 
 */
pub struct Datastore {
    pub (crate) root : String,

    /** Project URLs. 
       
        Contains both dead and live urls for the projects known. The latest (i.e. indexed) url for a project is its live url, all previous urls are its past urls currently considered dead.  
     */
    pub (crate) project_urls : Mutex<PropertyStore<String>>,

    /** Mappings of the objects the datastore keeps track of. 
     */
    pub (crate) commits : Mutex<DirectMapping<git2::Oid>>,
    pub (crate) users : Mutex<IndirectMapping<String>>,

}

impl Datastore {

    /** Creates datastore in the current working directory. 
     */
    pub fn from_cwd() -> Datastore {
        return Datastore::from(std::env::current_dir().unwrap().to_str().unwrap());
    }

    /** Creates datastore in the specified directory. 
     */
    pub fn from(root : & str) -> Datastore {
        let result = Datastore {
            root : root.to_owned(),
            project_urls : Mutex::new(PropertyStore::new(& format!("{}/project_urls.dat", root))),

            commits : Mutex::new(DirectMapping::new(& format!("{}/commits.dat", root))),
            users : Mutex::new(IndirectMapping::new(& format!("{}/users.dat", root)))
        };
        println!("Datastore loaded from {}:", root);
        println!("    projects: {}", result.project_urls.lock().unwrap().len());
        println!("    commits:  {}", result.commits.lock().unwrap().len());
        println!("    users:    {}", result.users.lock().unwrap().len());
        return result;
    }

    /** Fills the mappings of the datastore. 
     
        This is a separate function as the mappings are only needed for some processed and their loading actually takes considerable time. 
        
        TODO maybe this should go to the updater. 
     */
    pub fn fill_mappings(& mut self) {
        self.commits.lock().unwrap().fill();
        self.users.lock().unwrap().fill();
    }

    /** Creates a savepoint. 
     
        Flushes all held buffers and remembers the actual sizes of files that exist in the database so that any information stored *after* the latest savepoint can be easily rolled back if needed.
     */
    pub fn savepoint(& self) {
        unimplemented!();
    }

}

/*
/** The Datastore 
 
    project_urls = from project id to project url, latest record matters, indexing not necessary
    => we get both active and inactive urls... 

    project_heads = from project id to project heads, need to keep index 
 */
pub struct Datastore {
    root : String,

    /** Project urls - dead (past) and live (current)
     
        Not having the url means that the project is dead. This is geared towards quick updates of already known projects, but is bad for adding new projects where the projects must be disambiguated.  
     */
    //project_urls : Mutex<PropertyStore<String>>,
    /** Latest project heads. 
     */ 
    //project_heads : Mutex<PropertyStore<Heads>>,

    commits : Mutex<HashMap<git2::Oid, u64>>,
    users : Mutex<HashMap<String, u64>>,

}
impl Datastore {

    /** Initializes the datastore from current working directory.
     */
    pub fn new() -> Datastore {
        return Datastore::new_at(std::env::current_dir().unwrap().to_str().unwrap());
    }

    /** Initializes the datastore from given root folder. 
     
        Loads the necessary information to start the downloader. This includes the project urls, and disambiguation maps for other objects being stored.   
     */
    pub fn new_at(root : & str) -> Datastore {
        println!("Loading datastore from {}", root);
        return Datastore{
            root : String::from(root),
            //project_urls : Mutex::new(PropertyStore::new(& format!("{}/project_urls", root))),
            //project_heads : Mutex::new(PropertyStore::new(& format!("{}/project_heads", root))),
            commits : Mutex::new(HashMap::new()),
            users : Mutex::new(HashMap::new()),
        };
    }

    pub fn load_mappings(& mut self) {
        println!("Loading datastore mappings...");
        let commits = Datastore::load_hash_mapping(& self.root, "commit_hashes.dat");
        let users = Datastore::load_string_mapping(& self.root, "user_emails.dat");

    }

    /** Creates a savepoint. 
     
        Flushes all held buffers and remembers the actual sizes of files that exist in the database so that any information stored *after* the latest savepoint can be easily rolled back if needed.
     */
    pub fn savepoint(& self) {
        unimplemented!();
    }

    /*
    pub fn get_project_url(& self, id : u64) -> Option<String> {
        return self.project_urls.lock().unwrap().get(id);
    }

    pub fn get_project_heads(& self, id: u64) -> Option<Heads> {
        return self.project_heads.lock().unwrap().get(id);
    }
    */

    // helper functions


    /** Loads hash to ID mapping, which consists of sequentially stored hashes.
     */
    fn load_hash_mapping(root: & str, file : & str) -> HashMap<git2::Oid, u64> {
        println!("loading hash mapping {}", file);
        let mut result = HashMap::new();
        let path = format!("{}/{}", root, file);
        if std::path::Path::new(& path).exists() {
            let mut f = OpenOptions::new()
                .read(true)
                .open(format!("{}/{}", root, file)).unwrap();
            // read the mapping 
            let mut buffer = vec![0; 20];
            while let Ok(20) = f.read(& mut buffer) {
                result.insert(git2::Oid::from_bytes(& buffer).unwrap(), result.len() as u64);
            }
        }
        println!("    {} records loaded", result.len());
        return result;
    }

    fn load_string_mapping(root: & str, file : & str) -> HashMap<String, u64> {
        println!("loading string mapping {}", file);
        let mut result = HashMap::new();
        let path = format!("{}/{}", root, file);
        if std::path::Path::new(& path).exists() {
            let mut f = OpenOptions::new()
                .read(true)
                .open(format!("{}/{}", root, file)).unwrap();
            // read the mapping 
            while let Ok(size) = f.read_u32::<LittleEndian>() {
                let mut buffer = vec![0; size as usize];
                f.read(& mut buffer).unwrap();
                result.insert(String::from_utf8(buffer).unwrap(), result.len() as u64);
            }
        }
        println!("    {} records loaded", result.len());
        return result;
    }
}
*/


