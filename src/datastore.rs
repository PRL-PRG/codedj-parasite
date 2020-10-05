use std::sync::*;
use std::io::*;
use std::fs::{File, OpenOptions};

use sha1::{Sha1, Digest};

use crate::db::*;


use crate::records::*;



/** The datastore implementation. 
 
 */
pub struct Datastore {
    pub (crate) root : String,

    /** Project URLs. 
       
        Contains both dead and live urls for the projects known. The latest (i.e. indexed) url for a project is its live url, all previous urls are its past urls currently considered dead.  
     */
    pub (crate) project_urls : Mutex<PropertyStore<String>>,
    pub (crate) project_last_updates : Mutex<PropertyStore<UpdateLog>>,
    pub (crate) project_heads : Mutex<PropertyStore<Heads>>,
    pub (crate) projects_metadata : Mutex<LinkedPropertyStore<Metadata>>,

    /** Mappings of the objects the datastore keeps track of. 
     */
    pub (crate) commits : Mutex<DirectMapping<git2::Oid>>,
    pub (crate) commits_info : Mutex<PropertyStore<CommitInfo>>,
    pub (crate) commits_metadata : Mutex<LinkedPropertyStore<Metadata>>,
    pub (crate) users : Mutex<Mapping<String>>,
    pub (crate) users_metadata : Mutex<LinkedPropertyStore<Metadata>>,
    pub (crate) paths : Mutex<Mapping<String>>,
    pub (crate) paths_metadata : Mutex<LinkedPropertyStore<Metadata>>,
    pub (crate) hashes : Mutex<DirectMapping<git2::Oid>>,

    pub (crate) contents : Mutex<DirectMapping<u64>>,
    pub (crate) contents_data : Mutex<PropertyStore<ContentsData>>,
    pub (crate) contents_metadata : Mutex<LinkedPropertyStore<Metadata>>,

    /** Record of file sizes for each savepoint so that the database can be viewed at certain dates. 
     */ 
    savepoints : Mutex<File>,


}

impl Datastore {

    /** The version of the datastore. 
     
        Versions have backwards compatibility, but newer versions may add extra items, or metadata. When new version is executed, all projects & commits and other items are force updated to make sure that all data that should be obtained are obtained. 
     */
    pub (crate) const VERSION : u16 = 0;

    /** Creates datastore in the specified directory. 
     */
    pub fn from(root : & str) -> Datastore {
        let result = Datastore {
            root : root.to_owned(),
            project_urls : Mutex::new(PropertyStore::new(& format!("{}/project-urls.dat", root))),
            project_last_updates : Mutex::new(PropertyStore::new(& format!("{}/project-updates.dat", root))),
            project_heads : Mutex::new(PropertyStore::new(& format!("{}/project-heads.dat", root))),
            projects_metadata : Mutex::new(LinkedPropertyStore::new(& format!("{}/projects-metadata.dat", root))),

            commits : Mutex::new(DirectMapping::new(& format!("{}/commits.dat", root))),
            commits_info : Mutex::new(PropertyStore::new(& format!("{}/commits-info.dat", root))),
            commits_metadata : Mutex::new(LinkedPropertyStore::new(& format!("{}/commits-metadata.dat", root))),
            users : Mutex::new(Mapping::new(& format!("{}/users.dat", root))),
            users_metadata : Mutex::new(LinkedPropertyStore::new(& format!("{}/users-metadata.dat", root))),
            paths : Mutex::new(Mapping::new(& format!("{}/paths.dat", root))),
            paths_metadata : Mutex::new(LinkedPropertyStore::new(& format!("{}/paths-metadata.dat", root))),
            hashes : Mutex::new(DirectMapping::new(& format!("{}/hashes.dat", root))),

            contents : Mutex::new(DirectMapping::new(& format!("{}/contents.dat", root))),
            contents_data : Mutex::new(PropertyStore::new(& format!("{}/contents-data.dat", root))),
            contents_metadata : Mutex::new(LinkedPropertyStore::new(& format!("{}/contents-metadata.dat", root))),

            savepoints : Mutex::new(OpenOptions::new().read(true).write(true).create(true).open(& format!("{}/savepoints.dat", root)).unwrap()),
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
        let sp = self.create_savepoint();
        // and save the savepoint
        let mut x = self.savepoints.lock().unwrap();
        x.seek(SeekFrom::End(0)).unwrap();
        Savepoint::write(& mut x, & sp);
    }

    pub fn get_savepoint(& self, time : i64) -> Savepoint {
        let mut x = self.savepoints.lock().unwrap();
        let end = x.seek(SeekFrom::End(0)).unwrap();
        x.seek(SeekFrom::Start(0)).unwrap();
        println!("Seeking closest savepoint...");
        let mut result = self.create_savepoint();
        while x.seek(SeekFrom::Current(0)).unwrap() < end {
            let sp = Savepoint::read(& mut x);
            if sp.time() > time {
                break;
            }
            result = sp;
        }
        return result;        
    }

    /** Creates savepoint corresponding to current datastore state, but does not store it. 
     */
    fn create_savepoint(& self) -> Savepoint {
        let mut sp = Savepoint::new();
        sp.add_entry("project_urls", & mut self.project_urls.lock().unwrap().f);
        sp.add_entry("project_last_updates", & mut self.project_last_updates.lock().unwrap().f);
        sp.add_entry("project_heads", & mut self.project_heads.lock().unwrap().f);
        sp.add_entry("projects_metadata", & mut self.projects_metadata.lock().unwrap().f);
        sp.add_entry("commits", & mut self.commits.lock().unwrap().writer.f);
        sp.add_entry("commits_info", & mut self.commits_info.lock().unwrap().f);
        sp.add_entry("commits_metadata", & mut self.commits_metadata.lock().unwrap().f);
        sp.add_entry("users", & mut self.users.lock().unwrap().writer.f);
        sp.add_entry("users_metadata", & mut self.users_metadata.lock().unwrap().f);
        sp.add_entry("paths", & mut self.paths.lock().unwrap().writer.f);
        sp.add_entry("paths_metadata", & mut self.paths_metadata.lock().unwrap().f);
        sp.add_entry("hashes", & mut self.hashes.lock().unwrap().writer.f);
        sp.add_entry("contents", & mut self.contents.lock().unwrap().writer.f);
        sp.add_entry("contents_data", & mut self.contents_data.lock().unwrap().f);
        sp.add_entry("contents_metadata", & mut self.contents_metadata.lock().unwrap().f);
        return sp;
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
        project_last_updates.set(id, & UpdateLog::Ok{time : 0, version : 0});
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

    pub fn store_contents(& self, value : & Vec<u8>) -> (u64, bool) {
        let mut hasher = Sha1::new();
        hasher.update(value);
        let hash = git2::Oid::from_bytes(& hasher.finalize()).unwrap();
        // first create snapshot and then create contents
        let (snapshot_id, _) = self.hashes.lock().unwrap().get_or_create(& hash); 
        return self.store_contents_for_snapshot_id(snapshot_id, value);
    }

    pub fn store_contents_for_snapshot_id(& self, snapshot_id : u64, value : & Vec<u8>) -> (u64, bool) {
        let (contents_id, is_new) = self.contents.lock().unwrap().get_or_create(& snapshot_id);
        if is_new {
            //let mut enc = flate2::write::GzEncoder::new(Vec::new(), Compression::best());
            //enc.write_all(value).unwrap();
            //let encoded = enc.finish().unwrap();
            self.contents_data.lock().unwrap().set(contents_id, value);
        }
        return (contents_id, is_new);
    }

}
