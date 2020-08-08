use std::sync::Mutex;

use crate::*;

/** R/W manager for the downloader database to be used by the downloade & friends.
 
    
 */
pub struct DatabaseManager {
    // root folder where all the data lives
    root_ : String, 
    // set of live urls for each active project so that we can easily check for project duplicites
    // TODO in the future, we also need set of dead urls
    // and we really only need to build this lazily when needed IMHO
    live_urls_ : Mutex<HashSet<String>>,

    // number of projects (dead and alive), used for generating new project ids...
    num_projects_ : Mutex<u64>,
}

impl DatabaseManager {

    /** Creates new database manager and initializes its database in the given folder.
     
        If the folder exists, all its contents is deleted first. 
     */
    pub fn initialize_new(root_folder : String) -> DatabaseManager {
        // initialize the folder
        if std::path::Path::new(& root_folder).exists() {
            std::fs::remove_dir_all(& root_folder).unwrap();
        }
        std::fs::create_dir_all(& root_folder).unwrap();
        // create the necessary files

        // create the manager and return it
        return DatabaseManager{
            root_ : root_folder,
            live_urls_ : Mutex::new(HashSet::new()),
            num_projects_ : Mutex::new(0),
        };
    }

    /** Creates database manager from existing database folder.
     */
    pub fn from(root_folder : String) -> DatabaseManager {
        unimplemented!();
    }

    /** Creates new project with given url and source.
     
        If the url is new, returns the id assigned to the project, ortherwise returns None. The project log is initialized with init message of the appropriate url and source.  
     */
    pub fn add_project(& self, url : String) -> Option<ProjectId> {
        let mut live_urls = self.live_urls_.lock().unwrap(); // we lock for too long, but not care now
        // don't know how to do this on single lookup in rust yet
        if live_urls.contains(& url) {
            return None;
        }
        // get the project id
        let mut num_projects = self.num_projects_.lock().unwrap();
        let id = *num_projects as ProjectId;
        // get the project folder and create it 
        let project_folder = self.get_project_folder(id);
        std::fs::create_dir_all(& project_folder).unwrap();
        // initialize the log for the project
        {
            let mut project_log = record::ProjectLog::new();
            project_log.add(record::ProjectLogEntry::init(url.clone()));
            project_log.save(& self.get_project_folder(id));
        }
        // now that the log is ok, increment total number of projects, add the live url and return the id
        *num_projects += 1;
        live_urls.insert(url);
        return Some(id);
    }


    /** Returns the root folder for project of given id. 
     */
    fn get_project_folder(& self, id : ProjectId) -> String {
        return format!("{}/projects/{}/{}", self.root_, id % 1000, id);
    }

}