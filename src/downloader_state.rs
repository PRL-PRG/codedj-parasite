use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, Condvar};
use std::fs::OpenOptions;
use crate::*;

/** The state of the downloader. 
 


 */
pub struct DownloaderState {
    // basic downloader state
    pub dcd_ : DCD,

    // set of live urls
    live_urls_ : Mutex<HashSet<String>>,

    // email to user translation and a file to append users to
    users_ : Mutex<HashMap<String, u64>>,
    users_file_ : Mutex<File>,

}


impl DownloaderState {

    /** Creates new downloader in the given folder, clearing all previous data. 
     
        Be sure you understand what you are doing. 
     */
    pub fn create_new(root_folder : & str) -> DownloaderState {
        if std::path::Path::new(& root_folder).exists() {
            std::fs::remove_dir_all(& root_folder).unwrap();
        }
        std::fs::create_dir_all(root_folder).unwrap();
        let users_file = format!("{}/users.csv", root_folder);
        {
            let mut f = File::create(& users_file).unwrap();
            writeln!(& mut f, "id,email,name");
        }

        let result = DownloaderState{
            dcd_ : DCD::new(root_folder),
            live_urls_ : Mutex::new(HashSet::new()),
            users_ : Mutex::new(HashMap::new()),
            users_file_ : Mutex::new(OpenOptions::new().append(true).open(& users_file).unwrap()),
        };

        return result;
    }

    pub fn continue_from(root_folder : & str) -> DownloaderState {
        panic!("not implemented");
    }

    /** Adds a project. 
     */
    pub fn add_project(& mut self, url : & str) -> Option<Project> {
        let mut live_urls = self.live_urls_.lock().unwrap();
        if live_urls.insert(String::from(url)) {
            let id = self.dcd_.num_projects_;
            let p = Project::create_new(id, & url, & self.dcd_.get_project_root(id));
            self.dcd_.num_projects_ += 1;
            return Some(p);
        } else {
            return None;
        }
    }

    // helper for the user creation so that we hold the mutex for shortest time
    fn get_or_create_user_in_mem(& self, email : & str) -> (u64, bool) {
        let mut users = self.users_.lock().unwrap();
        match users.get(email) {
            Some(id) => {
                return (*id, false);
            },
            None => {
                let id = users.len() as u64;
                users.insert(email.to_string(), id);
                return (id, true);
            }
        }
    }

    pub fn get_or_create_user(& self, email : & str, name : & str) -> u64 {
        match self.get_or_create_user_in_mem(email) {
            (id, false) => {
                return id;
            },
            (id, true) => {
                let user = User{ id, email : email.to_string(), name : name.to_string()};
                let mut users_file = self.users_file_.lock().unwrap();
                user.write_to_csv(& mut users_file);
                return id;
            }
        }
    }

    /*    
    pub fn add_projects<T>(& mut self, projects: & mut Iterator<Item = (String, & T)>, initializer : Option<fn(& mut Project, & T)>) -> u64 {
        let mut live_urls = self.live_urls_.lock().unwrap();
        let mut count = 0;
        for (url, source) in projects {
            if live_urls.insert(String::from(& url)) {
                let id = self.dcd_.num_projects_;
                let mut p = Project::create_new(id, & url, & self.dcd_.get_project_root(id));
                if let Some(init) = initializer {
                    init(& mut p, source);
                }
                self.dcd_.num_projects_ += 1;
                count += 1;
            }
        }
        return count;
    }
    */
}