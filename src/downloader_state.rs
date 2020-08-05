use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, Condvar};
use crate::*;

/** The state of the downloader. 
 


 */
pub struct DownloaderState {
    // basic downloader state
    pub dcd_ : DCD,

    // set of live urls
    live_urls_ : Mutex<HashSet<String>>

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
        let result = DownloaderState{
            dcd_ : DCD::new(root_folder),
            live_urls_ : Mutex::new(HashSet::new()),
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