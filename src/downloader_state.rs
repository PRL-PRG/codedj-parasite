use std::collections::hash_map::{HashMap, Entry};
use std::collections::HashSet;
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

    commit_ids_ : Mutex<HashMap<git2::Oid, u64>>,
    commit_ids_file_ : Mutex<File>,
    commits_ : Mutex<Vec<Commit>>,
    commits_file_ : Mutex<File>,
    commit_parents_file_ : Mutex<File>,

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
        let commit_ids_file = format!("{}/commit_ids.csv", root_folder);
        {
            let mut f = File::create(& commit_ids_file).unwrap();
            writeln!(& mut f, "hash,id");
        }
        let commits_file = format!("{}/commits.csv", root_folder);
        {
            let mut f = File::create(& commits_file).unwrap();
            writeln!(& mut f, "id,authorId,aythorTime,committerId,committerTime,source");
        }
        let commit_parents_file = format!("{}/commit_parents.csv", root_folder);
        {
            let mut f = File::create(& commit_parents_file).unwrap();
            writeln!(& mut f, "id,parentId");
        }

        let result = DownloaderState{
            dcd_ : DCD::new(root_folder),
            live_urls_ : Mutex::new(HashSet::new()),
            users_ : Mutex::new(HashMap::new()),
            users_file_ : Mutex::new(OpenOptions::new().append(true).open(& users_file).unwrap()),
            commit_ids_ : Mutex::new(HashMap::new()),
            commit_ids_file_ : Mutex::new(OpenOptions::new().append(true).open(& commit_ids_file).unwrap()),
            commits_ : Mutex::new(Vec::new()),
            commits_file_ : Mutex::new(OpenOptions::new().append(true).open(& commits_file).unwrap()),
            commit_parents_file_ : Mutex::new(OpenOptions::new().append(true).open(& commit_parents_file).unwrap()),
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
    fn get_or_create_user_(& self, email : & str) -> (u64, bool) {
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
        match self.get_or_create_user_(email) {
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

    /** Takes iterator of git2 hashes and returns a map converting each hash to id and a set of new hashes.
     */
    pub fn get_or_add_commits<'a>(& self, hashes : & mut std::iter::Iterator<Item = &'a git2::Oid>) -> (HashMap<git2::Oid, CommitId>, HashSet<CommitId>) {
        let mut commits = HashMap::new();
        let mut new_commits = HashSet::new();
        let mut delta = HashMap::new();
        {
            let mut commit_ids = self.commit_ids_.lock().unwrap();
            for h in hashes {
                let new_id = commit_ids.len() as u64;
                match commit_ids.entry(*h) {
                    Entry::Occupied(ref entry) => {
                        commits.insert(*h, *entry.get());
                    },
                    Entry::Vacant(entry) => {
                        entry.insert(new_id);
                        new_commits.insert(new_id);
                        commits.insert(*h, new_id);
                        delta.insert(*h, new_id);
                    }
                }
            }
        }
        // write the delta commits
        let mut commit_ids_file = self.commit_ids_file_.lock().unwrap();
        for x in delta {
            writeln!(*commit_ids_file, "{},{}", x.0, x.1).unwrap();
        }
        // and return what is new
        return (commits, new_commits);
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