use std::collections::*;
use std::sync::*;
use crate::*;
use crate::datastore::*;
use crate::records::*;
use crate::helpers::*;
use crate::updater::*;

/* This is the updater. 

   Manage the workers and the 
 */
pub struct RepoUpdater<'a> {
    q : ProjectQueue<'a>,
    u : &'a Updater,
    extensions: HashSet<&'static str>,
}

impl<'a> RepoUpdater<'a> {

    /** Creates the updater for given datastore. 
     
        Fills in the datastore mappings and initializes the updater queue based on valid dates. 
     */
    pub fn new(u : & Updater) -> RepoUpdater {
        println!("Creating projects queue...");
        let q = ProjectQueue::new(& u);
        println!("    projects queueued: {}", q.len());
        println!("    valid time:        {}", q.valid_time());
        // create the updater and return it
        return RepoUpdater{
            q,
            u,
            extensions : [
                // generic files
                "README",
                // C
                "c",
                // C++
                "cpp", "h",
                // javascript
                "js",
            ].iter().cloned().collect(),
        };
    } 

    /** Determines whether the contents of given file should be archived or not. 
     
        By default, we archive source code files. 
     */
    pub fn want_contents_of(& self, filename : & str) -> bool {
        let parts = filename.split(".").collect::<Vec<& str>>();
        return self.extensions.contains(parts[parts.len() - 1]);
    }

    /** Single worker thread implementation. 
     */
    pub (crate) fn worker(& self) {
        self.u.thread_start();
        while self.u.thread_next() {
            let t = helpers::now();
            let (id, version) = self.q.deque();
            let task = self.u.new_task(format!("{}", id));
            // if the datastore version is different than the last update version, force the update
            let force = true; //version != Datastore::VERSION;
            let url = self.u.ds.get_project_url(id);
            if force {
                task.update().set_url(& format!("{} [FORCED]", url));
            } else {
                task.update().set_url(& url);
            }
            // TODO update metadata and project url 
            match self.update_project_contents(id, & url, force, & task) {
                Err(e) => {
                    self.u.ds.project_last_updates.lock().unwrap().set(id, & UpdateLog::Error{
                        time : t,
                        version : Datastore::VERSION,
                        error : e.message().to_owned()
                    });
                },
                Ok(_) => {
                    // TODO determine whether there have been any changes, or not to store appropriate result
                    self.u.ds.project_last_updates.lock().unwrap().set(id, & UpdateLog::Ok{
                        time : t,
                        version : Datastore::VERSION
                    });
                }
            }
        }
        self.u.thread_done();
    }

    /** Updates the contents of the project. 
     
     */
    fn update_project_contents(& self, id : u64, url : & str, force : bool, task : & Task) -> Result<(), git2::Error> {
        task.update().set_message("analyzing remote heads...");
        let old_heads = self.u.ds.get_project_heads(id);
        // time to create the repository
        let repo_path = format!("{}/{}", self.u.tmp_folder, id);
        let repo = git2::Repository::init_bare(repo_path.clone())?;
        let mut remote = repo.remote("dcd", & url)?;
        remote.connect(git2::Direction::Fetch)?;
        let new_heads = self.fetch_remote_heads(& mut remote)?;
        // compare the old and new heads, if there are changes, download the repository contents and analyze the inputs 
        let heads_to_be_updated = self.compare_remote_heads(& old_heads, & new_heads, force);
        if ! heads_to_be_updated.is_empty() {
            // fetch the project
            self.fetch_contents(& mut remote, & heads_to_be_updated, task)?;
            // add the new commits 
            let mut commits_updater = CommitsUpdater::new(& repo, self, force, task);
            commits_updater.update(& heads_to_be_updated)?;
            // update the remote heads
            self.u.ds.project_heads.lock().unwrap().set(id, & self.translate_heads(& new_heads));
        }
        // delete the repository from disk
        std::fs::remove_dir_all(& repo_path).unwrap();        
        return Ok(());
    }

    fn fetch_remote_heads(& self, remote : & mut git2::Remote) -> Result<HashMap<String, git2::Oid>, git2::Error> {
        let mut result = HashMap::<String, git2::Oid>::new();
        for x in remote.list()? {
            if x.name().starts_with("refs/heads/") {
                result.insert(String::from(x.name()), x.oid());
            }
        }        
        return Ok(result);
    }

    fn translate_heads(& self, heads : & HashMap<String, git2::Oid>) -> Heads {
        let commits = self.u.ds.commits.lock().unwrap();
        return heads.iter().map(|(name, hash)| (name.to_owned(), commits.get(hash).unwrap())).collect();
    }

    /** Compares the latest heads to current heads and returns the heads that need to be fetched. 
     
        If the list is empty, it means that no changes have been recorded since the last update.
     */
    fn compare_remote_heads(& self, last : & Heads, current : & HashMap<String, git2::Oid>, force : bool) -> Vec<(String, git2::Oid)> {
        let mut result = Vec::<(String,git2::Oid)>::new();
        // lock the commits and check for each new head if it exists in the old ones and if the commit id is the same (and found)
        let commits = self.u.ds.commits.lock().unwrap();
        for (name, hash) in current {
            if ! force {
                if let Some(id) = last.get(name) {
                    if let Some(current_id) = commits.get(hash) {
                        if *id == current_id {
                        }
                    }
                } 
            }
            result.push((name.to_owned(), *hash));
        }
        return result;
    }

    /** Fetches the contents of the respository. 
     */
    fn fetch_contents(& self, remote : & mut git2::Remote, heads : & Vec<(String, git2::Oid)>, task : & Task) -> Result<(), git2::Error> {
        let mut callbacks = git2::RemoteCallbacks::new();
        // TODO the callbacks should actually report stuff
        callbacks.transfer_progress(|progress : git2::Progress| -> bool {
            task.update().set_message(& format!("downloading contents {} / {}",
                progress.received_objects() + progress.indexed_deltas() + progress.indexed_objects(),
                progress.total_deltas() + progress.total_objects() * 2
            ));
            return true;
        });
        let mut opts = git2::FetchOptions::new();
        opts.remote_callbacks(callbacks); 
        let head_names : Vec<String> = heads.iter().map(|(name, _)| name.to_owned()).collect();
        return remote.fetch(& head_names, Some(&mut opts), None);        
    }


    fn update_github_project(& self, id : u64, url : & str) {
        if !url.starts_with("https://github.com") {
            return;
        }

    }

}

/** Commits updater. 
 
    Updates commits in a single repository. 
 
 */
struct CommitsUpdater<'a> {
    repo : &'a git2::Repository,
    ru : &'a RepoUpdater<'a>, 
    task : &'a Task<'a>,
    force : bool,
    visited_commits : HashSet<u64>,
    q : Vec<(git2::Oid, u64)>,
    num_commits : u64, 
    num_snapshots : u64,
    num_changes : u64,
    num_diffs : u64,
    

}

impl<'a> CommitsUpdater<'a> {
    pub fn new(repo : &'a git2::Repository, ru: &'a RepoUpdater, force : bool, task : &'a Task) -> CommitsUpdater<'a> {
        return CommitsUpdater{ repo, ru, force, visited_commits : HashSet::new(), q : Vec::new(), task, num_commits : 0, num_snapshots : 0, num_changes : 0, num_diffs : 0 };
    }

    /** Updates the commits. 
     */
    pub fn update(& mut self, heads : & Vec<(String, git2::Oid)>) -> Result<(), git2::Error> {
        // add the heads
        for (_, hash) in heads {
            self.add_commit(hash);
        }
        // while the queue is not empty process each commit 
        while let Some((hash, id)) = self.q.pop() {
            self.update_status();
            // get the commit information
            let commit = self.repo.find_commit(hash)?;
            let mut commit_info = CommitInfo::new();
            commit_info.committer = self.get_or_create_user(& commit.committer());
            commit_info.committer_time = commit.time().seconds();
            let author = commit.author();
            commit_info.author = self.get_or_create_user(& author);
            commit_info.author_time = author.when().seconds();
            commit_info.message = to_string(commit.message_raw_bytes());
            commit_info.parents = commit.parents().map(|x| self.add_commit(& x.id())).collect();
            // calculate the changes
            commit_info.changes = self.get_commit_changes(& commit)?;
            // output the commit info
            {
                let mut commits_info = self.ru.u.ds.commits_info.lock().unwrap();
                if ! commits_info.has(id) {
                    commits_info.set(id, & commit_info);
                }
            }
            self.num_commits += 1;

        }
        return Ok(());
    }

    fn update_status(& mut self) {
        self.task.update().set_message(& format!("analyzing commits: q: {}, c: {}, s: {}, ch: {}, d: {}", self.q.len(), self.num_commits, self.num_snapshots, self.num_changes, self.num_diffs));
    }

    /** Adds given commit to the queue and returns its id. 
     
        If the commit is already known to the datastore it will not be added to the queue as someone else has already analyzed it, or is currently analyzing. 
     */
    fn add_commit(& mut self, hash : & git2::Oid) -> u64 {
        let (id, is_new) = self.ru.u.ds.commits.lock().unwrap().get_or_create(hash); 
        if self.force {
            if ! self.visited_commits.contains(& id) {
                self.visited_commits.insert(id);
                self.q.push((*hash, id));
            }
        } else {
            if is_new {
                self.q.push((*hash, id));
            }
        }
        return id;
    }

    fn get_or_create_user(& mut self, user : & git2::Signature) -> u64 {
        let (id, is_new) = self.ru.u.ds.users.lock().unwrap().get_or_create(& to_string(user.email_bytes()));
        // add name as metadata in case the user is new
        if is_new {
            // TODO 
        }    
        return id;
    }

    /** Gets commit changes and stores the contents for the documents we care about. 
     */
    fn get_commit_changes(& mut self, commit : & git2::Commit) -> Result<HashMap<u64, u64>, git2::Error> {
        let mut changes = HashMap::<String,git2::Oid>::new();
        if commit.parent_count() == 0 {
            self.calculate_tree_diff(None, Some(& commit.tree()?), & mut changes)?;
            self.num_diffs += 1;
            self.update_status();
        } else {
            for p in commit.parents() {
                self.calculate_tree_diff(Some(& p.tree()?), Some(& commit.tree()?), & mut changes)?;
                self.num_diffs += 1;
                self.update_status();
            }
        }
        // now that we have changes ready, time to convert paths and contents hashes, we do this under a single lock of paths and hashes
        let mut result = HashMap::<u64,u64>::new();
        let mut new_snapshots = Vec::<(String, u64,git2::Oid)>::new();
        {
            let mut paths = self.ru.u.ds.paths.lock().unwrap();
            let mut hashes = self.ru.u.ds.hashes.lock().unwrap();
            for (path, hash) in changes.iter() {
                self.num_changes += 1;
                if self.num_changes % 1000 == 0 {
                    self.update_status();
                }
                let (path_id, _) = paths.get_or_create(path);
                let (hash_id, is_new) = hashes.get_or_create(hash);
                if is_new || self.force {
                    new_snapshots.push((path.to_owned(), hash_id, *hash));
                }
                result.insert(path_id, hash_id);
            }
        }
        // look at the new snapshots, determine if they are to be downloaded and download those that we are interested in. 
        for (path, id, hash) in new_snapshots {
            if self.ru.want_contents_of(& path) {
                let (contents_id, is_new) = self.ru.u.ds.contents.lock().unwrap().get_or_create(& id);
                if let Ok(blob) = self.repo.find_blob(hash) {
                    let bytes = ContentsData::from(blob.content());
                    if is_new {
                        self.ru.u.ds.contents_data.lock().unwrap().set(contents_id, & bytes);
                    }
                    self.num_snapshots += 1;
                    if self.num_snapshots % 1000 == 0 {
                        self.update_status();
                    }
                }
            }
        }

        return Ok(result);
    }

    fn calculate_tree_diff(& mut self, parent : Option<& git2::Tree>, commit : Option<& git2::Tree>, changes : & mut HashMap<String, git2::Oid>) -> Result<(), git2::Error> {
        let diff = self.repo.diff_tree_to_tree(parent, commit, None)?;
        for delta in diff.deltas() {
            match delta.status() {
                git2::Delta::Added | git2::Delta::Modified | git2::Delta::Deleted | git2::Delta::Copied => {
                    if let Some(p) = delta.new_file().path().unwrap().to_str() {
                        changes.insert(String::from(p), delta.new_file().id());
                    }
                },
                git2::Delta::Renamed => {
                    if let Some(po) = delta.old_file().path().unwrap().to_str() {
                        changes.insert(String::from(po), git2::Oid::zero());
                        if let Some(p) = delta.new_file().path().unwrap().to_str() {
                            changes.insert(String::from(p), delta.new_file().id());
                        }
                    }
                },
                // this should not really happen in diffs of commits
                _ => {
                    panic!("What to do?");
                }
            }
        }
        return Ok(());
    }    
}



/** Priority queue for the projects based on their update times with the oldest projects having the highest priority. 
 */
struct ProjectQueue<'a> {
    q : Mutex<BinaryHeap<std::cmp::Reverse<QueuedProject>>>,
    qcv : Condvar,
    u : &'a Updater,
}

impl<'a> ProjectQueue<'a> {
    /** Creates new projects queue and fills it with projects from given datastore. 
     */
    fn new(u : & Updater) -> ProjectQueue {
        let result = ProjectQueue{
            q : Mutex::new(BinaryHeap::new()),
            qcv : Condvar::new(),
            u, 
        };
        {
            let mut q = result.q.lock().unwrap();
            for (id, last_update) in u.ds.project_last_updates.lock().unwrap().latest_iter() {
                if last_update.is_ok() {
                    q.push(std::cmp::Reverse(QueuedProject{
                        id, 
                        last_update_time : last_update.time(),
                        version : last_update.version()
                    }));
                }
            }
        }
        return result;
    }

    fn deque(& self) -> (u64, u16) {
        let mut projects = self.q.lock().unwrap();
        while projects.is_empty() {
            self.u.thread_running_to_idle();
            projects = self.qcv.wait(projects).unwrap();
            self.u.thread_idle_to_running();
        }
        let x = projects.pop().unwrap().0;
        return (x.id, x.version);
    }

    fn enqueue(& self, id : u64, time : i64) {
        let mut projects = self.q.lock().unwrap();
        projects.push(std::cmp::Reverse(QueuedProject{id, last_update_time : time, version : Datastore::VERSION }));
        self.qcv.notify_one();
    }

    /** Returns the number of projects in the queue.
     */
    fn len(& self) -> usize {
        return self.q.lock().unwrap().len();
    }

    /** Returns the oldest update time. 
     */
    fn valid_time(& self) -> i64 {
        let q = self.q.lock().unwrap();
        if q.is_empty() {
            return 0;
        } else {
            return q.peek().unwrap().0.last_update_time;
        }
    }
}


/** The queued object record. 
  
    The records are ordered by the time of the last update. 
 */
#[derive(Eq)]
struct QueuedProject {
    id : u64, 
    last_update_time : i64,
    version : u16,
}

impl Ord for QueuedProject {
    fn cmp(& self, other : & Self) -> std::cmp::Ordering {
        return self.last_update_time.cmp(& other.last_update_time);
    }
}

impl PartialOrd for QueuedProject {
    fn partial_cmp(& self, other : & Self) -> Option<std::cmp::Ordering> {
        return Some(self.last_update_time.cmp(& other.last_update_time));
    }
}

impl PartialEq for QueuedProject {
    fn eq(& self, other : & Self) -> bool {
        return self.last_update_time == other.last_update_time;
    }
}



