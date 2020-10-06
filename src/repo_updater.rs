use std::collections::*;
use crate::datastore::*;
use crate::records::*;
use crate::helpers::*;
use crate::updater::*;
use crate::github::*;

/* This is the updater. 

   Manage the workers and the 
 */
pub struct RepoUpdater<'a, 'b> {
    ds : &'a Datastore,
    gh : &'b Github,
}

impl<'a, 'b> RepoUpdater<'a, 'b> {

    /** Creates the updater for given datastore. 
     
        Fills in the datastore mappings and initializes the updater queue based on valid dates. 
     */
    pub fn new(ds : &'a Datastore, gh : &'b Github) -> RepoUpdater<'a,'b> {
        // create the updater and return it
        return RepoUpdater{
            ds,
            gh
        };
    } 

    /** Updates the project and returns the update result. 
     
        This is either an error (in case of an error, the updater must guarantee that any information already committed to the data store is valid and complete), false if there were no changes since the last time the project was updated, or true if there were any changes. 
     */
    pub fn update_project(& self, tmp_folder: & str, id : u64, version : u16, task : & Task) -> Result<bool, std::io::Error> {
        // if the datastore version is different than the last update version, force the update
        let force = version != Datastore::VERSION;
        let mut url = self.ds.get_project_url(id);
        if force {
            task.update().set_url(& format!("{} [FORCED]", url));
        } else {
            task.update().set_url(& url);
        }
        // update metadata and project url 
        task.update().set_message("checking metadata...");
        let mut updated = self.update_github_project(id, & mut url, task)?;
        match self.update_project_contents(tmp_folder, id, & url, force, task) {
            Ok(value) => updated = updated || value,
            Err(cause) => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", cause)));
            }
        }
        return Ok(updated);
    }

    /** Updates the contents of the project. 
     
     */
    fn update_project_contents(& self, tmp_folder: & str, id : u64, url : & str, force : bool, task : & Task) -> Result<bool, git2::Error> {
        task.update().set_message("analyzing remote heads...");
        let old_heads = self.ds.get_project_heads(id);
        // time to create the repository
        let repo_path = format!("{}/{}", tmp_folder, id);
        let repo = git2::Repository::init_bare(repo_path.clone())?;
        let mut remote = repo.remote("dcd", & url)?;
        remote.connect(git2::Direction::Fetch)?;
        let new_heads = self.fetch_remote_heads(& mut remote)?;
        // compare the old and new heads, if there are changes, download the repository contents and analyze the inputs 
        let heads_to_be_updated = self.compare_remote_heads(& old_heads, & new_heads, force);
        let updated = ! heads_to_be_updated.is_empty();
        if updated {
            // fetch the project
            self.fetch_contents(& mut remote, & heads_to_be_updated, task)?;
            // add the new commits 
            let mut commits_updater = CommitsUpdater::new(& repo, self.ds, force, task);
            commits_updater.update(& heads_to_be_updated)?;
            // update the remote heads
            self.ds.project_heads.lock().unwrap().set(id, & self.translate_heads(& new_heads));
        }
        // delete the repository from disk
        std::fs::remove_dir_all(& repo_path).unwrap();        
        return Ok(updated);
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
        let commits = self.ds.commits.lock().unwrap();
        return heads.iter().map(|(name, hash)| (name.to_owned(), commits.get(hash).unwrap())).collect();
    }

    /** Compares the latest heads to current heads and returns the heads that need to be fetched. 
     
        If the list is empty, it means that no changes have been recorded since the last update.
     */
    fn compare_remote_heads(& self, last : & Heads, current : & HashMap<String, git2::Oid>, force : bool) -> Vec<(String, git2::Oid)> {
        let mut result = Vec::<(String,git2::Oid)>::new();
        // lock the commits and check for each new head if it exists in the old ones and if the commit id is the same (and found)
        let commits = self.ds.commits.lock().unwrap();
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


    fn update_github_project(& self, id : u64, url : & mut String, task : & Task) -> Result<bool, std::io::Error> {
        if !url.starts_with("https://github.com") {
            return Ok(false);
        }
        task.update().set_message("github metadata");
        let mut metadata = self.gh.get_repo(& url, task)?;
        let new_url = format!("{}.git",metadata["html_url"]).to_lowercase();
        let mut changed = false;
        if *url != new_url {
            // update the url
            *url = new_url;
            task.update().set_url(url);
            self.ds.project_urls.lock().unwrap().set(id, & url);
            changed = true;
        }
        // now minimize the metadata object and see if it has changed since last time
        RepoUpdater::filter_github_metadata_keys(& mut metadata, true);
        let metadata_str = metadata.to_string();
        // try storing always, if new, no need to check the old value as it must have been different
        // TODO this is really wasteful - can &[u8] be made the value type of the contents property store?
        let (contents_id, is_new) = self.ds.store_contents(& Vec::from(metadata_str.as_bytes()));
        let mut metadata_change = is_new;
        if ! metadata_change {
            let old_id = self.ds.projects_metadata.lock().unwrap().get_metadata(id, "github_metadata");
            match old_id {
                Some(real_old_id) => metadata_change = contents_id != real_old_id.parse::<u64>().unwrap(),
                None => metadata_change = true,
            }
        }
        if metadata_change {
            self.ds.projects_metadata.lock().unwrap().set(id, & Metadata{key : "github_metadata".to_owned(), value : format!("{}", contents_id)});
            changed = true;
        }

        return Ok(changed);
    }

    fn filter_github_metadata_keys(json : & mut json::JsonValue, is_root : bool) {
        let mut x = Vec::new();
        for (key, value) in json.entries_mut() {
            if is_root && key == "html_url" {
                // do nothing
            } else if key.ends_with("_url") || key == "url" {
                x.push(key.to_string());
                continue;
            } 
            RepoUpdater::filter_github_metadata_keys(value, false);
        }
        for k in x {
            json.remove(&k);
        }
    }

}

/** Commits updater. 
 
    Updates commits in a single repository. 
 
 */
struct CommitsUpdater<'a> {
    repo : &'a git2::Repository,
    ds : &'a Datastore, 
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
    pub fn new(repo : &'a git2::Repository, ds: &'a Datastore, force : bool, task : &'a Task) -> CommitsUpdater<'a> {
        return CommitsUpdater{ repo, ds, force, visited_commits : HashSet::new(), q : Vec::new(), task, num_commits : 0, num_snapshots : 0, num_changes : 0, num_diffs : 0 };
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
            self.update_status("commit");
            // get the commit information
            let commit = self.repo.find_commit(hash)?;
            self.update_status("commit found");
            let mut commit_info = CommitInfo::new();
            commit_info.committer = self.get_or_create_user(& commit.committer());
            commit_info.committer_time = commit.time().seconds();
            self.update_status("committer found");
            let author = commit.author();
            commit_info.author = self.get_or_create_user(& author);
            commit_info.author_time = author.when().seconds();
            commit_info.message = to_string(commit.message_raw_bytes());
            self.update_status("commit parents");
            commit_info.parents = commit.parents().map(|x| self.add_commit(& x.id())).collect();
            // calculate the changes
            commit_info.changes = self.get_commit_changes(& commit)?;
            // output the commit info
            {
                let mut commits_info = self.ds.commits_info.lock().unwrap();
                if ! commits_info.has(id) {
                    commits_info.set(id, & commit_info);
                }
            }
            self.num_commits += 1;

        }
        return Ok(());
    }

    fn update_status(& mut self, action : & str) {
        self.task.update().set_message(& format!("analyzing commits: q: {}, c: {}, s: {}, ch: {}, d: {} [{}]", self.q.len(), self.num_commits, self.num_snapshots, self.num_changes, self.num_diffs, action));
    }

    /** Adds given commit to the queue and returns its id. 
     
        If the commit is already known to the datastore it will not be added to the queue as someone else has already analyzed it, or is currently analyzing. 
     */
    fn add_commit(& mut self, hash : & git2::Oid) -> u64 {
        let (id, is_new) = self.ds.commits.lock().unwrap().get_or_create(hash); 
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
        let (id, is_new) = self.ds.users.lock().unwrap().get_or_create(& to_string(user.email_bytes()));
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
            self.update_status("diff");
            self.calculate_tree_diff(None, Some(& commit.tree()?), & mut changes)?;
            self.num_diffs += 1;
        } else {
            for p in commit.parents() {
                self.update_status("diff");
                self.calculate_tree_diff(Some(& p.tree()?), Some(& commit.tree()?), & mut changes)?;
                self.num_diffs += 1;
            }
        }
        self.update_status("changes");
        // now that we have changes ready, time to convert paths and contents hashes, we do this under a single lock of paths and hashes
        let mut result = HashMap::<u64,u64>::new();
        let mut new_snapshots = Vec::<(String, u64,git2::Oid)>::new();
        {
            let mut paths = self.ds.paths.lock().unwrap();
            let mut hashes = self.ds.hashes.lock().unwrap();
            for (path, hash) in changes.iter() {
                self.num_changes += 1;
                if self.num_changes % 1000 == 0 {
                    self.update_status("changes");
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
            if let Some(category_) = Updater::want_contents_of(& path) {
                if self.ds.contents.lock().unwrap().get(& id).is_none() {
                    if let Ok(blob) = self.repo.find_blob(hash) {
                        let bytes = ContentsData::from(blob.content());
                        self.ds.store_contents_for_snapshot_id(id, & bytes);
                        self.num_snapshots += 1;
                        if self.num_snapshots % 100 == 0 {
                            self.update_status("snapshots");
                        }
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
