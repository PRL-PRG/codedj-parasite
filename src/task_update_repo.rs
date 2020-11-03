use std::collections::*;

use crate::datastore::*;
use crate::updater::*;
use crate::records::*;
use crate::helpers;


/** Provides a full update of the given repository. 
 
    - figure out the latest update, i.e. if there is one, if it was error (in which case ignore)
    - then we have to decide whether we have the appropriate substore in memory
    - clone the project
    - in theory change the substore and store the information
    - see if we have the substore in memory
    - update the project
    
 */
pub (crate) fn task_update_repo(updater : & Updater, task : TaskStatus) -> Result<(), std::io::Error> {
    let mut ru = RepoUpdater::new(updater, task);
    if ru.can_be_updated() {
        // validate the url and project metadata
        ru.check_metadata()?;
        // update the project contents
        // TODO the check
        ru.update_repository();
    }

    return Ok(());
}

/** A convenience struct because I do not want to drag everything as function arguments.
 */
struct RepoUpdater<'a> {
    updater : &'a Updater,
    ds : &'a Datastore,
    task : TaskStatus<'a>,
    id : u64,
    project : Project,
    force : bool,
    substore : StoreKind,
    changed : bool,
    contents_changed : bool,
    local_folder : String,
    visited_commits : HashMap<Hash, u64>,
    users : HashMap<String, u64>,
    paths : HashMap<String, u64>,
    q : Vec<(Hash, u64)>,
}

impl<'a> RepoUpdater<'a> {

    /** Creates new repository updater. 
     */
    fn new(updater : &'a Updater, task : TaskStatus<'a>) -> RepoUpdater<'a> {
        if let Task::UpdateRepo{id, last_update_time : _ } = task.task {
            return RepoUpdater {
                updater,
                ds : & updater.ds,
                task,
                id,
                project : updater.ds.get_project(id).unwrap(),
                force : false,
                substore : StoreKind::Unspecified,
                changed : false,
                contents_changed : false,
                local_folder : format!("{}/repo_clones/{}", updater.ds.root_folder(), id),
                visited_commits : HashMap::new(),
                users : HashMap::new(),
                paths : HashMap::new(),
                q : Vec::new(),
            };
        } else {
            panic!("Invalid task kind");
        }
    }

    /** Checks whether the current project can be updated at this time.
     
        TODO we should ideally do something smatrter when there is an error during the update, i.e. dependning on the error, etc. 
     */
    fn can_be_updated(& mut self) -> bool {
        // check if there was error during the update, in which case we do not attempt to update the project again
        if let Some(last_update) = self.ds.get_project_last_update(self.id) {
            match last_update {
                ProjectUpdateStatus::Error{time : _, version : _, error : _ } => return false,
                _ => {}
            }
            // if the version of the last update differs from current version of the datastore, a forced update should be performed
            if Datastore::VERSION != last_update.version() {
                self.force = true;
            }
        }
        // get the substore and determine if the substore is not active, in which case do not allow the update. Unspecified projects are always allowed to do the first update
        self.substore = self.ds.get_project_substore(self.id);
        if self.substore != StoreKind::Unspecified && ! self.ds.substore(self.substore).is_loaded() {
            return false;
        }
        return true;
    }

    fn check_metadata(& mut self) -> Result<(), std::io::Error> {
        match & self.project {
            /* There is nothing extra we can do for raw git projects as there are no metadata associated with them. 
             */
            Project::Git{url : _} => {
                // nop
            },
            /* For github projects, we get github metadata. Store these if changed and update the project url, if different (this is a project rename).  
             */
            Project::GitHub{user_and_repo} => {
                self.task.info("checking metadata...");
                let mut metadata = self.updater.github.get_repo(user_and_repo, & self.task)?;
                // check project rename
                let new_url = format!("{}.git",metadata["html_url"]).to_lowercase();
                self.check_url_change(& new_url)?;
                // clean the metadata and store, if applicable
                filter_github_metadata_keys(& mut metadata, true);
                self.changed = self.ds.update_project_metadata_if_differ(self.id, Metadata::GITHUB_METADATA.to_owned(), metadata.to_string());
            }
        }
        return Ok(());
    }

    /** Compares the newly obtained project url to the one stored and records project rename if applicable. 
     */
    fn check_url_change(& mut self, new_url : & str) -> Result<(), std::io::Error> {
        if let Some(new_project) = Project::from_url(new_url) {
            if self.project != new_project {
                self.project = new_project;
                self.task.info(format!("project url changed to {}", new_url));
                self.ds.update_project(self.id, & self.project);
                self.changed = true;
            }
            return Ok(());
        } else {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Invalid new url {}", new_url)));
        }
    }

    /** Updates the repository. 
     
        First loads the previous heads, if any and compares these to the heads fetched from the repository. If there are differences, clones the full repository and performs an update of its contents. 
     */
    fn update_repository(& mut self) -> Result<(), git2::Error> {
        // create local repository
        // TODO reuse repository if found on disk already? 
        let repo = git2::Repository::init_bare(self.local_folder.clone())?;
        let mut remote = repo.remote("dcd", & self.project.clone_url())?;
        remote.connect(git2::Direction::Fetch)?;
        // get own and remote heads and compare them 
        let last_heads = self.get_latest_heads();
        let mut remote_heads = self.get_remote_heads(& mut remote)?;
        let heads_to_fetch = self.compare_project_heads(& last_heads, & mut remote_heads);
        // fetch the repository from the remote and analyze its contents
        if ! heads_to_fetch.is_empty() {
            self.clone_repository(& mut remote, & heads_to_fetch)?;
            // TODO determine the substore for the project / update when necessary and either terminate the update, or continue
            let substore = self.ds.substore(StoreKind::Unspecified);
            let mut i = 1;
            for head in heads_to_fetch.iter() {
                self.task.info(format!("analyzing branch {} ({} of {})", head, i, heads_to_fetch.len()));
                let (id, hash) = remote_heads.get_mut(head).unwrap();
                *id = self.analyze_branch(& repo, *hash, substore)?;
                i += 1;
            }
        }
        // if either the heads to fetch were not empty (i.e. there was a content to download), or there was no content, but the number of heads is different (some heads were deleted), store the updated heads
        if ! heads_to_fetch.is_empty() || remote_heads.len() != last_heads.len() {
            self.ds.update_project_heads(self.id, & remote_heads);
            self.contents_changed = true;
        }
        return Ok(());
    }

    /** Returns the remote heads as of last analysis. 
     
        If no previous update is found, returns empty heads. Note that if project changes store, it receives an extra tombstone empty heads so that the update in the new substore will be full. 
     */
    fn get_latest_heads(& mut self) -> ProjectHeads {
        // if we are performing a forced update, return empty result so that all heads are fetched again 
        if self.force {
            return ProjectHeads::new();
        }
        if let Some(heads) = self.ds.get_project_heads(self.id) {
            return heads;
        } else {
            return ProjectHeads::new();
        }
    }

    /** Returns current heads from the remote. 
     
        Does not assign ids to the obtained heads, as these will be obtained later from the latest heads, or from the datastore itself. 
     */
    fn get_remote_heads(& mut self, remote : & mut git2::Remote) -> Result<ProjectHeads, git2::Error> {
        let mut result = ProjectHeads::new();
        for x in remote.list()? {
            if x.name().starts_with("refs/heads/") {
                result.insert(String::from(x.name()), (0, x.oid()));
            }
        }        
        return Ok(result);
    }

    /** Compares the last heads of the repository with the new ones and returns the list of heads to be downloaded.

        For unchanged heads, updates their id from the last records. 
    */
    fn compare_project_heads(& self, last : & ProjectHeads, current : & mut ProjectHeads) -> Vec<String> {
        let mut result = Vec::<String>::new();
        for (name, (id, hash)) in current.iter_mut() {
            if let Some((last_id, last_hash)) = last.get(name) {
                if hash == last_hash {
                    *id = *last_id;
                    if ! self.force {
                        continue;
                    }
                }
            }
            // if the hashes differ, or the head is not present, add it to the list of heads to be fetched 
            result.push(name.to_owned());
        }
        return result;
    }

    /** Clones the repository from given remote. 
     
        Clones the specified refs and reports the progress via the task message updates. 
     */
    fn clone_repository(& mut self, remote : & mut git2::Remote, heads : & Vec<String>) -> Result<(), git2::Error> {
        self.task.info("downloading repository contents...");
        let mut callbacks = git2::RemoteCallbacks::new();
        callbacks.transfer_progress(|progress : git2::Progress| -> bool {
            self.task.progress(
                progress.received_objects() + progress.indexed_deltas() + progress.indexed_objects(),
                progress.total_deltas() + progress.total_objects() * 2
            );
            return true;
        });
        let mut opts = git2::FetchOptions::new();
        opts.remote_callbacks(callbacks); 
        return remote.fetch(& heads, Some(&mut opts), None);        
    }

    /** Analyzes given branch, starting at a head commit and returns the id of the head commit. 
     
     */
    fn analyze_branch(& mut self, repo : & git2::Repository, head : Hash, substore : & Substore) -> Result<u64, git2::Error> {
        // add head to the queue
        let head_id = self.add_commit(& head, substore);
        // process the queue
        while let Some((hash, id)) = self.q.pop() {
            // get the commit and process it
            let commit = repo.find_commit(hash)?;
            let mut commit_info = CommitInfo::new();
            // get committer & author information
            commit_info.committer = self.get_or_create_user(& commit.committer(), substore);
            commit_info.committer_time = commit.time().seconds();
            let author = commit.author();
            commit_info.author = self.get_or_create_user(& author, substore);
            commit_info.author_time = author.when().seconds();
            // get parent ids and add parents to the queue
            commit_info.parents = commit.parents().map(|x| self.add_commit(& x.id(), substore)).collect();
            // and finally, calculate the changes
            commit_info.changes = self.get_commit_changes(repo, & commit, substore)?;
            // store the commit info
            substore.add_commit_info_if_missing(id, & commit_info);
        }
        return Ok(head_id);
    }

    /** Adds the given commit to the queue.
     
        Returns the id assigned to the commit. Only adds the commit to the queue if the commit did not exist before. Before going to the datastore, local cache is consulted first. 

        If the update is forced, all commits are reanalyzed even if they exist in the datastore
     */ 
    fn add_commit(& mut self, hash : & Hash, substore : & Substore) -> u64 {
        if let Some(id) = self.visited_commits.get(hash) {
            if ! self.force {
                return *id;
            }
        }
        let (id, is_new) = substore.get_or_create_commit_id(hash);
        self.visited_commits.insert(*hash, id);
        if is_new {
            self.q.push((*hash, id)); 
        }
        return id;
    }

    fn get_or_create_user(& mut self, user : & git2::Signature, substore : & Substore) -> u64 {
        let email = helpers::to_string(user.email_bytes());
        if let Some(id) = self.users.get(& email) {
            return *id;
        } else {
            let (id, _) = substore.get_or_create_user_id(& email);
            // add to cache
            self.users.insert(email, id);
            // TODO check the username against usernames in the metadata of the user and so on? 
            return id;
        }
    }

    fn get_commit_changes(& mut self, repo : & git2::Repository, commit : & git2::Commit, substore : & Substore) -> Result<HashMap<u64, u64>, git2::Error> {
        // first create the changes map and populate it by changes between the commit and its parents, or the full commit if the commit has no parents
        let mut changes = HashMap::<String, Hash>::new();
        if commit.parent_count() == 0 {
            calculate_tree_diff(repo, None, Some(& commit.tree()?), & mut changes)?;
        } else {
            for p in commit.parents() {
                calculate_tree_diff(repo, Some(& p.tree()?), Some(& commit.tree()?), & mut changes)?;
            }
        }
        // time to convert paths to hashes
        let result = self.convert_and_register_changes(changes, substore);
        // now let's look over the changes and see if there is any file that we should snapshot
        for (path_id, hash_id, path, hash, is_new_hash) in result.iter() {
            // TODO TODO TODO TODO
            // TODO TODO TODO TODO
            // TODO TODO TODO TODO
            // TODO TODO TODO TODO
            // TODO TODO TODO TODO
            // TODO TODO TODO TODO
            // TODO TODO TODO TODO
        }
        // finally get only the things we need for changes and return
        return Ok(result.into_iter().map(|(path_id, hash_id, _, _, _)| (path_id, hash_id)).collect());
    }

    /** Converts the paths and hashes expressed as strings and SHA hashes to their respective ids and returns a vector containing all. 
     
        The visited paths are cached locally for better performance and we try to avoid grabbing the lock in the datastore unless we really need to. 

        Returns : path id, hash id, path, hash, is hash new?
     */
    fn convert_and_register_changes(& mut self, changes : HashMap<String, Hash>, substore : & Substore) -> Vec<(u64, u64, String, Hash, bool)> {
        // contents hashes are easy, we just go straight to the substore to get us the hash ids and whether they are new or not
        let hashes = changes.iter().map(|(_, hash)| *hash ).collect::<Vec<Hash>>();
        let hash_ids = substore.convert_hashes_to_ids(& hashes);
        // for paths we use two stage process, first convert what we can from the local cache, then convert the others via the substore and merge
        let mut unknown_paths = Vec::<String>::new();
        let mut paths = changes.into_iter().map(|(path, hash)| { // keep the hash around so that we can zip once
            if let Some(id) = self.paths.get(& path) {
                return (*id, path, hash);
            } else {
                unknown_paths.push(path.clone());
                return (0, path, hash);
            }
        }).collect::<Vec<(u64, String, Hash)>>();
        // get the missing path ids
        if ! unknown_paths.is_empty() {
            let path_ids = substore.convert_paths_to_ids(& unknown_paths);
            let mut i = path_ids.iter();
            for (id, _, _) in paths.iter_mut() {
                if *id == 0 {
                    *id = i.next().unwrap().0;
                }
            }
        }
        return paths.into_iter().zip(hash_ids.into_iter()).map(|((path_id, path, hash), (hash_id, is_new_hash))| {
            return (path_id, hash_id, path, hash, is_new_hash);
        }).collect();
    } 

}


/** Removes all redundant url records from github metadata JSON object. 
 
    Removes all `_url` suffixed fields from the metadata record with the exception of `html_url` 
 */
fn filter_github_metadata_keys(json : & mut json::JsonValue, is_root : bool) {
    let mut x = Vec::new();
    for (key, value) in json.entries_mut() {
        if is_root && key == "html_url" {
            // do nothing
        } else if key.ends_with("_url") || key == "url" {
            x.push(key.to_string());
            continue;
        } 
        filter_github_metadata_keys(value, false);
    }
    for k in x {
        json.remove(&k);
    }
}

/** Calculates the output of two git trees and adds / updates any changes in the given hashmap. 
 */
fn calculate_tree_diff(repo : & git2::Repository,  parent : Option<& git2::Tree>, commit : Option<& git2::Tree>, changes : & mut HashMap<String, Hash>) -> Result<(), git2::Error> {
    let diff = repo.diff_tree_to_tree(parent, commit, None)?;
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

