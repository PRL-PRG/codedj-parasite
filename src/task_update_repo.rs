use std::collections::*;

use crate::datastore::*;
use crate::updater::*;
use crate::records::*;
use crate::helpers;
use crate::github::*;


/** Provides a full update of the given repository. 
 
    - figure out the latest update, i.e. if there is one, if it was error (in which case ignore)
    - then we have to decide whether we have the appropriate substore in memory
    - clone the project
    - in theory change the substore and store the information
    - see if we have the substore in memory
    - update the project
    
 */
pub (crate) fn task_update_repo(ds : & Datastore, gh : & Github, task : TaskStatus, force : bool, load_substore : bool) -> Result<(), std::io::Error> {
    let mut ru = RepoUpdater::new(ds, gh, task, force, load_substore);
    match ru.update() {
        Err(e) => {
                // if there was an error, report the error and exit
                ru.ds.update_project_update_status(ru.id, ProjectLog::Error{
                    time : helpers::now(),
                    version : Datastore::VERSION,
                    error : format!("{:?}", e),
                });
                return Err(e);
        },
        Ok(()) => {
            return Ok(());
        },
    }
}

/** A convenience struct because I do not want to drag everything as function arguments.
 */
struct RepoUpdater<'a> {
    ds : &'a Datastore,
    gh : &'a Github,
    task : TaskStatus<'a>,
    id : ProjectId,
    project : ProjectUrl,
    force : bool,
    load_substore : bool,
    /** The substore, or tentative substore for the project (see check_repository_substore function for more details). 
     */
    tentative_substore : StoreKind,
    changed : bool,
    local_folder : String,
    visited_commits : HashMap<SHA, CommitId>,
    users : HashMap<String, UserId>,
    paths : HashMap<String, PathId>,
    q : Vec<(SHA, CommitId)>,
    snapshots : usize,
}

impl<'a> Drop for RepoUpdater<'a> {
    fn drop(& mut self) {
        match std::fs::remove_dir_all(& self.local_folder) {
            _ => {},
        }
    }
}

impl<'a> RepoUpdater<'a> {

    /** Creates new repository updater. 
     */
    fn new(ds : &'a Datastore, gh : &'a Github, task : TaskStatus<'a>, force : bool, load_substore : bool) -> RepoUpdater<'a> {
        if let Task::UpdateRepo{id, last_update_time : _ } = task.task {
            return RepoUpdater {
                ds,
                gh,
                task,
                id,
                project : ds.get_project(id).unwrap(),
                force,
                load_substore,
                tentative_substore : StoreKind::Unspecified,
                changed : false,
                local_folder : format!("{}/repo_clones/{}", ds.root_folder(), u64::from(id)),
                visited_commits : HashMap::new(),
                users : HashMap::new(),
                paths : HashMap::new(),
                q : Vec::new(),
                snapshots : 0,
            };
        } else {
            panic!("Invalid task kind");
        }
    }

    /** Updates the repository. 
     
        
     */
    fn update(& mut self) -> Result<(), std::io::Error> {
        self.task.extra_url(self.project.name(), self.project.clone_url());
        if self.can_be_updated() {
            self.check_metadata()?;
            // update the project contents
            match self.update_repository() {
                Err(e) => {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e.message())));
                },
                Ok(processed) => {
                    // if there was no error and the task was not cancelled, report the change / no-change 
                    if processed {
                        if self.changed {
                            self.ds.update_project_update_status(self.id, ProjectLog::Ok{
                                time : helpers::now(),
                                version : Datastore::VERSION,
                            });
                            self.task.info("ok");
                            self.task.color("\x1b[92m");
                        } else {
                            self.ds.update_project_update_status(self.id, ProjectLog::NoChange{
                                time : helpers::now(),
                                version : Datastore::VERSION,
                            });
                            self.task.info("no change");
                            self.task.color("\x1b[90m");
                        }
                        return Ok(());
                    }
                },
            }
        } 
        self.task.info("cancelled");
        self.task.color("\x1b[96m");
        return Ok(());
    }

    /** Checks whether the current project can be updated and whether the update should be forced. 
     
        TODO we should ideally do something smatrter when there is an error during the update, i.e. dependning on the error, etc. 
     */
    fn can_be_updated(& mut self) -> bool {
        // check if there was error during the update, in which case we do not attempt to update the project again
        if let Some(last_update) = self.ds.get_project_last_update(self.id) {
            // if the version of the last update differs from current version of the datastore, we might need to do something special
            if Datastore::VERSION != last_update.version() {
                self.new_version_update(last_update.version(), Datastore::VERSION);
            }
        }
        return true;
    }

    /** Determines what to do if the datastore version has increased since the latest project update. 
     
        The default action is to do force update, which is technically not always what we want to do and different version situations should actually be covered here. 
     */
    fn new_version_update(& mut self, _old : u16, _new : u16) {
        self.force = true;
    }

    fn check_metadata(& mut self) -> Result<(), std::io::Error> {
        match & self.project {
            /* There is nothing extra we can do for raw git projects as there are no metadata associated with them. 
             */
            ProjectUrl::Git{url : _} => {
                // nop
            },
            /* For github projects, we get github metadata. Store these if changed and update the project url, if different (this is a project rename).  
             */
            ProjectUrl::GitHub{user_and_repo} => {
                self.task.info("checking metadata...");
                let mut metadata = self.gh.get_repo(user_and_repo, Some(& self.task))
                ?;
                // check project rename
                let new_url = format!("{}.git",metadata["html_url"]).to_lowercase();
                self.check_url_change(& new_url)?;
                // clean the metadata and store, if applicable
                filter_github_metadata_keys(& mut metadata, true);
                self.changed = self.ds.update_project_metadata_if_differ(self.id, Metadata::GITHUB_METADATA.to_owned(), metadata.to_string());
                // update the project store if the language is provided in the metadata, i.e. hold the substore as provided by the metadata tentatively in the substore field, when the project is updated, the tentative value and the real value obtained from the datastore will be reconciled
                if metadata["language"].is_string() {
                    if let Some(substore) = StoreKind::from_string(metadata["language"].as_str().unwrap()) {
                        self.tentative_substore = substore;
                    }
                }
            }
        }
        return Ok(());
    }

    /** Compares the newly obtained project url to the one stored and records project rename if applicable. 
     */
    fn check_url_change(& mut self, new_url : & str) -> Result<(), std::io::Error> {
        if let Some(new_project) = ProjectUrl::from_url(new_url) {
            if self.project != new_project {
                self.project = new_project;
                self.task.info(format!("project url changed to {}", new_url));
                self.task.extra_url(self.project.name(), self.project.clone_url());
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
    fn update_repository(& mut self) -> Result<bool, git2::Error> {
        // determine the actual substore of the project from the datastore
        let mut substore = self.ds.get_project_substore(self.id);
        // create local repository
        // TODO reuse repository if found on disk already?, for now make sure there is no leftover repo present
        let path = std::path::Path::new(& self.local_folder);
        if path.exists() {
            std::fs::remove_dir_all(& path).unwrap();
        } 
        // create the repository and add its remote
        let repo = git2::Repository::init_bare(self.local_folder.clone())?;
        let mut remote = repo.remote("dcd", & self.project.clone_url())?;
        remote.connect(git2::Direction::Fetch)?;
        // get own and remote heads and compare them 
        let last_heads = self.get_latest_heads();
        let mut remote_heads = self.get_remote_heads(& mut remote)?;
        let heads_to_fetch = self.compare_project_heads(& last_heads, & mut remote_heads, substore);
        // fetch the repository from the remote and analyze its contents
        if ! heads_to_fetch.is_empty() {
            self.clone_repository(& mut remote, & heads_to_fetch)?;
            // check the repository's substore and terminate if the substore is not loaded should not be loaded
            substore = self.update_repository_substore(& repo, substore)?;
            if ! self.ds.substore(substore).is_loaded() {
                if self.load_substore {
                    self.ds.substore(substore).load(& self.task);
                } else {
                    return Ok(false);
                }
            }
            // analyze the fetched heads
            let ds_s = self.ds.substore(substore);
            let mut i = 0;
            self.task.progress(i, heads_to_fetch.len());
            for head in heads_to_fetch.iter() {
                self.task.info(format!("analyzing branch {} ({} of {})", head, i, heads_to_fetch.len()));
                self.task.progress(i, heads_to_fetch.len());
                let (id, hash) = remote_heads.get_mut(head).unwrap();
                *id = self.analyze_branch(& repo, *hash, ds_s)?;
                i += 1;
                self.task.progress(i, heads_to_fetch.len());
            }
        }
        // if either the heads to fetch were not empty (i.e. there was a content to download), or there was no content, but the number of heads is different (some heads were deleted), store the updated heads
        if ! heads_to_fetch.is_empty() || remote_heads.len() != last_heads.len() {
            self.ds.update_project_heads(self.id, & remote_heads);
            self.changed = true;
        }
        return Ok(true);
    }

    /** Check the repository to determine the substore that should be used for the update. 
     
        Returns the store kind for the project, taking the current  store kind as a hint. 
     
     
     */
    fn update_repository_substore(& mut self, repo : & git2::Repository, current_substore : StoreKind) -> Result<StoreKind, git2::Error> {
        let mut substore = current_substore;
        // all ubspecified projects start as small projects
        if substore == StoreKind::Unspecified {
            substore = StoreKind::SmallProjects; 
        }
        // if the substore is that of small projects, we must verify that the project still has no more than N commits
        if substore == StoreKind::SmallProjects {
            if self.get_repo_commits(repo, Datastore::SMALL_PROJECT_THRESHOLD)? >= Datastore::SMALL_PROJECT_THRESHOLD {
                substore = StoreKind::Unspecified;
            }
        }
        // if the substore is not small project at this point, it is open to change
        if substore != StoreKind::SmallProjects {
            // if tentative substore has been found out, set the substore accordingly
            if self.tentative_substore != StoreKind::Unspecified {
                substore = self.tentative_substore;
            // otherwise if the substore is unspecified, we must pick a substore, so determine one. 
            } else if substore == StoreKind::Unspecified || substore == StoreKind::Generic {
                // TODO Determine some better substore than this
                substore = StoreKind::Generic;
            }
        }
        // check if the substore changed and if so, update the substore information. 
        if substore != current_substore {
            self.ds.update_project_substore(self.id, substore);
        }
        return Ok(substore);
    }

    /** Counts commits in the repository up to given limit. 
     
        Determines the number of commits in the repository. If the number of commits is at least the given limit, stops looking further. 
     */
    fn get_repo_commits(& self, repo : & git2::Repository, limit : usize) ->Result<usize, git2::Error> {
        let mut commits = HashSet::<SHA>::new();
        let mut q = Vec::<SHA>::new();
        for reference in repo.references()? {
            let x = reference?;
            if let Ok(commit) = x.peel_to_commit() {
                if commits.insert(commit.id()) {
                    q.push(commit.id());
                }
                if commits.len() >= limit {
                    break;
                }
            }
        }
        while commits.len() < limit && ! q.is_empty() {
            let hash = q.pop().unwrap();
            let commit = repo.find_commit(hash)?;
            for parent in commit.parents() {
                if commits.insert(parent.id()) {
                    q.push(parent.id());
                }
            }
        }
        return Ok(commits.len());
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
            // TODO this is an issue in libgit2 it seems that a branch must be valid utf8, otherwise we will fail. For now that seems ok as it affects only a really small amount of projects
            let name = x.name().to_owned();
            if name.starts_with("refs/heads/") {
                result.insert(name, (CommitId::INVALID, x.oid()));
            }
        }        
        return Ok(result);
    }

    /** Compares the last heads of the repository with the new ones and returns the list of heads to be downloaded.

        For unchanged heads, updates their id from the last records. 
    */
    fn compare_project_heads(& self, last : & ProjectHeads, current : & mut ProjectHeads, substore : StoreKind) -> Vec<String> {
        let mut result = Vec::<String>::new();
        for (name, (id, hash)) in current.iter_mut() {
            if let Some((last_id, last_hash)) = last.get(name) {
                if hash == last_hash {
                    *id = *last_id;
                    // force update always analyzes all
                    if ! self.force {
                        continue;
                    }
                }
            }
            // if the hashes differ, or the head is not present, add it to the list of heads to be fetched 
            result.push(name.to_owned());
        }
        // if the actual substore is small projects *and* there are changes, update *all* heads so that we can correctly calculate the number of commits of the project
        if substore == StoreKind::SmallProjects && ! result.is_empty() {
            result = current.iter().map(|(name, _)| name.to_owned()).collect();
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
    fn analyze_branch(& mut self, repo : & git2::Repository, head : SHA, substore : & Substore) -> Result<CommitId, git2::Error> {
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
            // get commit message
            commit_info.message = helpers::to_string(commit.message_bytes());
            // get parent ids and add parents to the queue
            commit_info.parents = commit.parents().map(|x| self.add_commit(& x.id(), substore)).collect();
            // and finally, calculate the changes
            commit_info.changes = self.get_commit_changes(repo, & commit, substore)?;
            // store the commit info
            substore.add_commit_info_if_missing(id, & commit_info);
            // update the information
            self.update_task();
        }
        return Ok(head_id);
    }

    /** Adds the given commit to the queue.
     
        Returns the id assigned to the commit. Only adds the commit to the queue if the commit did not exist before. Before going to the datastore, local cache is consulted first. 

        If the update is forced, all commits are reanalyzed even if they exist in the datastore
     */ 
    fn add_commit(& mut self, hash : & SHA, substore : & Substore) -> CommitId {
        if let Some(id) = self.visited_commits.get(hash) {
            return *id;
        }
        let (id, is_new) = substore.get_or_create_commit_id(hash);
        self.visited_commits.insert(*hash, id);
        if is_new || self.force {
            self.q.push((*hash, id)); 
        }
        return id;
    }

    fn get_or_create_user(& mut self, user : & git2::Signature, substore : & Substore) -> UserId {
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

    fn get_commit_changes(& mut self, repo : & git2::Repository, commit : & git2::Commit, substore : & Substore) -> Result<HashMap<PathId, HashId>, git2::Error> {
        // first create the changes map and populate it by changes between the commit and its parents, or the full commit if the commit has no parents
        let mut changes = HashMap::<String, SHA>::new();
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
        for (_path_id, hash_id, path, hash, is_new_hash) in result.iter() {
            if *is_new_hash {
                if let Some(path_kind) = ContentsKind::from_path(path) {
                    if let Ok(blob) = repo.find_blob(*hash) {
                        let contents = blob.content();
                        if let Some(kind) = ContentsKind::from_contents(contents, path_kind) {
                            substore.add_file_contents(*hash_id, kind, & Vec::from(contents));
                            self.snapshots += 1;
                        }
                    } 
                }
            }
        }
        // finally get only the things we need for changes and return
        return Ok(result.into_iter().map(|(path_id, hash_id, _, _, _)| (path_id, hash_id)).collect());
    }

    /** Converts the paths and hashes expressed as strings and SHA hashes to their respective ids and returns a vector containing all. 
     
        The visited paths are cached locally for better performance and we try to avoid grabbing the lock in the datastore unless we really need to. 

        Returns : path id, hash id, path, hash, is hash new?
     */
    fn convert_and_register_changes(& mut self, changes : HashMap<String, SHA>, substore : & Substore) -> Vec<(PathId, HashId, String, SHA, bool)> {
        // contents hashes are easy, we just go straight to the substore to get us the hash ids and whether they are new or not
        let hashes = changes.iter().map(|(_, hash)| *hash ).collect::<Vec<SHA>>();
        let hash_ids = substore.convert_hashes_to_ids(& hashes);
        // for paths we use two stage process, first convert what we can from the local cache, then convert the others via the substore and merge
        let mut unknown_paths = Vec::<String>::new();
        let mut paths = changes.into_iter().map(|(path, hash)| { // keep the hash around so that we can zip once
            if let Some(id) = self.paths.get(& path) {
                return (*id, path, hash);
            } else {
                unknown_paths.push(path.clone());
                return (PathId::EMPTY, path, hash);
            }
        }).collect::<Vec<(PathId, String, SHA)>>();
        // get the missing path ids
        if ! unknown_paths.is_empty() {
            let path_ids = substore.convert_paths_to_ids(& unknown_paths);
            let mut i = path_ids.iter();
            for (id, _, _) in paths.iter_mut() {
                if *id == PathId::EMPTY {
                    *id = i.next().unwrap().0;
                }
            }
        }
        return paths.into_iter().zip(hash_ids.into_iter()).map(|((path_id, path, hash), (hash_id, is_new_hash))| {
            return (path_id, hash_id, path, hash, is_new_hash);
        }).collect();
    } 

    /** Updates the task information. 
     */
    fn update_task(& self) {
        self.task.info(format!("q: {}, c: {}, s: {}", self.q.len(), self.visited_commits.len(), self.snapshots));
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
fn calculate_tree_diff(repo : & git2::Repository,  parent : Option<& git2::Tree>, commit : Option<& git2::Tree>, changes : & mut HashMap<String, SHA>) -> Result<(), git2::Error> {
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
