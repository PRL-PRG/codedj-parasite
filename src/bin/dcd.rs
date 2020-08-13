use std::collections::*;
use std::sync::*;
use dcd::*;
use dcd::db_manager::*;

#[derive(Eq)]
struct QueuedProject {
    id : ProjectId,
    last_updated : i64,
}

struct ProjectsQueue {
    q_ : Mutex<BinaryHeap<std::cmp::Reverse<QueuedProject>>>,
    qcv_ : Condvar,
}

impl ProjectsQueue {
    fn new() -> ProjectsQueue {
        return ProjectsQueue {
            q_ : Mutex::new(BinaryHeap::new()),
            qcv_ : Condvar::new(),
        }
    }

    fn enqueue(& self, id : ProjectId, update_time : i64) {
        let mut q = self.q_.lock().unwrap();
        q.push(std::cmp::Reverse(
            QueuedProject{
                id, 
                last_updated : update_time
            }
        ));
        self.qcv_.notify_one();
    }

    /*
    fn dequeue(& self) -> ProjectId {
        let mut q = self.q_.lock().unwrap();
        while q.is_empty() {
            q = self.qcv_.wait(q).unwrap();
        }
        return q.pop().unwrap().0.id;
    }
    */

    fn dequeue_non_blocking(& self) -> Option<ProjectId> {
        let mut q = self.q_.lock().unwrap();
        if q.is_empty() {
            return None;
        }
        println!("Remainig {} projects...", q.len());
        return Some(q.pop().unwrap().0.id);
    }
}

/** Fire up the database and start downloading...
 */
fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        panic!{"Invalid usage - dcd PATH_TO_DATABASE"}
    }
    let db = DatabaseManager::from(& args[1]);
    db.load_incomplete_commits();
    // clear the temporary folder if any 
    let tmp_folder = format!("{}/tmp", db.root());
    if std::path::Path::new(& tmp_folder).exists() {
        std::fs::remove_dir_all(tmp_folder).unwrap();
    }

    let q = ProjectsQueue::new();
    println!("Analyzing projects (total {})...", db.num_projects());
    for x in 0 .. db.num_projects() {
        q.enqueue(x as ProjectId, 0);
    }

    crossbeam::thread::scope(|s| {
        // start the worker threads
        for _x in 0..32 {
            s.spawn(|_| {
                loop {
                    match q.dequeue_non_blocking() {
                        Some(project_id) => {
                            let mut project = Project::from_database(project_id, & db);
                            project.log.add(record::ProjectLogEntry::update_start(Source::GitHub));
                            match update_project(& mut project, & db) {
                                Ok(true) => {
                                    project.log.add(record::ProjectLogEntry::update(Source::GitHub));
                                },
                                Ok(false) => {
                                    project.log.add(record::ProjectLogEntry::no_change(Source::GitHub));
                                },
                                Err(err) => {
                                    println!("ERROR: project {} : {:?}", project_id, err);
                                    project.log.clear();
                                    project.log.add(record::ProjectLogEntry::error(Source::GitHub, err.message().to_owned()));
                                }
                            }
                            project.log.append();
                            let tmp_folder = format!("{}/tmp/{}", db.root(), project.id);
                            if std::path::Path::new(& tmp_folder).exists() {
                                std::fs::remove_dir_all(tmp_folder).unwrap();
                            }
                        },
                        None => {
                            return;
                        }
                    }
                }
            });
        }
     }).unwrap();
     println!("All done.");
}

impl Ord for QueuedProject {
    fn cmp(& self, other : & Self) -> std::cmp::Ordering {
        return self.last_updated.cmp(& other.last_updated);
    }
}

impl PartialOrd for QueuedProject {
    fn partial_cmp(& self, other : & Self) -> Option<std::cmp::Ordering> {
        return Some(self.last_updated.cmp(& other.last_updated));
    }
}

impl PartialEq for QueuedProject {
    fn eq(& self, other : & Self) -> bool {
        return self.last_updated == other.last_updated;
    }
}





/** This is a more detailed project information for updating purposes.
 */

struct Project {
    id : ProjectId, 
    url : String, 
    last_update : i64, 
    update_start : i64,
    metadata : HashMap<String, (String,Source)>,
    heads : Vec<(String, git2::Oid, Source)>,
    log : record::ProjectLog,
}








/** Performs a single update round on the project.

    First we have to analyze the project information, the we can start the git download & things...
 */
fn update_project(project : & mut Project, db : & DatabaseManager) -> Result<bool, git2::Error> {
    // create the bare git repository 
    // TODO in the future, we can check whether the repo exists and if it does do just update 
    let mut changed = false;
    let mut repo = git2::Repository::init_bare(format!("{}/tmp/{}", db.root(), project.id))?;
    let new_heads = project.fetch_new_heads(& mut repo, db)?;
    if new_heads.is_empty() {
        return Ok(changed);
    } else {
        changed = true;
        // now we have new heads, so we should analyze the commits
        update_commits(project, & new_heads, & mut repo, db)?;
    }

    //println!("{} : {}, new heads: {}", project.id, project.url, new_heads.len());

    return Ok(changed);
}



/** Updates the commits identified by hashes, if they need to be. 
 
 */ 
fn update_commits(project : & mut Project, commits : & HashSet<git2::Oid>, repo : & mut git2::Repository, db : & DatabaseManager) -> Result<(), git2::Error> {
    // queue of commits and whether they are open or not
    let mut q : VecDeque<(git2::Oid, bool)> = commits.iter().map(|x| (*x, false)).collect();
    while ! q.is_empty() {
        if helpers::now() - project.update_start >= 3600 {
            return Err(git2::Error::from_str("DCD Timeout"));
        }
        let (hash, open) = q.pop_back().unwrap();
        let (commit_id, state) = db.get_or_create_commit_id(hash);
        // if the commit is not open, we have to first deal with its parents
        if ! open {
            // if the commit exists, there is nothing to do
            if state == RecordState::Existing {
                continue;
            }
            q.push_back((hash, true)); // open the commit
            let commit = repo.find_commit(hash)?;
            // push its parents
            for parent in commit.parents() {
                q.push_back((parent.id(), false));
            }
        // otherwise if the commit is already open the we know all its parents are ok and we can proceed to analyze the commit, starting with the compulsory information of commit record and its parents
        // no need to check whether the commit exists, if it has been opened, it must be either new or invalid
        } else {
            let commit = repo.find_commit(hash)?;
            let committer = commit.committer();
            let committer_time = commit.time().seconds();
            let committer_id = db.get_or_create_user(
                & String::from_utf8_lossy(committer.email_bytes()), 
                & String::from_utf8_lossy(committer.name_bytes()),
                Source::GitHub
            );

            let author = commit.author();
            let author_time = author.when().seconds();
            let author_id = db.get_or_create_user(
                & String::from_utf8_lossy(author.email_bytes()),
                & String::from_utf8_lossy(author.name_bytes()),
                Source::GitHub
            );

            let parents : HashSet<CommitId> = commit.parents().map(|x| db.get_commit_id(x.id()).unwrap().0).collect();

            // get the message
            let msg = commit.message_bytes();
            db.append_commit_message(commit_id, msg);

            // get the changes and append them to the database
            let (changes, additions, deletions) = calculate_commit_diff(repo, & commit, db)?;
            let changes_only : Vec<(PathId, SnapshotId)> = changes.iter().map(|(path_id, (snapshot_id, _))| (*path_id, *snapshot_id)).collect();
            db.append_commit_changes(commit_id, & changes_only, additions, deletions);


            // update the parents
            db.append_commit_parents_record(commit_id, & parents);
            // append the record, which also completes the commit
            db.append_commit_record(commit_id, committer_id, committer_time, author_id, author_time, Source::GitHub);

        }
    }
    return Ok(());
}


/** Calculates the diff for given commit. 
 
    Returns the actual diff (PathId-> (SnapshotId, is the snapshot new?)) and returns the cummlative additions & deletions across all parent changes. 
 */
fn calculate_commit_diff(repo : & git2::Repository, commit : & git2::Commit, db : & DatabaseManager) -> Result<(HashMap<PathId,(SnapshotId,bool)>, usize, usize), git2::Error> {
    let mut diff = HashMap::<String, git2::Oid>::new();
    let mut additions = 0;
    let mut deletions = 0;
    if commit.parent_count() == 0 {
        let (a,d) = calculate_tree_diff(repo, None, Some(& commit.tree()?), & mut diff)?;
        additions = a;
        deletions = d;
    } else {
        for p in commit.parents() {
            let (a, d) = calculate_tree_diff(repo, Some(& p.tree()?), Some(& commit.tree()?), & mut diff)?;
            additions += a;
            deletions += d;
        }
    }
    return Ok((db.translate_commit_changes(&diff), additions, deletions));
}


fn calculate_tree_diff(repo: & git2::Repository,  parent : Option<& git2::Tree>, commit : Option<& git2::Tree>, changes : & mut HashMap<String, git2::Oid>) -> Result<(usize, usize), git2::Error> {
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
    let stats = diff.stats()?;
    return Ok((stats.insertions(), stats.deletions()));
}




// Structs impls & helper functions

impl Project {
    pub fn from_database(id : ProjectId, db : & DatabaseManager) -> Project {
        let mut result = Project {
            id,
            url : String::new(),
            last_update : 0,
            update_start : helpers::now(),
            metadata : HashMap::new(),
            heads : Vec::new(),
            log : record::ProjectLog::new(db.get_project_log_filename(id)),
        };
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .double_quote(false)
            .escape(Some(b'\\'))
            .from_path(db.get_project_log_filename(id)).unwrap();
        let mut clear_heads = false;
        for x in reader.records() {
            match record::ProjectLogEntry::from_csv(x.unwrap()) {
                record::ProjectLogEntry::Init{ time : _, source : _, url } => {
                    result.url = url;
                },
                record::ProjectLogEntry::UpdateStart{ time : _, source : _ } => {
                    clear_heads = true;
                },
                record::ProjectLogEntry::Update{ time, source : _ } => {
                    result.last_update = time;
                },
                record::ProjectLogEntry::Error{ time : _, source : _, message : _ } => {
                    // TODO do nothing for now...
                },
                record::ProjectLogEntry::NoChange{ time, source : _} => {
                    result.last_update = time;
                },
                record::ProjectLogEntry::Metadata{ time : _, source, key, value } => {
                    result.metadata.insert(key, (value, source));
                },
                record::ProjectLogEntry::Head{ time : _, source, name, hash} => {
                    if clear_heads {
                        result.heads.clear();
                        clear_heads = false;
                    } 
                    result.heads.push((name, hash, source));
                }
            }
        }
        return result;
    }

    pub fn fetch_new_heads(& mut self, repo : & mut git2::Repository, db : & DatabaseManager) -> Result<HashSet<git2::Oid>, git2::Error> {
        // create a remote to own url and connect
        let mut remote = repo.remote("ghm", & self.url)?;
        remote.connect(git2::Direction::Fetch)?;
        // now load the heads from remote,
        let mut remote_heads = HashSet::<(String, git2::Oid, Source)>::new();
        for x in remote.list()? {
            if x.name().starts_with("refs/heads/") {
                remote_heads.insert((String::from(x.name()), x.oid(), Source::GitHub));
            }
        }
        // once we obtained the remote heads, we must check if there are (a) any changes to the heads we have stored already and (b) if there are any new head commits that we might have to traverse. Note that if there are no changes to hashes, then trivially there are no new commits, but the reverse is not true 
        let new_heads = self.update_heads(& remote_heads);
        // check if the head commits have to be updated (i.e. they are unknown or incomplete) and if they do, clone the repository
        if db.commits_require_update(& mut new_heads.iter()) {
             let mut callbacks = git2::RemoteCallbacks::new();
             // TODO the callbacks should actually report stuff
             callbacks.transfer_progress(|_progress : git2::Progress| -> bool {
                 return true;
             });
             let mut opts = git2::FetchOptions::new();
             opts.remote_callbacks(callbacks); 
             let heads_to_fetch : Vec<String> = remote_heads.iter().map(|(name, _, _)| name.to_owned()).collect();
             remote.fetch(&heads_to_fetch, Some(&mut opts), None)?;
        }
        // return the new heads that need to be analyze
        return Ok(new_heads);
    }

    /** Updates the heads of the project to the given state if there is any change. 
    
        Returns list of new commit hashes that differ from the previously stored heads (including their source).

        This is wasteful as each update of heads stores *all* heads, maybe in the future we want new log messages that will just replace/add or remove heads from previous iterations in cases the changes are just incremental. 
     */
    fn update_heads(& mut self, remote_heads : & HashSet<(String, git2::Oid, Source)>) -> HashSet<git2::Oid> {
        if self.heads.len() == remote_heads.len() {
            let old_heads : HashSet<(String, git2::Oid, Source)> = self.heads.iter().map(|(name, hash, source)| (name.to_owned(), *hash, *source)).collect();
            // if there is no difference between the old and new heads, return false
            if remote_heads.symmetric_difference(& old_heads).next().is_none() {
                return HashSet::new();
            }
        }
        // first calculate new heads
        let new_heads : HashSet<git2::Oid> = remote_heads.iter()
            .map(|(_, hash, source)| (*hash, *source)).collect::<HashSet<(git2::Oid, Source)>>()
            .difference(
                & self.heads.iter()
                .map(|(_, hash, source)| (*hash, *source))
                .collect::<HashSet<(git2::Oid, Source)>>()
            ).map(|(hash, _)| *hash).collect();
        // then clear the heads stored in project and update the log accordingly (without committing it)
        self.heads.clear();
        for (name, hash, source) in remote_heads {
            self.heads.push((name.to_owned(), *hash, *source));
            self.log.add(record::ProjectLogEntry::head(*source, name.to_owned(), *hash));
        }
        return new_heads;
    }
}



