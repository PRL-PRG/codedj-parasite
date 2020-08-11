use std::collections::*;

use dcd::*;
use dcd::db_manager::*;


/** Fire up the database and start downloading...
 */
fn main() {
    let mut db = DatabaseManager::from("/dejavuii/dejacode/dataset-tiny");
    db.load_incomplete_commits();
    // clear the temporary folder if any 
    std::fs::remove_dir_all(format!("{}/tmp", db.root()));



    println!("Analyzing projects (total {})...", db.num_projects());
    for x in 0 .. db.num_projects() {
        if let Err(err) = update_project(x as ProjectId, & db) {
            println!("ERROR: project {} : {:?}", x, err);
        }
    }
}



/** This is a more detailed project information for updating purposes.
 */

struct Project {
    id : ProjectId, 
    url : String, 
    last_update : i64, 
    metadata : HashMap<String, (String,Source)>,
    heads : Vec<(String, git2::Oid, Source)>,
    log : record::ProjectLog,
}








/** Performs a single update round on the project.

    First we have to analyze the project information, the we can start the git download & things...
 */
fn update_project(id : ProjectId, db : & DatabaseManager) -> Result<bool, git2::Error> {
    let mut project = Project::from_database(id, db);
    // create the bare git repository 
    // TODO in the future, we can check whether the repo exists and if it does do just update 
    let mut repo = git2::Repository::init_bare(format!("{}/tmp/{}", db.root(), id))?;
    let new_heads = project.fetch_new_heads(& mut repo, db)?;
    // now we have new heads, so we should analyze the commits
    update_commits(& new_heads, & mut repo, db)?;

    println!("{} : {}, new heads: {}", project.id, project.url, new_heads.len());

    return Ok(true);
}

/** Updates the commits identified by hashes, if they need to be. 
 
 */ 
fn update_commits(commits : & HashSet<git2::Oid>, repo : & mut git2::Repository, db : & DatabaseManager) -> Result<(), git2::Error> {
    // queue of commits and whether they are open or not
    let mut q : VecDeque<(git2::Oid, bool)> = commits.iter().map(|x| (*x, false)).collect();
    while ! q.is_empty() {
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
            let committer_id = db.get_or_create_user(committer.email().unwrap(), committer.name().unwrap(), Source::GitHub);

            let author = commit.author();
            let author_time = author.when().seconds();
            let author_id = db.get_or_create_user(author.email().unwrap(), author.name().unwrap(), Source::GitHub);

            let parents : HashSet<CommitId> = commit.parents().map(|x| db.get_commit_id(x.id()).unwrap().0).collect();

            // get the message
            let msg = commit.message_bytes();
            db.append_commit_message(commit_id, msg);

            // get the changes



            db.append_commit_record(commit_id, committer_id, committer_time, author_id, author_time, Source::GitHub);
            db.append_commit_parents_record(commit_id, & parents);

        }
    }
    return Ok(());
}

/*
fn calculate_commit_diff(repo : git2::Repository, commit : & git2::Commit, db : & DatabaseManager) -> Result(HashMap<PathId,SnapshotId>, git2::Error) {



    

} */




//     /** Get the diff of the commit. 

//         TODO We actually need to calculate the files ourselves as libgit provides no such feature - the diff provided inside is a full diff that is an overkill for our purposes, but for now I am just using the default diff. 
        
//      */
//     fn store_commit_diff(& self, repo : & git2::Repository, root: &str, commit : & git2::Commit) -> Result<(), git2::Error> {
//         // first calculate the diff
//         let mut diff = HashMap::new();
//         if commit.parent_count() == 0 {
//             Project::calculate_diff(& repo, None, Some(& commit.tree()?), & mut diff)?;
//         } else {
//             for p in commit.parents() {
//                 Project::calculate_diff( & repo, Some(& p.tree()?), Some(& commit.tree()?), & mut diff)?;
//             }
//         }
//         // then get all the hash ids for paths and for hashes
//         let (contents, new_contents) = self.ghm.create_new_hash_ids(& diff.values().cloned().collect());        
//         let (paths, new_paths) = self.ghm.create_new_path_ids(& diff.keys().cloned().collect());        
//         // store the diff in the commit
//         let mut f = File::create(format!("{}/changes.csv", root)).unwrap();
//         writeln!(& mut f, "pathId,changeId");
//         for x in diff {
//             writeln!(& mut f, "{},{}", paths.get(& x.0).unwrap(), contents.get(& x.1).unwrap());
//         }
//         // store the snapshots
//         // TODO determine which snapshots we want to be stored and which not
//         for hash in & new_contents {
//             let id = contents.get(& hash).unwrap();
//             if let Ok(blob) = repo.find_blob(*hash) {
//                 let snapshotRoot = String::from(format!("{}/snapshots/{}", self.ghm.root, id % 100));
//                 std::fs::create_dir_all(& snapshotRoot);
//                 let mut f = File::create(format!("{}/{}", & snapshotRoot, id)).unwrap();
//                 f.write_all(blob.content());
//             } else {
//                 // TODO we are dealing with sth like submodule most likely
//             }
//         }
//         // when stored, update the hash & path ids
//         self.ghm.append_hashes(& contents, & new_contents);
//         self.ghm.append_paths(& paths, & new_paths);
//         Ok(())
//     }

//     /** Calculates the diff between the two tree nodes. 
        
//         Deal with renames and other things too
//      */
//     fn calculate_diff(repo : & git2::Repository, parent : Option<& git2::Tree>, commit : Option<& git2::Tree>, diff : & mut HashMap<String, git2::Oid>) -> Result<(), git2::Error> {
//         let d = repo.diff_tree_to_tree(parent, commit, None)?;
//         for di in d.deltas() {
//             match di.status() {
//                 git2::Delta::Added | git2::Delta::Modified | git2::Delta::Deleted | git2::Delta::Copied => {
//                     if let Some(p) = di.new_file().path().unwrap().to_str() {
//                         diff.insert(String::from(p), di.new_file().id());
//                     }
//                 },
//                 git2::Delta::Renamed => {
//                     if let Some(po) = di.old_file().path().unwrap().to_str() {
//                         diff.insert(String::from(po), git2::Oid::zero());
//                         if let Some(p) = di.new_file().path().unwrap().to_str() {
//                             diff.insert(String::from(p), di.new_file().id());
//                         }
//                     }
//                 },
//                 // this should not really happen in diffs of commits
//                 _ => {
//                     panic!("What to do?");
//                 }
//             }
//         }
//         Ok(())
//     }


// Structs impls & helper functions

impl Project {
    pub fn from_database(id : ProjectId, db : & DatabaseManager) -> Project {
        let mut result = Project {
            id,
            url : String::new(),
            last_update : 0,
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
             callbacks.transfer_progress(|progress : git2::Progress| -> bool {
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



