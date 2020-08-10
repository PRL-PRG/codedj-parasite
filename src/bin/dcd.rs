use std::collections::*;

use dcd::*;
use dcd::db_manager::DatabaseManager;


/** Fire up the database and start downloading...
 */

fn main() {
    let mut db = DatabaseManager::from("/dejavuii/dejacode/dataset-small");

    for x in 0 .. db.num_projects() {
        update_project(x as ProjectId, & db);
    }
}



/** This is a more detailed project information for updating purposes.
 */

struct Project {
    id : ProjectId, 
    url : String, 
    last_update : u64, 
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
    let new_heads = project.fetch_new_heads(& mut repo)?;
    println!("{} : {}, new heads: {}", project.id, project.url, new_heads.len());

    return Ok(true);
}

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

    pub fn fetch_new_heads(& mut self, repo : & mut git2::Repository) -> Result<HashSet<git2::Oid>, git2::Error> {
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
        // now determine if we need to download the contents - this is when new heads are non-empty and the commits are unknown, or their source is not github...
        // TODO TODO TODO 
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



