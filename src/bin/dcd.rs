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
    let project = Project::from_database(id, db);
    println!("{} : {}", project.id, project.url);
    // create the bare git repository 
    // TODO in the future, we can check whether the repo exists and if it does do just update 
    let repo = git2::Repository::init_bare(format!("{}/tmp/{}", db.root(), id))?;


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
        let mut remote_heads = HashMap::<String, git2::Oid>::new();
        for x in remote.list()? {
            if x.name().starts_with("refs/heads/") {
                remote_heads.insert(String::from(x.name()), x.oid());
            }
        }
        // now determine new heads and if there are any, update the project log accordingly
        let mut result = HashSet::<git2::Oid>::new();
        



        return Ok(result);
    }
}



