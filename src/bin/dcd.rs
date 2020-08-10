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
    heads : HashMap<String, (git2::Oid, Source)>,
}








/** Performs a single update round on the project.

    First we have to analyze the project information, the we can start the git download & things...
 */
fn update_project(id : ProjectId, db : & DatabaseManager) {
    let project = Project::from_database(id, db);
    println!("{} : {}", project.id, project.url);

}

// Structs impls & helper functions

impl Project {
    pub fn from_database(id : ProjectId, db : & DatabaseManager) -> Project {
        let mut result = Project {
            id,
            url : String::new(),
            last_update : 0,
            metadata : HashMap::new(),
            heads : HashMap::new(),
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
                    result.heads.insert(name, (hash, source));
                }
            }
        }
        return result;
    }
}



