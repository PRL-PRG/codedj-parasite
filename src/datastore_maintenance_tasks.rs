use crate::updater::*;
use crate::records::*;
use crate::helpers;
use crate::datastore::*;

/** Adds projects to the datastore. 
 
    To do this we must check the project urls for which the datastore needs to load all urls it knows. If the hashmap is not populated, it is loaded first. Then projects from the source can be added. 
 */
pub (crate) fn task_add_projects(ds : & Datastore, source : String,  task : TaskStatus) -> Result<(), std::io::Error> {
    ds.load_project_urls(| progress | {
        task.info(format!("loading datastore project urls ({}) ", helpers::pretty_value(progress)));
    });
    let mut added = 0;
    let mut existing = 0;
    let mut invalid = 0;
    if source.ends_with(".csv") {
        add_projects_from_csv(ds, source, & task, & mut added, & mut existing, & mut invalid)?;
    } else {
        add_project(ds, & source, & mut added, & mut existing, & mut invalid);
    }
    task.info(format!("Finished: {} added, {} existing, {} invalid", added, existing, invalid));
    return Ok(());
}

fn add_project(ds : & Datastore, url : & str, added : & mut usize, existing : & mut usize, invalid : & mut usize) {
    match Project::from_url(url) {
        Some(project) => {
            match ds.add_project(& project) {
                Some(_id) => {
                    // don't actually schedule the update, it has to be explicitly enabled by the user
                    //updater.schedule(Task::UpdateRepo{ id, last_update_time : Updater::NEVER });
                    *added += 1;
                },
                _ => {
                    *existing += 1;
                },
            }
        }, 
        None => *invalid += 1,
    }
} 

fn add_projects_from_csv(ds : & Datastore, source : String, task : & TaskStatus, added : & mut usize, existing : & mut usize, invalid : & mut usize) -> Result<(), std::io::Error>{
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(source)?;
    let headers = reader.headers()?;
    let mut col_id = if let Some(id) = find_repo_url_column(& headers) {
        add_project(ds, & headers[id], added, existing, invalid);
        id
    } else {
        std::usize::MAX
    };
    for x in reader.records() {
        let record = x.unwrap();
        if col_id == std::usize::MAX {
            if let Some(id) = find_repo_url_column(& record) {
                col_id = id;
            } else {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "Cannot determine column containing project urls"));
            }
        }
        add_project(ds, & record[col_id], added, existing, invalid);
        if (*added + *existing + *invalid) % 1000 == 0 {
            task.info(format!("{} added, {} existing, {} invalid, using column {}", added, existing, invalid, col_id));
        }
    }
    return Ok(());
}

/** Determines if there is a column whose contents looks like a url so that it can be used to construct projects. 
 */
fn find_repo_url_column(row : & csv::StringRecord) -> Option<usize> {
    let mut i : usize = 0;
    let mut result : usize = std::usize::MAX;
    for x in row {
        match Project::from_url(x) {
            Some(_) => {
                // there are multiple indices that could be urls, so we can't determine 
                if result != std::usize::MAX {
                    return None;
                }
                result = i;
            },
            None => {}
        }
        i += 1;
    }
    if result != std::usize::MAX {
        return Some(result);
    } else {
        return None;
    }
}

/** Creates new savepoint of given name. 
 
    TODO make sure that savepoint with given name does not exist yet
 */
pub (crate) fn task_create_savepoint(ds : & Datastore, task : TaskStatus) -> Result<(), std::io::Error> {
    if let Task::CreateSavepoint{name} = & task.task {
        let sp = ds.create_and_save_savepoint(name.to_owned());
        task.info(format!("Created savepoint {}, total size {}", sp.name(), helpers::pretty_size(sp.size())));
     } else {
        panic!("Invalid task kind");
    }
    return Ok(());
}
