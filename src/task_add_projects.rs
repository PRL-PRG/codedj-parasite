use std::collections::*;

use crate::datastore::*;
use crate::updater::*;
use crate::records::*;
use crate::helpers;

/** Adds projects to the datastore. 
 
    To do this we must check the project urls for which the datastore needs to load all urls it knows. If the hashmap is not populated, it is loaded first. Then projects from the source can be added. 
 */
pub (crate) fn task_add_projects(updater : & Updater, task_name : & str, source : String,  tx : & Tx) -> Result<(), std::io::Error> {
    updater.ds.load_project_urls(| progress | {
        tx.send(TaskMessage::Info{
            name : task_name.to_owned(),
            info : format!("loading datastore project urls ({}) ", helpers::pretty_value(progress))
        }).unwrap();
    });
    let mut added = 0;
    let mut existing = 0;
    let mut invalid = 0;
    if source.ends_with(".csv") {
        add_projects_from_csv(updater, source, task_name, tx, & mut added, & mut existing, & mut invalid)?;
    } else {
        add_project(updater, & source, & mut added, & mut existing, & mut invalid);
    }
    tx.send(TaskMessage::Info{
        name : task_name.to_owned(),
        info : format!("Finished: {} added, {} existing, {} invalid", added, existing, invalid)
    }).unwrap();
    return Ok(());
}

fn add_project(updater : & Updater, url : & str, added : & mut usize, existing : & mut usize, invalid : & mut usize) {
    match Project::from_url(url) {
        Some(project) => {
            match updater.ds.add_project(& project) {
                Some(id) => {
                    updater.schedule_project_update(id, Updater::NEVER);
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

fn add_projects_from_csv(updater : & Updater, source : String, task_name : & str, tx : & Tx, added : & mut usize, existing : & mut usize, invalid : & mut usize) -> Result<(), std::io::Error>{
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(source)?;
    let headers = reader.headers()?;
    let mut col_id = if let Some(id) = find_repo_url_column(& headers) {
        add_project(updater, & headers[id], added, existing, invalid);
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
        add_project(updater, & record[col_id], added, existing, invalid);
        if (*added + *existing + *invalid) % 1000 == 0 {
            tx.send(TaskMessage::Info{
                name : task_name.to_owned(),
                info : format!("{} added, {} existing, {} invalid, using column {}", added, existing, invalid, col_id)
            }).unwrap();
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
