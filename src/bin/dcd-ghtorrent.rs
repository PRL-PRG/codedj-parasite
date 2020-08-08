use std::collections::*;

use dcd::*;
use dcd::db_manager::DatabaseManager;


/** Actually initializes stuff from ghtorrent.
 */
fn main() {
    let mut db = DatabaseManager::initialize_new("/dejavuii/dejacode/dataset-small".to_owned());
    //let mut dcd = DownloaderState::create_new("/dejavuii/dejacode/dataset");
    //ghtorrent::import("/dejavuii/dejacode/ghtorrent/dump", & mut dcd);
}




fn initialize_projects(root : & str, db : & mut DatabaseManager) -> HashMap<u64, ProjectId> {
    let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/projects.csv", root)).unwrap();
    let mut records = 0;
    let mut project_ids = HashMap::<u64,ProjectId>::new();
    println!("Adding new projects...");
    for x in reader.records() {
        let record = x.unwrap();
        if records % 1000 == 0 {
            helpers::progress_line(format!("    records: {}, new projects: {}, pending forks: {}", records, project_ids.len(), pending_forks.len()));
        }
        records += 1;
        // ignore deleted projects and forks so check these first...
        let forked_from = record[7].parse::<u64>();
        let deleted = record[8].parse::<u64>().unwrap();
        if deleted != 0 || forked_from.is_ok()  {
            continue;
        }
        // it's a valid project, get its url
        let api_url : Vec<&str> = record[1].split("/").collect();
        let name = api_url[api_url.len() - 1].to_lowercase();
        let user = api_url[api_url.len() - 2].to_lowercase();
        let url = format!("https://github.com/{}/{}.git", user, name);
        // see if the project should be added 
        // get the user and repo names
        if let Some(own_id) = db.add_project(& url) {
            // add the ght_id and language to the project's metadata
            let mut md = record::ProjectMetadata::new();
            md.insert(String::from("ght_id"), String::from(& record[0]));
            md.insert(String::from("ght_language"), String::from(& record[5]));
            md.save(& db.get_project_root(own_id));
            // add the ght to own id mapping to the result projects...
            let ght_id = record[0].parse::<u64>().unwrap();
            project_ids.insert(ght_id, own_id);
        }
    }
    return project_ids;
}


