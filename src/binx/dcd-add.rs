use dcd::*;
use dcd::db_manager::*;

/** Adds specified projects to selected database.
 */
fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        panic!{"Invalid usage - dcd-add PATH_TO_DATABASE PATH_TO_CSV_WITH_URLS"}
    }
    let db = DatabaseManager::from(& args[1]);
    // and load existing projects urls
    db.load_project_urls();
    // TODO we need to check that the projects do not exist yet, but for now, I care not
    let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(& args[2]).unwrap();
    let mut records = 0;
    let mut valid = 0;
    for x in reader.records() {
        let record = x.unwrap();
        let url = String::from(& record[0]);
        records += 1;
        if let Some(id) = db.add_project(url, Source::Manual) {
            println!("id: {}, url: {}", id, & record[0]);
            valid += 1;
        } else {
            println!("already present: url: {}", & record[0]);
        }
    }
    db.commit_created_projects();
    println!("Total records read: {}", records);
    println!("Actually added:     {}", valid);
}