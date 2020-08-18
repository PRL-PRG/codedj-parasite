use dcd::*;
use dcd::db_manager::*;

fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        panic!{"Invalid usage - dcd-verify PATH_TO_DATABASE"}
    }
    DatabaseManager::from(& args[1]);
    let dcd = DCD::new(String::from(& args[1]));
    for project in dcd.projects() {
        for _commit in dcd.commits_from(& project) {
        }
    }
}