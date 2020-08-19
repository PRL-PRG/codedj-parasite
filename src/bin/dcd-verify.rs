use dcd::*;
use dcd::db_manager::*;

fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() < 2 || (args.len() == 3 && args[2] != *"--exclude-commits") || args.len() > 3  {
        panic!{"Invalid usage - dcd-verify PATH_TO_DATABASE [--exclude-commits]"}
    }
    DatabaseManager::from(& args[1]);
    let dcd = DCD::new(String::from(& args[1]));
    let mut num_projects = 0;
    for project in dcd.projects() {
        num_projects += 1;
        helpers::progress_line(format!("    projects: {}", num_projects));
        if args.len() == 2 {
            for _commit in dcd.commits_from(& project) {
            }
        }
    }
    println!("    projects: {}", num_projects);
}