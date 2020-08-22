use dcd::*;
use dcd::db_manager::*;

fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() < 2 || (args.len() == 3 && args[2] != *"--exclude-commits") || args.len() > 3  {
        panic!{"Invalid usage - dcd-verify PATH_TO_DATABASE [--exclude-commits]"}
    }
    //let _db = DatabaseManager::from(& args[1]);
    let dcd = DCD::new(String::from(& args[1]));
    let mut num_projects = 0;
    let mut num_commits = 0;
    for project in dcd.projects() {
        num_projects += 1;
        //helpers::progress_line(format!("    projects: {}, commits : {}", num_projects, num_commits));
        if args.len() == 2 {
            let mut commits_in_project = 0;
            for _commit in FastProjectCommitIter::from(& dcd, & project) {
                num_commits += 1;
                commits_in_project += 1;
            }
            println!("{},{},{}", project.metadata["ght_language"], project.url, commits_in_project);
        }
    }
    println!("    projects: {}, commits : {}", num_projects, num_commits);
}