use dcd::*;

/** Describes the given dataset.
 
    A simple function for the paper.

    TODO latex output
 */  
fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 2 && args.len() != 3  {
        panic!{"Invalid usage - dcd-describe PATH_TO_DATABASE [latex output]"}
    }
    let dcd = DCD::new(String::from(& args[1]));
    let mut valid_projects = 0;
    for _project in dcd.projects() {
        valid_projects += 1;
    }
    println!("# of projects:       {}", dcd.num_projects());
    println!("# of valid projects: {}", valid_projects);
    println!("# of commits:        {}", dcd.num_commits());
    println!("# of paths:          {}", dcd.num_file_paths());
    println!("# of users:          {}", dcd.num_users());
}