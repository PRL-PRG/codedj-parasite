use dcd::*;

/** Datastore Viewer
 
    This is a simple command-line tool to perform various exploratory tasks on the datastore that should also serve as an example of how to use the datastore api. 

    Usage:

    dcd-view CMD ARGS

    Where CMD is one of the following:

    And arguments can be: 
 */
fn main() {

    let args : Vec<String> = std::env::args().collect();
    let ds = DatastoreView::new(& args[1]);
    project_updates(& ds);
}

fn summary(ds : & DatastoreView) {
    println!("{}", ds.summary());
}

fn datastore_size(ds : & DatastoreView) {
    println!("Savepoints:\n{}", ds.savepoints_size());
    println!("Projects:\n{}", ds.projects_size());
    println!("Commits:\n{}", ds.commits_size());
    println!("Contents:\n{}", ds.contents_size());
    println!("Paths:\n{}", ds.paths_size());
    println!("Users:\n{}", ds.users_size());
    println!("Total:\n{}", ds.datastore_size());
}

fn savepoints(ds : & DatastoreView) {
    let mut s = ds.savepoints();
    let mut num = 0;
    for (_, sp) in s.iter() {
        println!("{}", sp);
        num += 1;
    }
    println!("Total {} savepoints found.", num);
}

fn project_updates(ds : & DatastoreView) {
    let sp = ds.get_savepoint("after_emery").unwrap();
    for (id, log) in ds.project_log().iter(& sp) {
        match log {
            ProjectLog::Ok{time, version: _} => println!("{:?},{},ok", id, time),
            _ => {},
        }
    }
}
