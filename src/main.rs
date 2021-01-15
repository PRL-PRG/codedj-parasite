use std::collections::*;

#[macro_use]
extern crate lazy_static;

mod helpers;
#[allow(dead_code)]
mod db;
#[allow(dead_code)]
mod datastore;
#[allow(dead_code)]
mod records;
#[allow(dead_code)]
mod updater;
mod task_add_projects;
mod task_update_repo;
mod task_update_substore;
mod task_load_substore;
mod task_drop_substore;
mod task_verify_substore;
mod github;
mod settings;

use datastore::*;
use updater::*;

use parasite::*;

use settings::SETTINGS;

/** The incremental downloader and command-line interface
 
 */
fn main() {
    // this also initializes the settings from the commandline implicitly, meh....
    LOG!("Parasite v. 0.3");
    LOG!("    interactive :    {}", SETTINGS.interactive);
    LOG!("    verbose :        {}", SETTINGS.verbose);
    LOG!("    threads :        {}", SETTINGS.num_threads);
    LOG!("    datastore root : {}", SETTINGS.datastore_root);
    LOG!("    command :        {}", SETTINGS.command.join(" "));
    // execute either the interactive updater, or the command line tool
    if SETTINGS.interactive {
        start_interactive();
    } else {
        execute_command();
    }
}


/** Starts the interactive mode text user interface for the downloader. 

    If a command was given on the command line it will be automatically executed in the interactive mode. Otherwise the application will wait for a command to be entered. 
 */
fn start_interactive() {
    let ds = Datastore::new(& SETTINGS.datastore_root, false);
    let u = Updater::new(ds);
    u.run(SETTINGS.command.join(" "));
}

/** Executes given command in a non-interactive mode.
 */
fn execute_command() {
    if SETTINGS.command.is_empty() {
        return datastore_size();
    }
    match SETTINGS.command[0].as_str() {
        // maintenance commands 
        "size" => datastore_size(),
        "summary" => datastore_summary(),
        "savepoints" => datastore_savepoints(),
        // example commands
        "active-projects" => example_active_projects(
            SETTINGS.command.get(1).map(|x| { x.parse::<i64>().unwrap() }).unwrap_or(90 * 24 * 3600)
        ),
        "show-project" => example_show_project(
            SETTINGS.command.get(1).unwrap(),
            SETTINGS.command.get(2).map(|x| x.as_str())
        ),
        // debug commands
        "contents-compression" => datastore_contents_compression(),
        "debug" => datastore_debug(),
        _ => println!("ERROR: Unknown command {}", SETTINGS.command[0]),
    }
}

fn datastore_summary() {
    let ds = DatastoreView::new(& SETTINGS.datastore_root);
    println!("{}", ds.summary());
}

fn datastore_size() {
    let ds = DatastoreView::new(& SETTINGS.datastore_root);
    println!("kind,contents,indices");
    println!("savepoints,{}", ds.savepoints_size());
    println!("projects,{}", ds.projects_size());
    println!("commits,{}", ds.commits_size());
    println!("contents,{}", ds.contents_size());
    println!("paths,{}", ds.paths_size());
    println!("users,{}", ds.users_size());
    println!("total,{}", ds.datastore_size());
}

fn datastore_savepoints() {
    let ds = DatastoreView::new(& SETTINGS.datastore_root);
    let mut s = ds.savepoints();
    let mut num = 0;
    for (_, sp) in s.iter() {
        println!("{}", sp);
        num += 1;
    }
    println!("Total {} savepoints found.", num);
}

/** Displays active projects per substore. 
 
    A simple example of the library interface. 
 */
fn example_active_projects(max_age : i64) {
    // create the datastore view with latest info (the latest savepoint is created ad hoc for the current state of the datastore)
    let ds = DatastoreView::new(& SETTINGS.datastore_root);
    let sp = ds.current_savepoint();
    // get all projects 
    let projects = ds.projects(& sp);
    let mut total_valid = 0;
    let mut total_active = 0;
    println!("value,name");
    // on a per substore basis, determine the heads, then get their times from the substore and report
    for substore in ds.substores() {
        let mut heads = HashMap::<CommitId, i64>::new();
        let mut valid = 0;
        let mut total = 0;
        let mut commits = substore.commits_info();
        for (_id, p) in projects.iter().filter(|(_, p)| { p.substore == substore.kind() }) {
            total += 1;
            if let Some(_) = p.latest_valid_update_time() {
                for (_branch, (commit_id, _hash)) in p.heads.iter() {
                    heads.entry(*commit_id).or_insert_with(|| { 
                        return commits.get(*commit_id).unwrap().committer_time;
                    });
                }
                valid += 1;
            }
        }
        // calculate which projects are active
        let active = projects.iter().filter(|(_, p)| { p.substore == substore.kind() }).filter(|(_id, p)| {
            for (_branch, (commit_id, _hash)) in p.heads.iter() {
                if let Some(time) = heads.get(& commit_id) {
                    if sp.time() - time <= max_age {
                        return true;
                    }
                }
            }
            return false;
        }).count();
        println!("{}, {:?}_projects", total, substore.kind());
        println!("{}, {:?}_valid_projects", valid, substore.kind());
        println!("{}, {:?}_active_projects", active, substore.kind());
        total_valid += total;
        total_active += active;
    }
    println!("{}, total_projects", projects.len());
    println!("{}, total_valid_projects", total_valid);
    println!("{}, total_active_projects", total_active);
}

/** Shows full information about given project. 
 */
fn example_show_project(url : & str, savepoint : Option<& str>) {
    // create the datastore and savepoint
    let ds = DatastoreView::new(& SETTINGS.datastore_root);
    let sp = ds.get_savepoint(savepoint).unwrap();
    // determine the ID of the project
    let p = ds.project_urls().iter(& sp).filter(|(_, p)| p.matches_url(url)).next();
    if let Some((id, _)) = p {
        // get the project
        let projects = ds.projects(& sp);
        let p = projects.get(& id).unwrap(); // must be valid
        println!("Project id: {}, url: {}", id, p.url.clone_url());
        // now get all log entries and filter those of our project
        let log : Vec<ProjectLog> = ds.project_log().iter(& sp).filter(|(log_id, _)| id == *log_id ).map(|(_, p)| p).collect();
        println!("log: {} entries", log.len());
        for l in log {
            println!("    {}", l);
        }
        // print the heads too
        println!("heads: {} entries", p.heads.len());
        for (name, (id, hash)) in p.heads.iter() {
            println!("    {}: {} (id {})", name, p.url.get_commit_terminal_link(*hash), id);
        }
        // and get all commits, for which we have a convenience function in the API, because why not 
        let commits = ds.project_commits(&p);
        let ss = ds.get_substore(p.substore);
        let mut commit_hashes = ss.commits();
        let mut users = ss.users();
        let mut paths = ss.paths_strings();
        let mut hashes = ss.hashes();
        println!("commits: {} entries", commits.len());
        for (id, commit) in commits {
            let commit_hash = commit_hashes.get(id).unwrap();
            println!("    {}", p.url.get_commit_terminal_link(commit_hash));
            println!("        committer: {} (id {}), time {}", users.get(commit.committer).unwrap(), commit.committer, helpers::pretty_timestamp(commit.committer_time));
            println!("        author: {} (id {}), time {}", users.get(commit.author).unwrap(), commit.author, helpers::pretty_timestamp(commit.author_time));
            print!("        parents:");
            for pid in commit.parents {
                print!(" {} (id {})", p.url.get_commit_terminal_link(commit_hashes.get(pid).unwrap()), pid);
            }
            println!("");
            println!("        message: {}", commit.message);
            println!("        changes:");
            for (path_id, hash_id) in commit.changes {
                let hash = hashes.get(hash_id).unwrap();
                println!("            {} : {} (id {} : id {})", p.url.get_change_terminal_link(commit_hash, & paths.get(path_id).unwrap(), hash), hash, path_id, hash_id);
            }
            println!("");
        }

    } else {
        println!("ERROR: No project matches the given url {}", url);
    }
}

fn datastore_contents_compression() {
    let ds = DatastoreView::new(& SETTINGS.datastore_root);
    let sp = ds.current_savepoint();
    let mut compressed : usize = 0;
    let mut uncompressed : usize = 0;
    for ss in ds.substores() {
        let comp = ss.contents_size().contents;
        compressed = compressed + comp;
        let mut uncomp = 0;
        for (_id, _kind, contents) in ss.contents().iter(& sp) {
            uncomp = uncomp + 16 + contents.len(); // id + size
        }
        uncompressed += uncomp;
        println!("{:?}: compressed : {}, uncompressed : {}", ss.kind(), comp, uncomp);
    }
    println!("TOTAL: compressed : {}, uncompressed : {}", compressed, uncompressed);
}
fn datastore_debug() {
    let ds = DatastoreView::new(& SETTINGS.datastore_root);
    let sp = ds.current_savepoint();
    ds.projects(& sp);
}


