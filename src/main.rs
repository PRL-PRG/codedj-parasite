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
        "size" => datastore_size(),
        "summary" => datastore_summary(),
        "savepoints" => datastore_savepoints(),
        "contents-compression" => datastore_contents_compression(),
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

fn datastore_contents_compression() {
    let ds = DatastoreView::new(& SETTINGS.datastore_root);
    let sp = ds.latest();
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


