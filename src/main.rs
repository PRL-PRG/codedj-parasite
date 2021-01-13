
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

use datastore::*;
use updater::*;

use parasite::*;

/** The incremental downloader and command-line interface
 
 */
fn main() {
    // defaults, i.e. no interactive mode, dataset in current directory
    let mut interactive : bool = false;
    let mut verbose : bool = false;
    let mut root = String::from(std::env::current_dir().unwrap().to_str().unwrap());
    // analyze the common command line arguments to determine the mode to be used and the 
    let args : Vec<String> = std::env::args().collect();
    let mut arg_i = 1;
    while arg_i < args.len() {
        let arg = & args[arg_i];
        if arg == "-ds" || arg == "--datastore" {
            root = args.get(arg_i + 1).expect("Datastore root path missing").to_owned();
            arg_i += 2;
        } else if arg == "-i" || arg == "--interactive" {
            interactive = true;
            arg_i += 1;
        } else if arg == "-v" || arg == "--verbose" {
            verbose = true;
            arg_i += 1;
        } else {
            break;
        }
    }
    // the rest of arguments form the command (or commands)
    let cmd : Vec<String> = args[arg_i..].iter().map(|x| { x.to_owned() }).collect();
    if verbose {
        println!("Parasite v. 0.3");
        println!("    interactive :    {}", interactive);
        println!("    verbose :        {}", verbose);
        println!("    datastore root : {}", root);
        println!("    command :        {}", cmd.join(" "));
    }
    // execute either the interactive updater, or the command line tool
    if interactive {
        start_interactive(root, verbose, cmd);
    } else {
        execute_command(root, verbose, cmd);
    }
}


/** Starts the interactive mode text user interface for the downloader. 

    If a command was given on the command line it will be automatically executed in the interactive mode. Otherwise the application will wait for a command to be entered. 
 */
fn start_interactive(root : String, _verbose : bool, command : Vec<String>) {
    let ds = Datastore::new(& root, false);
    let u = Updater::new(ds);
    u.run(command.join(" "));
}

/** Executes given command in a non-interactive mode.
 */
fn execute_command(root : String, _verbose : bool, command : Vec<String>) {
    if command.is_empty() {
        return datastore_summary(root);
    }
    match command[0].as_str() {
        "size" => datastore_size(root),
        "savepoints" => datastore_savepoints(root),
        "contents_compression" => datastore_contents_compression(root),
        _ => println!("ERROR: Unknown command {}", command[0]),
    }
}

fn datastore_summary(root: String) {
    let ds = DatastoreView::new(& root);
    println!("{}", ds.summary());
}

fn datastore_size(root : String) {
    let ds = DatastoreView::new(& root);
    println!("kind,contents,indices");
    println!("savepoints,{}", ds.savepoints_size());
    println!("projects,{}", ds.projects_size());
    println!("commits,{}", ds.commits_size());
    println!("contents,{}", ds.contents_size());
    println!("paths,{}", ds.paths_size());
    println!("users,{}", ds.users_size());
    println!("total,{}", ds.datastore_size());
}

fn datastore_savepoints(root : String) {
    let ds = DatastoreView::new(& root);
    let mut s = ds.savepoints();
    let mut num = 0;
    for (_, sp) in s.iter() {
        println!("{}", sp);
        num += 1;
    }
    println!("Total {} savepoints found.", num);
}

fn datastore_contents_compression(root : String) {
    let ds = DatastoreView::new(& root);
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


