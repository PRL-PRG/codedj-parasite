use std::collections::*;

mod db;
mod datastore;

use db::*;
use datastore::*;

fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        help()
    }
    match args[1].as_str() {
        "init" => dcd_init(args),
        "add" => dcd_add(args),
        "update" => dcd_update(args),
        &_ => help(),
    }
}

/** Initializes the datastore in current directory.  
 */
fn dcd_init(args : Vec<String>) {
    let mut x = PropertyStore::<String>::new("/home/peta/test.dat");
    x.set(0, & String::from("zeroth"));
    x.set(1, & String::from("first"));
    x.set(2, & String::from("second"));
    x.set(3, & String::from("third"));
    for (id, value) in x.iter() {
        println!("{}:{}", id, value);
    }
}

/** Adds projects from given file or a single url project to the datastore. 
 
    To be able to add projects, the datastore must be loaded 
 */
fn dcd_add(args : Vec<String>) {
    if (args.len() < 3) {
        help();
    }
    let ds = Datastore::from_cwd();
    println!("Loading known project urls...");
    let mut project_urls = ds.project_urls.lock().unwrap();
    let mut urls = HashSet::<String>::new();
    for (id, url) in project_urls.iter() {
        urls.insert(url);
    }
    println!("    urls: {}", urls.len());
    // now go through all arguments and see if they can be added
    for arg in & args[2..] {
        if arg.starts_with("https://") {
            println!("Adding project {}", arg);
            if urls.contains(arg) {
                println!("    already exists");
            } else {
                let id = project_urls.len() as u64;
                project_urls.set(id, arg);
                println!("    added as id: {}", id);
            }
        } else {
            println!("Unrecognized project file or url format: {}", arg);
            help();
        }
    }

}


fn dcd_update(args : Vec<String>) {

}

fn help() {
    println!("Usage:");

    std::process::exit(-1);
}