use dcd::datastore::*;
use dcd::db::*;



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

}

/** Adds projects from given file or a single url project to the datastore. 
 
    To be able to add projects, the datastore must be loaded 
 */
fn dcd_add(args : Vec<String>) {
    let ds = Datastore::load();
    

}


fn dcd_update(args : Vec<String>) {

}

fn help() {
    println!("Usage:");

    std::process::exit(-1);
}