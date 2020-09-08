use dcd::db_manager::*;
/** Initializes database at given path.
 */

 fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        panic!{"Invalid usage - dcd-init PATH_TO_DATABASE"};
    }
    DatabaseManager::initialize_new(& args[1]);
 }