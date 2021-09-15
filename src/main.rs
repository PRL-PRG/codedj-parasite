#[macro_use]
extern crate lazy_static;
mod settings;

use settings::SETTINGS;

fn main() {
    // SETTINGS is a static variable, that when created initiliazes automatically the values provided at the command line.
    start_interactive();
}

/** Starts the interactive mode text user interface for the downloader. 

    If a command was given on the command line it will be automatically executed in the interactive mode. Otherwise the application will wait for a command to be entered. 
 */
fn start_interactive() {
    //let ds = Datastore::new(& SETTINGS.datastore_root, false);
    // let u = Updater::new(ds);
    // u.run(SETTINGS.command.join(" "));
}
