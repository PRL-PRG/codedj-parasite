use log::*;


#[macro_use]
extern crate clap;
use clap::{App, ArgMatches};


use parasite::stamp;
use parasite::codedj::*;

fn main() {
    println!("codedj-parasite ver.{}", stamp::stamp());

    let yaml = load_yaml!("parasite-cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    let env = env_logger::Env::default()
        .filter_or("RUST_LOG", match matches.occurrences_of("verbose") {
            0 => "warn",
            1 => "info",
            _ => "debug",
        });
    env_logger::init_from_env(env);


    match matches.subcommand() {
        ("create", Some(args)) => create(& matches, args),
        ("add", Some(args)) => add(& matches, args),
        _ => {
            panic!("Invalid command-line arguments");
        }
    }
}

fn force(args : & ArgMatches) -> bool { args.is_present("force") }

fn datastore(args : & ArgMatches) -> String { args.value_of("datastore").unwrap().to_owned() }

fn check_stable_version(args: & ArgMatches) {
    if stamp::is_modified_version() {
        if force(args) {
            warn!("Current parasite version is dirty, but the action will be forced anyways");
        } else {
            panic!("Unable to proceed - parasite version is dirty. Make sure all your local changes are committed before running the command, or specify --force at your own peril");
        }
    }
}



/** Creates new CodeDJ superstore at the given folder. 
 
    The folder is not expected to exist, or the command must be forced, in which case the existing folder will be deleted. 
 */
fn create(args : & ArgMatches, _cmd_args : & ArgMatches) {
    check_stable_version(args);
    if force(args) {
        CodeDJ::force_create(datastore(args)).unwrap();
    } else {
        CodeDJ::create(datastore(args)).unwrap();
    }
} 

fn add(_args : & ArgMatches, _cmd_args : & ArgMatches) {
    
} 