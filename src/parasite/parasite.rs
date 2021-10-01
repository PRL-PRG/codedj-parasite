use log::*;


#[macro_use]
extern crate clap;
use clap::{App, ArgMatches};


#[allow(dead_code)]
mod updater;

#[allow(dead_code)]
mod project_updater;


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
        ("log", Some(args)) => command_log(& matches, args),
        ("add", Some(args)) => add(& matches, args),
        _ => {
            panic!("Invalid command-line arguments");
        }
    }
}

/** Shorthand that returns if the commands are to be forced. 
 */
fn force(args : & ArgMatches) -> bool { args.is_present("force") }

/** Shorthand that returns the dataset path. 
 */
fn datastore(args : & ArgMatches) -> String { args.value_of("datastore").unwrap().to_owned() }

/** Checks that the current version of parasite is clean, that is all local changes are committed to GitHub (we don't really check for pushes, but kind of assume they happen). Panics if not, unless the command is forced, in which case we let the user do whatever stupidity they want. 
 */
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
    let mut cdj = if force(args) {
        CodeDJ::force_create(datastore(args)).unwrap()
    } else {
        CodeDJ::create(datastore(args)).unwrap()
    };
    cdj.start_command().unwrap();
    cdj.end_command().unwrap();
} 

/** Prints the command log associated with the superstore. 
 */
fn command_log(args : & ArgMatches, _cmd_args : &ArgMatches) {
    let mut cdj = CodeDJ::open(datastore(args)).unwrap();
    let mut start_time = 0;
    let mut start_version = String::new();
    let mut start_cmd = String::new();
    let mut in_progress = false;
    println!("status, start, duration, version, command");
    for entry in cdj.command_log() {
        match entry {
            // TODO maybe rename log to log entry to spare us this monstrosity? 
            parasite::codedj::Log::CommandStart{time, version, cmd} => {
                if in_progress {
                    println!("UNTERMINATED, \"{}\", \"?\", {}, \"{}\"", parasite::pretty_epoch(start_time), version, cmd);
                }
                start_time = time;
                start_version = version;
                start_cmd = cmd;
                in_progress = true;
            },
            parasite::codedj::Log::CommandEnd{time} => {
                if ! in_progress {
                    println!("MISSING_START, \"{}\", \"?\", \"?\"", time);
                } else {
                    println!("OK, \"{}\", {}, {}, \"{}\"", parasite::pretty_epoch(start_time), parasite::pretty_duration(time - start_time), start_version, start_cmd);
                    in_progress = false;
                }
            }
        }
    }

}

fn add(_args : & ArgMatches, _cmd_args : & ArgMatches) {
    
} 