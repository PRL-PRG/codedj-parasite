#[macro_use]
extern crate clap;
use clap::{App, ArgMatches};

fn main() {
    let yaml = load_yaml!("parasite-cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    match matches.subcommand() {
        ("create", Some(args)) => create(& matches, args),
        ("add", Some(args)) => add(& matches, args),
        _ => {
            panic!("Invalid command-line arguments");
        }
    }
}

fn create(_args : & ArgMatches, _cmd_args : & ArgMatches) {

} 

fn add(_args : & ArgMatches, _cmd_args : & ArgMatches) {
    
} 