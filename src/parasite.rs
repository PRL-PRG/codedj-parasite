//use log::*;


#[macro_use]
extern crate clap;
use clap::{App, ArgMatches};

#[allow(dead_code)]
mod stamp;

fn main() {
    println!("codedj-parasite ver.{}", stamp::stamp());

    let yaml = load_yaml!("parasite-cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    let env = env_logger::Env::default()
        .filter_or("RUST_LOG", if matches.is_present("verbose") { "debug" } else { "info" });
    env_logger::init_from_env(env);


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