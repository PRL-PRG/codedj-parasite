use serde::Deserialize;
use std::fs::File;

lazy_static! {
    pub static ref SETTINGS : Settings = Settings::parse_from_commandline();
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub interactive : bool,
    pub verbose : bool,
    pub datastore_root : String, 
    pub github_tokens : String,
    pub num_threads : usize,
    pub command : Vec<String>,
}

impl Settings {
    fn default() -> Settings {
        return Settings{
            interactive : false,
            verbose : false,
            //datastore_root : ".".to_owned(),
            //github_tokens : Some("github-tokens.csv".to_owned());
            datastore_root : "/dejavuii/dcd3".to_owned(),
            github_tokens : "/mnt/data/github-tokens.csv".to_owned(),
            num_threads : 16,
            command : Vec::new(),
        };
    }

    /** Parses the commandline arguments into the global settings and returns the remaining command. 
     */
    fn parse_from_commandline() -> Settings {
        let mut settings = Settings::default();
        let args : Vec<String> = std::env::args().collect();
        let mut arg_i = 1;
        while arg_i < args.len() {
            let arg = & args[arg_i];
            if arg == "-cf" || arg == "--config_file" {
                
                let config_file_path = args.get(arg_i + 1).expect("path to configuration file not provided").to_owned();
                let file = File::open(config_file_path).unwrap();
                settings = serde_json::from_reader(file).expect("JSON was not well-formatted");
                arg_i += 2;
            } else if arg == "-ds" || arg == "--datastore" {
                settings.datastore_root = args.get(arg_i + 1).expect("Datastore root path missing").to_owned();
                arg_i += 2;
            } else if arg == "-i" || arg == "--interactive" {
                settings.interactive = true;
                arg_i += 1;
            } else if arg == "-v" || arg == "--verbose" {
                settings.verbose = true;
                arg_i += 1;
            } else if arg == "-ght" || arg == "--github-tokens" {
                settings.github_tokens = args.get(arg_i + 1).expect("Github tokens path missing").to_owned();
                arg_i += 2;
            } else if arg == "-n" || arg == "--num-threads" {
                settings.num_threads = args.get(arg_i + 1).expect("Number of threads missing").parse::<usize>().unwrap();
                arg_i += 2;
            } else {
                break;
            }
        }
        // the rest of arguments form the command (or commands)
        settings.command = args[arg_i..].iter().map(|x| { x.to_owned() }).collect();
        return settings;
    }
}

#[macro_export]
macro_rules! LOG {
    ($($tts:tt)*) => { {
        if SETTINGS.verbose { println!($($tts)*) };
    } }
}