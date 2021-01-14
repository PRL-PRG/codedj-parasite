
static mut SETTINGS_ : Option<Settings> = None;

struct Settings {
    interactive : bool,
    verbose : bool,
    datastore_root : String, 
    github_tokens : String,
    num_threads : usize
}

/** Parses the commandline arguments into the global settings and returns the remaining command. 
 */
pub fn parse_command_line_args(args : Vec<String>) -> Vec<String> {
    unsafe {
        let mut settings = Settings::default();
        let mut arg_i = 1;
        while arg_i < args.len() {
            let arg = & args[arg_i];
            if arg == "-ds" || arg == "--datastore" {
                settings.datastore_root = args.get(arg_i + 1).expect("Datastore root path missing").to_owned();
                arg_i += 2;
            } else if arg == "-i" || arg == "--interactive" {
                settings.interactive = true;
                arg_i += 1;
            } else if arg == "-v" || arg == "--verbose" {
                settings.verbose = true;
                arg_i += 1;
            } else if arg == "-ght" || arg == "--github-tokens" {
                settings.datastore_root = args.get(arg_i + 1).expect("Github tokens path missing").to_owned();
                arg_i += 2;
            } else if arg == "-n" || arg == "--num-threads" {
                settings.num_threads = args.get(arg_i + 1).expect("Number of threads missing").parse::<usize>().unwrap();
                arg_i += 2;
            } else {
                break;
            }
        }
        SETTINGS_ = Some(settings);
        // the rest of arguments form the command (or commands)
        return args[arg_i..].iter().map(|x| { x.to_owned() }).collect();
    }
}



pub fn github_tokens() -> String {
    unsafe {
        if let Some(settings) = & SETTINGS_ {
            return settings.github_tokens.to_owned();
        }
        unreachable!();
    }
}

pub fn datastore_root() -> String {
    unsafe {
        if let Some(settings) = & SETTINGS_ {
            return settings.datastore_root.to_owned();
        }
        unreachable!();
    }
}

pub fn verbose() -> bool {
    unsafe {
        let raw_ptr = & SETTINGS_ as *const Option<Settings>;        
        println!("Address: {:?}", raw_ptr);
        if let Some(settings) = & SETTINGS_ {
            return settings.verbose;
        }
        unreachable!();
    }
}

pub fn interactive() -> bool {
    unsafe {
        if let Some(settings) = & SETTINGS_ {
            return settings.interactive;
        }
        unreachable!();
    }
}

pub fn num_threads() -> usize {
    unsafe {
        if let Some(settings) = & SETTINGS_ {
            return settings.num_threads;
        }
        unreachable!();
    }
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
        };

    }
}

#[macro_export]
macro_rules! LOG {
    ($($tts:tt)*) => { {
        if (settings::verbose()) { println!($($tts)*) };
    } }
}