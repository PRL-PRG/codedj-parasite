
pub (crate) struct Settings {
    pub interactive : bool,
    pub verbose : bool,
    pub datastore_root : String, 
    pub github_tokens : String,
    pub command : Vec<String>,
    pub num_threads : usize
}

impl Settings {
    pub fn new(args : Vec<String>) -> Settings {
        let mut result = Settings{
            interactive : false,
            verbose : false,
            // TODO: These are the correct values, while the ones below are shortcuts for simple testing
            //datastore_root : String::from(std::env::current_dir().unwrap().to_str().unwrap()),
            //github_tokens : "github-tokens.csv".to_owned(),
            datastore_root : "/dejavuii/dcd3".to_owned(),
            github_tokens : "/mnt/data/github-tokens.csv".to_owned(),
            command : Vec::new(),
            num_threads : 16,
        };

        let mut arg_i = 1;
        while arg_i < args.len() {
            let arg = & args[arg_i];
            if arg == "-ds" || arg == "--datastore" {
                result.datastore_root = args.get(arg_i + 1).expect("Datastore root path missing").to_owned();
                arg_i += 2;
            } else if arg == "-i" || arg == "--interactive" {
                result.interactive = true;
                arg_i += 1;
            } else if arg == "-v" || arg == "--verbose" {
                result.verbose = true;
                arg_i += 1;
            } else if arg == "-ght" || arg == "--github-tokens" {
                result.datastore_root = args.get(arg_i + 1).expect("Github tokens path missing").to_owned();
                arg_i += 2;
            } else if arg == "-n" || arg == "--num-threads" {
                result.num_threads = args.get(arg_i + 1).expect("Number of threads missing").parse::<usize>().unwrap();
                arg_i += 2;
            } else {
                break;
            }
        }
        // the rest of arguments form the command (or commands)
        result.command = args[arg_i..].iter().map(|x| { x.to_owned() }).collect();
        return result;
    }
}
