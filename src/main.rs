
mod helpers;
#[allow(dead_code)]
mod db;
#[allow(dead_code)]
mod datastore;
#[allow(dead_code)]
mod records;
#[allow(dead_code)]
mod updater;
mod task_add_projects;
mod task_update_repo;
mod task_update_substore;
mod task_load_substore;
mod task_drop_substore;
mod task_verify_substore;
mod github;

use datastore::*;
use updater::*;

/** The main program.
 
    Really simple. 
 */
fn main() {
    println!("Dejacode Downloader v3 (datastore version {}", Datastore::VERSION);
    let mut datastore_root = String::from(std::env::current_dir().unwrap().to_str().unwrap());
    let mut command = "run".to_owned();
    let args : Vec<String> = std::env::args().collect();
    let mut arg_i = 1;
    while arg_i < args.len() {
        let arg = & args[arg_i];
        if arg == "-ds" || arg == "--datastore" {
            datastore_root = args.get(arg_i + 1).expect("Datastore root path missing").to_owned();
            arg_i += 2;
        } else {
            command = args[arg_i].to_owned();
            arg_i += 1;
        }
    }
    println!("    datastore root: {}", datastore_root);
    println!("    command: {}", command);
    let ds = Datastore::new(& datastore_root);
    let u = Updater::new(ds);
    u.run(command);
    println!("DCD terminated normally. Good bye!");
}



/*

use datastore::*;
use updater::*;

fn main() {
    println!("Dejacode Downloader mark III");
    let mut ds = datastore3::Datastore::new("/home/peta/ds3test");
    /*
    let mut x = db3::Store::<records3::Hash>::new("foo", "bar");
    let mut iter = db3::SplitIterator{
        iter : x.iter(),
        prefix : records3::StoreKind::Clojure,
    };*/



    /*
    let args : Vec<String> = std::env::args().collect();
    let mut arg_i = 1;
    // determine the working directory, which is either current directory, or can be specified with -o at first position
    let mut working_directory = String::from(std::env::current_dir().unwrap().to_str().unwrap());
    if arg_i < args.len() && args[arg_i].starts_with("-=o") {
        working_directory = args[arg_i][3..].to_string();
        arg_i += 1;
    }
    // see if the command position is specified and if the command is known, if not, enter the updater's GUI by default
    // TODO
    */
    
    
    


    println!("DejaCode Downloader mark II");
    let args : Vec<String> = std::env::args().collect();
    let mut i = 1;
    if args.len() <= i {
        help()
    }
    let mut wd = String::from(std::env::current_dir().unwrap().to_str().unwrap());
    if args[i].starts_with("-o=") {
        wd = args[i][3..].to_string();
        i += 1;
    }
    if args.len() <= i {
        help()
    }
    let cmd = & args[i];
    i += 1;
    match cmd.as_str() {
        "init" => dcd_init(& wd, & args[i..]),
        "add" => dcd_add(& wd, & args[i..]),
        "update" => dcd_update(& wd, & args[i..]),
        "export" => dcd_export(& wd, & args[i..]),
        &_ => help(),
    }
}

/** Initializes the datastore in current directory.  
 */
fn dcd_init(working_dir : & str, _args : & [String]) {
    // clear and create the working directory
    let wd_path = std::path::Path::new(working_dir);
    if wd_path.exists() {
        std::fs::remove_dir_all(&wd_path).unwrap();
    }
    std::fs::create_dir_all(&wd_path).unwrap();
    // create the datastore and initialize the basic values
    let ds = Datastore::from(working_dir);
    println!("Initializing new repository with common values...");
    ds.hashes.lock().unwrap().get_or_create(& git2::Oid::zero());
    println!("    hash 0");
}

/** Adds projects from given file or a single url project to the datastore. 

    For now, for project to be added, it must have unique url across all of the known urls, including the dead ones. This is correct for most cases, but one can imagive a project being created, then developed, then deleted and then a project of the same name, but different one being created as well. Or even moved and then old name reused.

    TODO how to actually handle this?
 */
fn dcd_add(working_dir : & str, args : & [String]) {
    if args.len() < 1 {
        help();
    }
    let ds = Datastore::from(working_dir);
    println!("Loading known project urls...");
    let mut urls = HashSet::<String>::new();
    for (_, url) in ds.project_urls.lock().unwrap().all_iter() {
        urls.insert(url);
    }
    println!("    urls: {}", urls.len());
    // now go through all arguments and see if they can be added
    for arg in args {
        if arg.starts_with("https://") {
            let url = translate_url(arg.to_owned());            
            println!("Adding project {}", & url);
            if urls.contains(& url) {
                println!("    already exists");
            } else {
                println!("    added as id: {}", ds.add_project(& url));
                urls.insert(url);
            }
        } else if arg.ends_with(".csv") {
            add_projects_from_csv(arg, & ds, & mut urls);
        } else {
            println!("Unrecognized project file or url format: {}", arg);
            help();
        }
    }
}

/** Given a csv file determines if it contains headers or not and determines the column that contains urls and adds projects from these urls to the datastore. 
 
    Column contains url if it starts with `https://`. Only one column can contain url for the csv to be parsed correctly and the column must stay the same for the entire file. 
 */
fn add_projects_from_csv(filename : & str, ds : & Datastore, urls : & mut HashSet<String>) {
    println!("Adding projects from csv file {}", filename);
    let mut records = 0;
    let mut added = 0;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(filename).unwrap();
    let headers = reader.headers().unwrap();
    let mut col_id = if let Some(id) = analyze_csv_row(& headers) {
        records += 1;
        let url = translate_url(String::from(& headers[id]));
        if ! urls.contains(& url) {
            ds.add_project(& url);
            urls.insert(url);
            added += 1;
        }
        id
    } else {
        std::usize::MAX
    };
    for x in reader.records() {
        let record = x.unwrap();
        if col_id == std::usize::MAX {
            if let Some(id) = analyze_csv_row(& record) {
                col_id = id;
            } else {
                println!("Cannot determine which column contains git urls.");
                help();
            }
        }
        records += 1;
        let url = translate_url(String::from(& record[col_id]));
        if ! urls.contains(& url) {
            ds.add_project(& url);
            urls.insert(url);
            added += 1;
        }
    }
    println!("    {} records", records);
    println!("    {} projects already exist", records - added);
    println!("    {} projects added", added);
}

fn translate_url(mut url: String) -> String {
    url = url.to_ascii_lowercase();
    if url.starts_with("https://api.github.com/repos/") {
        return format!("https://github.com/{}.git", & url[29..]);
    } else {
        return url;
    }
}

fn analyze_csv_row(row : & csv::StringRecord) -> Option<usize> {
    let mut i : usize = 0;
    let mut result : usize = std::usize::MAX;
    for x in row {
        if x.starts_with("https://") {
            // there are multiple indices that could be urls, so we can't determine 
            if result != std::usize::MAX {
                return None;
            }
            result = i;
        }
        i += 1;
    }
    if result != std::usize::MAX {
        return Some(result);
    } else {
        return None;
    }
}

/** Runs the incremental updater. 
 
    Creates the updater and starts the continuous update of the projects. 
 */ 
fn dcd_update(working_dir : & str, _args : & [String]) {
    let mut updater = Updater::new(Datastore::from(working_dir));
    updater.run();

}

fn dcd_export(working_dir : & str, _args : & [String]) {
    let dsview = DatastoreView::new(working_dir, helpers::now());
    /*
    for (id, data) in dsview.contents() {
        println!("{}:\n\n{}\n\n", id, helpers::to_string(& data));
    }
    */
}

fn help() {
    println!("Usage:");

    std::process::exit(-1);
}

*/