use std::time::{UNIX_EPOCH, Duration};
use std::collections::*;
use std::fs::{OpenOptions, File};
use std::io::{Write};
use std::path::{Path};
extern crate clap;
use clap::{Arg, App, SubCommand};

use parasite::*;

fn main() {
    let cmdline = App::new("Mistletoe")
        .about("Taps to parasite datastore and does useful stuff not just around xmas.")
        .arg(Arg::with_name("datastore")
            .short("ds")
            .long("datastore")
            .value_name("PATH")
            .help("Determines the path to parasite datastore to be used.")
            .takes_value(true))
        .arg(Arg::with_name("v")
            .short("v")
            .help("Sets the level of verbosity"))
        .subcommand(SubCommand::with_name("show-project")
                    .about("Shows information about a given project in the datastore.")
                    .arg(Arg::with_name("project")
                        .long("project")
                        .short("p")
                        .takes_value(true)
                        .help("name/url of the project to be exported"))
                    .arg(Arg::with_name("id")
                        .long("id")
                        .short("id")
                        .takes_value(true)
                        .help("Id of the project to be exported"))
            )
        .subcommand(SubCommand::with_name("export-project")
            .about("Creates a copy of the given project storing all files in the datastore as they existed in the project")
            .arg(Arg::with_name("project")
                .long("project")
                .short("p")
                .takes_value(true)
                .help("name/url of the project to be exported"))
            .arg(Arg::with_name("id")
                .long("id")
                .short("id")
                .takes_value(true)
                .help("Id of the project to be exported"))
            .arg(Arg::with_name("projects")
                .long("projects")
                .takes_value(true)
                .help("csv file that stores project ids to be exported"))
            .arg(Arg::with_name("column")
                .long("column")
                .short("col")
                .takes_value(true)
                .help("column in the projects csv file to be used for the ids"))
            .arg(Arg::with_name("into")
                .long("into")
                .takes_value(true)
                .help("Path into which the contents of the files will be saved, or where the summary will be written"))
            .arg(Arg::with_name("commit")
                .required(false)
                .takes_value(true)
                .help("Commit hash to be checked out (or its beginning)"))
            .arg(Arg::with_name("with-contents")
                .long("--with-contents")
                .required(false)
                .takes_value(false)
                .help("Exports also the contents of the project"))
        )
        .subcommand(SubCommand::with_name("show-commits")
            .about("Outputs information about all specified commits or a single commit determined by either its hash or id")
            .arg(Arg::with_name("id")
                .long("id")
                .takes_value(true)
                .help("Id of the project to be exported"))
            .arg(Arg::with_name("hash")
                .long("hash")
                .takes_value(true)
                .help("Hash of the commit to be displayed"))
        )
        .subcommand(SubCommand::with_name("check-heads")
            .about("Checks the head mappings")
        )
        .subcommand(SubCommand::with_name("check-projects")
            .about("Checks the projects, which are ok, and which are errors")
        )
        .get_matches();
    match cmdline.subcommand() {
        ("show-project",  Some(args)) => {
            show_project(& cmdline, args);
        },
        ("export-project",  Some(args)) => {
            export_project(& cmdline, args);
        },
        ("show-commits", Some(args)) => {
            show_commits(& cmdline, args);
        },
        ("check-heads", Some(args)) => {
            check_heads(& cmdline, args);
        },
        ("check-projects", Some(args)) => {
            check_projects(& cmdline, args);
        },
        
        _                       => {}, // Either no subcommand or one not tested for...
    }        
}

fn check_projects(cmdline : & clap::ArgMatches, args : & clap::ArgMatches) {
    // create the datastore and savepoint
    let ds = DatastoreView::from(cmdline.value_of("datastore").unwrap_or("."));
    let updates = ds.project_updates();
    let mut errors = HashSet::<ProjectId>::new();
    let mut max_id = 0;
    for (pid, update) in updates.into_iter() {
        if (max_id < u64::from(pid)) {
            max_id = u64::from(pid);
        }
        match update {
            ProjectLog::Error{ .. } => {
                errors.insert(pid);
            },
            _ => {
                errors.remove(&pid);
            }
        }
    }
    println!("Total projects: {}", max_id);
    println!("Errors:         {}", errors.len());
}

fn check_heads(cmdline : & clap::ArgMatches, args : & clap::ArgMatches) {
    // create the datastore and savepoint
    let ds = DatastoreView::from(cmdline.value_of("datastore").unwrap_or("."));
    let mut project_substores = HashMap::new();
    for (pid, substore) in ds.project_substores() {
        project_substores.insert(pid, substore);
    }
    println!("pid,substore,hash,id,actual_id");
    for store_kind in StoreKind::all() {
        let hashes : HashMap<SHA, CommitId> = ds.commits(store_kind).into_iter().map(|(id, hash)| (hash, id)).collect();
        let commits = ds.commits(store_kind);
        for (pid, heads) in ds.project_heads().filter(|(pid, _)| project_substores.get(pid).map(|x| *x == store_kind).unwrap_or(false)) {
            for (_, (id, hash)) in heads {
                match hashes.get(& hash) {
                    Some(actual_id) => {
                        if *actual_id != id {
                            println!("{},{:?},{},{},{}", pid, store_kind, hash, id, actual_id);
                        }
                    },
                    None => {
                        println!("ERROR");
                    }
                }
            }
        }
    }
}

/* Shows full information about given project. 
 
    A debugging command that finds a project with given url (or historical url) in the datastore and shows its stored information. This includes the current url and project id, the full log of the project and its heads.

    Then all commits of the project are printed, for each commit the parents, commit & author info, message and changes are printed. Each change shows the path and hash. 

    Change paths and commit hashes are displayed as terminal links, where supported. 
 */
fn show_project(cmdline : & clap::ArgMatches, args : & clap::ArgMatches) {
    // create the datastore and savepoint
    let ds = DatastoreView::from(cmdline.value_of("datastore").unwrap_or("."));
    let project = get_project_id(& ds, args);
    if let Some(pid) = project {
        // get the project
        let purl = get_project_url(& ds, pid);
        println!("Project id: {}, url: {}", pid, purl.clone_url());
        // now get all log entries and filter those of our project
        let log : Vec<ProjectLog> = ds.project_updates().filter(|(log_id, _)| pid == *log_id ).map(|(_, p)| p).collect();
        println!("log: {} entries", log.len());
        for l in log {
            println!("    {}", l);
        }
        // show the metadata
        if let Some(md) = ds.project_metadata().filter(|(id, _)| *id == pid).map(|(_, s)| s).last() {
            println!("Metadata: {}", md.value);
        }
        // determine the project's substore
        let substore = ds.project_substores().filter(|(id, _)| *id == pid).map(|(_, s)| s).last().unwrap();
        println!("substore: {:?}", substore);
        // if they exist, print heads and then the rest of the commits and their changes
        if let Some((_, heads)) =  ds.project_heads().filter(|(id, _)| *id == pid).last() {
            println!("heads: {} entries", heads.len());
            for (name, (id, hash)) in heads.iter() {
                println!("    {}: {} (id {})", name, purl.get_commit_terminal_link(*hash), id);
            }
            let mut commit_hashes = ds.commits(substore);
            let mut users = ds.users(substore);
            let mut paths = ds.paths_strings(substore);
            let mut hashes = ds.hashes(substore);
            for (commit_id, commit) in ProjectCommitsIterator::new(& heads, ds.commits_info(substore)) {
                let commit_hash = commit_hashes.get(commit_id).unwrap();
                println!("    {} (id {})", purl.get_commit_terminal_link(commit_hash), commit_id);
                println!("        committer: {} (id {}), time {}", users.get(commit.committer).unwrap(), commit.committer, pretty_timestamp(commit.committer_time));
                println!("        author: {} (id {}), time {}", users.get(commit.author).unwrap(), commit.author, pretty_timestamp(commit.author_time));
                print!("        parents:");
                for pid in commit.parents {
                    print!(" {} (id {})", purl.get_commit_terminal_link(commit_hashes.get(pid).unwrap()), pid);
                }
                println!("");
                println!("        message: {}", commit.message);
                println!("        changes:");
                for (path_id, hash_id) in commit.changes {
                    let hash = hashes.get(hash_id).unwrap();
                    println!("            {} : {} (id {} : id {})", purl.get_change_terminal_link(commit_hash, & paths.get(path_id).unwrap(), hash), hash, path_id, hash_id);
                }
                println!("");
    
            }
        }       
    } else {
        println!("ERROR: No matching project found");
    }
}


/** Exports all contents of a given project at given commit. 
 
    If commit is not given, uses the head commit. 
  */
fn export_project(cmdline : & clap::ArgMatches, args : & clap::ArgMatches) {
    // create the datastore and savepoint
    let ds = DatastoreView::from(cmdline.value_of("datastore").unwrap_or("."));
    let o_dir : String;
    let mut o_file : File;
    if args.is_present("with-contents") {
        o_dir = args.value_of("into").unwrap_or(".").to_owned();
        o_file = OpenOptions::new().write(true).create(true).open(format!("{}/{}", &o_dir, "export-project.csv")).unwrap();
    } else {
        o_dir = String::new();
        o_file = OpenOptions::new().write(true).create(true).open(args.value_of("into").unwrap_or("export-project.csv")).unwrap();        
    }
    writeln!(o_file, "pid,path,hash_id").unwrap();
    if let Some(projects) = args.value_of("projects") {
        println!("Exporting projects from {}", projects);
        // read the csv 
        let col_id = args.value_of("column").unwrap_or("0").parse::<usize>().unwrap();
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .double_quote(false)
            .escape(Some(b'\\'))
            .from_path(projects).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let pid = ProjectId::from(record[col_id].parse::<u64>().unwrap());
            println!("{}", pid);
            export_single_project(&ds, pid, & mut o_file, & o_dir);
        }
        return;
    } else {
        let project = get_project_id(& ds, args);
        if let Some(pid) = project {
            export_single_project(& ds, pid, & mut o_file, & o_dir);
            return;
        } 
    }
    println!("ERROR: No matching project found");
}

/** Trivial pretty printer for unix epoch */
fn pretty_timestamp(ts : i64) -> String {
    let d = UNIX_EPOCH + Duration::from_secs(ts as u64);
    let dt : chrono::DateTime<chrono::offset::Utc> = d.into();
    return dt.format("%F %T").to_string();
}


fn get_project_id(ds : & DatastoreView, args : & clap::ArgMatches) -> Option<ProjectId> {
    if let Some(id) = args.value_of("id") {
        return Some(ProjectId::from(id.parse::<u64>().unwrap()));
    } else if let Some(project) = args.value_of("project") {
        if let Some((pid, _)) = ds.project_urls().into_iter().filter(|(_, p)| p.matches_url(project)).next() {
            return Some(pid);
        }
    } 
    return None;
}

fn get_project_url(ds : & DatastoreView, id : ProjectId) -> ProjectUrl {
    return ds.project_urls().get(id).unwrap();
}

fn get_project_main_branch(ds : & DatastoreView, pid : ProjectId) -> Option<String> {
    // since we do may not have an index available, just scan linearly
    if let Some(metadata) = ds.project_metadata().filter(|(id, metadata)| {
        return *id == pid && metadata.key == Metadata::GITHUB_METADATA;
    }).last() {
        if let Ok(metadata_json) = json::parse(& metadata.1.value) {
            let x = & metadata_json["default_branch"];
            if x.is_string() {
                return Some(x.to_string());
            } else {
                return None;
            }
        }
    }
    return None;
}

fn export_single_project(ds : & DatastoreView, pid : ProjectId, output : & mut File, out_dir : & String) {
    // get the project
    // determine the project's substore
    let substore = ds.project_substores().filter(|(id, _)| *id == pid).map(|(_, s)| s).last().unwrap();
    // let latest metadata and determine main branch
    let main_branch = format!("refs/heads/{}", get_project_main_branch(& ds, pid).unwrap_or("master".to_owned()));
    println!("main branch: {}", main_branch);
    // now get the head commit
    let mut commit : Option<CommitId> = None;
    if let Some((_, heads)) = ds.project_heads().filter(|(id, _)| *id == pid).last() {
        for (name, (id, _hash)) in heads.iter() {
            if main_branch.eq(name) {
                commit = Some(*id);
                break;
            }
        }
    }
    // we have the commit to checkout, perform the checkout
    if let Some(id) = commit {
        let changes = checkout_commit(& ds, id, substore);
        let mut contents = ds.contents(substore);
        for (path, hash) in changes {
            writeln!(output, "{},\"{}\",{}", pid, path, hash).unwrap();
            // if given the output directory, we should also check if we have the contents and if so, store them appropriately
            if ! out_dir.is_empty() {
                if let Some(bytes) = contents.get(hash) {
                    let pstr = format!("{}/{}/{}", out_dir, pid, path);
                    let p = Path::new(pstr.as_str());
                    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
                    let mut f = File::create(p).unwrap();
                    f.write_all(& bytes.1).unwrap();
                }
            }
        }
    }
}

/** Iterate over all parent commits and determine the tree state. 
 */
fn checkout_commit(ds : & DatastoreView, commit : CommitId, substore : StoreKind) -> HashMap<String, HashId> {
    let mut tree = HashMap::<PathId,HashId>::new();
    let mut q = Vec::<CommitId>::new();
    let mut visited = HashSet::<CommitId>::new();
    q.push(commit);
    let mut commits = ds.commits_info(substore);
    while let Some(commit_id) = q.pop() {
        // ignore if already visited
        if visited.contains(& commit_id) {
            continue;
        } else {        
            visited.insert(commit_id);
        }
        // get the commit info, add new changes and parents
        if let Some(commit_info) = commits.get(commit_id) {
            commit_info.changes.iter().for_each(|(path_id, hash_id)| {
                if ! tree.contains_key(path_id) {
                    tree.insert(*path_id, *hash_id);
                }
            });
            commit_info.parents.iter().for_each(|id| {
                q.push(*id);
            });
        }
    }
    // now convert the tree to a hashmap with real paths, ignoring deleted files
    let mut path_strings = ds.paths_strings(substore);
    return tree.into_iter()
        .filter(|(_path_id, hash_id)| HashId::DELETED != *hash_id)
        .map(|(path_id, hash_id)| (path_strings.get(path_id).unwrap(), hash_id))
        .collect();
}

/** Shows the commits */
fn show_commits(cmdline : & clap::ArgMatches, args : & clap::ArgMatches) {
    // create the datastore and savepoint
    let ds = DatastoreView::from(cmdline.value_of("datastore").unwrap_or("."));
    let substore = StoreKind::JavaScript;
    if args.is_present("id") {
        let id = CommitId::from(args.value_of("id").unwrap().parse::<u64>().unwrap());
        match ds.commits(substore).get(id) {
            Some(hash) => {
                println!("Commit id: {}, hash {}", id, hash);
                let mut commit_hashes = ds.commits(substore);
                let mut users = ds.users(substore);
                let mut paths = ds.paths_strings(substore);
                let mut hashes = ds.hashes(substore);
                let commit = ds.commits_info(substore).get(id).unwrap();
                println!("        committer: {} (id {}), time {}", users.get(commit.committer).unwrap(), commit.committer, pretty_timestamp(commit.committer_time));
                println!("        author: {} (id {}), time {}", users.get(commit.author).unwrap(), commit.author, pretty_timestamp(commit.author_time));
                print!("        parents:");
                for pid in commit.parents {
                    print!(" {} (id {})", commit_hashes.get(pid).unwrap(), pid);
                }
                println!("");
                println!("        message: {}", commit.message);
                println!("        changes:");
                for (path_id, hash_id) in commit.changes {
                    let hash = hashes.get(hash_id).unwrap();
                    println!("            {} : {} (id {} : id {})", & paths.get(path_id).unwrap(), hash, path_id, hash_id);
                }
            },
            None => println!("Commit {} not found", id),
        }
    } else {
        for (commit_id, hash) in ds.commits(StoreKind::JavaScript) {
            println!("Commit id: {}, hash {}", commit_id, hash);
        }
    }
}

