use std::collections::*;
use std::io::{Seek, SeekFrom};
use byteorder::*;

#[macro_use]
extern crate lazy_static;

mod helpers;
#[allow(dead_code)]
mod db;
#[allow(dead_code)]
mod datastore;
#[allow(dead_code)]
mod records;
#[allow(dead_code)]
mod updater;
mod datastore_maintenance_tasks;
mod task_update_repo;
mod task_update_substore;
mod task_verify_substore;
mod github;
mod settings;
#[allow(dead_code)]
mod reporter;

use datastore::*;
use updater::*;
use github::*;

use parasite::*;
use reporter::*;

use settings::SETTINGS;
use task_update_repo::*;

use crate::db::Serializable;
use crate::db::Indexable;
use crate::db::SplitKind;

/** The incremental downloader and command-line interface
 
 */
fn main() {
    // this also initializes the settings from the commandline implicitly, meh....
    LOG!("Parasite v. 0.3");
    LOG!("    interactive :    {}", SETTINGS.interactive);
    LOG!("    verbose :        {}", SETTINGS.verbose);
    LOG!("    threads :        {}", SETTINGS.num_threads);
    LOG!("    datastore root : {}", SETTINGS.datastore_root);
    LOG!("    command :        {}", SETTINGS.command.join(" "));
    // execute either the interactive updater, or the command line tool
    if SETTINGS.interactive {
        start_interactive();
    } else {
        execute_command();
    }
}


/** Starts the interactive mode text user interface for the downloader. 

    If a command was given on the command line it will be automatically executed in the interactive mode. Otherwise the application will wait for a command to be entered. 
 */
fn start_interactive() {
    let ds = Datastore::new(& SETTINGS.datastore_root, false);
    let u = Updater::new(ds);
    u.run(SETTINGS.command.join(" "));
}

/** Executes given command in a non-interactive mode.
 */
fn execute_command() {
    if SETTINGS.command.is_empty() {
        return datastore_size();
    }
    match SETTINGS.command[0].as_str() {
        // maintenance commands 
        "size" => datastore_size(),
        "summary" => datastore_summary(),
        "savepoints" => datastore_savepoints(),
        "add" => datastore_add(SETTINGS.command.get(1).unwrap()),
        "create-savepoint" => datastore_create_savepoint(SETTINGS.command.get(1).unwrap()),
        "revert-to-savepoint" => datastore_revert_to_savepoint(SETTINGS.command.get(1).unwrap()),
        "update-project" => datastore_update_project(
            SETTINGS.command.get(1).unwrap(),
            SETTINGS.command.get(2),
        ),
        "merge" => datastore_merge(
            SETTINGS.command.get(1).unwrap(), // source path
            SETTINGS.command.get(2).unwrap(), // source substore
            SETTINGS.command.get(3).unwrap() // target substore
        ),
        "merge-all" => datastore_merge_all(
            SETTINGS.command.get(1).unwrap(), // source path
            SETTINGS.command.get(2).unwrap() // target substore
        ),
        // maintenance commands
        "fix-moving-sentinel-values" => datastore_fix_moving_sentinel_values(

        ),
        // example commands
        "active-projects" => example_active_projects(
            SETTINGS.command.get(1).map(|x| { x.parse::<i64>().unwrap() }).unwrap_or(90 * 24 * 3600)
        ),
        "show-project" => example_show_project(
            SETTINGS.command.get(1).unwrap(),
        ),
        // debug commands
        "contents-compression" => datastore_contents_compression(),
        "debug" => datastore_debug(),
        _ => println!("ERROR: Unknown command {}", SETTINGS.command[0]),
    }
}

fn datastore_summary() {
    /*
    let ds = DatastoreView::from(& SETTINGS.datastore_root);
    
    println!("{}", ds.summary());
    */
}

fn datastore_size() {
    /*
    let ds = DatastoreView::from(& SETTINGS.datastore_root);
    println!("kind,contents,indices");
    println!("savepoints,{}", ds.savepoints_size());
    println!("projects,{}", ds.projects_size());
    println!("commits,{}", ds.commits_size());
    println!("contents,{}", ds.contents_size());
    println!("paths,{}", ds.paths_size());
    println!("users,{}", ds.users_size());
    println!("total,{}", ds.datastore_size());
    */
}

fn datastore_savepoints() {
    let ds = DatastoreView::from(& SETTINGS.datastore_root);
    let mut num = 0;
    for sp in ds.savepoints() {
        println!("{}", sp);
        num += 1;
    }
    println!("Total {} savepoints found.", num);
}

/** Adds the given project or projects specified in a csv file to the datastore. 
 */
fn datastore_add(url_or_file : & str) {
    TerminalReporter::report(|reporter : & TerminalReporter| {
        let ds = Datastore::new(& SETTINGS.datastore_root, false);
        reporter.run_task(Task::AddProjects{source : url_or_file.to_owned()}, |ts| {
            return datastore_maintenance_tasks::task_add_projects(& ds, url_or_file.to_owned(), ts);
        });
    });
}

/** Creates a savepoint of given name from the current datastore state. 
 */
fn datastore_create_savepoint(name : & str) {
    TerminalReporter::report(|reporter : & TerminalReporter| {
        let ds = Datastore::new(& SETTINGS.datastore_root, false);
        reporter.run_task(Task::CreateSavepoint{name : name.to_owned()}, |ts| {
            return datastore_maintenance_tasks::task_create_savepoint(& ds, ts);
        });
    });
}

/** Reverts the datastore to given saveopoint. 
 */
fn datastore_revert_to_savepoint(name : & str) {
    {
        let ds = Datastore::new(& SETTINGS.datastore_root, false);
        let sp = ds.get_savepoint(name).unwrap();
        ds.revert_to_savepoint(&sp);
    }
    datastore_size();
}

/** Forces the update of given project. 
 */
fn datastore_update_project(project : & str, force_opt : Option<& String>) {
    let mut force = false;
    if let Some(opt) = force_opt {
        if opt == "--force" {
            force = true;
        } else {
            panic!("Unknown option {}", opt);
        }
    }
    TerminalReporter::report(|reporter : & TerminalReporter| {
        let ds = Datastore::new(& SETTINGS.datastore_root, false);
        let gh = Github::new(& SETTINGS.github_tokens);
        let p = ds.projects.lock().unwrap().iter_all().filter(|(_, p)| p.matches_url(project)).next();
        if let Some((id, _)) = p {
            reporter.run_task(Task::UpdateRepo{
                id : id, 
                last_update_time : ds.get_project_last_update(id).map(|x| x.time()).or(Some(0)).unwrap()
            }, |ts| {
                return task_update_repo(& ds, & gh, ts, force, true);
            });
        } else {
            panic!("No project named {} found", project);
        }
    });
}

/** Merges specific substore of given source datastore to itself. 
 */

fn datastore_merge(source_path : & str, source_substore : & str, target_substore : & str) {
    // TODO check that we are not merging same substore of same datastore
    // TODO can we actually merge same datastore but different substores?
    let mut merger = DatastoreMerger::new(& SETTINGS.datastore_root, source_path);
    merger.merge_substore(
        StoreKind::from_string(target_substore).unwrap(),
        StoreKind::from_string(source_substore).unwrap(),
        ValidateAll::new(),
    );
}

/** Merges all substores from source to given substore in target.
 */
fn datastore_merge_all(source_path : & str, target_substore : & str) {
    // TODO check that we are not merging same substore of same datastore
    // TODO can we actually merge same datastore but different substores?
    let mut merger = DatastoreMerger::new(& SETTINGS.datastore_root, source_path);
    for substore in StoreKind::all() {
        merger.merge_substore(
            StoreKind::from_string(target_substore).unwrap(),
            substore,
            ValidateAll::new(),
        );
    }
}

/** Fixes the empty values for StoreKind and ContentsKind indices that the olden datasets used. 

    This is used in the following things:alloc

    - in each substore, kinds

    So that we do not pollute the codebase with the fix and that the fix won't change with possible newer versions, the code is hardcoded here. 

 */
fn datastore_fix_moving_sentinel_values() {
    let ds = Datastore::new(& SETTINGS.datastore_root, false);
    // we need to update the stored project substores and add 1 so that we make space for the new sentinel
    println!("Fixing project substores...");
    let mut substores = ds.project_substores.lock().unwrap();
    let ref mut f = substores.f;
    f.seek(SeekFrom::Start(0)).unwrap();
    loop {
        let offset = f.seek(SeekFrom::Current(0)).unwrap();
        if let Ok(id) = f.read_u64::<LittleEndian>() {
            let sstore = records::StoreKind::deserialize(f);
            f.seek(SeekFrom::Start(offset)).unwrap();
            f.write_u64::<LittleEndian>(id).unwrap();
            records::StoreKind::serialize(f, & records::StoreKind::from_number(sstore as u64 + 1));
        } else {
            break;
        }
    }
    // let mut substores = ds.project_substores.lock().unwrap();
    // we have to do contents for each substore:
    let mut t = 0;
    for sstore in ds.substores {
        let mut contents = sstore.contents.lock().unwrap();
        let mut st = 0;
        println!("Analyzing substore {}", sstore.prefix);
        for i in 0..contents.indexer.len() {
            let idx = records::HashId::from(i as u64);
            if let Some(x) = contents.indexer.get(idx) {
                if x.offset == u64::EMPTY && x.kind != records::ContentsKind::None {
                    st += 1;
                    contents.indexer.set(idx, & db::SplitOffset{offset : u64::EMPTY, kind : records::ContentsKind::None});
                } else {
                    // otherwise increase the kind for the None kind that comes first
                    contents.indexer.set(idx, & db::SplitOffset{offset : x.offset, kind : records::ContentsKind::from_number(x.kind as u64 + 1)});
                }
            }
        }
        println!("    {} sentinels fixed", st);
        t += st;
    }
    println!("TOTAL invalid sentinels: {}", t);

}

/** Displays active projects per substore. 
 
    A simple example of the library interface. Looks at heads of all projects on a per substore basis as the commit information is in a substore and calculates which active projects, which is projects whose latest commit has happened `max_age` before now.
 */
fn example_active_projects(max_age : i64) {
    let ds = DatastoreView::from(& SETTINGS.datastore_root);
    // get all projects 
    let projects = Project::assemble(& ds);
    let mut total_valid = 0;
    let mut total_active = 0;
    let now = helpers::now();
    println!("value,name");
    // on a per substore basis, determine the heads, then get their times from the substore and report
    for substore in StoreKind::all() {
        let mut heads = HashMap::<CommitId, i64>::new();
        let mut valid = 0;
        let mut total = 0;
        let mut commits = ds.commits_info(substore);
        for (_id, p) in projects.iter().filter(|(_, p)| { p.substore == substore }) {
            total += 1;
            if let Some(_) = p.latest_valid_update_time() {
                for (_branch, (commit_id, _hash)) in p.heads.iter() {
                    heads.entry(*commit_id).or_insert_with(|| { 
                        return commits.get(*commit_id).unwrap().committer_time;
                    });
                }
                valid += 1;
            }
        }
        // calculate which projects are active
        let active = projects.iter().filter(|(_, p)| { p.substore == substore }).filter(|(_id, p)| {
            for (_branch, (commit_id, _hash)) in p.heads.iter() {
                if let Some(time) = heads.get(& commit_id) {
                    if now - time <= max_age {
                        return true;
                    }
                }
            }
            return false;
        }).count();
        println!("{}, {:?}_projects", total, substore);
        println!("{}, {:?}_valid_projects", valid, substore);
        println!("{}, {:?}_active_projects", active, substore);
        total_valid += total;
        total_active += active;
    }
    println!("{}, total_projects", projects.len());
    println!("{}, total_valid_projects", total_valid);
    println!("{}, total_active_projects", total_active);
}

/** Shows full information about given project. 
 
    A debugging command that finds a project with given url (or historical url) in the datastore and shows its stored information. This includes the current url and project id, the full log of the project and its heads.

    Then all commits of the project are printed, for each commit the parents, commit & author info, message and changes are printed. Each change shows the path and hash. 

    Change paths and commit hashes are displayed as terminal links, where supported. 
 */
fn example_show_project(url : & str) {
    // create the datastore and savepoint
    let ds = DatastoreView::from(& SETTINGS.datastore_root);
    // determine the ID of the project
    let p = ds.project_urls().into_iter().filter(|(_, p)| p.matches_url(url)).next();
    if let Some((pid, purl)) = p {
        // get the project
        println!("Project id: {}, url: {}", pid, purl.clone_url());
        // now get all log entries and filter those of our project
        let log : Vec<ProjectLog> = ds.project_updates().filter(|(log_id, _)| pid == *log_id ).map(|(_, p)| p).collect();
        println!("log: {} entries", log.len());
        for l in log {
            println!("    {}", l);
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
                println!("    {}", purl.get_commit_terminal_link(commit_hash));
                println!("        committer: {} (id {}), time {}", users.get(commit.committer).unwrap(), commit.committer, helpers::pretty_timestamp(commit.committer_time));
                println!("        author: {} (id {}), time {}", users.get(commit.author).unwrap(), commit.author, helpers::pretty_timestamp(commit.author_time));
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
        println!("ERROR: No project matches the given url {}", url);
    }
}



fn datastore_contents_compression() {
    let ds = DatastoreView::from(& SETTINGS.datastore_root);
    let mut total_compressed = 0;
    let mut total_uncompressed = 0;
    for substore in StoreKind::all() {
        let mut contents = ds.contents(substore);
        let compressed = contents.filesize();
        let uncompressed = contents.into_iter().fold(0, |sum, (_, (_kind, data))| sum + data.len());
        println!("{:?}: compressed : {}, uncompressed : {}", substore, compressed, uncompressed);
        total_compressed += compressed;
        total_uncompressed += uncompressed;
    }
    println!("TOTAL: compressed : {}, uncompressed : {}", total_compressed, total_uncompressed);
}

fn datastore_debug() {
    /*
    let ds = DatastoreView::from(& SETTINGS.datastore_root);
    let sp = ds.current_savepoint();
    ds.projects(& sp);
    */
}


