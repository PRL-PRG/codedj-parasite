use std::collections::*;
use std::io::Read;
use std::fs::*;
use std::io::Write;
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
mod reporter;

use datastore::*;
use updater::*;
use github::*;

use parasite::*;
use reporter::*;

use settings::SETTINGS;
use task_update_repo::*;

/** The incremental downloader and command-line interface
 
 */
fn main() {
    // execute either the interactive updater, or the command line tool
    if SETTINGS.interactive {
        println!("ERROR: Interactive mode not supported for ECOOP artifact commands.");
    } 
    if SETTINGS.command.is_empty() {
        println!("ERROR: No command specified.");
    }
    match SETTINGS.command[0].as_str() {
        "convert-1" => convert_1(
            SETTINGS.command.get(1).unwrap(), // source path
            SETTINGS.command.get(2).unwrap() // target substore
        ),
        "compact" => compact(
            SETTINGS.command.get(1).unwrap(), // input
            SETTINGS.command.get(2).unwrap(), // output pl
            SETTINGS.command.get(3).unwrap(), // output p
        ),
        "export" => export(
            SETTINGS.command.get(1).unwrap(), // input projects (file or --all)
            SETTINGS.command.get(2).unwrap(), // output file
            SETTINGS.command.get(3) // MAX_T, optional
        ),
        _ => println!("ERROR: Unknown command {}", SETTINGS.command[0]),
    }
}

/** For each project:
 
    - url
    - latest update
    - latest metadata
    - 
 
time,source,kind,key,value
 */
fn convert_project(id : u64, source_path : & str, ds : & Datastore, target_substore : records::StoreKind, commit_mapping : & HashMap<SHA,records::CommitId>) -> Result<bool, Box<dyn std::error::Error>> {
    use crate::db::Id;

    let mut url = String::new();
    let mut metadata = Vec::<(String, String)>::new();
    let mut heads = Vec::<(String, SHA)>::new();
    let mut ok = false;
    let mut reader = csv::ReaderBuilder::new()
    .has_headers(true)
    .double_quote(false)
    .escape(Some(b'\\'))
    .from_path(format!("{}/projects/0/{}/{}.csv", source_path, id % 1000, id))?;
    let mut clear_heads = false;
    for x in reader.records() {
        let record = x.unwrap();
        // we are only interested in projects that have been obtained through github
        if & record[1] == "GH" {
            ok = true;
        }
        match & record[2] {
            "init" => {
                url = record[4].to_owned();
            },
            "start" => {
                clear_heads = true;
            },
            "update" => {

            },
            "nochange" => { //???

            },
            "meta" => {
                metadata.push((record[3].to_owned(), record[4].to_owned()));
            },
            "head" => {
                if clear_heads {
                    heads.clear();
                    clear_heads = false;
                }
                heads.push((record[3].to_owned(), SHA::from_str(& record[4]).unwrap()));
            },
            // if there is an error, discard the project and move on
            "error" => {
                ok = false;
                break;
            },
            _ => {
            }
        }
    }
    // if the project is to be stored, store it
    if ok {
        if let Some(target_id) = ds.add_project(& records::ProjectUrl::from_url(& url).unwrap()) {
            ds.update_project_substore(target_id, target_substore);

            // translate the project heads (name -> (CommitID, SHA))
            let target_heads : records::ProjectHeads = heads.iter().map(|(name, sha)| 
            (name.to_owned(), (commit_mapping[sha], sha.to_owned()))
            ).collect();
            ds.update_project_heads(target_id, & target_heads);

            // add the metadata
            let mut pm = ds.project_metadata.lock().unwrap();
            for (key,value) in metadata {
                pm.set(target_id, & records::Metadata{key, value})
            }

            // and add the log
            ds.update_project_update_status(target_id, records::ProjectLog::Ok{time : helpers::now(), version : Datastore::VERSION});

        } else {
            ok = false;
        }


    }


    return Ok(ok);

}

/** Converts the v1 datastore contents into current version (v3).


 */
fn convert_1(source_path : & str, target_substore : & str) {
    use records::CommitInfo;
    use records::HashId;
    use records::CommitId;
    use records::PathId;
    use records::UserId;
    use crate::db::Id;
    use crate::records::*;
    let ds = Datastore::new(& SETTINGS.datastore_root, false);
    let substore = ds.substore(records::StoreKind::from_string(target_substore).unwrap());
    let mut commit_ids = HashMap::<u64, SHA>::new();
    let mut commits = HashMap::<u64, CommitInfo>::new();
    let mut users = HashMap::<UserId, UserId>::new();
    let mut paths = HashMap::<PathId, PathId>::new();
    let mut hashes = HashMap::<HashId, HashId>::new();
    let mut commit_mapping = HashMap::<CommitId, CommitId>::new();
    let mut commit_hashes = HashMap::<SHA, CommitId>::new();
    {
        println!("Reading commit ids...");
        let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(format!("{}/commit_ids.csv", source_path)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let hash = SHA::from_str(& record[0]).unwrap();
            let id = record[1].parse::<u64>().unwrap();
            commit_ids.insert(id, hash);
            commit_hashes.insert(hash,CommitId::from(id));
        }
        println!("    {} commit ids found", commit_ids.len());
    }
    {
        println!("Constructing commit info - records");
        let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(format!("{}/commit_records.csv", source_path)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();

            let id = record[1].parse::<u64>().unwrap();
            let committer_id = UserId::from(record[2].parse::<u64>().unwrap());
            let committer_time = record[3].parse::<i64>().unwrap();
            let author_id = UserId::from(record[4].parse::<u64>().unwrap());
            let author_time = record[5].parse::<i64>().unwrap();
            match commits.entry(id) {
                hash_map::Entry::Occupied(mut e) => {
                    let ci = e.get_mut();
                    ci.committer = committer_id;
                    ci.committer_time = committer_time;
                    ci.author = author_id;
                    ci.author_time = author_time;
                },
                hash_map::Entry::Vacant(e) => {
                    e.insert(CommitInfo{
                        committer : committer_id,
                        committer_time,
                        author : author_id,
                        author_time,
                        parents : Vec::new(),
                        changes : HashMap::new(),
                        message : String::new(),
                    });
                }
            }
        }
        println!("    {} commit records loaded", commits.len());
        println!("Adding users...");
        for (_, ci) in commits.iter() {
            users.insert(ci.committer, UserId::NONE);
            users.insert(ci.author, UserId::NONE);
        }
        println!("    {} users found", users.len());
    }
    {
        println!("Loading commit messages...");
        let mut f = OpenOptions::new().read(true).open(format!("{}/commit_messages.dat", source_path)).unwrap();
        let mut msgs = 0;
        loop {
            if let Ok(id) = f.read_u64::<LittleEndian>() {
                let size = f.read_u32::<LittleEndian>().unwrap();
                let mut buffer = vec![0; size as usize];
                f.read(&mut buffer).unwrap();
                match commits.entry(id) {
                    hash_map::Entry::Occupied(mut e) => {
                        let ci = e.get_mut();
                        ci.message = helpers::to_string(& buffer);
                        msgs += 1;
                    },
                    _ => {},
                }
            } else {
                break;
            }
        }
        println!("    {} messages", msgs);
    }
    {
        println!("Loading commit parents...");
        let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(format!("{}/commit_parents.csv", source_path)).unwrap();
        let mut records = 0;
        for x in reader.records() {
            let record = x.unwrap();

            let commit_id = record[1].parse::<u64>().unwrap();
            let parent_id = record[2].parse::<u64>().unwrap();
            if commits.contains_key(& parent_id) {
                match commits.entry(commit_id) {
                    hash_map::Entry::Occupied(mut e) => {
                        let ci = e.get_mut();
                        ci.parents.push(CommitId::from(parent_id));
                        records += 1;
                    },
                    hash_map::Entry::Vacant(e) => { }
                }
            }
        }
        println!("    {} parent records added", records);
    }
    {
        println!("Loading commit changes...");
        let mut f = OpenOptions::new().read(true).open(format!("{}/commit_changes.dat", source_path)).unwrap();
        let mut changes = 0;
        loop {
            if let Ok(id) = f.read_u64::<LittleEndian>() {
                let mut num_changes = f.read_u32::<LittleEndian>().unwrap() as usize;
                f.read_u64::<LittleEndian>().unwrap(); // additions and deletions which we ignore in v3
                f.read_u64::<LittleEndian>().unwrap();
                match commits.entry(id) {
                    hash_map::Entry::Occupied(mut e) => {
                        let ci = e.get_mut();
                        ci.changes.clear();
                        while num_changes > 0 {
                            num_changes -= 1;
                            let path_id = PathId::from(f.read_u64::<LittleEndian>().unwrap());
                            let hash_id = HashId::from(f.read_u64::<LittleEndian>().unwrap());
                            ci.changes.insert(path_id, hash_id);
                            paths.insert(path_id, PathId::NONE);
                            hashes.insert(hash_id, HashId::NONE);
                        }
                        changes += 1;
                    },
                    _ => {
                        while num_changes > 0 {
                            num_changes -= 1;
                            f.read_u64::<LittleEndian>().unwrap();
                            f.read_u64::<LittleEndian>().unwrap();
                        }
                    },
                }
            } else {
                break;
            }
        }
        println!("    {} changes commit records", changes);
    }
    // now add users & paths & hashes that were used in the commits
    {
        println!("Updating users...");
        let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(format!("{}/user_ids.csv", source_path)).unwrap();
        let mut records = 0;
        for x in reader.records() {
            let record = x.unwrap();
            let email = record[0].to_owned();
            let id = UserId::from(record[1].parse::<u64>().unwrap());
            match users.entry(id) {
                hash_map::Entry::Occupied(mut e) => {
                    let target_id = substore.get_or_create_user_id(& email).0;
                    e.insert(target_id);
                    records += 1;
                },
                _ => {
                },
            }
        }
        println!("    {} records updated out of {}", records, users.len());
    }
    {
        println!("Updating paths...");
        let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(format!("{}/path_ids.csv", source_path)).unwrap();
        let mut records = 0;
        for x in reader.records() {
            let record = x.unwrap();
            let path = record[0].to_owned();
            let id = PathId::from(record[1].parse::<u64>().unwrap());
            match paths.entry(id) {
                hash_map::Entry::Occupied(mut e) => {
                    let target_id = substore.get_or_create_path_id(& path).0;
                    e.insert(target_id);
                    records += 1;
                },
                _ => {
                },
            }
        }
        println!("    {} records updated out of {}", records, paths.len());
    }
    {
        println!("Updating hashes...");
        let mut records = 0;
        let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(format!("{}/snapshot_ids.csv", source_path)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let sha = SHA::from_str(& record[0]).unwrap();
            let id = HashId::from(record[1].parse::<u64>().unwrap());
            match hashes.entry(id) {
                hash_map::Entry::Occupied(mut e) => {
                    let target_id = substore.get_or_create_hash_id(& sha).0;
                    e.insert(target_id);
                    records += 1;
                },
                _ => {
                },
            }
        }
        println!("    {} records updated out of {}", records, hashes.len());
    }
    // now store all commits
    for (id, sha) in commit_ids.iter() {
        let target_id = substore.get_or_create_commit_id(sha).0;
        commit_mapping.insert(CommitId::from(*id), target_id);
    }
    println!("    {} commit mappings", commit_mapping.len());
    // translate commit information
    for (id, mut ci) in commits {
        //ci.author = users.get(& ci.author).map(|x| *x).or_else(|| Some(UserId::NONE)).unwrap();
        //ci.committer = users.get(& ci.committer).map(|x| *x).or_else(|| Some(UserId::NONE)).unwrap();
        if ! users.contains_key(& ci.author) {
            println!("User {} not found", ci.author);
        }
        if ! users.contains_key(& ci.committer) {
            println!("User {} not found", ci.committer);
        }
        ci.author = users[& ci.author];
        ci.committer = users[& ci.committer];
        ci.parents = ci.parents.iter().map(|x| commit_mapping[x]).collect();
        ci.changes = ci.changes.iter().map(|(path,hash)| (paths[path], hashes[hash])).collect();

        substore.add_commit_info_if_missing(commit_mapping[& CommitId::from(id)], & ci);
    }
    // after this, we have all substore data and should look at projects    
    // convert commit mappings
    println!("Converting commit hash mappings...");
    commit_hashes = commit_hashes.iter().map(|(hash,id)| (*hash, commit_mapping[id])).collect();
    println!("Converting projects...");
    let mut pid = 0;
    let mut projects = 0;
    while let Ok(x) = convert_project(pid, source_path, & ds, substore.prefix, & commit_hashes) {
        if x {
            projects += 1;
        }
        pid += 1;
    }
    println!("    {} converted", projects);

}


/* Loads the full dataset and summarizes it by projects / language. 
For each project/language keeps:
   - # of commits
   - # of bugs
   - # file changes
   - % of commits in project
   - % of file changes in project
   - % of bugs
   - % of languages
   
The summarizes by projects alone:
   - # of languages
   - # of commits
   - # of file changes
   - # of bugs
 */

struct ProjectLangStats {
    commits : u64, 
    bugs : u64, 
    files : u64
}

impl ProjectLangStats {
    fn new() -> ProjectLangStats {
        return ProjectLangStats{
            commits : 0,
            bugs : 0,
            files : 0,
        };
    }

    fn update_with(& mut self, is_bug : bool, files : u64) {
        self.commits += 1;
        if is_bug {
            self.bugs += 1;
        } 
        self.files += files;
    }
}

struct ProjectStats {
    languages : HashSet<String>,
    commits : HashSet<String>,
    files : u64,
    bugs : u64,
    committers : HashSet<u64>,
    min_commit : i64, 
    max_commit : i64,
}

impl ProjectStats {
   fn new() -> ProjectStats {
       return ProjectStats {
           languages : HashSet::new(),
           commits : HashSet::new(),
           files : 0,
           bugs : 0,
           committers : HashSet::new(),
           min_commit : std::i64::MAX,
           max_commit : std::i64::MIN,
       };
   }

   fn update_with(& mut self, language : String, hash : String, files : u64, is_bug : bool, committer: u64, date : i64) {
       self.languages.insert(language);
       self.files += files;
       if self.commits.insert(hash) {
           if is_bug {
               self.bugs += 1;
           }
           self.committers.insert(committer);
           if self.min_commit > date {
               self.min_commit = date;
           }
           if self.max_commit < date {
               self.max_commit = date;
           }
       }

   }
}

fn compact(input : & str, output_pl : & str, output_p : & str) {
   let input_file = String::from(input);
   let mut fpl = File::create(output_pl).unwrap();
   let mut fp = File::create(output_p).unwrap();

   let mut project_lang = HashMap::<u64, HashMap::<String, ProjectLangStats>>::new();
   let mut projects = HashMap::<u64, ProjectStats>::new();

   let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(input_file).unwrap();
   for x in reader.records() {
       let record = x.unwrap();
       let language = String::from(& record[0]);
       let project = record[5].parse::<u64>().unwrap();
       let hash = String::from(& record[6]);
       let files = record[7].parse::<u64>().unwrap();
       let committer = record[8].parse::<u64>().unwrap();
       let commit_date = record[9].parse::<i64>().unwrap();
       let is_bug = record[13].parse::<u64>().unwrap() != 0;

       project_lang.entry(project).or_insert(HashMap::new()).entry(language.clone()).or_insert(ProjectLangStats::new()).update_with(is_bug, files);

       projects.entry(project).or_insert(ProjectStats::new()).update_with(language, hash, files, is_bug, committer, commit_date);
   }

   writeln!(& mut fpl, "project,language,commits,bugs,changes,pctCommits,pctBugs,pctChanges,pctLanguages").unwrap();
   writeln!(& mut fp, "project,languages,commits,bugs,changes,autors,age").unwrap();
   for (pid, p) in projects.iter() {
       let languages = p.languages.len() as u64;
       let commits = p.commits.len() as u64;
       let committers = p.committers.len() as u64;
       writeln!(& mut fp, "{},{},{},{},{},{},{}", pid, languages, commits, p.bugs, p.files, committers, p.max_commit - p.min_commit).unwrap();
       for (lang, pl) in project_lang[pid].iter() {
           writeln!(& mut fpl, "{},{},{},{},{},{},{},{},{}",
               pid,
               lang, 
               pl.commits,
               pl.bugs,
               pl.files,
               pl.commits * 100 / commits,
               if p.bugs > 0 {pl.bugs * 100 / p.bugs } else { 0 },
               pl.files * 100 / p.files,
               1 * 100 / languages
           ).unwrap();
       }
   }
}

/** Exports the artifact related information for the specified projects, or for all projects in the dataset.
 */

fn export(projects : & str, output_file : & str, max_time : Option<& std::string::String>) {
    let dcd = DatastoreView::from(& SETTINGS.datastore_root);
    let projects_file = String::from(projects);
    let max_t = if let Some(x) = max_time { x.parse::<i64>().unwrap() } else { std::i64::MAX };
    if projects_file == *"--all" {
        let mut heads = HashMap::<ProjectId, ProjectHeads>::new();
        for (project_id, h) in dcd.project_heads() {
            heads.insert(project_id, h);
        }
        export_projects(& dcd, heads, output_file, max_t);
    } else {
        if std::path::Path::new(& projects_file).is_dir() {
            for entry in read_dir(& projects_file).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if ! path.is_dir() {
                    let filename = entry.file_name().into_string().unwrap();
                    println!("Exporting {}", filename);
                    export_filtered_projects(& dcd,
                        & format!("{}/{}", projects_file, filename),
                        & format!("{}/{}", output_file, filename),
                        max_t
                    );               
                }
            }
        } else {
            export_filtered_projects(& dcd, projects, output_file, max_t);
        }
    }
}


fn export_filtered_projects(dcd : & DatastoreView, filter : & str, output : & str, max_t : i64) {
    let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(filter).unwrap();
    let filtered : HashSet<ProjectId> = reader.records().map(|x| {
        let record = x.unwrap();
        return ProjectId::from(record[0].parse::<u64>().unwrap());
    }).collect();
    let mut heads = HashMap::<ProjectId, ProjectHeads>::new();
    for (project_id, h) in dcd.project_heads() {
        if filtered.contains(& project_id) {
            heads.insert(project_id, h);
        }
    }
    export_projects(dcd, heads, output, max_t);
}

fn export_projects(dcd : & DatastoreView, heads : HashMap<ProjectId, ProjectHeads>, output : & str, max_t : i64) {
    let mut f = File::create(output).unwrap();
    writeln!(& mut f, "language,typeclass,langclass,memoryclass,compileclass,project,sha,files,committer,commit_date,commit_age,insertion,deletion,isbug,bug_type,phase,domain,btype1,btype2").unwrap();
    let mut commits = dcd.commits(StoreKind::Generic);
    let mut commits_info = dcd.commits_info(StoreKind::Generic);
    let mut paths = dcd.paths_strings(StoreKind::Generic);
    let mut path_langs = HashMap::<PathId, String>::new();
    for (pid, heads) in heads {
        let mut visited = HashSet::<CommitId>::new();
        let mut q : Vec<CommitId> = heads.iter().map(|(_, (id, _))| *id).collect();
        while ! q.is_empty() {
            let id = q.pop().unwrap();
            if ! visited.contains(& id) {
                let hash = commits.get(id).unwrap();
                let ci = commits_info.get(id).unwrap();
                // get the languages for the changed paths
                for (path_id, _) in ci.changes.iter() {
                    if ! path_langs.contains_key(& path_id) {
                        path_langs.insert(*path_id, get_file_language(& paths.get(*path_id).unwrap()));
                    }
                }
                // add parents
                for p in ci.parents.iter() {
                    q.push(*p);
                }
                visited.insert(id);
                // and analyze the commit
                analyze_commit(pid, hash, ci, & mut f, id, max_t, & path_langs);
            }
        }
    }
}

fn analyze_commit(pid : ProjectId, hash : SHA, ci : CommitInfo, f : & mut File, id : CommitId, max_t : i64, path_langs : & HashMap<PathId, String> ) {
    let is_bug = is_bugfixing_commit(& ci);
    let mut language_counts = HashMap::<String, u64>::new();
    for (path_id, _) in ci.changes.iter() {
        //println!("path_id: {}", path_id);
        let lang = path_langs.get(path_id).unwrap().to_owned();
        if !lang.is_empty() {
            (*language_counts.entry(lang).or_insert(0)) += 1;
        }
    }
    for (lang, num_files) in language_counts {
        writeln!(f,"{},,,,,{},{},{},{},{},,{},{},{},,,,,", 
            lang,
            pid,
            hash,
            num_files,
            ci.committer,
            ci.committer_time,
            0, // additions
            0, // deletions 
            if is_bug { 1 } else { 0 }
        ).unwrap();
    }    
}

/** Determines whether a commit is bugfixing, or not.
 
    If the commit is missing message or changes, returns None. Otherwise translates the message to lowercase (I think the original paper did this too) and then looks for subexpressions mentioned in the paper. 
 */
fn is_bugfixing_commit(ci : & CommitInfo) -> bool {
    for substr in &["error", "bug", "fix", "issue", "mistake", "incorrect", "fault", "defect", "flaw"] {
        if let Some(_) = ci.message.find(substr) {
            return true;
        }
    }
    return false;
} 

/** Detects the language of a partiocular file.
 
    Uses extensions listed for the languages on Wikipedia as of August 2020. This by no means correct, or even precise, but is reasonable approximation of what the original did. Like them, we take anything that is ".ts" as typescript and also ignore header files for C, C++ and ObjC entirely (including hpp & friends for C++).
 */
fn get_file_language(path : & str) -> String {
    if let Some(ext) = std::path::Path::new(path).extension() {
        match ext.to_str().unwrap() {
            "c" => return "C".to_owned(),
            "C" | "cc" | "cpp" | "cxx" | "c++" => return "C++".to_owned(),
            "cs" => return "C#".to_owned(),
            "m" | "mm" | "M" => return "Objective-C".to_owned(),
            "go" => return "Go".to_owned(),
            "java" => return "Java".to_owned(),
            "coffee" | "litcoffee" => return "Coffeescript".to_owned(),
            "js" | "mjs" => return "Javascript".to_owned(),
            "ts" | "tsx" => return "Typescript".to_owned(),
            "rb" => return "Ruby".to_owned(),
            "php" | "phtml" | "php3" | "php4" | "php5" | "php7" | "phps" | "php-s" | "pht" | "phar" => return "Php".to_owned(),
            "py" | "pyi" | "pyc" | "pyd" | "pyo" | "pyw" | "pyz" => return "Python".to_owned(),
            "plx"| "pl" | "pm" | "xs" | "t" | "pod" => return "Perl".to_owned(),
            "clj" | "cljs" | "cljc" | "edn" => return "Clojure".to_owned(),
            "erl" | "hrl" => return "Erlang".to_owned(),
            "hs" | "lhs" => return "Haskell".to_owned(),
            "scala" | "sc" => return "Scala".to_owned(),
            _ => {},
        }
    }
    return String::new();
}

