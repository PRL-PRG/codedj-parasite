use std::io::*;
use std::fs::*;
use std::collections::*;
use dcd::*;

/** Imports the data for artifact. 
 
    This should go to the paper repo once I am done with it. 



    Output columns:

    -   language
    - x typeClass (ignored)
    - x langClass (ignored)
    - x memoryClass (ignored)
    - x compileClass (ignored)
    -   project (name only)
    -   sha (ignored)
    -   files
    -   committer (can be our id)
    -   commit_date
    -   commit_age
    - x insertion (ignored, do not have)
    - x deletion (ignored, do not have)
    -   isbug (must calculate)
    - x bug_type (ignored)
    - x phase (ignored)
    - x domain (ignored)
    - x btype1 (ignored)
    - x btype2 (ignored)

 */

fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 4 && args.len() != 5 {
        panic!{"Invalid usage - dcd PATH_TO_DATABASE PROJECTS OUTPUT_FILE [MAX_T]"}
    }
    let dcd = DCD::new(args[1].to_owned());
    let mut projects = Vec::<ProjectId>::new();

    let projects_file = String::from(& args[2]);
    let mut reader = csv::ReaderBuilder::new().has_headers(false).double_quote(false).escape(Some(b'\\')).from_path(projects_file).unwrap();
    for x in reader.records() {
        let record = x.unwrap();
        projects.push(record[0].parse::<u64>().unwrap() as ProjectId);
    }
    println!("{} projects selected", projects.len());





    let max_t = if args.len() == 4 { std::i64::MAX } else { args[4].parse::<i64>().unwrap() };
    let mut f = File::create(& args[3]).unwrap();
    writeln!(& mut f, "language,typeclass,langclass,memoryclass,compileclass,project,sha,files,committer,commit_date,commit_age,insertion,deletion,isbug,bug_type,phase,domain,btype1,btype2").unwrap();
    for pid in projects {
        let project = dcd.get_project(pid).unwrap();
        println!("{} (id {})", project.url, project.id);
        for commit in dcd.commits_from(& project) {
            if commit.committer_time < max_t {
                analyze_commit(& commit, & project, & mut f, & dcd);
            }
        }
    }
    /*
    for project in dcd.projects() {
        println!("{} (id {})", project.url, project.id);
        for commit in dcd.commits_from(& project) {
            if commit.committer_time < max_t {
                analyze_commit(& commit, & project, & mut f, & dcd);
            }
        }
    }
    */
}

/** Detects the language of a partiocular file.
 
    Uses extensions listed for the languages on Wikipedia as of August 2020. This by no means correct, or even precise, but is reasonable approximation of what the original did. Like them, we take anything that is ".ts" as typescript and also ignore header files for C, C++ and ObjC entirely (including hpp & friends for C++).
 */
fn get_file_language(path : & str) -> Option<String> {
    if let Some(ext) = std::path::Path::new(path).extension() {
        match ext.to_str().unwrap() {
            "c" => return Some("C".to_owned()),
            "C" | ".cc" | "cpp" | "cxx" | "c++" => return Some("C++".to_owned()),
            "m" | "mm" | "M" => return Some("Objective-C".to_owned()),
            "go" => return Some("Go".to_owned()),
            "java" => return Some("Java".to_owned()),
            "coffee" | "litcoffee" => return Some("Coffeescript".to_owned()),
            "js" | "mjs" => return Some("Javascript".to_owned()),
            "ts" | "tsx" => return Some("Typescript".to_owned()),
            "rb" => return Some("Ruby".to_owned()),
            "php" | "phtml" | "php3" | "php4" | "php5" | "php7" | "phps" | "php-s" | "pht" | "phar" => return Some("Php".to_owned()),
            "py" | "pyi" | "pyc" | "pyd" | "pyo" | "pyw" | "pyz" => return Some("Python".to_owned()),
            "plx"| "pl" | "pm" | "xs" | "t" | "pod" => return Some("Perl".to_owned()),
            "clj" | "cljs" | "cljc" | "edn" => return Some("Clojure".to_owned()),
            "erl" | "hrl" => return Some("Erlang".to_owned()),
            "hs" | "lhs" => return Some("Haskell".to_owned()),
            "scala" | "sc" => return Some("Scala".to_owned()),
            _ => return None
        }
    } else {
        return None;
    }
}

/** Determines whether a commit is bugfixing, or not.
 
    If the commit is missing message or changes, returns None. Otherwise translates the message to lowercase (I think the original paper did this too) and then looks for subexpressions mentioned in the paper. 
 */
fn is_bugfixing_commit(commit : & Commit) -> Option<bool> {
    if let Some(msg) = & commit.message {
        let msg = String::from_utf8_lossy(msg).to_lowercase();
        for substr in &["error", "bug", "fix", "issue", "mistake", "incorrect", "fault", "defect", "flaw"] {
            if let Some(_) = msg.find(substr) {
                return Some(true);
            }
        }
        return Some(false);
    } else {
        return None;
    }
} 

fn analyze_commit(commit : & Commit, project : & Project, output : & mut File, dcd : & DCD) {
    if let Some(is_bug) = is_bugfixing_commit(commit) {
        if let Some(changes) = & commit.changes {
            let mut language_counts = HashMap::<String, u64>::new();
            for (path_id, _) in changes {
                //println!("path_id: {}", path_id);
                if let Some(lang) = get_file_language(& dcd.get_file_path(*path_id).unwrap().path) {
                    (*language_counts.entry(lang).or_insert(0)) += 1;
                }
            }
            for (lang, num_files) in language_counts {
                writeln!(output,"{},,,,,{},{},{},{},{},,{},{},{},,,,,", 
                    lang,
                    project.id,
                    commit.hash,
                    num_files,
                    commit.committer_id,
                    commit.committer_time,
                    commit.additions.unwrap(),
                    commit.deletions.unwrap(),
                    if is_bug { 1 } else { 0 }
                ).unwrap();
            }
    
        }
    }
} 