use std::io::*;
use std::fs::*;
use std::collections::*;
use std::cmp::*;
use dcd::*;

/** This exports the summarized table for the model, i.e. instead of per commit, we already calculate the information on per project and language basis
 
    Columns:

    - project
    - language
    - commits
    - tins (sum insertions)
    - max_commit_age (oldest commit in days, must be greater than 1)
    - bug commits
    - developers (committers)
 */


fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 3 && args.len() != 4 {
        panic!{"Invalid usage - dcd PATH_TO_DATABASE OUTPUT_FILE [MAX_T]"}
    }
    let dcd = DCD::new(args[1].to_owned());
    let max_t = if args.len() == 3 { std::i64::MAX } else { args[3].parse::<i64>().unwrap() };
    let mut f = File::create(& args[2]).unwrap();
    writeln!(& mut f, "project,language,commits,tins,max_commit_age,bcommits,devs").unwrap();
    for project in dcd.projects() {
        summarize_project(& project, & dcd, max_t, & mut f);
        /*
        let result = summarize_project(& project, & dcd);
        for (lang,  (commits, tins, max_commit_age, bcommits, devs)) in result {
            writeln!(& mut f, "{},{},{},{},{},{},{}", project.id, lang, commits, tins, max_commit_age, bcommits, devs);
        }
        */
    }
}

/** Detects the language of a partiocular file.
 
    Uses extensions listed for the languages on Wikipedia as of August 2020. This by no means correct, or even precise, but is reasonable approximation of what the original did. Like them, we take anything that is ".ts" as typescript and also ignore header files for C, C++ and ObjC entirely (including hpp & friends for C++).
 */
fn get_file_language(path : & str) -> Option<String> {
    if let Some(ext) = std::path::Path::new(path).extension() {
        match ext.to_str().unwrap() {
            "c" => return Some("C".to_owned()),
            "C" | "cc" | "cpp" | "cxx" | "c++" => return Some("C++".to_owned()),
            "cs" => return Some("C#".to_owned()),
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

/*
fn get_languages(commit : & Commit, dcd : & DCD) -> HashSet<String> {
    let mut result = HashSet::<String>::new();
    for (path_id, _) in commit.changes.unwrap().iter() {
        if let Some(lang) = get_file_language(& dcd.get_file_path(*path_id).unwrap().path) {
            result.insert(lang);
        }
    }
    return result;
}
*/


struct SummaryDev {
    commits : usize,
    tins : u64,
    bcommits : usize,
    devs : HashSet<UserId>,
    oldest : i64,
    newest : i64,
}

impl SummaryDev {
    fn new() -> SummaryDev {
        return SummaryDev{
            commits : 0,
            tins : 0,
            bcommits : 0,
            devs : HashSet::new(),
            oldest : std::i64::MAX,
            newest : std::i64::MIN,
        };
    }

    fn update_with(& mut self, commit : & Commit, is_bug : bool) {
        self.commits += 1;
        if is_bug {
            self.bcommits += 1;
        }
        self.devs.insert(commit.author_id);
        self.tins += commit.additions.unwrap();
        self.oldest = min(self.oldest, commit.author_time);
        self.newest = max(self.newest, commit.author_time);
    }
}

/** Summarizes the project infromation per language */
fn summarize_project(project : & Project, dcd : & DCD, max_t : i64, into : & mut File) {
    let mut result = HashMap::<String, SummaryDev>::new();
    for commit in dcd.commits_from(& project) {
        // count only commits younget than the given date
        if commit.committer_time < max_t {
            // where we can determine whether they are bugfixes or not
            if let Some(is_bug) = is_bugfixing_commit(& commit) {
                // where we can get the changes
                if let Some(changes) = & commit.changes {
                    // and where the changes go to useful files
                    let mut languages = HashSet::<String>::new();
                    for (path_id, _) in changes {
                        //println!("path_id: {}", path_id);
                        if let Some(lang) = get_file_language(& dcd.get_file_path(*path_id).unwrap().path) {
                            languages.insert(lang);
                        }
                    }
                    // for all languages touched by the commit, update the summary
                    for lang in languages {
                        result.entry(lang).or_insert(SummaryDev::new()).update_with(& commit, is_bug);
                    }
                }
            }
        }
    }
    // now remove all project - language pairs that have fewer than 20 commits per language
    // and append the rest to the vector
    for (language, summary) in result {
        if summary.commits < 20 {
            continue;
        }
        writeln!(into, "{},{},{},{},{},{},{}",
            project.id,
            language,
            summary.commits,
            summary.tins,
            max((summary.newest - summary.oldest) / (3600 * 24), 1),
            summary.bcommits,
            summary.devs.len()
        ).unwrap();
    }
}