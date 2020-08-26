use std::io::*;
use std::fs::*;
use rand::*;
use rand::seq::*;
use std::collections::*;
use dcd::*;

/** Takes the database, loads all projects, randomly samples N per language and generates their csvs. 
 */
fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        panic!{"Invalid usage - dcd-export-sampler PATH_TO_DATABASE OUTPUT_DIR PROJECTS_PER_LANGUAGE NUM_SAMPLES"}
    }
    let mut samples = args[4].parse::<u64>().unwrap();
    let projects_per_language = args[3].parse::<usize>().unwrap();
    let output_folder = String::from(& args[2]);
    let dcd = DCD::new(args[1].to_owned());
    let projects = load_projects(& dcd);
    while samples > 0 {
        let sampled = sample_projects(& projects, projects_per_language);
        println!("Sample {}, projects: {}", samples, sampled.len());
        let mut f = File::create(format!("{}/{}.csv", output_folder, samples)).unwrap();
        writeln!(& mut f, "language,typeclass,langclass,memoryclass,compileclass,project,sha,files,committer,commit_date,commit_age,insertion,deletion,isbug,bug_type,phase,domain,btype1,btype2").unwrap();
        for project in sampled {
            for commit in dcd.commits_from(& project) {
                analyze_commit(& commit, & project, & mut f, & dcd);
            }
        }
        samples -= 1;
    }
}


fn load_projects(dcd : & DCD) -> HashMap<String, Vec<Project>> {
    println!("Loading projects...");
    let mut result = HashMap::<String, Vec<Project>>::new();
    for project in dcd.projects() {
        result.entry(project.metadata["ght_language"].to_owned()).or_insert(Vec::new()).push(project);
    }
    for (lang, projects) in result.iter() {
        println!("{} : {} projects", lang, projects.len());
    }
    return result;
}

fn sample_projects(projects: & HashMap<String, Vec<Project>>, projects_per_language : usize) -> Vec<Project> {
    let mut result = Vec::<Project>::new();
    let mut rng = thread_rng();
    for (_lang, projs) in projects {
        let x : Vec<usize>  = (0 ..  projs.len()).choose_multiple(& mut rng, projects_per_language);
        for index in x {
            result.push(projs[index].clone());
        }
    }
    return result;
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