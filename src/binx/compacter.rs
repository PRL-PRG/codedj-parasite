use std::collections::*;
use std::io::*;
use std::fs::*;

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

 fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        panic!{"Invalid usage - compacter INPUT OUTPUT_PL OUTPUT P"}
    }
    let input_file = String::from(& args[1]);

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

    let mut fpl = File::create(& args[2]).unwrap();
    let mut fp = File::create(& args[3]).unwrap();
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