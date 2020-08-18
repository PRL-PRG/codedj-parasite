use std::collections::*;

use dcd::*;
use dcd::db_manager::*;
use dcd::record::*;

/** Loads issues to projects. 
 */
fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        panic!{"Invalid usage - dcd-issues DATABASE GHTORRENT_DUMP"}
    }
    let mut db = DatabaseManager::from(& args[1]);
    let projects = get_projects(& mut db);
    let issues = get_project_issues(& args[2], & projects);
    write_issues(& mut db, & issues);
}

/** Loads all projects that are valid and do not yet have issues information in them. 
 */
fn get_projects(db : & mut DatabaseManager) -> HashMap<u64, ProjectId> {
    println!("Loading projects missing issues...");
    let mut result = HashMap::<u64, ProjectId>::new();
    for x in 0 .. db.num_projects() {
        let log = ProjectLog::new(db.get_project_log_filename(x as ProjectId));
        let mut valid = true;
        let mut ght_id = 0;
        log.analyze(|e| {
            match e {
                ProjectLogEntry::Error{time : _, source: _, message: _}=> {
                    valid = false;
                    return false;
                },
                ProjectLogEntry::Metadata{time: _, source: _, key, value } => {
                    if key == *"ght_issues" {
                        valid = false;
                    } else if key == *"ght_id" {
                        ght_id = value.parse::<u64>().unwrap();
                    }
                },
                _ => {}
            }
            return true;
        });
        if valid {
            result.insert(ght_id, x as ProjectId);
        }
    }
    println!("    {} projects found", result.len());
    return result;
}

fn get_project_issues(ght_root : & str, projects : & HashMap<u64, ProjectId>) -> HashMap<ProjectId, Vec<(i64, bool)>> {
    let mut bug_labels = HashSet::<u64>::new();
    {
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/repo_labels.csv", ght_root)).unwrap();
        println!("Getting bug labels...");
        let mut records = 0;
        for x in reader.records() {
            let record = x.unwrap();
            if records % 1000 == 0 {
                helpers::progress_line(format!("    records: {}", records));
            }
            records += 1;
            let label = String::from(& record[2]);
            if *label == *"bug" {
                bug_labels.insert(record[0].parse::<u64>().unwrap());
            }
        }
        println!("    {} different bug labels", bug_labels.len());
    }
    let mut bug_issues = HashSet::<u64>::new();
    {
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/issue_labels.csv", ght_root)).unwrap();
        println!("Getting issue labels...");
        let mut records = 0;
        for x in reader.records() {
            let record = x.unwrap();
            if records % 1000 == 0 {
                helpers::progress_line(format!("    records: {}", records));
            }
            records += 1;
            let label_id = record[0].parse::<u64>().unwrap();
            if bug_labels.contains(& label_id) {
                bug_issues.insert(record[1].parse::<u64>().unwrap());
            }
        }
    }
    // id = 0, repo_id = 1, created_at = 6
    let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/issues.csv", ght_root)).unwrap();
    println!("Analyzing project issues...");
    let mut result = HashMap::<ProjectId, Vec<(i64, bool)>>::new();
    let mut records = 0;
    for x in reader.records() {
        let record = x.unwrap();
        if records % 1000 == 0 {
            helpers::progress_line(format!("    records: {}", records));
        }
        records += 1;
        let project_id = record[1].parse::<u64>().unwrap();
        if let Some(own_id) = projects.get(& project_id) {
            let time = helpers::to_unix_epoch(& record[6]);
            let id = record[0].parse::<u64>().unwrap();
            result.entry(*own_id).or_insert(Vec::new()).push((time, bug_issues.contains(& id)));
        }
    }
    return result;
}

fn write_issues(db : & mut DatabaseManager, project_issues: & HashMap<ProjectId, Vec<(i64, bool)>>) {
    for (pid, issues) in project_issues {
        println!("Project {}, issues {}", pid, issues.len());
        let mut log = ProjectLog::new(db.get_project_log_filename(*pid));
        let mut buggy = 0;
        let mut non_buggy = 0;
        for (time, is_bug) in issues {
            if *is_bug {
                buggy += 1;
                log.add(ProjectLogEntry::Metadata{
                    time : *time, 
                    source : Source::GHTorrent,
                    key : "ght_issue_bug".to_owned(),
                    value : buggy.to_string()
                });
            } else {
                non_buggy += 1;
                log.add(ProjectLogEntry::Metadata{
                    time : *time, 
                    source : Source::GHTorrent,
                    key : "ght_issue".to_owned(),
                    value : non_buggy.to_string()
                });
            }
        }
        log.append();
    }
}