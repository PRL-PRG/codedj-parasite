use std::io::*;
use std::fs::*;
use std::collections::*;
use dcd::*;
use std::process::Command;

/** Describes the given dataset.

    Simple program that describes the dataset in two csv files
 */  
fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 3  {
        panic!{"Invalid usage - dcd-describe PATH_TO_DATABASE OUTPUT"}
    }
    let dcd = DCD::new(String::from(& args[1]));
    let mut output_projects = File::create(format!("{}-projects.csv", args[2])).unwrap();
    writeln!(& mut output_projects, "id,language,commits,authors,committers,issues,buggyIssues,url").unwrap();
    let mut output_sizes = File::create(format!("{}-sizes.csv", args[2])).unwrap();
    writeln!(& mut output_sizes, "name,size").unwrap();
    let mut valid_projects = 0;
    for project in dcd.projects() {
        valid_projects += 1;
        let mut authors = HashSet::<UserId>::new();
        let mut committers = HashSet::<UserId>::new();
        let mut commits = 0;
        for commit in ProjectCommitIter::from(& dcd, & project) {
            commits += 1;
            authors.insert(commit.author_id);
            committers.insert(commit.author_id);
        }
        let mut issues = 0;
        let mut buggy_issues = 0;
        if let Some(ght_issue) = project.metadata.get("ght_issue") {
            issues = ght_issue.parse::<u64>().unwrap();
        }
        if let Some(ght_issue_bug) = project.metadata.get("ght_issue_bug") {
            buggy_issues = ght_issue_bug.parse::<u64>().unwrap();
        }
        writeln!(& mut output_projects, "{},{},{},{},{},{},{},\"{}\"", project.id, project.metadata["ght_language"],commits, authors.len(), committers.len(), issues, buggy_issues, project.url).unwrap();
    }
    writeln!(& mut output_sizes, "projects,{}", dcd.num_projects()).unwrap();
    writeln!(& mut output_sizes, "validProjects,{}", valid_projects).unwrap();
    writeln!(& mut output_sizes, "validCommits,{}", dcd.num_commit_messages()).unwrap();
    writeln!(& mut output_sizes, "validPaths,{}", dcd.num_file_paths()).unwrap();
    let output = Command::new("du").arg("-sb").arg(& args[1]).output().unwrap();
    let output_str = String::from(String::from_utf8_lossy(& output.stdout).split("\t").next().unwrap());
    writeln!(& mut output_sizes, "bytes,{}", output_str.parse::<u64>().unwrap()).unwrap();
}