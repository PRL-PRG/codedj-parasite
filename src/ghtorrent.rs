use std::collections::{HashMap, HashSet, BinaryHeap};
use crate::downloader_state::*;
use crate::project::*;
use crate::helpers;


pub struct GHTorrent {
    // root where the extracted ghtorrent stuff is
    root_ : String, 



}

impl GHTorrent {

    pub fn new(root : & str) -> GHTorrent {
        return GHTorrent{
            root_ : String::from(root),
        }
    }

    pub fn add_projects(& mut self, dcd : & mut DownloaderState) -> HashMap<u64,u64> {
        let mut reader = csv::Reader::from_path(format!("{}/projects.csv", self.root_)).unwrap();
        let mut records = 0;
        // hashmap from ghtorrent ids to own ids...
        let mut project_ids = HashMap::<u64,u64>::new();
        let mut pending_forks = HashMap::<u64,u64>::new();
        println!("Adding new projects...");
        for x in reader.records() {
            if let Ok(record) = x {
                if records % 1000 == 0 {
                    helpers::progress_line(format!("    records: {}, new projects: {}, pending forks: {}", records, project_ids.len(), pending_forks.len()));
                }
                records += 1;
                let gh_id = record[0].parse::<u64>().unwrap();
                let api_url : Vec<&str> = record[1].split("/").collect();
                let language = & record[5];
                let created_at = & record[6];
                let forked_from = record[7].parse::<u64>();
                let deleted = record[8].parse::<u64>().unwrap();
                // ignore deleted projects
                if deleted != 0 {
                    continue;
                }
                // get the user and repo names
                let name = api_url[api_url.len() - 1].to_lowercase();
                let user = api_url[api_url.len() - 2].to_lowercase();
                let url = format!("https://github.com/{}/{}.git", user, name);
                if let Some(p) = dcd.add_project(& url) {
                    // add the project to project ids
                    project_ids.insert(gh_id, p.id);
                    let mut md = ProjectMetadata::new();
                    md.insert(String::from("ght_id"), String::from(& record[0]));
                    md.insert(String::from("ght_language"), String::from(language));
                    // if the project is a fork, determine if we know its original already, if not add it to pending forks
                    if let Ok(fork_id) = forked_from {
                        if let Some(own_fork_id) = project_ids.get(& fork_id) {
                            md.insert(String::from("fork_of"), format!("{}", own_fork_id));    
                        } else {
                            md.insert(String::from("fork_of"), format!("ght_id: {}", gh_id));
                            pending_forks.insert(p.id, fork_id);
                        }
                    }
                    md.save(& dcd.dcd_.get_project_root(p.id));
                }
            }
        }
        println!("\nPatching missing fork information...");
        {
            let mut broken = HashSet::<u64>::new();
            println!("    pending projects: {}", pending_forks.len());
            for x in pending_forks {
                if let Some(fork_id) = project_ids.get(& x.1) {
                    let mut md = ProjectMetadata::new();
                    md.insert(String::from("fork_of"), format!("{}", fork_id));
                    md.append(& dcd.dcd_.get_project_root(x.0));
                } else {
                    broken.insert(x.0);
                }
            }
            println!("    broken projects: {}", broken.len());
        }

        return project_ids;
    }
}


/*
struct GHTProjectsIterator<'r> {
    records_ : csv::StringRecordsIter<'r, std::io::Read>,
}

impl GHTProjectsIterator {
    fn new(root_folder : & str) -> GHTProjectsIterator {
        return GHTProjectsIterator {
            reader_ : csv::Reader::from_path(format!("{}/projects.csv", root_folder)).unwrap(),
            records_ : reader_.records(),
        }
    }
}
*/

/*
impl<'a> std::iter::Iterator for GHTProjectsIterator<'a> {

    type Item = (String, csv::StringRecord);

    fn next(& mut self) -> Option<(String, csv::StringRecord)> {
        if let Ok(record) = self.records_.next() {
            return None;
        } else {
            return None;
        }
    }
}
*/
