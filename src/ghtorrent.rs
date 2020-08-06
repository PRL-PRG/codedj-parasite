use std::collections::{HashMap, HashSet, BinaryHeap};
use crate::downloader_state::*;
use crate::project::*;
use crate::helpers;


/** GHTorrent Source Representation & Actions
 
    - add projects
    - select which commits we have and to what project they belong
    - add commit parents & calculate heads
    - the heads are a bit annoying because they can change in time a lot, merging them together is not easiest thing to do
 */

pub struct GHTorrent {
    // root where the extracted ghtorrent stuff is
    root_ : String, 

    // translates gh torrent user ids to own ids
    user_ids_ : HashMap<u64,u64>,
    // ghtorrent users (gh torrent id -> name)
    users_ : HashMap<u64, String>,

    // ghtorrent project id to own ids
    project_ids_ : HashMap<u64, u64>,

    // ght ids of valid commits (i.e. commits belonging to the newly added projects)
    valid_commits_ : HashSet<u64>,



}

impl GHTorrent {

    pub fn new(root : & str) -> GHTorrent {
        return GHTorrent{
            root_ : String::from(root),
            user_ids_ : HashMap::new(),
            users_ : HashMap::new(),
            project_ids_ : HashMap::new(),
            valid_commits_ : HashSet::new(),
        }
    }

    /** Loads the users so that they can be added to the database lazily. 
     
        Users in ghtorrent do not contain their emails and thanks to GDPR there is no way we can obtain it at the moment. A dirty trick here is to preserve the user identities via a fake email that just contains the ghtorrent user id so that the user can be identified later. 

        Note that this data is likely to change when full download is made because this will not account for non-registered contributors. 
     */
    pub fn load_users(& mut self) {
        let mut reader = csv::Reader::from_path(format!("{}/users.csv", self.root_)).unwrap();
        println!("Preloading ghtotrrent users...");
        for x in reader.records() {
            if let Ok(record) = x {
                if self.users_.len() % 1000 == 0 {
                    helpers::progress_line(format!("    records: {}", self.users_.len()));
                }
                let gh_id = record[0].parse::<u64>().unwrap();
                let name = String::from(& record[1]);
                self.users_.insert(gh_id, name);
            }
        }
        println!("    {} users loaded", self.users_.len());
    }

    /** Adds projects from ghtorrent SQL dump to the given downloader. 
     
        Returns a hashmap from ghtorrent project ids to own project ids so that correct project ids can be assigned later in the process when commits & stuff are added. 
     */
    pub fn add_projects(& mut self, dcd : & mut DownloaderState) {
        let mut reader = csv::Reader::from_path(format!("{}/projects.csv", self.root_)).unwrap();
        let mut records = 0;
        // hashmap from ghtorrent ids to own ids...
        let mut pending_forks = HashMap::<u64,u64>::new();
        println!("Adding new projects...");
        for x in reader.records() {
            if let Ok(record) = x {
                if records % 1000 == 0 {
                    helpers::progress_line(format!("    records: {}, new projects: {}, pending forks: {}", records, self.project_ids_.len(), pending_forks.len()));
                    // break prematurely after first 1k... TODO
                    if records > 0 {
                        break;
                    }
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
                    self.project_ids_.insert(gh_id, p.id);
                    let mut md = ProjectMetadata::new();
                    md.insert(String::from("ght_id"), String::from(& record[0]));
                    md.insert(String::from("ght_language"), String::from(language));
                    // if the project is a fork, determine if we know its original already, if not add it to pending forks
                    if let Ok(fork_id) = forked_from {
                        if let Some(own_fork_id) = self.project_ids_.get(& fork_id) {
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
                if let Some(fork_id) = self.project_ids_.get(& x.1) {
                    let mut md = ProjectMetadata::new();
                    md.insert(String::from("fork_of"), format!("{}", fork_id));
                    md.append(& dcd.dcd_.get_project_root(x.0));
                } else {
                    broken.insert(x.0);
                }
            }
            println!("    broken projects: {}", broken.len());
        }
    }

    pub fn filter_commits(& mut self) {
        let mut reader = csv::Reader::from_path(format!("{}/project_commits.csv", self.root_)).unwrap();
        let mut records = 0;
        println!("Filtering commits for newly added projects only...");
        for x in reader.records() {
            if let Ok(record) = x {
                if records % 1000 == 0 {
                    helpers::progress_line(format!("    records: {}, valid commits: {}", records, self.valid_commits_.len()));
                    // todo ignore this for now
                    if records > 5000000 {
                        break;
                    }
                }
                records += 1;
                let project_id = record[0].parse::<u64>().unwrap();
                if self.project_ids_.contains_key(& project_id) {
                    self.valid_commits_.insert(record[1].parse::<u64>().unwrap());
                }
            }
        }
        println!("    valid commits: {}", self.valid_commits_.len());
    }

    /** Takes the commits and loads their basic information. 
     
        - commits.csv for hash, author id, committer id and created_at which I guess is commit time
     */
    pub fn add_commits(& self, valid_commits : HashSet<u64>, dcd : & mut DownloaderState) {

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
