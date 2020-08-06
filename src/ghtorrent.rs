use std::collections::{HashMap, HashSet, BinaryHeap};
use crate::downloader_state::*;
use crate::project::*;
use crate::*;


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
    user_ids_ : HashMap<u64, UserId>,
    // ghtorrent users (gh torrent id -> name)
    users_ : HashMap<u64, String>,

    // ghtorrent project id to own ids
    project_ids_ : HashMap<u64, ProjectId>,

    // ght ids of valid commits (i.e. commits belonging to the newly added projects)
    valid_commits_ : HashSet<u64>,

    // actually created commits for which the information is constructed
    commits_ : HashMap<u64, Commit>,



}

impl GHTorrent {

    pub fn new(root : & str) -> GHTorrent {
        return GHTorrent{
            root_ : String::from(root),
            user_ids_ : HashMap::new(),
            users_ : HashMap::new(),
            project_ids_ : HashMap::new(),
            valid_commits_ : HashSet::new(),
            commits_ : HashMap::new(),
        }
    }

    /** Loads the users so that they can be added to the database lazily. 
     
        Users in ghtorrent do not contain their emails and thanks to GDPR there is no way we can obtain it at the moment. A dirty trick here is to preserve the user identities via a fake email that just contains the ghtorrent user id so that the user can be identified later. 

        Note that this data is likely to change when full download is made because this will not account for non-registered contributors. 
     */
    pub fn load_users(& mut self) {
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/users.csv", self.root_)).unwrap();
        println!("Preloading ghtotrrent users...");
        let mut i = 0;
        for x in reader.records() {
            let record = x.unwrap();
            if self.users_.len() % 1000 == 0 {
                helpers::progress_line(format!("    records: {}", self.users_.len()));
            }
            let gh_id = record[0].parse::<u64>().unwrap();
            i = gh_id;
            //println!("{}", gh_id);
            let name = String::from(& record[1]);
            self.users_.insert(gh_id, name);
        }
        println!("    {} users loaded", self.users_.len());
    }

    /** Adds projects from ghtorrent SQL dump to the given downloader. 
     
        Returns a hashmap from ghtorrent project ids to own project ids so that correct project ids can be assigned later in the process when commits & stuff are added. 
     */
    pub fn add_projects(& mut self, dcd : & mut DownloaderState) {
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/projects.csv", self.root_)).unwrap();
        let mut records = 0;
        // hashmap from ghtorrent ids to own ids...
        let mut pending_forks = HashMap::<u64,u64>::new();
        println!("Adding new projects...");
        for x in reader.records() {
            let record = x.unwrap();
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
        let mut reader = csv::ReaderBuilder::new().has_headers(false).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/project_commits.csv", self.root_)).unwrap();
        let mut records = 0;
        println!("Filtering commits for newly added projects only...");
        for x in reader.records() {
            let record = x.unwrap();
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
        println!("    valid commits: {}", self.valid_commits_.len());
    }



    fn get_or_create_user(& mut self, ght_id : u64, dcd : & mut DownloaderState) -> UserId {
        // if we have already seen the user, just return the new id
        if let Some(id) = self.user_ids_.get(& ght_id) {
            return *id;
        } else {
            if let Some(name) = self.users_.get(& ght_id) {
                let id = dcd.get_or_create_user(& format!("{}@ghtorrent", ght_id), name);
                self.user_ids_.insert(ght_id, id);
                return id;
            } else {
                println!("Unable to find user id {}", ght_id);
                panic!();
            }
        }
    }

    /** Takes the commits and loads their basic information. 
     
        - commits.csv for hash, author id, committer id and created_at which I guess is commit time
     */
    pub fn add_commits(& mut self, dcd : & mut DownloaderState) {
        // tentatively create commit records for all valid hashes here, then negotiate with the downloader state about how many we keep
        let mut hash_to_ght = HashMap::<git2::Oid, u64>::new();
        {
            let mut reader = csv::ReaderBuilder::new().has_headers(false).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/commits.csv", self.root_)).unwrap();
            let mut records = 0;
            println!("Adding new commits...");
            for x in reader.records() {
                let record = x.unwrap();
                if records % 1000 == 0 {
                    helpers::progress_line(format!("    records: {}, commits: {} (hashes: {})", records, self.commits_.len(), hash_to_ght.len()));
                    // break prematurely after first 1k... TODO
                    if self.commits_.len() > 1000 {
                        break;
                    }
                }
                records += 1;
                let ght_id = record[0].parse::<u64>().unwrap();
                // if the commit is not valid, ignore it
                if ! self.valid_commits_.contains(& ght_id) {
                    continue;
                }
                // if valid, create the object
                let hash = git2::Oid::from_str(& record[1]).unwrap();
                let mut commit = Commit::new(0, Source::GHTorrent);
                commit.author_id = self.get_or_create_user(record[2].parse::<u64>().unwrap(), dcd);
                commit.committer_id = self.get_or_create_user(record[3].parse::<u64>().unwrap(), dcd);
                commit.committer_time = helpers::to_unix_epoch(& record[5]);
                self.commits_.insert(ght_id, commit);
                hash_to_ght.insert(hash, ght_id);
            }
        }
        // now obtain the ids of the commits we need to analyze 
        let (commit_ids, new_commit_ids) = dcd.get_or_add_commits(& mut hash_to_ght.keys());


        // so we added commits, time to add their parents, which is all we get for a commit
        {
            let mut reader = csv::ReaderBuilder::new().has_headers(false).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/commit_parents.csv", self.root_)).unwrap();
            let mut records = 0;
            println!("Translating commit parents...");
            for x in reader.records() {
                let record = x.unwrap();
            }
        }

    }
}
