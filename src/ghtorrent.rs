use std::collections::{HashMap, HashSet, BinaryHeap};
use crate::downloader_state::*;
use crate::project::*;
use crate::*;

/** No OOP, just pass everything as arguments.
 */
pub fn import(rootFolder : & str, dcd : & mut DownloaderState) {
    // first import the projects and obtain the project ght id to project id mapping 
    let project_ids = import_projects(rootFolder, dcd);
    // then determine which commits belong to the projects we are adding and create their objects and mapping from projects to commits
    let (mut commits, mut project_commits) = filter_commits(rootFolder, & project_ids);
    // now load commit details, keeping user ids in ght format as we are not saving the commits yet
    let sha_to_ght = load_commit_details(rootFolder, & mut commits);
    // get own ids for all the commits used and determine which ones are new (source == GhTorrent)
    let sha_to_own = get_or_create_commit_ids(dcd, & sha_to_ght, & mut commits);
    // we can now augment the commits with their parent information
    load_commit_parents(rootFolder, & mut commits);
    // calculate project heads (commits that do not have children in the project)
    let mut project_heads = calculate_project_heads(& project_commits, & commits);
    project_commits.clear();

    // translate project ids and clone ids to own ids
    translate_commit_ids_to_own(& mut commits);
    // and translate project heads to SHA keys directly so that incremental updates work
    let mut project_heads_sha = translate_project_heads_to_hashes(& project_heads, & sha_to_ght);
    project_heads.clear();
    
    // now let's ditch any commits we do not have to store (i.e. those that do not have source as GHTorrent)
    prune_commits(& mut commits);

    // finally now that we have only commits we need and proper commit ids everywhere
    load_and_update_users(rootFolder, & mut commits, dcd);

    // write the commits 
    dcd.append_new_commits(& mut commits.values());






    // writing - write the commits first

    // then write their parents

    // finally write the project heads


    // now that we have ids of all commits, we can translate project commits to own ids...
    //translate_project_commit_ids_to_own(& mut project_commits, & commits);

}

fn import_projects(root : & str, dcd : & mut DownloaderState) -> HashMap<u64, ProjectId> {
    let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/projects.csv", root)).unwrap();
    let mut records = 0;
    let mut project_ids = HashMap::<u64,ProjectId>::new();
    // hashmap from ghtorrent ids to own ids...
    let mut pending_forks = HashMap::<u64,u64>::new();
    println!("Adding new projects...");
    for x in reader.records() {
        let record = x.unwrap();
        if records % 1000 == 0 {
            helpers::progress_line(format!("    records: {}, new projects: {}, pending forks: {}", records, project_ids.len(), pending_forks.len()));
        }
        records += 1;
        let gh_id = record[0].parse::<u64>().unwrap();
        let api_url : Vec<&str> = record[1].split("/").collect();
        let language = & record[5];
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
            // !!!!!!!!!!!!!!!!!!!! TODO turn this on once we want to create the projects
            //md.save(& dcd.dcd_.get_project_root(p.id));
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

pub fn filter_commits(root : & str, project_ids : & HashMap<u64, ProjectId>) -> (HashMap<u64, Commit>, HashMap<u64, HashSet<u64>>) {
    let mut reader = csv::ReaderBuilder::new().has_headers(false).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/project_commits.csv", root)).unwrap();
    let mut records = 0;
    let mut commits = HashMap::<u64, Commit>::new();
    let mut project_commits = HashMap::<u64, HashSet<u64>>::new();
    println!("Filtering commits for newly added projects only...");
    for x in reader.records() {
        let record = x.unwrap();
        if records % 1000 == 0 {
            helpers::progress_line(format!("    records: {}, valid commits: {}", records, commits.len()));
        }
        records += 1;
        let project_id = record[0].parse::<u64>().unwrap();
        if project_ids.contains_key(& project_id) {
            let commit_id = record[1].parse::<u64>().unwrap();
            commits.insert(commit_id, Commit::new(0, Source::NA));
            project_commits.entry(project_id).or_insert(HashSet::new()).insert(commit_id);
        }
    }
    println!("    valid commits: {}\x1b[K", commits.len());
    return (commits, project_commits);
}


fn load_commit_details(root : & str, commits : & mut HashMap<u64, Commit>) -> HashMap<git2::Oid, u64> {
    let mut sha_to_ght = HashMap::<git2::Oid, u64>::new();
    println!("Adding new commits...");
    let mut reader = csv::ReaderBuilder::new().has_headers(false).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/commits.csv", root)).unwrap();
    let mut records = 0;
    let mut updates = 0;
    for x in reader.records() {
        let record = x.unwrap();
        if records % 1000 == 0 {
            helpers::progress_line(format!("    records: {}, hashes: {}", records, sha_to_ght.len()));
        }
        records += 1;
        let ght_id = record[0].parse::<u64>().unwrap();
        // if the commit is not valid, ignore it
        if ! commits.contains_key(& ght_id) {
            continue;
        }
        // if valid, create the object
        let hash = git2::Oid::from_str(& record[1]).unwrap();
        let ref mut commit = commits.get_mut(& ght_id).unwrap();
        //commit.author_id = self.get_or_create_user(record[2].parse::<u64>().unwrap(), dcd);
        //commit.committer_id = self.get_or_create_user(record[3].parse::<u64>().unwrap(), dcd);
        commit.committer_time = helpers::to_unix_epoch(& record[5]);
        sha_to_ght.insert(hash, ght_id);
        updates += 1;
    }
    println!("    records: {}, hashes: {}", records, sha_to_ght.len());
    return sha_to_ght;
}

fn get_or_create_commit_ids(dcd : & mut DownloaderState, sha_to_ght : & HashMap<git2::Oid, u64>, commits : & mut HashMap<u64, Commit>) -> HashMap<git2::Oid, CommitId> {
    println!("Pruning commits and generating ids...");
    let (sha_to_own, new_own) = dcd.get_or_add_commits(& mut sha_to_ght.keys());
    for ref x in sha_to_own.iter() {
        let ght_id = sha_to_ght[x.0];
        let ref mut commit = commits.get_mut(& ght_id).unwrap();
        commit.id = *x.1;
        if new_own.contains(x.1) {
            commit.source = Source::GHTorrent;
        }
    }
    return sha_to_own;
}

/** Loads the commit parents. 
 
    The commit parents are in ght indexes for now. 
 */
fn load_commit_parents(root : & str, commits : & mut HashMap<u64, Commit>) {
    println!("Loading commit parents...");
    let mut reader = csv::ReaderBuilder::new().has_headers(false).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/commit_parents.csv", root)).unwrap();
    let mut records = 0;
    let mut parents = 0;
    for x in reader.records() {
        let record = x.unwrap();
        if records % 1000 == 0 {
            helpers::progress_line(format!("    records: {}, valid parents: {} ", records, parents));
        }
        records += 1;
        let ght_id = record[0].parse::<u64>().unwrap();
        if commits.contains_key(& ght_id) {
            let parent_id_ght = record[1].parse::<u64>().unwrap() as CommitId;
            match commits.get_mut(& ght_id) {
                Some(commit) => {
                    commit.parents.push(parent_id_ght);
                    parents += 1;
                },
                _ => {}
            }
        }
    }
}

/** Determines project heads. 
 
    Given all commits in a project, project heads can be obtained by removing all commits that are parents of some other commits in the project. This only leaves the top commits with a guarantee that all other project commits are accessible through them.
 */
fn calculate_project_heads(project_commits : & HashMap<u64, HashSet<u64>>, commits : & HashMap<u64, Commit>) -> HashMap<u64,HashSet<u64>> {
    println!("Calculating project heads...");
    let mut project_heads = HashMap::<u64,HashSet<u64>>::new();
    for (project_id, commit_ids) in project_commits.iter() {
        if project_heads.len() % 1000 == 0 {
            helpers::progress_line(format!("    projects: {}", project_heads.len()));
        }
        let mut heads = commit_ids.clone();
        for commit_id in commit_ids.iter() {
            let commit = & commits[commit_id];
            for parent_id in & commit.parents {
                heads.remove(& parent_id);
            }
        }
        //println!("  project {}, commits {}, heads {}", project_id, commit_ids.len(), heads.len());
        project_heads.insert(* project_id, heads);
    }
    return project_heads;
}

fn translate_commit_ids_to_own(commits : & mut HashMap<u64, Commit>) {
    println!("Building ght to own id mapping for commit ids...");
    let ght_to_own : HashMap<u64, u64> = commits.iter()
        .map(|(ght_id, commit)| (*ght_id, commit.id))
        .collect();
    println!("Translating commit parents...");
    for (_, commit) in commits.iter_mut() {
        commit.parents = commit.parents.iter()
            .map(|ght_id| ght_to_own[ght_id])
            .collect();
    }
}

fn translate_project_heads_to_hashes(project_heads : & HashMap<u64, HashSet<u64>>, sha_to_ght : & HashMap<git2::Oid, u64>) -> HashMap<u64, Vec<(String,git2::Oid)>> {
    println!("Building reverse iterator from ght id to sha...");
    let ght_to_sha : HashMap<u64, git2::Oid> = sha_to_ght.iter().
        map(|(sha, id)| (*id, *sha)).
        collect();
    println!("Translating project heads...");
    let mut result = HashMap::<u64, Vec<(String,git2::Oid)>>::new();
    for (project_id, heads) in project_heads.iter() {
        let heads_sha : Vec<(String, git2::Oid)> = heads.iter()
            .map(|ght_id| (String::new(), ght_to_sha[ght_id]))
            .collect();
        result.insert(*project_id, heads_sha);
    }
    return result;
}

/** Prunes the commits so that only new commits that need to be stored remain.
 
    This is determined by the source tag of the commit, commits with GHTorrent as their source are new commits, those with NA do not have to be saved
 */
fn prune_commits(commits : & mut HashMap<u64, Commit>) {
    println!("Detecting commits to be pruned...");
    let to_be_deleted : Vec<u64> = commits.iter()
        .filter(|(_, commit)| commit.source != Source::GHTorrent)
        .map(|(ght_id, _)| *ght_id)
        .collect();
    println!("Pruning...");
    for x in to_be_deleted {
        commits.remove(& x);
    }
}

fn load_and_update_users(root : & str, commits : & mut HashMap<u64, Commit>, dcd : & mut DownloaderState) {
    println!("Calculating valid users...");
    let mut valid_users = HashSet::<u64>::new();
    for (_, commit) in commits.iter() {
        valid_users.insert(commit.author_id as u64);
        valid_users.insert(commit.committer_id as u64);
    }
    println!("    {} users detected", valid_users.len());
    println!("Getting or creating own ids...");
    let mut ght_to_own = HashMap::<u64, UserId>::new();
    let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/users.csv", root)).unwrap();
    for x in reader.records() {
        let record = x.unwrap();
        let ght_id = record[0].parse::<u64>().unwrap();
        if valid_users.contains(& ght_id) {
            let name = String::from(& record[1]);
            let email = format!("{}@ghtorrent", ght_id);
            let id = dcd.get_or_create_user(& email, & name);
            ght_to_own.insert(ght_id, id);
        }
    }
    println!("Updating user ids...");
    for (_, commit) in commits.iter_mut() {
        commit.author_id = ght_to_own[& commit.author_id];
        commit.committer_id = ght_to_own[& commit.committer_id];
    }
}

