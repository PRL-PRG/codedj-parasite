use std::collections::*;

use dcd::*;
use dcd::db_manager::*;


/** Actually initializes stuff from ghtorrent.
 */
fn main() {
    let mut db = DatabaseManager::initialize_new("/dejavuii/dejacode/dataset-tiny");
    let root = String::from("/dejavuii/dejacode/ghtorrent/dump-tiny");
    // first filter the projects 
    let project_ids = initialize_projects(& root, & mut db);
    db.commit_created_projects();
    // the filter the commits and assign them to their projects
    let (commits, project_commits) = filter_commits(& root, & project_ids);
    // load all users as they come cheap
    let users = load_users(& root);
    // now we can load commit details, storing the new commits as we get them and obtaining their ids
    let (new_commits, ght_to_sha_and_own) = load_commit_details(& root, commits, & users, & mut db);
    // load stargazers 
    load_stargazers(& root, & project_ids, & users, & mut db);
    // determine commit parents for all commits and store parents for new commits
    let commit_parents = load_commit_parents(& root, new_commits, & ght_to_sha_and_own, & mut db);
    // last thing we need to calculate is project heads
    calculate_project_heads(& project_ids, project_commits, commit_parents, ght_to_sha_and_own, & mut db);
    // mark all projects as updated with GHTorrent 
    finalize_project_updates(& project_ids, & mut db);
}

fn initialize_projects(root : & str, db : & mut DatabaseManager) -> HashMap<u64, ProjectId> {
    let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/projects.csv", root)).unwrap();
    let mut records = 0;
    let mut project_ids = HashMap::<u64,ProjectId>::new();
    println!("Adding new projects...");
    for x in reader.records() {
        let record = x.unwrap();
        if records % 1000 == 0 {
            helpers::progress_line(format!("    records: {}, new projects: {}", records, project_ids.len()));
        }
        records += 1;
        // ignore deleted projects and forks so check these first...
        let forked_from = record[7].parse::<u64>();
        let deleted = record[8].parse::<u64>().unwrap();
        if deleted != 0 || forked_from.is_ok()  {
            continue;
        }
        // it's a valid project, get its url
        let api_url : Vec<&str> = record[1].split("/").collect();
        let name = api_url[api_url.len() - 1].to_lowercase();
        let user = api_url[api_url.len() - 2].to_lowercase();
        let url = format!("https://github.com/{}/{}.git", user, name);
        // see if the project should be added 
        // get the user and repo names
        if let Some(own_id) = db.add_project(url.clone(), Source::GHTorrent) {
            // add the ght_id and language to the project's metadata
            let mut project_log = record::ProjectLog::new(db.get_project_log_filename(own_id));
            // start the update
            project_log.add(record::ProjectLogEntry::update_start(
                Source::GHTorrent
            ));
            // fill in metadata
            project_log.add(record::ProjectLogEntry::metadata(
                Source::GHTorrent, 
                "ght_id".to_owned(),
                String::from(& record[0])
            ));
            project_log.add(record::ProjectLogEntry::metadata(
                Source::GHTorrent, 
                "ght_language".to_owned(),
                String::from(& record[5])
            ));
            // append the log
            project_log.append();
            // add the ght to own id mapping to the result projects...
            let ght_id = record[0].parse::<u64>().unwrap();
            project_ids.insert(ght_id, own_id);
        }
    }
    println!("    records: {}, new projects: {}", records, project_ids.len());
    return project_ids;
}

/** Reads all commits in the ghtorrent database and determines which commits belong to the projects we are adding. 
 
    Returns first set of ght ids of all commits we are going to retain and a map from ght project id to a vector of its commits. 
 */
fn filter_commits(root : & str, project_ids : & HashMap<u64, ProjectId>) -> (HashSet<u64>, HashMap<u64,Vec<u64>>) {
    let mut reader = csv::ReaderBuilder::new().has_headers(false).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/project_commits.csv", root)).unwrap();
    let mut records = 0;
    let mut commits = HashSet::<u64>::new();
    let mut project_commits = HashMap::<u64, Vec<u64>>::new();
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
            commits.insert(commit_id);
            project_commits.entry(project_id).or_insert(Vec::new()).push(commit_id);
        }
    }
    println!("    records: {}, valid commits: {}", records, commits.len());
    return (commits, project_commits);
}

/** Loads all users in the ghtorrent database and returns a hashmap pointing from the user id to their name. 
 */
fn load_users(root : & str) -> HashMap<u64, String> {
    let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/users.csv", root)).unwrap();
    let mut result = HashMap::<u64, String>::new();
    println!("Loading users...");
    for x in reader.records() {
        if result.len() % 1000 == 0 {
            helpers::progress_line(format!("    users: {}", result.len()));
        }
        let record = x.unwrap();
        let ght_id = record[0].parse::<u64>().unwrap();
        let name = String::from(& record[1]);
        result.insert(ght_id, name);
    }
    println!("    users: {}", result.len());
    return result;
}

fn get_or_create_user(ght_id : u64, translated_users : & mut HashMap<u64, UserId>, ght_users : & HashMap<u64, String>, db : & mut DatabaseManager) -> UserId {
    if let Some(id) = translated_users.get(& ght_id) {
        return *id;
    } else {
        let id = db.get_or_create_user(& format!("{}@ghtorrent", ght_id), & ght_users[& ght_id], Source::GHTorrent);
        translated_users.insert(ght_id, id);
        return id;
    }
}

/** Loads details of new commits to the database. 
 
    Goes through all valid ghtorrent commits and obtains their ids from the database. For new commits determines the author and commiter ids (creates new users if necessary) and stores their records in the database.
 */
fn load_commit_details(root : & str, commits : HashSet<u64>, users : & HashMap<u64,String>, db : & mut DatabaseManager) -> (HashSet<u64>, HashMap<u64,(git2::Oid,CommitId)>) {
    let mut reader = csv::ReaderBuilder::new().has_headers(false).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/commits.csv", root)).unwrap();
    let mut records = 0;
    let mut new_commits = HashSet::<u64>::new();
    let mut ght_to_sha_and_own = HashMap::<u64,(git2::Oid, CommitId)>::new();
    let mut translated_users = HashMap::<u64, UserId>::new();
    println!("Loading commit details...");
    for x in reader.records() {
        let record = x.unwrap();
        if records % 1000 == 0 {
            helpers::progress_line(format!("    records: {}, new commits: {}", records, new_commits.len()));
        }
        records += 1;
        let ght_id = record[0].parse::<u64>().unwrap();
        // if the commit is valid, we must determine whether to store it or not
        if commits.contains(& ght_id) {
            let hash = git2::Oid::from_str(& record[1]).unwrap();
            match db.get_or_create_commit_id(hash) {
                // if the commit id is new, fill in the commit information
                (id, RecordState::New) => {
                    // not found, so we have to parse the details
                    let author_id = get_or_create_user(
                        record[2].parse::<u64>().unwrap(),
                        & mut translated_users, 
                        & users,
                        db);
                    let committer_id = get_or_create_user(
                        record[3].parse::<u64>().unwrap(),
                        & mut translated_users, 
                        & users,
                        db);
                    let committer_time = helpers::to_unix_epoch(& record[5]);
                    // and create new commit
                    db.append_commit_record(
                        id,
                        committer_id,
                        committer_time,
                        author_id,
                        0,
                        Source::GHTorrent
                     );
                     // store the mappings and mark the commit as new one
                     ght_to_sha_and_own.insert(ght_id, (hash, id));
                     new_commits.insert(ght_id);

                },
                // if the commit exists, do nothing, but keep the mapping, same for incomplete (although they are not expected to occur)
                (id, _) => {
                    ght_to_sha_and_own.insert(ght_id, (hash, id));
                },
            }
        }
    }
    println!("    records: {}, new commits: {}", records, new_commits.len());
    return (new_commits, ght_to_sha_and_own);
}

/** Loads stars information from ghtorrent and store them in the metadata of the project, one line per record. 
 
    These are called watchers in the GHTorrent because of an old GitHub API name they had.

    Note that this is not really super precise as GHTorrent has no provision of knowing whether stars were removed.
 */
fn load_stargazers(root: & str, project_ids : & HashMap<u64, ProjectId>, users : & HashMap<u64, String>, db : & mut DatabaseManager) {
    let mut reader = csv::ReaderBuilder::new().has_headers(false).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/watchers.csv", root)).unwrap();
    let mut stars = HashMap::<ProjectId, Vec<(UserId,i64)>>::new();
    let mut translated_users = HashMap::<u64, UserId>::new();
    println!("Loading stargazers...");
    for x in reader.records() {
        let record = x.unwrap();
        let project_ght_id = record[0].parse::<u64>().unwrap();
        if let Some(project_id) = project_ids.get(& project_ght_id) {
            let user_ght_id = record[1].parse::<u64>().unwrap();
            let time = helpers::to_unix_epoch(& record[2]);
            // this is not the speediest since some users have already been crated when we dealt with commits, but who cares for now...
            let user_id = get_or_create_user(
                user_ght_id,
                & mut translated_users,
                & users, 
                db
            );
            stars.entry(*project_id).or_insert(Vec::new()).push((user_id, time));
        }
    }
    println!("    {} projects have stars", stars.len());
    for (project_id, stars) in stars.iter_mut() {
        stars.sort_by(|(_, timea), (_, timeb) | timea.cmp(timeb));
        let mut log = record::ProjectLog::new(db.get_project_log_filename(*project_id));
        let mut num_stars = 0;
        for (_, time) in stars { // used to be userid
            num_stars += 1;
            log.add(record::ProjectLogEntry::Metadata{
                time : *time,
                source : Source::GHTorrent,
                key : "stars".to_owned(),
                value : format!("{}", num_stars),
            });
        }
        log.append();
    }
}

fn load_commit_parents(root : & str, new_commits : HashSet<u64>, ght_to_sha_and_own : & HashMap<u64,(git2::Oid, CommitId)>, db : & mut DatabaseManager) -> HashMap<u64,Vec<u64>> {
    let mut reader = csv::ReaderBuilder::new().has_headers(false).double_quote(false).escape(Some(b'\\')).from_path(format!("{}/commit_parents.csv", root)).unwrap();
    let mut records = 0;
    let mut commit_parents_ght = HashMap::<u64, Vec<u64>>::new();
    let mut commit_parents_own = Vec::<(CommitId, CommitId)>::new();
    println!("Loading commit parents...");
    for x in reader.records() {
        let record = x.unwrap();
        if records % 1000 == 0 {
            helpers::progress_line(format!("    records: {}, new records: {}", records, commit_parents_own.len()));
        }
        records += 1;
        let ght_id = record[0].parse::<u64>().unwrap();
        // if the commit is our own and so is the parent (in theory if commit is own, so must be the parent, but there are holes and errors in the GHTorrent database dumps)
        if let Some((_, own_id)) = ght_to_sha_and_own.get(& ght_id) {
            let parent_ght_id = record[1].parse::<u64>().unwrap();
            if let Some((_, parent_own_id)) = ght_to_sha_and_own.get(& parent_ght_id) {
                // store the mapping in the commit parents (which uses ght encoding)
                commit_parents_ght.entry(ght_id).or_insert(Vec::new()).push(parent_ght_id);
                // if the commit is new, add the parent information the own record
                if new_commits.contains(& ght_id) {
                    commit_parents_own.push((*own_id, *parent_own_id));
                }
            }
        }
    }
    println!("    records: {}, new records: {}", records, commit_parents_own.len());
    println!("Writing new parent records...");
    db.append_commit_parents_records(& mut commit_parents_own.iter());
    return commit_parents_ght;
}

/** Calculates project heads for the new projects. 
 
    The algorithm is fairly simple. For each project creates a set of commits the project contains. Then for each 
 */
fn calculate_project_heads(
    project_ids : & HashMap<u64, ProjectId>,
    project_commits : HashMap<u64, Vec<u64>>,
    commit_parents : HashMap<u64, Vec<u64>>,
    ght_to_sha_and_own : HashMap<u64, (git2::Oid, u64)>,
    db : & mut DatabaseManager) {
        let mut records = 0;
        println!("Calculating project heads...");
        for (ght_id, own_id) in project_ids {
            if records % 1000 == 0 {
                helpers::progress_line(format!("    projects: {}", records));
            }
            records += 1;
            // there can be projects with no commits 
            if let Some(pc) = project_commits.get(ght_id) {
                // TODO why must I do the map? 
                let mut heads : HashSet<u64> = pc.iter().map(|x| *x).collect();
                for ght_commit_id in pc {
                    // note that there are holes in the ghtorrent info and not every commit in a project may have parent information available, or it can be initial commit with no parents
                    if let Some(parent_commits) = commit_parents.get(ght_commit_id) {
                        for parent_id in parent_commits {
                            heads.remove(parent_id);
                        }
                    }
                }
                // create the head records and save the in the project's log
                let mut log = record::ProjectLog::new(db.get_project_log_filename(*own_id));
                for ght_commit_id in heads {
                    let hash = ght_to_sha_and_own[& ght_commit_id].0;
                    log.add(record::ProjectLogEntry::head(Source::GHTorrent, String::new(), hash));
                }
                // save the log 
                log.append();
            }
        }
        println!("    projects: {}", records);
    }

/** Finalizes update phase for the projects being added. 
 */
fn finalize_project_updates(project_ids : & HashMap<u64, ProjectId>, db : & mut DatabaseManager) {
    println!("Loading commit parents...");
    let mut records = 0;
    for (_, own_id) in project_ids {
        if records % 1000 == 0 {
            helpers::progress_line(format!("    projects: {}", records));
        }
        records += 1;
        // finalize project update with the update message
        let mut log = record::ProjectLog::new(db.get_project_log_filename(*own_id));
        log.add(record::ProjectLogEntry::update(Source::GHTorrent));
        log.append();
    }
    println!("    projects: {}", records);
}

