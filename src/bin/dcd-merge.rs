use std::collections::*;
use byteorder::*;
use std::fs::*;
use std::io::prelude::*;
use std::io::SeekFrom;

use dcd::*;
use dcd::db_manager::*;

/** Merges two datasets into one. 

    The merge is fairly primitive. We look at all commits (+ changes and messages), file paths, users, and determine which ones are new, add their ids (remember the calculation) and add their messages & changes. 
    
    Then we move all projects that are not in live urls already to the dataset without changing them. 
*/

fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() < 3 || args.len() > 4 || args.len() == 4 && (args[3] != "--new") {
        panic!{"Invalid usage - dcd-merge INTO_DATABASE OTHER [--new]"}
    }
    let mut db = DatabaseManager::from(& args[1]);
    let other_root = String::from(& args[2]);

    let users_table = merge_users(& mut db, & other_root);
    let paths_table = merge_paths(& mut db, & other_root); 
    let snapshots_table = merge_snapshots(& mut db, & other_root);
    merge_commits(& mut db, & other_root, & users_table, & paths_table, & snapshots_table);

    merge_projects(& mut db, & other_root);
}




fn merge_users(db : & mut DatabaseManager, other_root : & str) -> HashMap<UserId, UserId> {
    let mut result = HashMap::<UserId, UserId>::new();
    let mut to_be_added = HashMap::<UserId, String>::new(); 
    println!("Merging users...");
    {
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(DatabaseManager::get_user_ids_file(other_root)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let their_id = record[1].parse::<u64>().unwrap() as UserId;
            let email = String::from(&record[0]);
            if let Some(own_id) = db.get_user(&email) {
                result.insert(their_id, own_id);
            } else {
                to_be_added.insert(their_id, email);
            }
        }
    }
    println!("    {} existing users", result.len());
    println!("    {} users to be added", to_be_added.len());
    {
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(DatabaseManager::get_user_records_file(other_root)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let id = record[1].parse::<u64>().unwrap() as UserId;
            if let Some(email) = to_be_added.get(&id) {
                let name = String::from(& record[2]);
                let source = Source::from_str(& record[3]);
                let new_id = db.get_or_create_user(email, & name, source);
                result.insert(id, new_id);
            }
        }
    }
    return result;
}

fn merge_paths(db : & mut DatabaseManager, other_root : & str) -> HashMap<PathId, PathId> {
    let mut result = HashMap::<PathId, PathId>::new();
    let paths = db.num_paths();
    println!("Merging paths...");
    {
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(DatabaseManager::get_path_ids_file(other_root)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let their_id = record[1].parse::<u64>().unwrap() as UserId;
            let path = String::from(&record[0]);
            result.insert(their_id, db.get_or_create_path_id(& path));
        }
    }
    println!("    {} paths added", db.num_paths() - paths);
    return result;
}

fn merge_snapshots(db : & mut DatabaseManager, other_root : & str) -> HashMap<SnapshotId, SnapshotId> {
    let mut result = HashMap::<PathId, PathId>::new();
    let snapshots = db.num_snapshots();
    println!("Merging snapshots...");
    {
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(DatabaseManager::get_snapshot_ids_file(other_root)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let their_id = record[1].parse::<u64>().unwrap() as UserId;
            let hash = git2::Oid::from_str(& record[0]).unwrap();
            result.insert(their_id, db.get_or_create_snapshot_id(hash).0);
        }
    }
    println!("    {} snapshots added", db.num_snapshots() - snapshots);
    return result;
}

/** Merging commits.
 
    First look though all commits ids and determine the ones we will need to add. Then get their records (only take the latest ones)
 */ 
fn merge_commits(db : & mut DatabaseManager, 
                 other_root : & str,
                 users_table : & HashMap<UserId, UserId>,
                 paths_table : & HashMap<PathId, PathId>,
                 snapshots_table : & HashMap<SnapshotId, SnapshotId>,
    ) {
    let mut result = HashMap::<CommitId, CommitId>::new();
    let mut to_be_added = HashSet::<CommitId>::new(); 
    println!("Merging commits...");
    {
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(DatabaseManager::get_commit_ids_file(other_root)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let their_id = record[1].parse::<u64>().unwrap() as CommitId;
            let hash = git2::Oid::from_str(& record[0]).unwrap();
            if let Some((own_id, _)) = db.get_commit_id(hash) {
                result.insert(their_id, own_id);
            } else {
                to_be_added.insert(their_id);
                let own_id = db.get_or_create_commit_id(hash).0;
                result.insert(their_id, own_id);
            }
        }
    }
    println!("    {} existing commits", result.len());
    println!("    {} commits to be added", to_be_added.len());
    {
        let mut commit_records = HashMap::<CommitId, CommitBase>::new();
        let mut commit_sources = HashMap::<CommitId, Source>::new();
        {
            println!("Loading commit records...");
            let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(DatabaseManager::get_commit_records_file(other_root)).unwrap();
            for x in reader.records() {
                let record = x.unwrap();
                let id = record[1].parse::<u64>().unwrap() as UserId;
                if to_be_added.contains(&id) {
                    commit_records.insert(id, 
                        CommitBase{
                            parents : Vec::new(),
                            committer_id : record[2].parse::<u64>().unwrap() as UserId,
                            committer_time : record[3].parse::<i64>().unwrap(),
                            author_id : record[4].parse::<u64>().unwrap() as UserId,
                            author_time : record[5].parse::<i64>().unwrap(),
                        }
                    );
                    commit_sources.insert(id, Source::from_str(& record[6]));
                }
            }
        }
        {
            println!("Loading commit parents...");
            let mut parents_update_times = HashMap::<CommitId, i64>::new();
            let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(DatabaseManager::get_commit_parents_file(other_root)).unwrap();
            for x in reader.records() {
                let record = x.unwrap();
                let id = record[1].parse::<u64>().unwrap();
                if to_be_added.contains(&id) {
                    let t = record[0].parse::<i64>().unwrap();
                    // clear the parent records if the update time differs
                    if t != * parents_update_times.entry(id).or_insert(0) {
                        parents_update_times.insert(id, t);
                        commit_records.get_mut(& id).unwrap().parents.clear();
                    }
                    commit_records.get_mut(& id).unwrap().parents.push(record[2].parse::<u64>().unwrap() as CommitId);
                }
            }
        }
        // write the commits
        {
            println!("Writing commit records...");
            for (their_id, commit) in commit_records { // consume
                let own_id = result[& their_id];
                let committer = users_table[& commit.committer_id];
                let author = users_table[& commit.author_id];
                db.append_commit_record(own_id, committer, commit.committer_time, author, commit.author_time, commit_sources[& their_id]);
                if !commit.parents.is_empty() {
                    let own_parents : Vec<(CommitId, CommitId)> = commit.parents.iter().map(|x| (own_id, *x)).collect();
                    db.append_commit_parents_records(& mut own_parents.iter());
                }
            }
        }
        // now we look at commit messages
        {
            println!("Translating commit messages...");
            let mut f = OpenOptions::new().read(true).open(DatabaseManager::get_commit_messages_file(other_root)).unwrap();
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(true)
                .double_quote(false)
                .escape(Some(b'\\'))
                .from_path(DatabaseManager::get_commit_messages_index_file(other_root)).unwrap();
            for x in reader.records() {
                let record = x.unwrap();
                let commit_id = record[1].parse::<u64>().unwrap() as CommitId;
                if to_be_added.contains(& commit_id) {
                    let offset = record[2].parse::<u64>().unwrap();
                    let own_id = result[& commit_id];
                    f.seek(SeekFrom::Start(offset)).unwrap();
                    let id = f.read_u64::<LittleEndian>().unwrap();
                    assert_eq!(id, commit_id);
                    let size = f.read_u32::<LittleEndian>().unwrap();
                    let mut buffer = vec![0; size as usize];
                    f.read(&mut buffer).unwrap();
                    db.append_commit_message(own_id, & buffer);
                }
            }
        }
        {
            println!("Translating commit changes...");
            let mut f = OpenOptions::new().read(true).open(DatabaseManager::get_commit_changes_file(other_root)).unwrap();
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(true)
                .double_quote(false)
                .escape(Some(b'\\'))
                .from_path(DatabaseManager::get_commit_changes_index_file(other_root)).unwrap();
            for x in reader.records() {
                let record = x.unwrap();
                let commit_id = record[1].parse::<u64>().unwrap() as CommitId;
                if to_be_added.contains(& commit_id) {
                    let offset = record[2].parse::<u64>().unwrap();
                    let own_id = result[& commit_id];
                    f.seek(SeekFrom::Start(offset)).unwrap();
                    let id = f.read_u64::<LittleEndian>().unwrap();
                    assert_eq!(id, commit_id);
                    let num_changes = f.read_u32::<LittleEndian>().unwrap() as usize;
                    let additions = f.read_u64::<LittleEndian>().unwrap() as usize;
                    let deletions = f.read_u64::<LittleEndian>().unwrap() as usize;
                    let mut changes = Vec::<(PathId, SnapshotId)>::new();
                    while changes.len() < num_changes  {
                        changes.push((
                            paths_table[& (f.read_u64::<LittleEndian>().unwrap() as PathId)],
                            snapshots_table[& (f.read_u64::<LittleEndian>().unwrap() as SnapshotId)],
                        ));
                    } 
                    db.append_commit_changes(own_id, & changes, additions, deletions);
                }
            }
        }
    }
}

/** Take projects, get their urls and if found, copy all their log with current timestamp. 
 */
fn merge_projects(db : & mut DatabaseManager, other_root: & str) {
    db.load_project_urls();
    let num_projects = DatabaseManager::get_num_projects(other_root);
    println!("Merging {} projects ", num_projects);
    let mut added = 0;
    for pid in 0 .. num_projects {
        let mut log = record::ProjectLog::new(DatabaseManager::get_project_log_file(other_root, pid as ProjectId));
        log.read_all();
        let mut latest_url = String::new();
        for e in & log.entries_ {
            match e {
                record::ProjectLogEntry::Init{time : _, source : _, url} => {
                    latest_url = url.to_owned();
                },
                _ => {}
            }
        }
        if let Some(own_id) = db.add_project(latest_url, Source::NA) {
            log.filename_ = db.get_project_log_filename(own_id);
            patch_project_log_time(& mut log);
            log.create_and_save();
            added += 1;
        }
    }
    db.commit_created_projects();
    println!("    {} projects added", added);
}

fn patch_project_log_time(log : & mut record::ProjectLog) {
    let t = helpers::now();
    for x in  log.entries_.iter_mut() {
        match x {
            record::ProjectLogEntry::Init{time, source : _, url : _} => {
                *time = t;
            },
            record::ProjectLogEntry::Update{time, source: _} => {
                *time = t;
            },
            record::ProjectLogEntry::Error{time, source: _, message: _} => {
                *time = t;
            },
            record::ProjectLogEntry::UpdateStart{time, source: _} => {
                *time = t;
            },
            record::ProjectLogEntry::NoChange{time, source: _} => {
                *time = t;
            },
            record::ProjectLogEntry::Metadata{time, source : _, key : _, value : _} => {
                *time = t;
            },
            record::ProjectLogEntry::Head{time, source : _, name : _, hash : _} => {
                *time = t;
            },

        }
    }
}

