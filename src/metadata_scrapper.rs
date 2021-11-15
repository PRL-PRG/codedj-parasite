use std::collections::HashSet;
use std::fs::{OpenOptions};
use std::io::{Write};
use std::sync::Mutex;

#[macro_use]
extern crate lazy_static;


// we need all these because the v3 code is badly structured...
#[allow(dead_code)]
mod github;
#[allow(dead_code)]
mod helpers;
#[allow(dead_code)]
mod settings;
#[allow(dead_code)]
mod updater;
#[allow(dead_code)]
mod records;
#[allow(dead_code)]
mod reporter;
#[allow(dead_code)]
mod datastore;
#[allow(dead_code)]
mod db;
#[allow(dead_code)]
mod task_verify_substore;
#[allow(dead_code)]
mod datastore_maintenance_tasks;
#[allow(dead_code)]
mod task_update_substore;
#[allow(dead_code)]
mod task_update_repo;

use settings::SETTINGS;
use github::Github;





fn main() {
    let metadata_filename = format!("{}.with_metadata", & SETTINGS.datastore_root);
    let previous_results = load_previous_results(& metadata_filename);
    let todo = Mutex::new(load_project_urls(& SETTINGS.datastore_root, previous_results));
    // now that we have todos, spawn the threads and start downloading the metadata
    let (tx, rx) = crossbeam_channel::unbounded::<UpdateInfo>();
    crossbeam::thread::scope(|s| {


        for _ in 0..SETTINGS.num_threads {
            s.spawn(|_| {
                metadata_scrapper(& todo, tx.clone());
            });
        }
    }).unwrap();
    println!("ALL DONE.");
}


enum UpdateInfo {
    Ok{id : i64, csv_row : String},
    Fail{id : i64},
    Done,
}

type Tx = crossbeam_channel::Sender<UpdateInfo>;
type Rx = crossbeam_channel::Receiver<UpdateInfo>;


fn metadata_scrapper(projects : & Mutex<Vec<(i64, String)>>, tx : Tx) {
    while let Some(id) = next_project_to_update(projects) {
        // TODO update the project

    }
    tx.send(UpdateInfo::Done);
}

fn next_project_to_update(projects : & Mutex<Vec<(i64, String)>>) -> Option<i64> {
    let v = projects.lock().unwrap();
    // 
    return None;
}

struct ProjectInfo {
    id : i64,
    name : String,
    language : String,
    created : i64,
    fork : bool,
    disabled : bool,
    archived : bool,
    stars : u64,
    forks : u64, 
    network_count : u64, 
    subscribers : u64,
    size : u64,
}

impl ProjectInfo {
    fn from_json(json : & json::JsonValue) -> Option<ProjectInfo> {
        if let Some(id) = json["id"].as_i64() {
            let name = json["full_name"].as_str().unwrap().to_owned();
            let language = json["language"].as_str().map(|x| x.to_owned()).unwrap_or(String::new());
            let created = chrono::NaiveDateTime::parse_from_str(json["created_at"].as_str().unwrap(), "%Y-%m-%dT%H:%M:%SZ").unwrap().timestamp();
            let fork = json["fork"].as_bool().unwrap();
            let disabled = json["disabled"].as_bool().unwrap();
            let archived = json["archived"].as_bool().unwrap();
            let stars = json["stargazers_count"].as_u64().unwrap_or(0);
            let forks = json["forks_count"].as_u64().unwrap_or(0);
            let network_count = json["stargazers_count"].as_u64().unwrap_or(0);
            let subscribers = json["subscribers_count"].as_u64().unwrap_or(0);
            let size = json["size"].as_u64().unwrap_or(0);
            return Some(ProjectInfo{
                id,
                name,
                language,
                created,
                fork,
                disabled,
                archived,
                stars,
                forks,
                network_count,
                subscribers,
                size,
            });
        } else {
            return None;
        }
    }

    fn cvs_header() -> &'static str {
        "id,name,language,created,fork,disabled,archived,stars,forks,network_count,subscribers,size"
    }

    fn to_csv(& self) {
        format!("{},\"{}\", \"{}\",{},{},{},{},{},{},{},{},{}",
            self.id,
            self.name,
            self.language,
            self.created,
            if self.fork { 1 } else { 0 },
            if self.disabled { 1 } else { 0 },
            if self.archived { 1 } else { 0 },
            self.stars,
            self.forks,
            self.network_count,
            self.subscribers,
            self.size
        );
    }
}

fn load_previous_results(filename : & str) -> HashSet<i64> {
    let mut result = HashSet::<i64>::new();
    if let Ok(mut reader) = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(filename) {
        for x in reader.records() {
            if result.len() % 10000 == 0 { print!(".") }
            let record = x.unwrap();
            result.insert(record[0].parse::<i64>().unwrap());
        }
        println!("");
        println!("{} projects found", result.len());
    } else {
        println!("No previous data found");
    }
    return result;
}

fn load_project_urls(filename : & str, previous_results : HashSet<i64> ) -> Vec<(i64, String)> {
    let mut result = Vec::<(i64, String)>::new();
    if let Ok(mut reader) = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(filename) {
        let mut total = 0;
        for x in reader.records() {
            total += 1;
            if total % 10000 == 0 { print!(".") }
            let record = x.unwrap();
            let id = record[0].parse::<i64>().unwrap();
            if ! previous_results.contains(& id) {
                result.push((id, record[1].to_owned()));
            }
        }
        println!("");
        println!("{} projects found in total, {} todos", total, result.len());
    } else {
        println!("No todo projects found!");
    }
    return result;
}
