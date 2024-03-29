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
    let write_header = previous_results.is_empty();
    let todo = Mutex::new(load_project_urls(& SETTINGS.datastore_root, previous_results));
    let mut f = OpenOptions::new().create(true).append(true).open(& metadata_filename).unwrap();
    if write_header {
        writeln!(& mut f, "{}", ProjectInfo::csv_header()).unwrap();
    }
    // now that we have todos, spawn the threads and start downloading the metadata
    let (tx, rx) = crossbeam_channel::unbounded::<UpdateInfo>();
    let gh = Github::new(& SETTINGS.github_tokens);
    crossbeam::thread::scope(|s| {
        for _ in 0..SETTINGS.num_threads {
            s.spawn(|_| {
                metadata_scrapper(& todo, & gh, tx.clone());
            });
        }
        let mut active_threads = SETTINGS.num_threads;
        println!("Active threads: {}", active_threads);
        let mut valid = 0;
        let mut errors = 0;
        while let Ok(msg) = rx.recv() {
            match msg {
                UpdateInfo::Ok{id : _ , csv_row} => {
                    //println!("{}", id);
                    writeln!(& mut f, "{}", csv_row).unwrap();
                    valid += 1;
                },
                UpdateInfo::Fail{id, err} => {
                    println!("{}: error {}", id, err);
                    writeln!(& mut f, "{}", ProjectInfo::error_row(id, & err)).unwrap();
                    errors += 1;
                    
                },
                UpdateInfo::Done => {
                    println!("Worker done.");
                    active_threads -= 1;
                    if active_threads == 0 {
                        break;
                    }
                }
            }
            if (valid + errors) % 1000 == 0 {
                println!("Valid: {}, errors: {}", valid, errors);
            }
        }
    }).unwrap();
    println!("ALL DONE.");
}


enum UpdateInfo {
    Ok{id : i64, csv_row : String},
    Fail{id : i64, err: String},
    Done,
}

type Tx = crossbeam_channel::Sender<UpdateInfo>;
//type Rx = crossbeam_channel::Receiver<UpdateInfo>;


fn metadata_scrapper(projects : & Mutex<Vec<(i64, String)>>, gh : &Github, tx : Tx) {
    let mut limit = 100000;
    while let Some((id, full_name)) = next_project_to_update(projects) { 
        let metadata_request = format!("https://api.github.com/repos/{}", full_name);
        std::thread::sleep(std::time::Duration::from_millis(1000));
        match gh.request(& metadata_request, None) {
            Ok(json) => {
                if let Some(pinfo) = ProjectInfo::from_json(& json) {
                    tx.send(UpdateInfo::Ok{id, csv_row : pinfo.to_csv()}).unwrap();
                } else {
                    tx.send(UpdateInfo::Fail{id, err : format!("{}",json)}).unwrap();
                }
            },
            Err(e) =>{
                tx.send(UpdateInfo::Fail{id, err : format!("{}",e)}).unwrap();
            }
        }
        limit -= 1;
        if limit == 0 {
            println!("limit reached!");
            break;
        }
    }
    tx.send(UpdateInfo::Done).unwrap()
    ;
}

fn next_project_to_update(projects : & Mutex<Vec<(i64, String)>>) -> Option<(i64, String)> {
    let mut v = projects.lock().unwrap();
    let i = rand::random::<usize>() % v.len();
    let mut idx = i;
    loop {
        if ! v[idx].1.is_empty() {
            let tmp = std::mem::replace(& mut v[idx].1, String::new());
            return Some((v[idx].0, tmp));
        }
        idx = (idx + 1) % v.len();
        if idx == i {
            return None;
        }
    }
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

    fn csv_header() -> &'static str {
        "id,name,language,created,fork,disabled,archived,stars,forks,network_count,subscribers,size"
    }

    fn error_row(id : i64, err: & str) -> String {
        format!("{},\"\",\"{}\",0,0,0,0,0,0,0,0,0",id, str::replace(err.trim(), "\"", "\\\""))
    }

    fn to_csv(& self) -> String {
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
        )
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
            if result.len() % 1000000 == 0 { print!(".") }
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
            if total % 1000000 == 0 { print!(".") }
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
