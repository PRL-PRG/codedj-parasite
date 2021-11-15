use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write};

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
    let (mut last_id, mut records) = get_latest_results();
    let mut f = OpenOptions::new().create(true).append(true).open(& SETTINGS.datastore_root).unwrap();
    writeln!(& mut f, "id,name,language,created,fork,stars,forks,archived,disabled").unwrap();
    let gh = Github::new(& SETTINGS.github_tokens);
    loop {
        let request = format!("https://api.github.com/repositories?since={}", last_id);
        match gh.request(& request, None) {
            Ok(json::JsonValue::Array(repos)) => {
                for repo in repos {
                    let id = repo["id"].as_i64().unwrap();
                    //println!("id: {}", id);
                    let full_name = repo["full_name"].as_str().unwrap();
                    let fork = repo["fork"].as_bool().unwrap();
                    if last_id < id { last_id = id; }
                    // we have the basic info, now it's time to get the languages as well, which sadly costs us an extra request
                    let metadata_request = repo["url"].as_str().unwrap();
                    match gh.request(metadata_request, None) {
                        Ok(json) => {
                            let language = json["language"].as_str().map(|x| x.to_owned()).unwrap_or(String::new());
                            let created = chrono::NaiveDateTime::parse_from_str(json["created_at"].as_str().unwrap(), "%Y-%m-%dT%H:%M:%SZ").unwrap().timestamp();
                            let stars = json["stargazers_count"].as_i64().unwrap();
                            let forks = json["forks_count"].as_i64().unwrap();
                            let archived = json["archived"].as_bool().unwrap();
                            let disabled = json["disabled"].as_bool().unwrap();
                            writeln!(& mut f,"{},\"{}\",\"{}\",{},{},{},{},{},{}",
                                id,
                                full_name,
                                language,
                                created,
                                if fork { 1 } else { 0 },
                                stars,
                                forks,
                                if archived { 1 } else { 0 },
                                if disabled { 1 } else { 0 },
                            ).unwrap();
                            records += 1;
                        },
                        Err(_) => {
                            println!("Unable to load metadata for project {}", full_name);
                        }
                    }
                }
                println!("Moving to last_id {}, total records {}", last_id, records);
            },
            Ok(json) => {
                if json["message"].is_string() && json["message"].as_str().unwrap() == "Not Found" {
                    println!("No new projects found, exitting.");
                } else {
                    println!("unknown response format (query since {}) ", last_id);
                }
                break;
            }
            Err(e) => {
                println!("error {} (query since {}) ", e, last_id);
                break;
            }
        }
    }
}

/** Attempts to read the file as we have it and initialize the number of projects and latest id from it. 
 */
fn get_latest_results() -> (i64, i64) {
    println!("Loading previous results...");
    if let Ok(mut reader) = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(& SETTINGS.datastore_root) {
        let mut last_id : i64 = 0;
        let mut records : i64 = 0;
        for x in reader.records() {
            records += 1;
            if records % 10000 == 0 { print!(".") }
            let record = x.unwrap();
            last_id = record[0].parse::<i64>().unwrap();
        }
        println!("");
        println!("{} records found, last id: {}", records, last_id);
        return (last_id, records);
    } else {
        println!("No previous data found");
        return (0,0);
    }
}
