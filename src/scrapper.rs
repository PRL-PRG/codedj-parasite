use std::fs::{OpenOptions};
use std::io::{Write};

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
    if last_id == 0 {
        //writeln!(& mut f, "id,name,language,created,fork,stars,forks,archived,disabled").unwrap();
        writeln!(& mut f, "id,name,fork").unwrap();
    }
    let gh = Github::new(& SETTINGS.github_tokens);
    loop {
        let request = format!("https://api.github.com/repositories?since={}", last_id);
        match gh.request(& request, None) {
            Ok(json::JsonValue::Array(repos)) => {
                for repo in repos {
                    // there are nulls scarcely distributed in the results, skip them 
                    if repo.is_null() {
                        continue;
                    }
                    let id = repo["id"].as_i64().unwrap();
                    //println!("id: {}", id);
                    let full_name = repo["full_name"].as_str().unwrap();
                    let fork = repo["fork"].as_bool().unwrap();
                    if last_id < id { last_id = id; }
                    writeln!(& mut f, "{},\"{}\",{}", id, full_name, if fork { 1 } else { 0 }).unwrap();
                    records += 1;
                }
                println!("Moving to last_id {}, total records {}", last_id, records);
            },
            Ok(json) => {
                if json["message"].is_string() && json["message"].as_str().unwrap() == "Not Found" {
                    println!("No new projects found, exitting.");
                } else {
                    println!("unknown response format (query since {}): {} ", last_id, json);
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
            if records % 1000000 == 0 { print!(".") }
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

