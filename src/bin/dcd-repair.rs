use dcd::*;
use dcd::db_manager::*;

/** Repairs the project logs when errors were wrongly reported as updates and the intermediate records were stored in the logs as well.
 */

fn main() {
    let db = DatabaseManager::from("/dejavuii/dejacode/peta-tiny");
    for i in 0..db.num_projects() {
        let mut log = record::ProjectLog::new(db.get_project_log_filename(i as ProjectId));
        let mut reader = csv::Reader::from_path(& log.filename_).unwrap();
        let mut has_error = false;
        for x in reader.records() {
            if let Ok(record) = x {
                if record[2] == *"update" && ! record[3].is_empty() {
                    has_error = true;
                    log.entries_.push(record::ProjectLogEntry::Error{
                        time : record[0].parse::<i64>().unwrap(),
                        source : Source::from_str(& record[1]),
                        message : String::from(& record[3]),
                    });
                } else {
                    log.entries_.push(record::ProjectLogEntry::from_csv(record));
                }
            }
        }
        if has_error {
            println!("Project {} contains error, repairing...", i);
            let mut i = 0;
            let mut update_start = 0;
            while i < log.entries_.len() {
                match log.entries_[i] {
                    record::ProjectLogEntry::UpdateStart{time : _, source : _ } => {
                        update_start = i;
                    },
                    record::ProjectLogEntry::Error{ time : _, source : _, message : _} =>{
                        println!("    {} entries removed", i - update_start);
                        log.entries_.drain(update_start..i);
                        i = update_start;
                    },
                    _ => {
                        // don't do anything        
                    }
                }
                i += 1;
            }
            log.create_and_save();
        }
    }
}