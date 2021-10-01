use std::time::{SystemTime, Duration, UNIX_EPOCH};

#[allow(dead_code)]
mod serialization;

#[allow(dead_code)]
pub mod stamp;

#[allow(dead_code)]
mod folder_lock;

#[allow(dead_code)]
mod savepoints;

#[allow(dead_code)]
mod table_writers;

#[allow(dead_code)]
mod table_readers;

#[allow(dead_code)]
pub mod records;

#[allow(dead_code)]
pub mod datastore;

#[allow(dead_code)]
pub mod datastore_view;

#[allow(dead_code)]
pub mod codedj;

/** Returns current time in seconds
 */
pub fn now() -> i64 {
    return SystemTime::now().duration_since(UNIX_EPOCH).expect("Invalid time detected").as_secs() as i64;
}

/** Displays a human readable time format 
 
    Not necessarily pretty;-D
 */
pub fn pretty_epoch(epoch : i64) -> String {
    let d = UNIX_EPOCH + Duration::from_secs(epoch as u64);
    let dt : chrono::DateTime<chrono::offset::Utc> = d.into();
    return dt.format("%F %T").to_string();    
}

/** A simple pretty printer for a duration in seconds.
 */
pub fn pretty_duration(mut d : i64) -> String {
    let units = [ ("s", 60), ("m", 60), ("h", 24) , ("d", 7), ("w",4), ("m", i64::MAX) ];
    for (unit, max) in units {
        if d < max {
            return format!("{}[{}]", d, unit);
        }
        d = d / max;
    }
    return "forever".to_owned();
}

pub fn is_file(path : & str) -> bool {
    match std::fs::metadata(path) {
        Err(_) => false,
        Ok(m) => m.is_file()
    }
}

pub fn is_dir(path : & str) -> bool {
    match std::fs::metadata(path) {
        Err(_) => false,
        Ok(m) => m.is_dir()
    }
}
