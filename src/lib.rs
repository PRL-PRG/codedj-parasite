use std::time::{SystemTime, Duration, UNIX_EPOCH};
use std::str;

#[allow(dead_code)]
mod serialization;

#[allow(dead_code)]
pub mod stamp;

#[allow(dead_code)]
mod folder_lock;

#[allow(dead_code)]
mod savepoints;

#[allow(dead_code)]
pub mod table_writers;

#[allow(dead_code)]
pub mod table_readers;

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

pub fn encode_to_string(bytes: & [u8]) -> String {
    let mut result = String::new();
    let mut x = bytes;
    loop {
        match str::from_utf8(x) {
            // if successful, replace any bel character with double bel, add to the buffer and exit
            Ok(s) => {
                result.push_str(& s.replace("%", "%%"));
                return result;
            },
            Err(e) => {
                let (ok, bad) = x.split_at(e.valid_up_to());
                if !ok.is_empty() {
                    result.push_str(& str::from_utf8(ok).unwrap().replace("%","%%"));
                }
                // encode the bad character
                result.push_str(& format!("%{:x}", bad[0]));
                // move past the offending character
                x = & bad[1..];
            }
        }
    }
}
