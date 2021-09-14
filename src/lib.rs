use std::time::{SystemTime};

#[allow(dead_code)]
mod serialization;

#[allow(dead_code)]
mod tables;

#[allow(dead_code)]
mod records;

#[allow(dead_code)]
mod datastore;

/** Returns current time in seconds
 */
pub fn now() -> i64 {
    return SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("Invalid time detected").as_secs() as i64;
}
