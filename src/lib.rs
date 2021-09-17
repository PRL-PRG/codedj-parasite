use std::time::{SystemTime};

#[allow(dead_code)]
mod serialization;

#[allow(dead_code)]
mod folder_lock;

#[allow(dead_code)]
mod savepoints;

#[allow(dead_code)]
mod table_writer;

#[allow(dead_code)]
mod table_readers;

#[allow(dead_code)]
mod records;

#[allow(dead_code)]
mod datastore;

#[allow(dead_code)]
mod datastore_view;

#[allow(dead_code)]
mod codedj;

/** Returns current time in seconds
 */
pub fn now() -> i64 {
    return SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("Invalid time detected").as_secs() as i64;
}
