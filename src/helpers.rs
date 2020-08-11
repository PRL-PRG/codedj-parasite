use std::time::SystemTime;
use chrono::NaiveDateTime;
use std::io::Write;

/** Returns current time in milliseconds.
 */
pub fn now() -> i64 {
    return SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("Invalid time detected").as_secs() as i64;
}

pub fn to_unix_epoch(timestamp : & str) -> i64 {
    return NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S").unwrap().timestamp() as i64;
}

pub fn pretty_time(mut seconds : u64) -> String {
    let d = seconds / (24 * 3600);
    seconds = seconds % (24 * 3600);
    let h = seconds / 3600;
    seconds = seconds % 3600;
    let m = seconds / 60;
    seconds = seconds % 60;
    if d > 0 {
        return format!("{}d {}h {}m {}s", d, h, m, seconds);
    } else if h > 0 {
        return format!("{}h {}m {}s", h, m, seconds);
    } else if m > 0 {
        return format!("{}m {}s", m, seconds);
    } else {
        return format!("{}s", seconds);
    }
}

pub fn progress_line(s : String) {
    print!("{}\x1b[K\r", s);
    std::io::stdout().flush().unwrap();    
}