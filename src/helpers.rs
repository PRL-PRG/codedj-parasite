use std::time::SystemTime;
use chrono::NaiveDateTime;
use std::io::Write;
use std::str;

/** Lossless conversion from possibly non-UTF8 strings to valid UTF8 strings with the non-UTF bytes escaped. 
 
    Because we can, we use the BEL character as escape character because the chances of real text containing it are rather small, yet it is reasonably simple for further processing.   
 */
pub fn to_string(bytes : & [u8]) -> String {
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
                let (ok, bad) = bytes.split_at(e.valid_up_to());
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


/*
let mut output = String::new();

loop {
    match str::from_utf8(bytes) {
        Ok(s) => {
            // The entire rest of the string was valid UTF-8, we are done
            output.push_str(s);
            return output;
        }
        Err(e) => {
            let (good, bad) = bytes.split_at(e.valid_up_to());

            if !good.is_empty() {
                let s = unsafe {
                    // This is safe because we have already validated this
                    // UTF-8 data via the call to `str::from_utf8`; there's
                    // no need to check it a second time
                    str::from_utf8_unchecked(good)
                };
                output.push_str(s);
            }

            if bad.is_empty() {
                //  No more data left
                return output;
            }

            // Do whatever type of recovery you need to here
            output.push_str("<badbyte>");

            // Skip the bad byte and try again
            bytes = &bad[1..];
        }
    }
}


*/

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

pub fn encode_quotes(from : & str) -> String {
    return from.replace("%", "%37").replace("\"", "%34").replace("\\", "%92");
}

pub fn decode_quotes(from : & str) -> String {
    return from.replace("%34", "\"").replace("%92", "\\").replace("%37", "%");
}
