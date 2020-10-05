use std::time::SystemTime;
use std::str;

/** Returns current time in milliseconds.
 */
pub fn now() -> i64 {
    return SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("Invalid time detected").as_secs() as i64;
}

/** Lossless conversion from possibly non-UTF8 strings to valid UTF8 strings with the non-UTF bytes escaped. 
 
    Because we can, we use the BEL character as escape character because the chances of real text containing it are rather small, yet it is reasonably simple for further processing.   
 */

#[allow(dead_code)]
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
