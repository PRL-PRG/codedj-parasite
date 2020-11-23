use std::time::SystemTime;
use std::str;

pub fn pct(value : usize, max : usize) -> String {
    if max == 0 {
        return "??".to_owned();
    } else {
        return format!("{}", value * 100 / max);
    }
}

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

pub fn pretty_time(mut seconds : i64) -> String {
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

pub fn pretty_value(mut value : usize) -> String {
    if value < 1000 {
        return format!("{}", value);
    }
    value = value / 1000;
    if value < 1000 {
        return format!("{}k", value);
    }
    value = value / 1000;
    if value < 1000 {
        return format!("{}m", value);
    }
    value = value / 1000;
    return format!("{}b", value);
}

pub fn pretty_size(mut value : usize) -> String {
    if value < 1000 {
        return format!("{}", value);
    }
    value = value / 1000;
    if value < 1000 {
        return format!("{}kb", value);
    }
    value = value / 1000;
    if value < 1000 {
        return format!("{}mb", value);
    }
    value = value / 1000;
    return format!("{}gb", value);
}

/** Returns the process usage of memory and cpu. 
 
    Just use ps. i.e. ps -x -o pid,%mem,%cpu and then grep for our pid
 */
pub fn process_resources() -> (usize, usize) {
    let output : String = String::from_utf8(
        std::process::Command::new("sh")
            .arg("-c")
            .arg(format!("ps -x -o pid,%mem,%cpu | grep \"^ *{}\"", std::process::id()))
//            .arg("-x")
//            .arg("-o")
//            .arg("pid,%mem,%cpu")
//            .arg("|")
//            .arg("grep")
//            .arg(format!("^ *{}", std::process::id()))
            .output().unwrap().stdout
    ).unwrap();
    //println!("{:?}", output);
    let line : Vec<String> = output.split_whitespace().map( |x|{ x.to_owned()} ).collect();
    //println!("{:?}", line);
    return (
        (line[1].parse::<f64>().unwrap() * 100.0) as usize,
        line[2].parse::<f64>().unwrap() as usize
    );
}