/** Filter for GHTorrent projects. 

    A simple utility tool that reads the GHTorrent projects and filters & samples from them according to some basic criteria. These are:
    
    - createdAfter
    - createdBefore
    - language
 */
use std::collections::*;
use chrono::*;
use std::fs::*;
use std::io::*;
use rand::*;
use rand::seq::IteratorRandom;

fn parse_time(time : & str) -> Option<i64> {
    if let Ok(x) = NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M:%S") {
        return Some(x.timestamp());
    } else {
        return None;
    }
}

struct Settings {
    ght_root : String,
    output : String,
    created_after : Option<i64>,
    created_before : Option<i64>,
    //commits_min : Option<usize>,
    //commits_max : Option<usize>,
    sample : Option<usize>,
    include_forks : bool,
    languages : HashSet<String>
}

impl Settings {
    pub fn from_command_line() -> Settings {
        let args : Vec<String> = std::env::args().collect();
        let mut result = Settings{
            ght_root : args[1].clone(),
            output : args[2].clone(),
            created_after : None,
            created_before : None,
            //commits_min : None, 
            //commits_max : None,
            sample : None,
            include_forks : false,
            languages : HashSet::new(),
        };
        let mut i = 3;
        while i < args.len() {
            let arg = & args[i];
            match arg.as_str() {
                "--created-after" => {
                    if ! result.created_after.is_none() {
                        panic!("--created-after already specified");
                    }
                    i += 1;
                    result.created_after = Some(parse_time(& args[i]).unwrap()); // force the error
                },
                "--created-before" => {
                    if ! result.created_before.is_none() {
                        panic!("--created-before already specified");
                    }
                    i += 1;
                    result.created_before = Some(parse_time(& args[i]).unwrap()); // force the error
                },
                /*
                "--commits-min" => {
                    if ! result.commits_min.is_none() {
                        panic!("--commits-min already specified");
                    }
                    i += 1;
                    result.commits_min = Some(args[i].parse::<usize>().unwrap());
                },
                "--commits-max" => {
                    if ! result.commits_max.is_none() {
                        panic!("--commits-max already specified");
                    }
                    i += 1;
                    result.commits_max = Some(args[i].parse::<usize>().unwrap());
                },
                */
                "--include-forks" => {
                    result.include_forks = true;
                },
                "--sample" => {
                    if ! result.sample.is_none() {
                        panic!("--sample already specified");
                    }
                    i += 1;
                    result.sample = Some(args[i].parse::<usize>().unwrap());
                },
                _ => {
                    result.languages.insert(arg.clone().to_lowercase());
                }
            }
            i += 1;
        }
        return result;
    }
}


#[derive(Clone)]
struct ProjectInfo {
    id : usize,
    language : String,
    created : i64,
    commits : usize,
    url : String,
    forked : Option<usize>,
}

impl ProjectInfo {
    pub fn from_row( row : & csv::StringRecord) -> ProjectInfo {
        return ProjectInfo{
            id : row[0].parse::<usize>().unwrap(),
            language : row[5].to_string().to_lowercase(),
            created : parse_time(& row[6]).unwrap(),
            commits : 0,
            url : row[1].to_string(),
            forked : row[7].parse::<usize>().ok(),
        };
    }

    pub fn check_created(& self, settings : & Settings) -> bool {
        if let Some(x) = settings.created_after {
            if self.created < x {
                return false;
            }
        }
        if let Some(x) = settings.created_before {
            if self.created > x {
                return false;
            }
        }
        return true;
    }
}

fn filter_projects(settings : & Settings) -> Vec<ProjectInfo> {
    let mut projects = HashMap::<usize, ProjectInfo>::new();

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .double_quote(false)
        .escape(Some(b'\\'))
        .from_path(format!("{}/projects.csv",settings.ght_root)).unwrap();
    let mut total : usize = 0;
    for x in reader.records() {
        total += 1;
        let record = x.unwrap();
        let pinfo = ProjectInfo::from_row(& record);
        if ! settings.languages.is_empty() && ! settings.languages.contains(& pinfo.language) {
            continue;
        }
        if ! settings.include_forks && ! pinfo.forked.is_none() {
            continue;
        }
        if ! pinfo.check_created(settings) {
            continue;
        }
        projects.insert(pinfo.id, pinfo);
    }
    println!("    {} projects total", total);
    println!("    {} projects filtered", projects.len());
    return projects.values().cloned().collect();
}

fn output(projects : Vec<ProjectInfo>, settings : & Settings) {
    let f = File::create(& settings.output).unwrap();
    let mut writer = BufWriter::new(& f);
    writeln!(& mut writer, "id,url,language,created,commits,forked").unwrap();
    // if we are not sampling return all
    if settings.sample.is_none() {
        for p in projects.iter() {
            writeln!(& mut writer, "{},\"{}\",\"{}\",{},{},{}", p.id, p.url, p.language, p.created, p.commits, if let Some(x) = p.forked { x.to_string() } else { "-1".to_owned() }).unwrap();
        }
        println!("    {} results written (all)", projects.len());
    // otherwise do random sample
    } else {
        let mut rng = thread_rng();
        for p in projects.iter().choose_multiple(& mut rng, settings.sample.unwrap()) {
            writeln!(& mut writer, "{},\"{}\",\"{}\",{},{},{}", p.id, p.url, p.language, p.created, p.commits, if let Some(x) = p.forked { x.to_string() } else { "-1".to_owned() }).unwrap();
        }
        println!("    {} results written (sampled)", settings.sample.unwrap());
    }
}

fn main() {
    println!("GHTorrent Filter");
    let settings = Settings::from_command_line();
    let projects = filter_projects(& settings);
    output(projects, & settings);
}