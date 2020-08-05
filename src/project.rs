use std::collections::{HashMap, HashSet, BinaryHeap};
use std::fs::File;
use std::io::prelude::*;
use crate::*;


/** The project record.
 
    Projects can again come from different sources. 
    
 */
#[derive(Clone)]
pub struct Project {
    // id of the project
    pub id : u64,
    // url of the project (latest used)
    pub url : String,
    // time at which the project was updated last (i.e. time for which its data are valid)
    pub last_update: u64,
    // head refs of the project at the last update time
    pub heads : Option<HashMap<String, u64>>,
    // source the project data comes from    
    pub source : Source,
}

impl Project {

    /** Creates new project with given id and url at specified folder.
    
        Simply creates the log file and initializes it with the init message. That's all project initialization is required to do. 
     */     
    pub(crate) fn create_new(id : u64, url : & str, folder : & str) -> Project {
        // create the function
        std::fs::create_dir_all(folder).unwrap();
        // create log, add init & save it
        let mut log = ProjectLog::new();
        log.add(ProjectLogEntry::init(url));
        log.save(folder);
        // return the newly created project information
        return Project{
            id : id, 
            url : String::from(url),
            last_update : 0, 
            heads : None, 
            source : Source::NA,
        }
    }

    /** Creates new project.
     */
    fn new(id : u64, folder : & str) -> Project {
        let mut last_url = String::new();
        let mut last_update: u64 = 0;
        let mut last_source = Source::NA;
        let log = ProjectLog::read(folder);
        for entry in & log.entries_ {
            match entry {
                ProjectLogEntry::Init{time : _, url} => {
                    last_url = String::from(url);
                },
                ProjectLogEntry::Update{time, source} => {
                    last_update = *time;
                    last_source = *source;
                },
                _ => {
                }
            }
        }
        return Project{
            id,
            url : last_url, 
            last_update,
            heads : None,
            source : last_source,
        };
    }
}

// ------------------------------------------------------------------------------------------------
#[derive(Eq)]
struct MetadataValue {
    time : u64,
    value : String
}

// all this to have ordered map of metadata values...

impl Ord for MetadataValue {
    fn cmp(& self, other : & Self) -> std::cmp::Ordering {
        return self.time.cmp(& other.time);
    }
}

impl PartialOrd for MetadataValue {
    fn partial_cmp(& self, other : & Self) -> Option<std::cmp::Ordering> {
        return Some(self.time.cmp(& other.time));
    }
}

impl PartialEq for MetadataValue {
    fn eq(& self, other : & Self) -> bool {
        return self.time == other.time;
    }
}


pub(crate) struct ProjectMetadata {
    // project metadata
    metadata_ : HashMap<String, BinaryHeap<MetadataValue>>,
}

impl ProjectMetadata {

    pub(crate) fn new() -> ProjectMetadata {
        return ProjectMetadata{
            metadata_ : HashMap::new(),
        }
    }

    pub(crate) fn insert(& mut self, key : String, value : String) {
        self.metadata_.entry(key).or_insert(BinaryHeap::new()).push(MetadataValue{ time : helpers::now(), value });
    }

    pub(crate) fn save(& self, project_folder : & str) {
        let mut f = File::create(format!("{}/metadata.csv", project_folder)).unwrap();
        writeln!(& mut f, "key,time,value");
        for x in & self.metadata_ {
            for y in x.1 {
                writeln!(& mut f, "{},{},\"{}\"", & x.0, & y.time, & y.value);
            }
        }
    }

    pub(crate) fn append(& self, project_folder : & str) {
        let mut f = std::fs::OpenOptions::new().append(true).write(true).open(format!("{}/metadata.csv", project_folder)).unwrap();
        for x in & self.metadata_ {
            for y in x.1 {
                writeln!(& mut f, "{},{},\"{}\"", & x.0, & y.time, & y.value);
            }
        }
    }

}

// ------------------------------------------------------------------------------------------------

/** Project Log
 */
struct ProjectLog {
    entries_ : Vec<ProjectLogEntry>,
}

impl ProjectLog {

    fn new() -> ProjectLog {
        return ProjectLog{
            entries_ : Vec::new(),
        }
    }

    fn read(project_folder: & str) -> ProjectLog {
        let mut result = ProjectLog::new();
        let mut reader = csv::Reader::from_path(format!("{}/log.csv", project_folder)).unwrap();
        for x in reader.records() {
            if let Ok(record) = x {
                result.entries_.push(ProjectLogEntry::from_csv(record));
            }
        }
        return result;
    }

    fn add(& mut self, entry : ProjectLogEntry) {
        self.entries_.push(entry);
    }

    fn save(& self, project_folder : & str) {
         let mut f = File::create(format!("{}/log.csv", project_folder)).unwrap();
         writeln!(& mut f, "time,kind,comment");
         for x in & self.entries_ {
             writeln!(& mut f, "{}", x);
         }
    }

    fn append(& self, project_folder : & str) {
        let mut f = std::fs::OpenOptions::new().append(true).write(true).open(format!("{}/log.csv", project_folder)).unwrap();
        for x in & self.entries_ {
            write!(& mut f, "{}\n", x);
        }

    }
}

enum ProjectLogEntry {
    Init{time : u64, url : String },
    Update{time : u64, source : Source },
    NoChange{time : u64, source : Source },
}

impl ProjectLogEntry {

    fn from_csv(record : csv::StringRecord) -> ProjectLogEntry {
        if record[1] == *"init" {
            return ProjectLogEntry::Init{ time : record[0].parse::<u64>().unwrap(), url : String::from(& record[2]) };
        } else if record[1] == *"update" {
            return ProjectLogEntry::Update{ time : record[0].parse::<u64>().unwrap(), source : Source::from_string(& record[2])};
        } else if record[1] == *"nochange" {
            return ProjectLogEntry::NoChange{ time : record[0].parse::<u64>().unwrap(), source : Source::from_string(& record[2])};
        } else {
            panic!("Invalid log entry");
        }
    }

    fn init(url : & str) -> ProjectLogEntry {
        return ProjectLogEntry::Init{time : helpers::now(), url : String::from(url)};
    }

    fn update(source : Source) -> ProjectLogEntry {
        return ProjectLogEntry::Update{time : helpers::now(), source };
    }

    fn no_change(source : Source) -> ProjectLogEntry {
        return ProjectLogEntry::NoChange{time : helpers::now(), source };
    }
}

impl std::fmt::Display for ProjectLogEntry {
    fn fmt(& self, f: & mut std::fmt::Formatter) -> std::fmt::Result {
        match & self {
            ProjectLogEntry::Init{time,url} => {
                return write!(f, "{},init,\"{}\"", time, url);
            },
            ProjectLogEntry::Update{time, source} => {
                 return write!(f, "{},update,{}", time, source);
            },
            ProjectLogEntry::NoChange{time, source} => {
                return write!(f, "{},nochange,{}", time, source);
            }
        }
    }
}
