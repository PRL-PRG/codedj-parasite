use crate::*;

// Projects ---------------------------------------------------------------------------------------

pub(super) enum ProjectLogEntry {
    Init{time : u64, url : String },
    Update{time : u64, source : Source },
    NoChange{time : u64, source : Source },
}

impl ProjectLogEntry {

    pub(super) fn init(url : String) -> ProjectLogEntry {
        return ProjectLogEntry::Init{time : helpers::now(), url};
    }

    pub(super) fn update(source : Source) -> ProjectLogEntry {
        return ProjectLogEntry::Update{time : helpers::now(), source };
    }

    pub(super) fn no_change(source : Source) -> ProjectLogEntry {
        return ProjectLogEntry::NoChange{time : helpers::now(), source };
    }

    fn from_csv(record : csv::StringRecord) -> ProjectLogEntry {
        if record[1] == *"init" {
            return ProjectLogEntry::Init{ time : record[0].parse::<u64>().unwrap(), url : String::from(& record[2]) };
        } else if record[1] == *"update" {
            return ProjectLogEntry::Update{ time : record[0].parse::<u64>().unwrap(), source : Source::from_str(& record[2])};
        } else if record[1] == *"nochange" {
            return ProjectLogEntry::NoChange{ time : record[0].parse::<u64>().unwrap(), source : Source::from_str(& record[2])};
        } else {
            panic!("Invalid log entry");
        }
    }

    fn to_csv(& self, f : & mut File) -> Result<(), std::io::Error> {
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

pub(super) struct ProjectLog {
    entries_ : Vec<ProjectLogEntry>,
}

impl ProjectLog {

    pub(super) fn new() -> ProjectLog {
        return ProjectLog{
            entries_ : Vec::new(),
        }
    }

    pub(super) fn read(project_folder: & str) -> ProjectLog {
        let mut result = ProjectLog::new();
        let mut reader = csv::Reader::from_path(format!("{}/log.csv", project_folder)).unwrap();
        for x in reader.records() {
            if let Ok(record) = x {
                result.entries_.push(ProjectLogEntry::from_csv(record));
            }
        }
        return result;
    }

    pub(super) fn add(& mut self, entry : ProjectLogEntry) {
        self.entries_.push(entry);
    }

    pub(super) fn save(& self, project_folder : & str) {
        let mut f = File::create(format!("{}/log.csv", project_folder)).unwrap();
        writeln!(& mut f, "time,kind,comment").unwrap();
        for x in & self.entries_ {
            x.to_csv(& mut f).unwrap();
        }
    }

    pub(super) fn append(& self, project_folder : & str) {
        let mut f = std::fs::OpenOptions::new().append(true).write(true).open(format!("{}/log.csv", project_folder)).unwrap();
        for x in & self.entries_ {
            x.to_csv(& mut f).unwrap();
        }

    }
}


// Commits ----------------------------------------------------------------------------------------

struct Commit {
    time : u64,
    id : CommitId,
    committer_id : u64, 
    committer_time : u64,
    author_id : u64,
    author_time : u64,
    source : Source,
}
