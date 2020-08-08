use crate::*;

// Projects ---------------------------------------------------------------------------------------
// Projects - Log ---------------------------------------------------------------------------------

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
                return writeln!(f, "{},init,\"{}\"", time, url);
            },
            ProjectLogEntry::Update{time, source} => {
                 return writeln!(f, "{},update,{}", time, source);
            },
            ProjectLogEntry::NoChange{time, source} => {
                return writeln!(f, "{},nochange,{}", time, source);
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

// Projects - Metadata ----------------------------------------------------------------------------

#[derive(Eq)]
pub struct MetadataValue {
    time : u64,
    value : String,
    source : Source,
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


pub struct ProjectMetadata {
    // project metadata
    metadata_ : HashMap<String, BinaryHeap<MetadataValue>>,
}

impl ProjectMetadata {

    pub fn new() -> ProjectMetadata {
        return ProjectMetadata{
            metadata_ : HashMap::new(),
        }
    }

    pub fn add(& mut self, key : String, value : String, source : Source) {
        self.metadata_.entry(key).or_insert(BinaryHeap::new()).push(MetadataValue{ time : helpers::now(), value, source });
    }

    pub fn save(& self, project_folder : & str) {
        let mut f = File::create(format!("{}/metadata.csv", project_folder)).unwrap();
        writeln!(& mut f, "time,key,value,source").unwrap();
        for x in & self.metadata_ {
            for y in x.1 {
                writeln!(& mut f, "{},{},\"{}\",{}", & y.time, & x.0, & y.value, & y.source).unwrap();
            }
        }
    }

    pub fn append(& self, project_folder : & str) {
        let mut f = std::fs::OpenOptions::new().append(true).write(true).open(format!("{}/metadata.csv", project_folder)).unwrap();
        for x in & self.metadata_ {
            for y in x.1 {
                writeln!(& mut f, "{},{},\"{}\",{}", & y.time, & x.0, & y.value, & y.source).unwrap();
            }
        }
    }

}

// Commits ----------------------------------------------------------------------------------------

pub struct Commit {
    time : u64,
    id : CommitId,
    committer_id : UserId, 
    committer_time : UserId,
    author_id : u64,
    author_time : u64,
    source : Source,
}

impl Commit {
    pub fn new(id : CommitId, committer_id : UserId, committer_time : u64, author_id : UserId, author_time : u64, source : Source) -> Commit {
        return Commit{
            time : helpers::now(),
            id, 
            committer_id, 
            committer_time, 
            author_id, 
            author_time,
            source
        };
    }

    pub fn to_csv(& self, f : & mut File) -> Result<(), std::io::Error> {
        return writeln!(f, "{},{},{},{},{},{},{}", self.time, self.id, self.committer_id, self.committer_time, self.author_id, self.author_time, self.source);
    }


}

// Users ------------------------------------------------------------------------------------------

pub struct User {
    time : u64, 
    id : u64,
    name : String,
    source : Source,
}

impl User {
    pub fn new(id : u64, name : String, source : Source) -> User {
        return User{
            time : helpers::now(),
            id, name, source
        };
    }

    pub fn to_csv(& self, f : & mut File) -> Result<(), std::io::Error> {
        return writeln!(f, "{},{},\"{}\",{}", self.time, self.id, self.name, self.source);
    }
}
