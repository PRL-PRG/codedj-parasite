use std::fs::*;
use std::io::*;
use std::collections::hash_map::*;
use byteorder::*;

use crate::db::*;

/** Project heads
 
    For each project we store the latest project heads so that these can be compared against projects already used. 
 */
pub type Heads = HashMap<String, u64>;

impl FileWriter<Heads> for Heads {
    fn read(f : & mut File) -> Heads {
        let mut result = Heads::new();
        let records = f.read_u32::<LittleEndian>().unwrap() as usize;
        while result.len() < records {
            let commit_id = f.read_u64::<LittleEndian>().unwrap();
            let name = String::read(f);
            result.insert(name, commit_id);
        }
        return result;
    }

    fn write(f : & mut File, value : & Heads) {
        f.write_u32::<LittleEndian>(value.len() as u32).unwrap();
        for (name, commit_id) in value {
            u64::write(f, commit_id);
            String::write(f, name);
        }
    }
}

/** Basic commit information. 
 */
pub struct CommitInfo {
    pub committer : u64,
    pub committer_time : i64,
    pub author : u64,
    pub author_time : i64,
    pub parents : Vec<u64>,
    pub changes : HashMap<u64,u64>,
    pub message : String,
}

impl CommitInfo {
    pub fn new() -> CommitInfo {
        return CommitInfo{
            committer : 0,
            committer_time : 0,
            author : 0,
            author_time : 0,
            parents : Vec::new(),
            changes : HashMap::new(),
            message : String::new(),
        };
    }
}

impl FileWriter<CommitInfo> for CommitInfo {
    fn read(f : & mut File) -> CommitInfo {
        let mut result = CommitInfo::new();
        result.committer = f.read_u64::<LittleEndian>().unwrap();
        result.committer_time = f.read_i64::<LittleEndian>().unwrap();
        result.author = f.read_u64::<LittleEndian>().unwrap();
        result.author_time = f.read_i64::<LittleEndian>().unwrap();
        let num_parents = f.read_u32::<LittleEndian>().unwrap() as usize;
        while num_parents > result.parents.len() {
            result.parents.push(f.read_u64::<LittleEndian>().unwrap());
        }
        let num_changes = f.read_u32::<LittleEndian>().unwrap() as usize;
        while num_changes > result.changes.len() {
            let path = f.read_u64::<LittleEndian>().unwrap();
            let hash = f.read_u64::<LittleEndian>().unwrap();
            result.changes.insert(path, hash);
        }
        result.message = String::read(f);
        return result;
    }

    fn write(f : & mut File, value : & CommitInfo) {
        f.write_u64::<LittleEndian>(value.committer).unwrap();
        f.write_i64::<LittleEndian>(value.committer_time).unwrap();
        f.write_u64::<LittleEndian>(value.author).unwrap();
        f.write_i64::<LittleEndian>(value.author_time).unwrap();
        f.write_u32::<LittleEndian>(value.parents.len() as u32).unwrap();
        for id in value.parents.iter() {
            f.write_u64::<LittleEndian>(*id).unwrap();
        }
        f.write_u32::<LittleEndian>(value.changes.len() as u32).unwrap();
        for (path, hash) in value.changes.iter() {
            f.write_u64::<LittleEndian>(*path).unwrap();
            f.write_u64::<LittleEndian>(*hash).unwrap();
        }
        String::write(f, & value.message);
    }
}

/** Data about contents of a file. 
 
    This is just a dumb array of bytes. 
 */
pub type ContentsData = Vec<u8>;

impl FileWriter<ContentsData> for ContentsData {
    fn read(f : & mut File) -> ContentsData {
        let len = f.read_u64::<LittleEndian>().unwrap() as usize;
        let mut result = vec![0; len];
        if f.read(& mut result).unwrap() != len {
            panic!("Corrupted file");
        }
        return result;
    }

    fn write(f : & mut File, value : & ContentsData) {
        f.write_u64::<LittleEndian>(value.len() as u64).unwrap();
        f.write(value).unwrap();
    }
}



/** Update Log Entry. 
 */
pub enum UpdateLog {
    NoChange{time : i64, version : u16},
    Error{time : i64, version : u16, error : String},
    Ok{time : i64, version : u16}
}





/*

use crate::*;

// Projects ---------------------------------------------------------------------------------------
// Projects - Log ---------------------------------------------------------------------------------

pub enum ProjectLogEntry {
    Init{time : i64, source: Source, url : String },
    UpdateStart{time : i64, source : Source},
    Update{time : i64, source : Source },
    Error{time : i64, source : Source, message : String },
    NoChange{time : i64, source : Source },
    Metadata{time : i64, source: Source, key : String, value : String },
    Head{time : i64, source: Source, name: String, hash: git2::Oid }
}

impl ProjectLogEntry {

    pub fn init(source: Source, url : String) -> ProjectLogEntry {
        return ProjectLogEntry::Init{time : helpers::now(), source, url};
    }

    pub fn update_start(source : Source) -> ProjectLogEntry {
        return ProjectLogEntry::UpdateStart{time : helpers::now(), source };
    }

    pub fn update(source : Source) -> ProjectLogEntry {
        return ProjectLogEntry::Update{time : helpers::now(), source };
    }

    pub fn error(source : Source, message : String) -> ProjectLogEntry {
        return ProjectLogEntry::Error{time : helpers::now(), source, message};
    }

    pub fn no_change(source : Source) -> ProjectLogEntry {
        return ProjectLogEntry::NoChange{time : helpers::now(), source };
    }

    pub fn metadata(source: Source, key : String, value : String) -> ProjectLogEntry {
        return ProjectLogEntry::Metadata{time : helpers::now(), source, key, value};
    }

    pub fn head(source : Source, name: String, hash : git2::Oid) -> ProjectLogEntry {
        return ProjectLogEntry::Head{time : helpers::now(), source, name, hash};
    }

    pub fn from_csv(record : csv::StringRecord) -> ProjectLogEntry {
        if record[2] == *"init" {
            return ProjectLogEntry::Init{ 
                time : record[0].parse::<i64>().unwrap(),
                source : Source::from_str(& record[1]),
                url : String::from(& record[4])
            };
        } else if record[2] == *"update" {
            return ProjectLogEntry::Update{
                time : record[0].parse::<i64>().unwrap(),
                source : Source::from_str(& record[1])
            };
        } else if record[2] == *"start" {
            return ProjectLogEntry::UpdateStart{
                time : record[0].parse::<i64>().unwrap(),
                source : Source::from_str(& record[1])
            };
        } else if record[2] == *"error" {
            return ProjectLogEntry::Error{
                time : record[0].parse::<i64>().unwrap(),
                source : Source::from_str(& record[1]),
                message : String::from(& record[3]),
            };
        } else if record[2] == *"nochange" {
            return ProjectLogEntry::NoChange{ 
                time : record[0].parse::<i64>().unwrap(),
                source : Source::from_str(& record[1])
            };
        } else if record[2] == *"meta" {
            return ProjectLogEntry::Metadata{ 
                time : record[0].parse::<i64>().unwrap(),
                source : Source::from_str(& record[1]),
                key : String::from(& record[3]),
                value : String::from(& record[4])
            };
        } else if record[2] == *"head" {
            return ProjectLogEntry::Head{ 
                time : record[0].parse::<i64>().unwrap(),
                source : Source::from_str(& record[1]),
                name : String::from(& record[3]),
                hash : git2::Oid::from_str(& record[4]).unwrap()
            };
        } else {
            panic!("Invalid log entry");
        }
    }

    fn to_csv(& self, f : & mut File) -> Result<(), std::io::Error> {
        match & self {
            ProjectLogEntry::Init{time, source, url} => {
                return writeln!(f, "{},{},init,\"\",\"{}\"", time, source, url);
            },
            ProjectLogEntry::Update{time, source} => {
                return writeln!(f, "{},{},update,\"\",\"\"", time, source);
            },
            ProjectLogEntry::Error{time, source, message} => {
                return writeln!(f, "{},{},error,\"{}\",\"\"", time, source, message);
            },
            ProjectLogEntry::UpdateStart{time, source} => {
                return writeln!(f, "{},{},start,\"\",\"\"", time, source);
            },
            ProjectLogEntry::NoChange{time, source} => {
                return writeln!(f, "{},{},nochange,\"\",\"\"", time, source);
            },
            ProjectLogEntry::Metadata{time, source, key, value} => {
                return writeln!(f, "{},{},meta,\"{}\",\"{}\"", time, source, key, value);
            },
            ProjectLogEntry::Head{time, source, name, hash} => {
                return writeln!(f, "{},{},head,\"{}\",{}", time, source, name, hash);
            },
        }
    }
}

pub struct ProjectLog {
    // file where the log is stored
    pub filename_ : String,
    pub entries_ : Vec<ProjectLogEntry>,
}

impl ProjectLog {

    pub fn new(filename : String) -> ProjectLog {
        return ProjectLog{
            filename_ : filename,
            entries_ : Vec::new(),
        };
    }

    pub fn add(& mut self, entry : ProjectLogEntry) {
        self.entries_.push(entry);
    }

    pub fn clear(& mut self) {
        self.entries_.clear();
    }

    pub fn read_all(& mut self) {
        let mut reader = csv::Reader::from_path(& self.filename_).unwrap();
        for x in reader.records() {
            if let Ok(record) = x {
                self.entries_.push(ProjectLogEntry::from_csv(record));
            }
        }
    }

    pub fn analyze<T>(& self, mut f : T ) where T: FnMut(ProjectLogEntry) -> bool {
        let mut reader = csv::Reader::from_path(& self.filename_).unwrap();
        for x in reader.records() {
            if let Ok(record) = x {
                if ! f(ProjectLogEntry::from_csv(record)) {
                    break;
                }
            }
        }
    }

    fn write_headers(& self, f : & mut File) {
        writeln!(f, "time,source,kind,key,value").unwrap();
    }

    pub fn create_and_save(& self) {
        let mut f = File::create(& self.filename_).unwrap();
        self.write_headers(& mut f);
        for x in & self.entries_ {
            x.to_csv(& mut f).unwrap();
        }
    }

    pub fn append(& self) {
        let mut f = std::fs::OpenOptions::new().append(true).write(true).open(& self.filename_).unwrap();
        for x in & self.entries_ {
            x.to_csv(& mut f).unwrap();
        }

    }
}

// Commits ----------------------------------------------------------------------------------------

pub struct Commit {
    time : i64,
    id : CommitId,
    committer_id : UserId, 
    committer_time : i64,
    author_id : UserId,
    author_time : i64,
    source : Source,
}

impl Commit {
    pub fn new(id : CommitId, committer_id : UserId, committer_time : i64, author_id : UserId, author_time : i64, source : Source) -> Commit {
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
    time : i64, 
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
*/