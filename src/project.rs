use std::collections::{HashMap, HashSet, BinaryHeap};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use crate::*;


/** The project record.
 
    Projects can again come from different sources. 
    
 */
pub struct Project {
    // id of the project
    id : u64,
    // url of the project (latest used)
    url : String,
    // time at which the project was updated last (i.e. time for which its data are valid)
    last_update: u64,
    // head refs of the project at the last update time
    heads : Option<HashMap<String, u64>>,
    // project metadata
    metadata : HashMap<String, String>,
    // source the project data comes from    
    source : Source,
}

impl Project {

    /*
    fn new(id : u64, folder : & str) -> Project {


    }
    */



    


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
            return ProjectLogEntry::Update{ time : record[0].parse::<u64>().unwrap(), source : Source::from_string(& record[1])};
        } else if record[1] == *"nochange" {
            return ProjectLogEntry::NoChange{ time : record[0].parse::<u64>().unwrap(), source : Source::from_string(& record[1])};
        } else {
            panic!("Invalid log entry");
        }
    }

    fn update(source : Source) -> ProjectLogEntry {
        return ProjectLogEntry::Update{time : now(), source };
    }

    fn no_change(source : Source) -> ProjectLogEntry {
        return ProjectLogEntry::NoChange{time : now(), source };
    }
}


