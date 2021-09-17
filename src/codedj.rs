use std::io;
use std::io::{Read, Write};
use std::fs;
use std::fs::{OpenOptions};

extern crate variant_count;
use variant_count::VariantCount;

use byteorder::*;

use crate::folder_lock::*;
use crate::serialization::*;
use crate::datastore::*;
use crate::table_writer::*;
use crate::stamp::*;

/** Kinds of datastores that CodeDJ supports. 
 
    This list can be extended in time, each added language means a new folder in the CodeDJ super store that would contain a datastore for the specific kind of projects. 

    The expectation is that the datastor kinds will correspond to major programming languages used by the repositories contained within, but arguably, this does not have to be the case. 
    
    Another expectation is that a project exists in only one datastore within CodeDJ at a time, but technically, this is not necessary, all that codeDJ really should guarantee is that project ids are unique across all datastores. 
 */
#[derive(VariantCount, Copy, Clone, Debug)]
pub enum DatastoreKind {
    C,
    Cpp,
    CSharp,
    Clojure,
    CoffeeScript,
    Erlang,
    Go,
    Haskell,
    Html,
    Java,
    Julia,
    JavaScript,
    ObjectiveC,
    Perl,
    Php,
    Python,
    R,
    Ruby,
    Scala,
    Shell,
    TypeScript,    
}

/** Log enties in the CodeDJ superstore. 
 
    Each modifying access to any of the datastores is logged at the superstore level - i.e. any parasite's invocation that may alter the datastore is logged. Each such task consists of two entries - the opening entry which logs the time, version of parasite used to access the datastore and the full command line arguments. This must be matched by a closing entry which simply states time and closes. 
 */
pub enum Log {
    /** The start of a command. 
     
        Contains the time the command started, version of parasite's executable (the commit hash and whether it is dirty or not) used to execute the command and the command itself (i.e. complete commandline).
     */
    CommandStart{
        time : i64,
        version : String,
        cmd : String,
    },
    /** The end of a command. 
     
        Contains simply the time the last command finished. 
     */
    CommandEnd{
        time : i64,
    }
}

/** CodeDJ manages the set of language-specific datastores and their bookkeeping as well as bookkeeping from other actors, such as the downloader. 
 
    
 */
struct CodeDJ {

    /** The root folder of the CodeDJ superstore. 
     
        Contains the bookkeeping information relevant to the entire superstore and the datastores for relevant languages as subfolders. 
     */
    folder_lock : FolderLock, 

    /** Command logs */
    log : TableWriter<CodeDJLog>,

}

impl CodeDJ {

    /** Opens an existing CodeDJ superstore, throwing an error if one cannot be opened. 
     */
    pub fn open(folder : String) -> io::Result<CodeDJ> {
        let folder_lock = FolderLock::lock(folder)?;



        return Ok(CodeDJ {
            log : TableWriter::open_or_create(folder_lock.folder()),

            folder_lock,

        });
    }

    /** Creates a new CodeDJ superstore at given folder. 
     
        If the folder is not empty, or a superstore cannot be created there, returns an error. 
     */
    pub fn create(folder : String) -> io::Result<CodeDJ> {
        let folder_lock = FolderLock::lock(folder)?;
        return Ok(CodeDJ {
            log : TableWriter::open_or_create(folder_lock.folder()),

            folder_lock,
        });
    }

    /** Adds a log entry for a new command. 

     */
    pub fn start_command(& mut self) -> io::Result<()> {
        let time = crate::now();
        let version = GIT_VERSION.to_owned();
        let cmd = std::env::args().collect::<Vec<String>>().join(" ");
        let cmd_file = self.current_command_file();
        if crate::is_file(& cmd_file) {
            // TODO error, command already running
        }
        let mut f = OpenOptions::new().
            write(true).
            create(true).
            open(cmd_file)?;
        let offset = self.log.append(FakeId::ID, & Log::CommandStart{time, version, cmd});
        self.log.flush()?;
        u64::just_write_to(& mut f, & offset)?;
        return Ok(());
    }

    pub fn end_command(& mut self) {
        unimplemented!();

    }

    /** Creates the corresponding datastore and returns it. 
     */
    pub fn get_datastore(& mut self, kind : DatastoreKind) -> io::Result<Datastore> {
        let datastore_path = format!("{}/{}", self.folder_lock.folder(), kind.to_string());
        return Datastore::open_or_create(datastore_path);
    }

    fn current_command_file(& self) -> String { format!("{}/current-command", self.folder_lock.folder()) }



}


struct CodeDJLog { } impl TableRecord for CodeDJLog {
    type Id = FakeId;
    type Value = Log;
    const TABLE_NAME : &'static str = "log";
}

impl DatastoreKind {

    pub fn to_string(&self) -> String { format!("{:?}", self) }

}




impl Log {
    const COMMAND_START : u8 = 0;
    const COMMAND_END : u8 = 1;

}

impl Serializable for Log {
    type Item = Log;


    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Log> {
        let kind = f.read_u8()?; 
        *offset += 1;
        match kind {
            Self::COMMAND_START => {
                let time = i64::read_from(f, offset)?;
                let version = String::read_from(f, offset)?;
                let cmd = String::read_from(f, offset)?;
                return Ok(Log::CommandStart{time, version, cmd});
            },
            Self::COMMAND_END => {
                let time = i64::read_from(f, offset)?;
                return Ok(Log::CommandEnd{time});
            },
            _ => { panic!("Invalid log kind: {}", kind); }
        }
    }

    fn write_to(f : & mut dyn Write, item : & Log, offset : & mut u64) -> io::Result<()> {
        match item {
            Self::CommandStart{time, version, cmd} => {
                f.write_u8(Self::COMMAND_START)?;
                *offset += 1;
                i64::write_to(f, time, offset)?;
                String::write_to(f, version, offset)?;
                String::write_to(f, cmd, offset)?;
            },
            Self::CommandEnd{time} => {
                f.write_u8(Self::COMMAND_END)?;
                *offset += 1;
                i64::write_to(f, time, offset)?;
            }
        }
        return Ok(());
    }

}