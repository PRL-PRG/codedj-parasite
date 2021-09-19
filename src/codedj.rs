use std::io;
use std::io::{Seek, SeekFrom, Read, Write};
use std::fs;
use std::fs::{OpenOptions};
use log::*;


use strum::{IntoEnumIterator};
use strum_macros::{EnumIter};

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
#[derive(EnumIter, Copy, Clone, Debug)]
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
#[derive(Debug)]
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
pub struct CodeDJ {

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
        // open the datastore at given addess
        let mut result = Self::new(folder)?;
        // verify own tables
        result.log.verify()?;
        // check that there is no current command
        if let Some(current_command) = result.current_command()? {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Cannot create CodeDJ store in {} as there is unclosed command {:?}", result.folder(), current_command)));
        }
        // try opening all the datastores to check their validity
        for ds_kind in DatastoreKind::iter() {
            result.get_datastore(ds_kind)?;
        }
        return Ok(result);
    }

    /** Creates a new CodeDJ superstore at given folder. 
     
        If the folder already exists, or a superstore cannot be created there, returns an error. 
     */
    pub fn create(folder : String) -> io::Result<CodeDJ> {
        if crate::is_dir(& folder) {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Cannot create CodeDJ store in {}, already exists. Delete the folder first.", folder)));
        }
        // create the folder and lock it, within the lock create all the constituent tables and substores
        {
            info!("Creating CodeDJ superstore in {}", folder);
            fs::create_dir_all(& folder)?;
            let mut codedj = Self::new(folder.clone())?;
            for ds_kind in DatastoreKind::iter() {
                info!("Creating datastore for {:?}", ds_kind);
                codedj.get_datastore(ds_kind)?;
            }
        }
        // when done, perform normal store open, which also verifies the newly created datastore to some extent
        return Self::open(folder);
    }

    /** Force creates new CodeDJ superstore at given folder.  */
    pub fn force_create(folder : String) -> io::Result<CodeDJ> {
        if crate::is_dir(& folder) {
            info!("Deleting contents of folder {} to create CodeDJ superstore", folder);
            fs::remove_dir_all(& folder)?;
        }
        return Self::create(folder);
    }

    /** Just returns the folder of the superstore. 
     */
    pub fn folder(& self) -> & str { self.folder_lock.folder() }

    /** Adds a log entry for a new command. 
     
        This creates a command start entry in the log and also stores the offset of the opened command in the `.current-command` file so that we can both read command that is currently running via the offset *and* determine that a command is already running, or was not properly closed when the current command file exists. 
     */
    pub fn start_command(& mut self) -> io::Result<()> {
        let time = crate::now();
        let version = GIT_VERSION.to_owned();
        let cmd = std::env::args().collect::<Vec<String>>().join(" ");
        let cmd_file = self.current_command_file();
        if crate::is_file(& cmd_file) {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Command already running")));
        }
        let mut f = OpenOptions::new().
            write(true).
            create(true).
            open(cmd_file)?;
        info!("CodeDJ command start: {}", cmd);
        let offset = self.log.append(FakeId::ID, & Log::CommandStart{time, version, cmd});
        self.log.flush()?;
        u64::just_write_to(& mut f, & offset)?;
        return Ok(());
    }

    /** Ends the previously started command.
     
        Throws an error if there is no currently opened command. 
     */
    pub fn end_command(& mut self) -> io::Result<()> {
        match self.current_command() {
            Err(x) => return Err(x),
            Ok(None) => {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("No currently running command to end")));
            },
            Ok(Some(_)) => {
                let time = crate::now();
                self.log.append(FakeId::ID, & Log::CommandEnd{time});
                // remove the current command offset
                fs::remove_file(self.current_command_file())?;
                return Ok(());
            }
        }
    }

    /** Returns the currently active command if any. 
     */
    pub fn current_command(& self) -> io::Result<Option<Log>> {
        let cmd_file = self.current_command_file();
        if crate::is_file(& cmd_file) {
            let mut f = OpenOptions::new().
                read(true).
                open(cmd_file)?;
            let offset = u64::just_read_from(& mut f)?;
            let mut log = OpenOptions::new().
                read(true).
                open(record_table_path::<CodeDJLog>(self.folder_lock.folder()))?;
            log.seek(SeekFrom::Start(offset))?;
            FakeId::just_read_from(& mut log)?;
            return Ok(Some(Log::just_read_from(& mut log)?));
        } else {            
        return Ok(None);
        }
    }

    /** Returns the iterator to the commands log associated with the CodeDJ superstore. 
     
        We use mutable self to almost make sure that no-one else is playing with the superstore while we get the iterator as iterators not bounded by savepoints are generally unsafe. 
     */
    pub fn command_log(& mut self) -> impl Iterator<Item = Log> {
        return TableIterator::<CodeDJLog>::for_all(self.folder_lock.folder()).map(|(_, entry)| entry);
    }

    /** Creates the corresponding datastore and returns it. 
     */
    pub fn get_datastore(& mut self, kind : DatastoreKind) -> io::Result<Datastore> {
        return Datastore::open_or_create(self.datastore_folder(kind));
    }

    /** Simply creates the CodeDJ superstore without any checks. 
     */
    fn new(folder : String) -> io::Result<CodeDJ> {
        let folder_lock = FolderLock::lock(folder)?;
        return Ok(CodeDJ {
            log : TableWriter::open_or_create(folder_lock.folder()),
            folder_lock,
        });
    }

    /** Shortcut to get the file we use to store the current command offset. 
     */
    fn current_command_file(& self) -> String { format!("{}/.current-command", self.folder_lock.folder()) }

    /** Determines folder for a datastore.
     */
    fn datastore_folder(& self, kind : DatastoreKind) -> String { format!("{}/{:?}", self.folder_lock.folder(), kind) }

}

/** Record for the CodeDJ log that contains all the commands ever executed on the store. 
 */
struct CodeDJLog { } impl TableRecord for CodeDJLog {
    type Id = FakeId;
    type Value = Log;
    const TABLE_NAME : &'static str = "log";
}

impl DatastoreKind {

    pub fn to_string(&self) -> String { format!("{:?}", self) }

}




impl Log {
    // enum value tags for serialization
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