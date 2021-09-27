use std::io;
use std::fs;
use std::fs::{OpenOptions};
use log::*;

use crate::serialization::*;


/** A simple RAII folder lock. 
 
    When instantiated, creates a `.lock` file in the specified folder, marking the folder as locked. The lock file contains the time at which it was locked and the process id that holds the lock. An attempt to lock a folder that already contains the `.lock` file will result in error. 

    The folder lock forms another safety barrier making sure that no two write accesses on datastores are possible even across processes as this would quickly end up in datastore corruption. 
 */
pub struct FolderLock {
    folder : String        
}

impl FolderLock {

    /** Locks the given folder and returns the RAII lock. 
     
        If the folder is already locked, or cannot be locked by current process, returns an error. 
     */
    pub fn lock(folder : String) -> io::Result<FolderLock> {
        debug!("Locking folder {}", folder);
        let result = FolderLock{ folder };
        let lock_file = result.lock_file();
        // if the file already exists, can't lock 
        if let Ok(_) = fs::metadata(& lock_file) {
            let (_, pid) = result.read_lock()?;
            if pid == std::process::id() {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Folder {} already locked by current process", result.folder)));            
            } else {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Folder {} already locked by process {}", result.folder, pid)));            
            }
        }
        // all looks good, try creating the lock
        {
            let mut f = OpenOptions::new().
            write(true).
            create(true).
            truncate(true).
            open(& result.lock_file())?;
            i64::just_write_to(& mut f, & crate::now())?;
            u32::just_write_to(& mut f, & std::process::id())?;
        }
        // verify that it is indeed us who have locked
        result.verify()?;
        return Ok(result);
    }

    pub fn folder(& self) -> & str {
        return & self.folder;
    }

    /** The name of the lock file (.lock in the locked folder) */
    fn lock_file(& self) -> String { format!("{}/.lock", self.folder) }

    fn read_lock(& self) -> io::Result<(i64, u32)> {
        let mut f = OpenOptions::new().
            read(true).
            open(& self.lock_file())?;
        let t = i64::just_read_from(& mut f)?;
        let pid = u32::just_read_from(& mut f)?;
        return Ok((t, pid));
    }

    /** Verifies that it is us who holds the lock and that the lock exists. 
     
        Complains bitterly if this is not the case. 
     */
    fn verify(& self) -> io::Result<()> {
        let (_, pid) = self.read_lock()?;
        if pid != std::process::id() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Folder {} locked by different process, expected {}, found {}", self.folder, std::process::id(), pid)));            
        }
        return Ok(());
    }

}

impl Drop for FolderLock {
    /** Releases the lock when we go out of scope.
     */
    fn drop(& mut self) {
        debug!("Unlocking folder {}", self.folder);
        fs::remove_file(self.lock_file()).unwrap();
    }
}