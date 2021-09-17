use std::io;

extern crate variant_count;
use variant_count::VariantCount;



use crate::folder_lock::*;
use crate::datastore::*;

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

/** CodeDJ manages the set of language-specific datastores and their bookkeeping as well as bookkeeping from other actors, such as the downloader. 
 
    
 */
struct CodeDJ {

    /** The root folder of the CodeDJ superstore. 
     
        Contains the bookkeeping information relevant to the entire superstore and the datastores for relevant languages as subfolders. 
     */
    folder_lock : FolderLock, 

}

impl CodeDJ {

    /** Opens an existing CodeDJ superstore, throwing an error if one cannot be opened. 
     */
    pub fn open(folder : String) -> io::Result<CodeDJ> {
        let folder_lock = FolderLock::lock(folder)?;



        return Ok(CodeDJ {

            folder_lock,
        });
    }

    /** Creates a new CodeDJ superstore at given folder. 
     
        If the folder is not empty, or a superstore cannot be created there, returns an error. 
     */
    pub fn create(folder : String) -> io::Result<CodeDJ> {
        let folder_lock = FolderLock::lock(folder)?;
        return Ok(CodeDJ {

            folder_lock,
        });
    }

    /** Creates the corresponding datastore and returns it. 
     */
    pub fn get_datastore(& mut self, kind : DatastoreKind) -> io::Result<Datastore> {
        let datastore_path = format!("{}/{}", self.folder_lock.folder(), kind.to_string());
        return Datastore::open_or_create(datastore_path);
    }

}



impl DatastoreKind {

    pub fn to_string(&self) -> String { format!("{:?}", self) }

}