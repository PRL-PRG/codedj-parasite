use std::sync::{Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::{HashMap};

use parasite::codedj::*;
use parasite::records::*;
use parasite::datastore::*;
use parasite::table_writers::*;

/** The incremental updater. 
 
    V3 changes : use local git instance in own process to clone repositories as opposed to libgit so that we can timeout the downloads. 


    Extra Tables

    For the updater to work we need extra tables and indices available for a dataset. These live in a different folder than the dataset itself and provide fast access to the latest information so incremental updates can be performed and deduplication information. 

 
 */
pub struct Updater {

    /** The datastore the updater operates on.
     * 
     */
    cdj : CodeDJ,

    /** Signals the worker threads that the updater is going to die and they should terminate. 
     */
    terminate: AtomicBool, 

}


impl Updater {


    /** Returns truf if the workers should terminate immediately.
     */
    pub fn should_terminate(& self) -> bool { self.terminate.load(Ordering::SeqCst) }

}


/** Translated heads for a project. 
 
    Unline CodeDJ, which is only interested in commit ids for heads, the updater needs both commit ids *and* hashes so that hashes returned by the remote heads that might not have been added to the database yet can be compared. 
 */
pub type TranslatedHeads = HashMap<String, (SHA, CommitId)>;


/** CodeDJ datastore from the updater's point of view. 
 
    The updater's datastore is a wrapper around the basic CodeDJ datastore that provides some extra features such as indexing of project heads and logs and deduplication support so that the latest data acquired can be obtained and identical records found. Other tables are simply exposed directly from the underlying datastore. 

 */
pub struct UpdaterDatastore {
    /** The actual CodeDJ datastore 
     */
    ds : Datastore,

    /** Index files for latest information about project update logs and heads so that we can determine the latest state of a project when performing incremental update. 
     */

    /** Deduplication maps for commits, paths, contents and users. 
     */
    commit_hashes : Mutex<Mapping<Commits>>,
    path_hashes : Mutex<Mapping<Paths>>,
    content_hashes : Mutex<Mapping<Contents>>,
    email_hashes : Mutex<Mapping<Users>>,



}

impl UpdaterDatastore {


    pub fn get_latest_log(& self, id : ProjectId) -> Option<ProjectLog> {
        unimplemented!();
    }

    pub fn update_log(& self, id : ProjectId, log : & ProjectLog) {

    }

    pub fn get_latest_heads(& self, id : ProjectId) -> Option<TranslatedHeads> {
        unimplemented!();
    }

    /** Takes a vector of commit hashes and returns a hashmap of those commits from the vector that are already known to the datastore together with their assigned IDs. 
     */
    pub fn get_known_commits(& self, hashes : & Vec<SHA>) -> HashMap<SHA, CommitId> {
        unimplemented!();
    }

    /*
    pub fn update_heads(& self, id : ProjectId, heads : & Heads) {

    }
    */





}


/** Deduplication mapping. 
 
    The mapping provides a relatively fast two-way mapping between ids and hashes where the mapping from a hash to id isdone via an in-memory hash map, whereas the id to hash mapping is slower by using a file on disk. 
 */
pub struct Mapping<TABLE : TableRecord> {
    map : HashMap<SHA, TABLE::Id>,

}


