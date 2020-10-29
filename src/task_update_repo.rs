use std::collections::*;

use crate::datastore::*;
use crate::updater::*;
use crate::records::*;
use crate::helpers;


/** Provides a full update of the given repository. 
 
    - figure out the latest update, i.e. if there is one, if it was error (in which case ignore)
    - then we have to decide whether we have the appropriate substore in memory
    - clone the project
    - in theory change the substore and store the information
    - see if we have the substore in memory
    - update the project
    
 */
pub (crate) fn task_update_repo(updater : & Updater, task_name : & str, task : Task, tx : & Tx) -> Result<(), std::io::Error> {
    let mut ru = RepoUpdater::new(updater, task_name, task, tx);


    return Ok(());
}

/** A convenience struct because I do not want to drag everything as function arguments.
 */
struct RepoUpdater<'a> {
    updater : &'a Updater,
    ds : &'a Datastore,
    task_name : &'a str, 
    tx : &'a Tx,
    project_id : u64,
}

impl<'a> RepoUpdater<'a> {

    /** Creates new repository updater. 
     */
    fn new(updater : &'a Updater, task_name : &'a str, task : Task, tx : &'a Tx) -> RepoUpdater<'a> {
        if let Task::UpdateRepo{last_update_time, id : project_id, version : latest_version } = task {
            return RepoUpdater {
                updater,
                ds : & updater.ds,
                task_name,
                tx,
                project_id,
            };
        } else {
            panic!("Wrong task");
        }
    }

    /*
    fn fill_project_info(& self) -> Result<(), std::io::Error> {
        let project = self.ds.get_project(self.project_id);
        if ()
    }
    */

}