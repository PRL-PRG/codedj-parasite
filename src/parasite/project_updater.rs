use log::*;
use std::io;
use std::process::{Command, Stdio};

//use git2;

use parasite::*;
use parasite::records::*;
use parasite::datastore::*;

use crate::updater::*;

/** Single project updater. 
 */
pub struct ProjectUpdater<'a> {

    /** The actual updater that orchestrates the updates and provides access to the CodeDJ superstore and its datastores and the current datastore.
     */
    updater : &'a Updater,

    ds : &'a Datastore,

    /** The id and project url of the project we are updating. 
     */
    project_id : ProjectId,
    project : Project, 

    /** Path to which the project should be cloned / may already exist.
     */
    clone_path : String, 

    /** Heads of the project as of the latest update. 
     */
    latest_heads : Heads,

}

impl<'a> ProjectUpdater<'a> {

    /** Performs the update of the associated project. 
     
        This means determining whether the project indeed needs to be updated in the first place, downloading any new content, analyzing the content and adding it to the appropriate datastore. 
     */
    pub fn update(& mut self) -> io::Result<()> {
        // first determine the latest state of the project

        unimplemented!();
    }

    fn load_latest_state(& mut self) {

    }

    /** Clones or updates the project to its target folder. 

        If the target folder does not exist, performs a full clone. If the target folder exists attempts to merely fetch new information. If this errors, deletes the target folder and attempts a full clone. 
     */
    fn clone_or_update(& mut self) -> io::Result<()> {
        if is_dir(& self.clone_path) {
            let mut cmd = Command::new("git");
            cmd.arg("fetch")
                .arg("origin")
                .current_dir(& self.clone_path);
            // if fetch succeeds, return, otherwis
            match self.execute_process(cmd) {
                Ok(_) => return Ok(()),
                Err(_) => {},
            }
            // delete any remnants of the folder
            std::fs::remove_dir_all(& self.clone_path)?;
        } 
        // full clone
        let mut cmd = Command::new("git");
        cmd.arg("clone")
            .arg(self.project.clone_url())
            .arg("-o")
            .arg(& self.clone_path);
        return self.execute_process(cmd);
    }

    /** Executes the given process. 
     
        Spawns a new child process and waits for it to complete with periodic interrupts to allow the process immediate termination if necessary.
     */
    fn execute_process(& mut self, mut cmd : Command) -> io::Result<()> {
        cmd.stdout(Stdio::null())
           //.stderr(Stdio::null())
           .stdin(Stdio::null());
        let mut child = cmd.spawn()?;
        loop {
            // wait - oh this reads as python really:(
            std::thread::sleep(std::time::Duration::from_secs(1));
            // see if we *should* finish (timeout, or global termination flag in the updater)
            // TODO timeout? 
            if self.updater.should_terminate() {
                info!("Terminating command {:?} - terminate", cmd);
                break;
            }
            // see if we have finished
            match child.try_wait()? {
                Some(code) => {
                    if code.success() {
                        return Ok(());
                    }
                    // not a success exit code, return an error
                    return Err(io::Error::new(io::ErrorKind::Other, format!("Process exit code: {}", code)));
                }
                None => {} ,
            }
        }
        // kill the child if we are terminating
        return child.kill();
    }
}