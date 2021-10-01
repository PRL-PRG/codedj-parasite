use log::*;
use std::io;
use std::process::{Command, Stdio};
use std::collections::{HashMap};

use git2;

use parasite::*;
use parasite::records::*;

use crate::updater::*;

/** Single project updater. 
 */
pub struct ProjectUpdater<'a> {

    /** The actual updater that orchestrates the updates and provides access to the CodeDJ superstore and its datastores and the current datastore.
     */
    updater : &'a Updater,

    ds : &'a UpdaterDatastore,

    /** The id and project url of the project we are updating. 
     */
    project_id : ProjectId,
    project : Project, 

    /** Path to which the project should be cloned / may already exist.
     */
    clone_path : String, 
    tmp_path : String,

    /** Heads of the project as of the latest update. 
     */
    latest_heads : TranslatedHeads, // String -> (SHA, Id)

}

impl<'a> ProjectUpdater<'a> {

    /** Performs the update of the associated project. 
     
        This means determining whether the project indeed needs to be updated in the first place, downloading any new content, analyzing the content and adding it to the appropriate datastore. 
     */
    pub fn update(& mut self) -> io::Result<()> {
        // first determine the latest state of the project and figure out if we should proceed with update at all
        match self.ds.get_latest_log(self.project_id) {
            Some(ProjectLog::Error{time : _, msg : _}) => {
                debug!("Skipping project {:?} - last update failed", self.project_id);
                return Ok(());
            }
            _ => {},
        }
        // the project should attempt update, get the latest heads
        if let Some(heads) = self.ds.get_latest_heads(self.project_id) {
            self.latest_heads = heads;
        } 
        // let's see if heads have changed since the last time we checked the project
        let changed_heads = Self::git_to_io_error(self.get_changed_heads())?;
        // if there are any heads that need changing, we must clone or update the repository first and then update the branches
        if ! changed_heads.is_empty() {
            self.clone_or_update()?;
            // TODO should we also check if there is a change in the datastore? 
            for (head, known_sha) in changed_heads.iter() {
                Self::git_to_io_error(self.update_branch(head, *known_sha))?;

            }
        }


        unimplemented!();
    }

    fn git_to_io_error<T>( result : Result<T, git2::Error>) -> Result<T, io::Error> {
        unimplemented!();
    }

    /** Creates a fake repository and obtains the latest heads for the project from the origin, compares this to the heads we already have and returns a list of heads that must be updated. 
     
        Note that it is possible that this function returns an empty list, but the update must still be recorded. This corresponds to a branch deletion in the remote without any new commits made.
     */
    fn get_changed_heads(& mut self) -> Result<HashMap<String, SHA>, git2::Error> {
        let repo = git2::Repository::init_bare(& self.tmp_path)?;
        let mut remote = repo.remote("codedj", & self.project.clone_url())?;
        remote.connect(git2::Direction::Fetch)?;
        // get the remote heads now
        let mut remote_heads = HashMap::<String, SHA>::new();
        for x in remote.list()? {
            // TODO this is an issue in libgit2 it seems that a branch must be valid utf8, otherwise we will fail. For now that seems ok as it affects only a really small amount of projects
            let name = x.name().to_owned();
            if name.starts_with("refs/heads/") {
                remote_heads.insert(name, x.oid());
            }
        }        
        // now figure out any heads that have changed 
        let mut result = HashMap::<String, SHA>::new();
        for x in remote_heads.iter() {
            match self.latest_heads.get(x.0) {
                Some((sha, _)) => {
                    if sha != x.1 {
                        result.insert(x.0.clone(), *sha);
                    }
                }
                _ => { 
                    result.insert(x.0.clone(), git2::Oid::zero());
                },
            }
        }
        return Ok(result);
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

    /** Updates a single branch of the project. 
     
        This is mildly complicated by the fact that we must ensure that all updates to the datastore are done in valid order, namely:

        - all parent commits must be already analyzed and stored in the datastore before analyzing a child commit
        - before writing commit, all its paths, contents and users must be alredy stored in the datastore. 

        To do so, we first go back and create a queue of commits to analyze. Then analyze each commit. 
     */
    fn update_branch(& mut self, branch_name : & str, last_known_commit_hash : SHA) -> Result<(), git2::Error> {
        debug!("Analyzing branch {} for project {}, building branch commit queue", branch_name, self.project.clone_url());
        


        unimplemented!();
    }

    /** Analyzes the given commit (assuming its parents are alredy analyzed and stored)
     */
    fn update_commit(& mut self) -> Result<(), git2::Error> {
        unimplemented!();
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