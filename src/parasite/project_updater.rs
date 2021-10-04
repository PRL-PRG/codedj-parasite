use log::*;
use std::io;
use std::process::{Command, Stdio};
use std::collections::{HashMap, VecDeque, HashSet};

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

    /** Cache for already known information.
     
        This limits the need for interaction with the datastore while creating commit updates. We cache the commits, users and paths we have seen so far. This allows us to terminate brancha analyses sooner and speeds up the analysis of commits as their parents are guaranteed to already exist in the known_commits map when we get to them. 
     */
    known_commits : HashMap<SHA, CommitId>,
    known_users : HashMap<String, UserId>,
    known_paths: HashMap<String, PathId>,

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
        // the project should attempt update, get the latest heads and add them to the known commits
        if let Some(heads) = self.ds.get_latest_heads(self.project_id) {
            self.latest_heads = heads;
            for (name_, (sha, commit_id)) in self.latest_heads.iter() {
                self.known_commits.insert(*sha, *commit_id);
            }
        } 
        // let's see if heads have changed since the last time we checked the project
        let changed_heads = Self::git_to_io_error(self.get_changed_heads())?;
        // if there are any heads that need changing, we must clone or update the repository first and then update the branches
        if ! changed_heads.is_empty() {
            self.clone_or_update()?;
            // TODO should we also check if there is a change in the datastore? 
            for (head, newest_sha) in changed_heads.iter() {
                Self::git_to_io_error(self.update_branch(head, *newest_sha))?;

            }
        }


        unimplemented!();
    }

    fn git_to_io_error<T>( result : Result<T, git2::Error>) -> Result<T, io::Error> {
        unimplemented!();
    }

    /** Creates a fake repository and obtains the latest heads for the project from the origin, compares this to the heads we already have and returns a list of heads that must be updated. 
     
        Note that it is possible that this function returns an empty list, but the update must still be recorded. This corresponds to a branch deletion in the remote without any new commits made.

        For each head to be updated, its newest commit hash is returned.
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
                        result.insert(x.0.clone(), *x.1);
                    }
                }
                _ => { 
                    result.insert(x.0.clone(), *x.1);
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

     */
    fn update_branch(& mut self, branch_name : & str, newest_hash : SHA) -> Result<(), git2::Error> {
        debug!("PID: {:?}, Analyzing branch {}", self.project_id, branch_name);
        // get a handle to the repo
        let repo = git2::Repository::open(& self.clone_path)?;
        // get a list of new commits on the branch, i.e. commits from the current head to any of the already known commits (which are at this point heads of previous update and possibly any commits already analyzed in different branches of this update)
        let new_commits = self.get_new_commits(& repo, newest_hash)?;
        debug!("PID: {:?}, {} possibly new commits found in branch {}", self.project_id, new_commits.len(), branch_name);
        // as an optimization let's first check if any of the commits we already have in the datastore. The idea is this will be very useful when analyzing a cloned/forked repository where we already have the commits from other repo
        {
            let already_known = self.ds.get_known_commits(& new_commits);
            debug!("PID: {:?}, {} already known", self.project_id, already_known.len());
            // and add them to known commits... That's all that needs to be done at this point
            self.known_commits.extend(already_known.into_iter());
        }
        // now analyze the commits 
        for sha in new_commits {
            debug!("PID: {:?}, analyzing commit {}", self.project_id, sha);
            self.update_commit(& repo, sha)?;
        }
        debug!("PID: {:?}, done analyzing branch {}", self.project_id, branch_name);
        return Ok(());
    }


    /** Returns the new commits in topological order. 

        That is we are guaranteed to see a project in the resulting vector only *after* all its parents have been seen as well. 

        NOTE this is perhaps/likely not the fastest algorithm out there, but it's rather simple. For incremental updates this should not matter that much as the graph of commits for which we make topo list will be small.
     */
    fn get_new_commits(& mut self, repo : & git2::Repository, newest_hash : SHA)  -> Result<Vec<SHA>, git2::Error> {
        let mut in_result = HashSet::<SHA>::new();
        let mut result = Vec::<SHA>::new();
        let mut q = vec!((newest_hash, false)); // actually a stack
        while let Some((sha, is_ready)) = q.pop() {
            // if ready, that means we have already analyzed all parents and therefore can add ourselves, if not added before already
            if is_ready {
                if ! in_result.contains(& sha) {
                    in_result.insert(sha);
                    result.push(sha);
                }
            // otherwise we must add the current commit to the queue in the ready state and then add all parents (since it is really a stack, we'll make sure that when we get back to the ready state'd commit, all the parents would have been analyzed properly)
            } else {
                q.push((sha, true));
                let commit = repo.find_commit(sha)?;
                for parent_sha in commit.parent_ids() {
                    q.push((parent_sha, false));
                }
            }
        }
        return Ok(result);
    }

    /** Analyzes the given commit (assuming its parents are alredy analyzed and stored)
     */
    fn update_commit(& mut self, repo: & git2::Repository, hash : SHA) -> Result<(), git2::Error> {
        // don't do anything if we already know the commit (i.e. the commit is already in the datastore from different project/update or it has been already analyzed in this update as part of other branch)
        if self.known_commits.contains_key(& hash) {
            return Ok(());
        }
        // proceed as if the commit is not known, create the commit object, fill in the details, analyze the contents the commit changes and 
        let git_commit = repo.find_commit(hash)?;
        let mut commit = Commit::new(hash);
        let committer = git_commit.committer();
        let author = git_commit.author();
        let (committer_id, author_id) = self.get_or_create_users(& committer, & author);
        commit.committer = committer_id;
        commit.author = author_id;
        commit.committer_time = git_commit.time().seconds();
        commit.author_time = author.when().seconds();
        commit.message = parasite::encode_to_string(git_commit.message_bytes());
        // all parents must already be in the known commits by now so we can unwrap
        commit.parents.extend(git_commit.parent_ids().map(|x| self.known_commits.get(& x).unwrap()));
        // calculate commit changes now
        // TODO
        // and finally submit the finished commit to the datastore
        // TODO
        unimplemented!();
    }

    /** Converts the given git users to user ids. 
     
        Takes two arguments as it it expected to be used on a commit and therefore we need committer and author at the cost of a single lock of the users table. 
     */
    fn get_or_create_users(& mut self, u1 : & git2::Signature, u2 : & git2::Signature) -> (UserId, UserId) {
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
