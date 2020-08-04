use std::collections::{HashMap, HashSet, BinaryHeap};
use std::sync::{Mutex, Condvar};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use curl::easy::Easy;

/** Returns current time in milliseconds.
 */
fn now() -> u64 {
    use std::time::SystemTime;
    return SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("Invalid time detected").as_secs();
}

fn prettyTime(mut seconds : u64) -> String {
    let d = seconds / (24 * 3600);
    seconds = seconds % (24 * 3600);
    let h = seconds / 3600;
    seconds = seconds % 3600;
    let m = seconds / 60;
    seconds = seconds % 60;
    if d > 0 {
        return format!("{}d {}h {}m {}s", d, h, m, seconds);
    } else if h > 0 {
        return format!("{}h {}m {}s", h, m, seconds);
    } else if m > 0 {
        return format!("{}m {}s", m, seconds);
    } else {
        return format!("{}s", seconds);
    }
}


enum LogEntry {
    Initialize{time : u64, args: String},
    UpdateStart{time : u64, project_id: u64},
    UpdateEnd{time : u64, project_id: u64},
    Done{time: u64},
    Error{time: u64, project_id: u64, err: String},
}

impl LogEntry {
    fn initialize(args: &str) -> LogEntry {
        LogEntry::Initialize{time : now(), args : String::from(args) }
    }

    fn update_start(project_id : u64) -> LogEntry {
        LogEntry::UpdateStart{time : now(), project_id}
    }

    fn update_end(project_id : u64) -> LogEntry {
        LogEntry::UpdateEnd{time : now(), project_id}
    }

    fn done() -> LogEntry {
        LogEntry::Done{time: now()}
    }

    fn error(project_id : u64, err: &str) -> LogEntry {
        LogEntry::Error{time : now(), project_id, err : String::from(err) }
    }
}

impl std::fmt::Display for LogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            LogEntry::Initialize{time, args} => 
                return write!(f, "{},0,0,\"{}\"", time, args),
            LogEntry::UpdateStart{time, project_id} =>
                return write!(f, "{},1,{},\"\"", time, project_id),
            LogEntry::UpdateEnd{time, project_id} =>
                return write!(f, "{},2,{},\"\"", time, project_id),
            LogEntry::Error{time, project_id, err} =>
                return write!(f, "{},3,{},\"{}\"", time, project_id, err),
            LogEntry::Done{time} =>
                return write!(f, "{},4,0,\"\"", time)
        }
    }
}



struct GitHub {


} 

impl GitHub {
    /** 
     */ 
    fn get_project_metadata(& self, url : & str) {

    }

}







/** Per project log entry. 
    
    Each project contains a log of its own activity. 
 */
enum ProjectLogEntry {
    Init{time : u64, url : String},
    Update{time : u64},
    NoChange{time : u64},
}

impl ProjectLogEntry {
    fn from_csv(record : csv::StringRecord) -> ProjectLogEntry {
        if record[1] == *"init" {
            return ProjectLogEntry::Init{ time : record[0].parse::<u64>().unwrap(), url : String::from(& record[2]) };
        } else if record[1] == *"update" {
            return ProjectLogEntry::Update{ time : record[0].parse::<u64>().unwrap()};
        } else if record[1] == *"nochange" {
            return ProjectLogEntry::NoChange{ time : record[0].parse::<u64>().unwrap()};
        } else {
            panic!("Invalid log entry");
        }
    }

    fn update() -> ProjectLogEntry {
        return ProjectLogEntry::Update{time : now()};
    }

    fn no_change() -> ProjectLogEntry {
        return ProjectLogEntry::NoChange{time : now()};
    }
}

impl std::fmt::Display for ProjectLogEntry {
    fn fmt(& self, f: & mut std::fmt::Formatter) -> std::fmt::Result {
        match & self {
            ProjectLogEntry::Init{time,url} => {
                return write!(f, "{},init,\"{}\"", time, url);
            },
            ProjectLogEntry::Update{time} => {
                return write!(f, "{},update,\"\"", time);
            },
            ProjectLogEntry::NoChange{time} => {
                return write!(f, "{},nochange,\"\"", time);
            }
        }
    }
}

/** This is the basic project info. 
 
 */
struct Project<'ghm> {
    ghm: & 'ghm Ghm,
    // id of the project
    id : u64,
    // latest url of the project
    url: String,
    // root folder where the project's data should be stored
    root: String,

    // list of heads we have
    heads: std::collections::HashMap<String, git2::Oid>,

    log: Vec<ProjectLogEntry>,

}

impl<'ghm> Project<'ghm> {

    fn new(id : u64, ghm : & 'ghm Ghm) -> Project<'ghm> {
        let root = String::from(format!("{}/projects/{}/{}", ghm.root, id % 1000, id));
        let mut p = Project{
            ghm : ghm,
            id : id,
            url: String::new(),
            root : root,
            heads: std::collections::HashMap::new(),
            log : Vec::new(),
        };
        return p;
    }

    /** Creates new project record that can later be updated. 
     
        Creates the project data directory and initializes the log so that the project url can be loaded later. 
     */
    fn create(id : u64, url : & str, ghm : & Ghm) {
        let root = String::from(format!("{}/projects/{}/{}", ghm.root, id % 1000, id));
        std::fs::create_dir_all(& root);
        let mut f = File::create(format!("{}/log.csv", & root)).unwrap();
        writeln!(& mut f, "time,action,comment");
        writeln!(& mut f, "{},init,\"{}\"", now(), url);
    }

    /** Reads the log of the project to determine the url and other details. 
     
     */
    fn read_log(& mut self) {
        let mut reader = csv::Reader::from_path(format!("{}/log.csv", & self.root)).unwrap();
        for x in reader.records() {
            if let Ok(record) = x {
                match ProjectLogEntry::from_csv(record) {
                    ProjectLogEntry::Init{time, url} => {
                        self.url = url;
                    },
                    // TODO add other log states
                    _ => { },
                }
            }
        }
    }

    /** Saves the pending log messages and clears them. 
     */
    fn commit_log(& mut self) {
        let mut f = std::fs::OpenOptions::new().append(true).write(true).open(format!("{}/log.csv", & self.root)).unwrap();
        for x in & self.log {
            write!(& mut f, "{}\n", x);
        }
        self.log.clear();
    }

    /** Analyzes the log and determines the live and dead urls of the project, as well as the last update time of the project.   
     */
    fn get_urls_and_update_time(& self) -> (String, Vec<String>, u64) {
        let mut reader = csv::Reader::from_path(format!("{}/log.csv", & self.root)).unwrap();
        let mut current_url = String::new();
        let mut last_updated = 0;
        let mut dead_urls = Vec::<String>::new();
        for x in reader.records() {
            if let Ok(record) = x {
                match ProjectLogEntry::from_csv(record) {
                    ProjectLogEntry::Init{time, url} => {
                        current_url = url;
                    },
                    // TODO add other log states
                    _ => { },
                }
            }
        }
        return (current_url, dead_urls, last_updated);
    }    

    /** Updates the metadata of the project, if any. 
     
        For github, this means loading the github metadata and updating the url and other things. 
     */
    fn update_metadata(&mut self) {

    }

    /** Updates the repositorty contents details if necessary. 
     
     */
    fn update_contents(&mut self, local_path: &str) -> Result<bool, git2::Error> {
        std::fs::create_dir_all(local_path).unwrap();
        let mut repo = git2::Repository::init_bare(local_path)?;
        let new_heads = self.fetch_heads(&mut repo)?;
        if !new_heads.is_empty() {
            self.ghm.set_task(self.id, "calculating new commits");
            let mut commit_hashes = self.get_new_commits(& repo, & new_heads);
            /*
            let mut commit_hashes = std::collections::HashSet::new();
            // now process the updated heads and load all commits that can possibly be updated
            for x in new_heads.iter() {
                let hash = * self.heads.get(x).unwrap();
                self.add_parent_commits(& repo, & repo.find_commit(hash)?, & mut commit_hashes)?;
            }
            */
            // time to update the stored commits, i.e. get their hashes from the global map
            let (commits, new_commits) = self.ghm.create_new_hash_ids(& commit_hashes);
            // and analyze each new commit
            self.ghm.set_task(self.id, "analyzing new commits");
            self.ghm.set_progress(self.id, 0, new_commits.len() as u64);
            for hash in & new_commits {
                self.update_commit(*hash, &repo, &commits)?;
                self.ghm.add_progress(self.id, 1);
            }
            self.ghm.set_task(self.id, "finalizing");
            // add the commit hashes
            self.ghm.append_hashes(& commits, & new_commits);
            // update the heads of the project
            self.store_heads();
            // update the log of the project
            self.log.push(ProjectLogEntry::update());
            return Ok(true);
        } else {
            self.log.push(ProjectLogEntry::no_change());
            return Ok(false);
        }
    }

    /** Loads the last analyzed heads. 
     */ 
    fn load_heads(& mut self) {
        let filename = format!("{}/heads.csv", self.root);
        if std::path::Path::new(& filename).exists() {
            let mut reader = csv::Reader::from_path(& filename).unwrap();
            for x in reader.records() {
                if let Ok(record) = x {
                    if record.len() == 2 {
                        let refName = String::from(& record[0]);
                        let hash = git2::Oid::from_str(& record[1]).unwrap();
                        self.heads.insert(refName, hash);    
                    }
                }
            }
        }
    }

    /** Updates the local information about latest commits in branches and fetches the required data. 
     
        Returns the list of branches that have changed their heads and should be updated. 
     */
    fn fetch_heads(&mut self, repo: &mut git2::Repository) -> Result<Vec<String>, git2::Error> {
        self.ghm.set_task(self.id, "fetching heads");
        let mut remote = repo.remote("ghm", &self.url)?;
        remote.connect(git2::Direction::Fetch)?;
        let result : Vec<String> = self.update_heads(&mut remote)?;
        if !result.is_empty() {
            self.ghm.set_task(self.id, "fetching contents");
            let mut callbacks = git2::RemoteCallbacks::new();
            // TODO the callback should come from someone else and actually report to a different thread, for which I need to obviously understand how threads work properly
            callbacks.transfer_progress(|progress : git2::Progress| -> bool {
                self.ghm.set_progress(self.id,
                    (progress.received_objects() + progress.indexed_deltas()) as u64,
                    (progress.total_objects() + progress.total_deltas()) as u64 
                );
                return true;
            });
            let mut opts = git2::FetchOptions::new();
            opts.remote_callbacks(callbacks); 
            remote.fetch(&result, Some(&mut opts), None)?;
        }
        return Ok(result);
    }

    /** Updates the local information about latest commits in active branches and returns the list of those that changed. 
     */
    fn update_heads(&mut self, remote: & mut git2::Remote) -> Result<Vec<String>, git2::Error> {
        let mut result : Vec<String> = Vec::new();
        for x in remote.list()? {
            if x.name().starts_with("refs/heads/") {
                let i = self.heads.get(x.name());
                match i {
                    Some(h) if x.oid() == *h => {} ,
                    _ => {
                        self.heads.insert(String::from(x.name()), x.oid());
                        result.push(String::from(x.name()));
                    } 
                }
            }
        } 
        return Ok(result);
    }

    fn store_heads(& mut self) {
        let mut f = File::create(format!("{}/heads.csv", self.root)).unwrap();
        writeln!(& mut f, "ref,hash");
        for head in & self.heads {
            writeln!(& mut f, "\"{}\",{}", head.0, head.1);
        }
    }


    fn get_new_commits(& self, repo : & git2::Repository, new_heads : & Vec<String>) -> HashSet<git2::Oid> {
        let mut q = Vec::<git2::Oid>::new();
        let mut result = HashSet::new();
        // add new heads
        for head in new_heads {
            let hash = * self.heads.get(head).unwrap();
            q.push(hash);
        }
        // process the commit hashes in the queue
        while ! q.is_empty() {
            let hash = q.pop().unwrap();
            // if the commit has already been addressed, ignore it
            if result.contains(& hash) {
                continue;
            }
            // otherwise get the commit in the result and add its parents to the queue
            result.insert(hash);
            let commit = repo.find_commit(hash).unwrap();
            for parent in commit.parents() {
                q.push(parent.id());
            }
        }
        return result;
    }

        /*
    fn add_parent_commits(& self, repo : & git2::Repository, commit : & git2::Commit<'_>, hashes : & mut std::collections::HashSet<git2::Oid>) -> Result<(), git2::Error> {
        if ! hashes.contains(& commit.id()) {
            hashes.insert(commit.id());
            for parent in commit.parents() {
                match self.add_parent_commits(repo, & parent, hashes) {
                    Err(x) => { return Err(x) },
                    _ => {},
                }
            } 
        }
        return Ok(());
    }
    */

    fn update_commit(& self, hash : git2::Oid, repo : & git2::Repository, hash_ids : & HashMap<git2::Oid, u64>) -> Result<(), git2::Error> {
        let commit = repo.find_commit(hash)?;
        let id = hash_ids.get(& hash).unwrap();
        let root = String::from(format!("{}/commits/{}/{}", self.ghm.root, id % 1000, id));
        std::fs::create_dir_all(& root);
        // store the parents
        self.store_commit_parents(& root, & commit, hash_ids).unwrap();
        // store the diff
        self.store_commit_diff(& repo, &root, & commit).unwrap();
        // the message
        {
            let mut f = File::create(format!("{}/message", & root)).unwrap();
            f.write_all(commit.message_raw_bytes());
        }
        // store the author & committer and their times 
        {
            let mut f = File::create(format!("{}/times.csv", & root)).unwrap();
            writeln!(& mut f, "kind,name,email,time");
            let committer = commit.committer();
            writeln!(& mut f, "c,\"{}\",\"{}\",{}", committer.name().unwrap(), committer.email().unwrap(), commit.time().seconds());
            let author = commit.author();
            // TODO is the author's when really author time? 
            writeln!(& mut f, "a,\"{}\",\"{}\",{}", author.name().unwrap(), author.email().unwrap(), author.when().seconds());
        }
        return Ok(());
    }

    /** Stores the commit parents information */    
    fn store_commit_parents(& self, root : & str, commit : & git2::Commit, hash_ids : & HashMap<git2::Oid, u64>) -> std::io::Result<()> {
        let mut f = File::create(format!("{}/parents.csv", root)).unwrap();
        writeln!(& mut f, "parentId")?;
        for parent in commit.parents() {
            let id = hash_ids.get(& parent.id()).unwrap();
            writeln!(& mut f, "{}", id)?;
        }
        Ok(())
    }

    /** Get the diff of the commit. 

        TODO We actually need to calculate the files ourselves as libgit provides no such feature - the diff provided inside is a full diff that is an overkill for our purposes, but for now I am just using the default diff. 
        
     */
    fn store_commit_diff(& self, repo : & git2::Repository, root: &str, commit : & git2::Commit) -> Result<(), git2::Error> {
        // first calculate the diff
        let mut diff = HashMap::new();
        if commit.parent_count() == 0 {
            Project::calculate_diff(& repo, None, Some(& commit.tree()?), & mut diff)?;
        } else {
            for p in commit.parents() {
                Project::calculate_diff( & repo, Some(& p.tree()?), Some(& commit.tree()?), & mut diff)?;
            }
        }
        // then get all the hash ids for paths and for hashes
        let (contents, new_contents) = self.ghm.create_new_hash_ids(& diff.values().cloned().collect());        
        let (paths, new_paths) = self.ghm.create_new_path_ids(& diff.keys().cloned().collect());        
        // store the diff in the commit
        let mut f = File::create(format!("{}/changes.csv", root)).unwrap();
        writeln!(& mut f, "pathId,changeId");
        for x in diff {
            writeln!(& mut f, "{},{}", paths.get(& x.0).unwrap(), contents.get(& x.1).unwrap());
        }
        // store the snapshots
        // TODO determine which snapshots we want to be stored and which not
        for hash in & new_contents {
            let id = contents.get(& hash).unwrap();
            if let Ok(blob) = repo.find_blob(*hash) {
                let snapshotRoot = String::from(format!("{}/snapshots/{}", self.ghm.root, id % 100));
                std::fs::create_dir_all(& snapshotRoot);
                let mut f = File::create(format!("{}/{}", & snapshotRoot, id)).unwrap();
                f.write_all(blob.content());
            } else {
                // TODO we are dealing with sth like submodule most likely
            }
        }
        // when stored, update the hash & path ids
        self.ghm.append_hashes(& contents, & new_contents);
        self.ghm.append_paths(& paths, & new_paths);
        Ok(())
    }

    /** Calculates the diff between the two tree nodes. 
        
        Deal with renames and other things too
     */
    fn calculate_diff(repo : & git2::Repository, parent : Option<& git2::Tree>, commit : Option<& git2::Tree>, diff : & mut HashMap<String, git2::Oid>) -> Result<(), git2::Error> {
        let d = repo.diff_tree_to_tree(parent, commit, None)?;
        for di in d.deltas() {
            match di.status() {
                git2::Delta::Added | git2::Delta::Modified | git2::Delta::Deleted | git2::Delta::Copied => {
                    if let Some(p) = di.new_file().path().unwrap().to_str() {
                        diff.insert(String::from(p), di.new_file().id());
                    }
                },
                git2::Delta::Renamed => {
                    if let Some(po) = di.old_file().path().unwrap().to_str() {
                        diff.insert(String::from(po), git2::Oid::zero());
                        if let Some(p) = di.new_file().path().unwrap().to_str() {
                            diff.insert(String::from(p), di.new_file().id());
                        }
                    }
                },
                // this should not really happen in diffs of commits
                _ => {
                    panic!("What to do?");
                }
            }
        }
        Ok(())
    }

}

// GHM Itself

#[derive(Eq)]
struct GhmQueuedProject {
    id : u64,
    lastUpdated : u64
}

impl Ord for GhmQueuedProject {
    fn cmp(& self, other : & Self) -> std::cmp::Ordering {
        return self.lastUpdated.cmp(& other.lastUpdated);
    }
}

impl PartialOrd for GhmQueuedProject {
    fn partial_cmp(& self, other : & Self) -> Option<std::cmp::Ordering> {
        return Some(self.lastUpdated.cmp(& other.lastUpdated));
    }
}

impl PartialEq for GhmQueuedProject {
    fn eq(& self, other : & Self) -> bool {
        return self.lastUpdated == other.lastUpdated;
    }
}

struct GhmProjects {
    nextId : u64,
    liveUrls : HashSet<String>,
    deadUrls : HashMap<String, HashSet<u64>>,
    q : BinaryHeap<std::cmp::Reverse<GhmQueuedProject>>,
}

impl GhmProjects {
    fn new() -> GhmProjects {
        return GhmProjects{
            nextId : 0,
            liveUrls : HashSet::new(),
            deadUrls : HashMap::new(),
            q : BinaryHeap::new(),
        };
    }
}

struct ProjectsManager {
    /** The projects and their queue. 
     */
    projects : Mutex<GhmProjects>,
    qcv : Condvar,
} 

impl ProjectsManager {
    fn new() -> ProjectsManager {
        return ProjectsManager {
            projects : Mutex::new(GhmProjects::new()),
            qcv : Condvar::new(),
        };
    }

    /** Adds new project to the list of projects. 
     */
    fn add_project(& self, url : & str, ghm : & Ghm) -> Option<u64> {
        let projects = & mut self.projects.lock().unwrap();
        if projects.liveUrls.contains(url) {
            return None;
        } else if projects.deadUrls.contains_key(url) {
            // TODO check that the project is new, or resurrect the old ones
            return None;
        }
        Project::create(projects.nextId, url, ghm);
        let result = projects.nextId;
        projects.nextId += 1;
        // save the max project id
        // TODO in the future, update this so that projects can be added in bulk
        let filename = format!("{}/projects.csv", ghm.root);
        let mut f = File::create(filename).unwrap();
        writeln!(& mut f, "nextId");
        writeln!(& mut f, "{}", projects.nextId);
        // finally, add the project to the queue
        projects.q.push(std::cmp::Reverse(GhmQueuedProject{id : result, lastUpdated : 0}));
        self.qcv.notify_one();
        return Some(result);
    }

    /** Initializes the project information for the downloader. 
    
        The project information is held on two places. First we have the total number of projects, which tells us how many IDs need to be searched.
     */
    fn initialize_projects(& self, ghm : & Ghm) {
        let filename = format!("{}/projects.csv", ghm.root);
        let mut nextId = 0;
        if Path::new(& filename).exists() {
            let mut reader = csv::Reader::from_path(& filename).unwrap();
            for x in reader.records() {
                if let Ok(record) = x {
                    if record.len() == 1 {
                        nextId = record[0].parse::<u64>().unwrap();
                        self.projects.lock().unwrap().nextId = nextId;
                    }
                }
            }
            self.projects.lock().unwrap().nextId = nextId;
        } 
        // now, load all the projects we already have and add them to the queue
        for id in 0 .. nextId {
            let mut p = Project::new(id, ghm);
            let (live_url, dead_urls, last_updated) = p.get_urls_and_update_time();
            // TODO this is too coarse a lock, queue should move to its own lock
            let mut projects = self.projects.lock().unwrap();
            for url in dead_urls {
                projects.deadUrls.entry(String::from(url)).or_insert(HashSet::new()).insert(id);
            }
            if (! live_url.is_empty()) {
                projects.q.push(std::cmp::Reverse(GhmQueuedProject{id : id, lastUpdated : last_updated}));
            }
            self.qcv.notify_one();
        }
    }

    /** Returns new project to be analyzed and pops it from the queue.  
     */     
    fn dequeue(& self) -> u64 {
        let mut projects = self.projects.lock().unwrap();
        while (*projects).q.is_empty() {
            projects = self.qcv.wait(projects).unwrap();
        }
        return projects.q.pop().unwrap().0.id;
    }

    /** Returns the given project to the queue with the specified last update time. 
     */
    fn enqueue(& self, id : u64, update_time: u64) {
        let mut projects = self.projects.lock().unwrap();
        projects.q.push(std::cmp::Reverse(GhmQueuedProject{ id : id, lastUpdated : update_time }));
    }

}

/** Update status for a project.
 */
struct UpdateStatus {
    id : u64,
    // current task the updater works on
    task : String,
    url : String, 
    // time at which the update started and completed (so that we can calculate when to retire the objects)
    started : u64,
    ended : u64,
    // progress information. Note that maxProgress can grow as the information about tasks gets updated
    progress : u64,
    max_progress : u64,
}


impl std::fmt::Display for UpdateStatus {

    /** Formats the update status for printing. 
     */
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.ended == 0 {
            write!(f, "{}: {} - {}/{} ({}%), {} s\x1b[K\n", self.id, self.task, self.progress, self.max_progress, self.progress * 100 / self.max_progress, prettyTime(now() - self.started))?;
            write!(f, "    url: {}\x1b[K\n", self.url)?;
        } else {
            write!(f, "{}: {}\x1b[K\n", self.id, self.task)?;
            write!(f, "    url: {}\x1b[K\n", self.url)?;
        }
        return Ok(());
    }
}

impl UpdateStatus {
    fn new(id : u64) -> UpdateStatus {
        return UpdateStatus{
            id : id, 
            task : String::from("initialized"),
            url : String::new(),
            started : now(),
            ended : 0,
            progress : 0,
            max_progress : 1,
        };
    }
}

struct Ghm {
    root : String,
    hashes : Mutex<HashMap<git2::Oid, u64>>,
    paths : Mutex<HashMap<String, u64>>,
    hashes_file : Mutex<File>,
    paths_file : Mutex<File>,
    status : Mutex<HashMap<u64, UpdateStatus>>,
}

impl Ghm {

    fn new() -> Ghm {
        let root = String::from("/home/peta/ghmrs-linux");
        // create the root dir and the temporary directory for cloned projects. 
        std::fs::create_dir_all(format!("{}/tmp", root)).unwrap();
        let hashes_filename = format!("{}/{}", & root, "hashes.csv");
        let paths_filename = format!("{}/{}", & root, "paths.csv");
        let hashes = Ghm::initialize_hashes(& hashes_filename);
        let paths = Ghm::initialize_paths(& paths_filename);
        return Ghm {
            root,
            hashes : Mutex::new(hashes),
            paths : Mutex::new(paths),
            hashes_file : Mutex::new(std::fs::OpenOptions::new().append(true).write(true).open(hashes_filename).unwrap()),
            paths_file : Mutex::new(std::fs::OpenOptions::new().append(true).write(true).open(paths_filename).unwrap()),
            status : Mutex::new(HashMap::new()),
        };
    }


    /** Initializes the completed hashes and hash to id information held by the downloader. 
     
        Any hash already in the table is guaranteed to have its complete information already stored (i.e. commit information for commits and actual contents for those file contents we care about).
     */
    fn initialize_hashes(filename : & str) -> HashMap<git2::Oid, u64> {
        let mut hashes = HashMap::<git2::Oid, u64>::new();
        if std::path::Path::new(& filename).exists() {
            let mut reader = csv::Reader::from_path(& filename).unwrap();
            for x in reader.records() {
                if let Ok(record) = x {
                    if record.len() == 2 {
                        let id = record.get(1).unwrap().parse::<u64>().unwrap();
                        hashes.insert(git2::Oid::from_str(& record.get(0).unwrap()).unwrap(), id);
                    }
                }
            }
            println!(" Hashes loaded: {}", hashes.len());
        } else {
            let mut f = File::create(filename).unwrap();
            writeln!(& mut f, "hash,id");
        }
        return hashes;
    }

    /** Initializes the paths and their id information held by the downloader. 
     */
    fn initialize_paths(filename : & str) -> HashMap<String, u64> {
        let mut paths = HashMap::<String, u64>::new();
        if std::path::Path::new(& filename).exists() {
            let mut reader = csv::Reader::from_path(& filename).unwrap();
            for x in reader.records() {
                if let Ok(record) = x {
                    if record.len() == 2 {
                        let id = record.get(1).unwrap().parse::<u64>().unwrap();
                        paths.insert(String::from(record.get(0).unwrap()), id);
                    }
                }
            }
            println!(" paths loaded: {}", paths.len());
        } else {
            let mut f = File::create(filename).unwrap();
            writeln!(& mut f, "path,id");
        }
        return paths;
    }

    fn create_new_hash_ids(& self, hashes : & HashSet<git2::Oid>) -> (HashMap<git2::Oid, u64>, HashSet<git2::Oid>) {
        let mut result = HashMap::new();
        let mut new_hashes = HashSet::new();
        let mut hash_ids = self.hashes.lock().unwrap();
        for hash in hashes {
            match hash_ids.get(hash) {
                Some(x) => {
                    result.insert(*hash, *x);
                },
                None => {
                    let id = hash_ids.len() as u64;
                    hash_ids.insert(*hash, id);
                    new_hashes.insert(*hash);
                    result.insert(*hash, id);

                }
            }
        }
        return (result, new_hashes);
    }

    fn create_new_path_ids(& self, paths : & HashSet<String>) -> (HashMap<String, u64>, HashSet<String>) {
        let mut result = HashMap::new();
        let mut new_paths = HashSet::new();
        let mut path_ids = self.paths.lock().unwrap();
        for path in paths {
            match path_ids.get(path) {
                Some(x) => {
                    result.insert(path.clone(), *x);
                },
                None => {
                    let id = path_ids.len() as u64;
                    path_ids.insert(path.clone(), id);
                    new_paths.insert(path.clone());
                    result.insert(path.clone(), id);
                }
            }
        }
        return (result, new_paths);
    }

    fn append_hashes(& self, hashes : & HashMap<git2::Oid, u64>, new_hashes : & HashSet<git2::Oid>) {
        let f = & mut self.hashes_file.lock().unwrap();
        for hash in new_hashes {
            let id = hashes.get(hash).unwrap();
            writeln!(f, "{},{}", hash, id);
        }
    }

    fn append_paths(& self, paths : & HashMap<String, u64>, new_paths : & HashSet<String>) {
        let f = & mut self.paths_file.lock().unwrap();
        for path in new_paths {
            let id = paths.get(path).unwrap();
            writeln!(f, "\"{}\",{}", path, id);
        }
    }

    // reporting 

    fn set_url(& self, id : u64, url : & str) {
        let mut x = self.status.lock().unwrap();
        let e = x.entry(id).or_insert(UpdateStatus::new(id));
        e.url = String::from(url);
    }

    fn set_task(& self, id : u64, task : & str) {
        let mut x = self.status.lock().unwrap();
        let e = x.entry(id).or_insert(UpdateStatus::new(id));
        e.task = String::from(task);
    }

    fn add_max_progress(& self, id: u64, max_progress: u64) {
        let mut x = self.status.lock().unwrap();
        let e = x.entry(id).or_insert(UpdateStatus::new(id));
        e.max_progress = e.max_progress + max_progress;
    }

    fn add_progress(& self, id: u64, progress: u64) {
        let mut x = self.status.lock().unwrap();
        let e = x.entry(id).or_insert(UpdateStatus::new(id));
        e.progress = e.progress + progress;
    }

    fn set_progress(& self, id : u64, progress: u64, max_progress : u64) {
        let mut x = self.status.lock().unwrap();
        let e = x.entry(id).or_insert(UpdateStatus::new(id));
        e.progress = progress;
        e.max_progress = max_progress;
    }

    fn finish_task(& self, id : u64) {
        let mut x = self.status.lock().unwrap();
        let e = x.entry(id).or_insert(UpdateStatus::new(id));
        e.task = String::from("done.");
        e.ended = now();
    }

    fn error_task(& self, id : u64, error : & str) {
        let mut x = self.status.lock().unwrap();
        let e = x.entry(id).or_insert(UpdateStatus::new(id));
        e.task = format!("error - {}", error);
        e.ended = now();
    }
}

/** The projects updater thread. 
 */
fn updater(ghm : & Ghm, pm : & ProjectsManager) {
    loop {
        let id = pm.dequeue();
        let lastTime = now();
        let local_path = format!("{}/tmp/{}",ghm.root, id);
        let result = std::panic::catch_unwind(|| {
            let mut p = Project::new(id, ghm);
            p.read_log();
            ghm.set_url(id, & p.url);
            p.load_heads();
            p.update_contents(& local_path).unwrap();
            p.commit_log();
        });
        match result {
            // upon error, log the error, update the project's logs
            Err(what) => {
                let mut msg = String::from("Unknown error - see log for more details");
                if let Ok(x) = what.downcast::<String>() {
                    msg = * x;
                }
                /*
                match what.downcast::<String>() {
                    Ok(msg) => {
                    }
                    Err(_) => {
                        ghm.error_task(id, "Unknown error - see log for more details");
                    }
                }*/
                ghm.error_task(id, & msg);
                // TODO actually mark as error, log in the 
            },
            // if ok, add the project back to the queue
            _ => {
                ghm.finish_task(id);
                pm.enqueue(id, lastTime);
            }
        }
        // make sure to delete all files we have downloaded
        std::fs::remove_dir_all(local_path);
    }
}

/** The reporter thread.
 
    TODO in the future, this wants much more love than it has now. 
    */
fn reporter(ghm : & Ghm, pm : & ProjectsManager) {
    let start = now();
    loop {
        println!("\x1b[H");
        {
            print!("GHM - total time: {} | queue: {}\x1b[K\n", prettyTime(now() - start), pm.projects.lock().unwrap().q.len());
            print!("      hashes: {}, paths: {}\x1b[K\n", ghm.hashes.lock().unwrap().len(), ghm.paths.lock().unwrap().len());
        }
        {
            let status = & mut * ghm.status.lock().unwrap();
            // remove old entries
            status.retain(|key, value| {
                return value.ended == 0 || (now() - value.ended < 15);
            });
            // print those remaining
            for ref x in status {
                print!("{}",x.1);
            }
        }
        std::thread::sleep_ms(1000);
    }
}

fn main() {
    {
        println!("DEBUG! -- clearing the state and adding new projects...");
        std::fs::remove_dir_all("/home/peta/ghmrs-linux");
        let mut ghm = Ghm::new();
        let mut pm = ProjectsManager::new();
        println!("Adding projects...");
        pm.add_project("https://github.com/torvalds/linux.git", & ghm);
        /*
        pm.add_project("https://github.com/terminalpp/terminalpp.git", & ghm);
        pm.add_project("https://github.com/prl-prg/dejavuii.git", & ghm);
        pm.add_project("https://github.com/terminalpp/website.git", & ghm);
        pm.add_project("https://github.com/terminalpp/ropen.git", & ghm);
        pm.add_project("https://github.com/terminalpp/bypass.git", & ghm);
        pm.add_project("https://github.com/terminalpp/benchmarks.git", & ghm);
        pm.add_project("https://github.com/microsoft/CCF.git", & ghm);
        pm.add_project("https://github.com/microsoft/terminal.git", & ghm);
        pm.add_project("https://github.com/microsoft/PowerToys.git", & ghm);
        */
        println!("Reloading the ghm...")
    }
    let ghm = Ghm::new();
    let pm = ProjectsManager::new();
    crossbeam::thread::scope(|s| {
        // start the projects initializing thread
        s.spawn(|_| {
            pm.initialize_projects(& ghm);
        });
        // start the worker threads
        for x in 0..4 {
            s.spawn(|_| {
                updater(& ghm, & pm);
            });
        }
        // start the reporter thread
        s.spawn(|_| {
            reporter(& ghm, & pm);
        });
    }).unwrap();
}
