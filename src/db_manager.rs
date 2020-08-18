use std::sync::*;
use std::fs::*;
use std::io::*;
use std::collections::hash_map::*;
use byteorder::*;

use crate::*;

/** State of a record in the database. 
 
    A record can be existing inn which case its origin does not have to be analyzed, new, in which case it must be analyzed and then its records stored, or it can be incomplete, in which case it must be reanalyzed and any changes updated. 
 */
#[derive(Eq, PartialEq, Copy, Clone, Debug, Hash)]
pub enum RecordState {
    Existing,
    New,
    Incomplete
}

/** Code sugar for having the map and associated file we append to under a single mutex. 
 */
struct IDMapper<T> {
    map : T,
    writer : csv::Writer<File>,
    //file : File,
}
impl<T> IDMapper<T> {
    fn new(map : T, filename : String) -> IDMapper<T> {
        return IDMapper{
            map, 
            writer : csv::WriterBuilder::new()
                .delimiter(b',')
                .quote_style(csv::QuoteStyle::NonNumeric)
                .double_quote(false)
                .escape(b'\\')
                .from_writer(
                    OpenOptions::new()
                        .append(true)
                        .open(filename).unwrap()
                ),

            //file : OpenOptions::new().append(true).open(filename).unwrap(),
        };
    }

    fn behind_mutex(map : T, filename : String) -> Mutex<IDMapper<T>> {
        return Mutex::new(IDMapper::<T>::new(map, filename));
    }
}

/** R/W manager for the downloader database to be used by the downloade & friends.
 
    
 */
pub struct DatabaseManager {
    // root folder where all the data lives
    root_ : String, 
    // set of live urls for each active project so that we can easily check for project duplicites
    // TODO in the future, we also need set of dead urls
    // and we really only need to build this lazily when needed IMHO
    live_urls_ : Mutex<HashSet<String>>,

    // number of projects (dead and alive), used for generating new project ids...
    num_projects_ : Mutex<u64>,

    /* User email to user id mapping and file to which new mappings or updates should be written
     */
    user_ids_ : Mutex<IDMapper<HashMap<String, UserId>>>,
    user_records_file_ : Mutex<File>,

    /* SHA1 to commit id mapping and a file to which any new mappings should be written and a file to which new commit records are written. 

       TODO For now the API obtains locks every time a single commit is written, which is not super effective, this could be revisited in the future.
     */
    commit_ids_ : Mutex<IDMapper<HashMap<git2::Oid, CommitId>>>,
    commit_records_file_ : Mutex<File>,
    commit_parents_file_ : Mutex<File>,

    /* Unless commits come from git (or other csv file) their information is not reliable and can be updated in time. These structures allow the db to keep track of such commits. 
     */
    incomplete_commits_ : Mutex<HashMap<CommitId, IncompleteCommit>>,

    /** The index file csv and the commit messages file proper. 
     
        First file is index, second file is the actual messages
     */
    commit_messages_files_ : Mutex<(File, File)>, 

    /** Commit changes index (with additions and deletions) and commit changes. 
     */
    commit_changes_files_ : Mutex<(File, File)>,

    path_ids_ : Mutex<IDMapper<HashMap<String, PathId>>>,

    snapshot_ids_ : Mutex<IDMapper<HashMap<git2::Oid, SnapshotId>>>,

}

impl DatabaseManager {

    /** Creates new database manager and initializes its database in the given folder.
     
        If the folder exists, all its contents is deleted first. 
     */
    pub fn initialize_new(root : & str) -> DatabaseManager {
        // initialize the folder
        if std::path::Path::new(root).exists() {
            std::fs::remove_dir_all(root).unwrap();
        }
        std::fs::create_dir_all(root).unwrap();
        // create the necessary files
        {
            let mut f = File::create(Self::get_num_projects_file(root)).unwrap();
            writeln!(& mut f, "numProjects").unwrap();
            writeln!(& mut f, "0").unwrap();
        }
        {
            let mut f = File::create(Self::get_user_ids_file(root)).unwrap();
            writeln!(& mut f, "email,id").unwrap();
        }
        {
            let mut f = File::create(Self::get_user_records_file(root)).unwrap();
            writeln!(& mut f, "time,id,name,source").unwrap();
        }
        {
            let mut f = File::create(Self::get_commit_ids_file(root)).unwrap();
            writeln!(& mut f, "hash,id").unwrap();
        }
        {
            let mut f = File::create(Self::get_commit_records_file(root)).unwrap();
            writeln!(& mut f, "time,id,committerId,committerTime,authorId,authorTime,source").unwrap();
        }
        {
            let mut f = File::create(Self::get_commit_parents_file(root)).unwrap();
            writeln!(& mut f, "time,commitId,parentId").unwrap();
        }
        {
            let mut f = File::create(Self::get_commit_messages_index_file(root)).unwrap();
            writeln!(& mut f, "time,commitId,offset").unwrap();
        }
        {
            let mut f = File::create(Self::get_commit_changes_index_file(root)).unwrap();
            writeln!(& mut f, "time,commitId,offset").unwrap();
        }
        {
            let mut f = File::create(Self::get_path_ids_file(root)).unwrap();
            writeln!(& mut f, "path,id").unwrap();
            writeln!(& mut f, "\"\",0").unwrap();
        }
        {
            let mut f = File::create(Self::get_snapshot_ids_file(root)).unwrap();
            writeln!(& mut f, "hash,id").unwrap();
            writeln!(& mut f, "\"0000000000000000000000000000000000000000\",0").unwrap();
        }

        return Self::from(root);
    }

    /** Creates database manager from existing database folder.
     
        For this to work we need to load the right data and to open the right files. 
     */
    pub fn from(root : & str) -> DatabaseManager {
        println!("Loading database from {}", root);        
        let num_projects = Self::get_num_projects(root);
        println!("    {} projects", num_projects);
        let user_ids = Self::get_user_ids(root);
        println!("    {} users", user_ids.len());
        let commit_ids = Self::get_commit_ids(root);
        println!("    {} commits", commit_ids.len());
        let path_ids = Self::get_path_ids(root);
        println!("    {} paths", path_ids.len());
        let snapshot_ids = Self::get_snapshot_ids(root);
        println!("    {} snapshots", snapshot_ids.len());
        return DatabaseManager{
            root_ : String::from(root),
            // live urls will be lazy loaded as they are only necessary for adding new projects which should not happen often
            live_urls_ : Mutex::new(HashSet::new()),
            num_projects_ : Mutex::new(num_projects),

            user_ids_ : IDMapper::behind_mutex(user_ids, Self::get_user_ids_file(root)), 
            //user_ids_ : Mutex::new(user_ids),
            //user_ids_file_ : Mutex::new(OpenOptions::new().append(true).open(Self::get_user_ids_file(root)).unwrap()), 
            user_records_file_ : Mutex::new(OpenOptions::new().append(true).open(Self::get_user_records_file(root)).unwrap()),

            commit_ids_ : IDMapper::behind_mutex(commit_ids,Self::get_commit_ids_file(root)), 
            commit_records_file_ : Mutex::new(OpenOptions::new().append(true).open(Self::get_commit_records_file(root)).unwrap()),
            commit_parents_file_ : Mutex::new(OpenOptions::new().append(true).open(Self::get_commit_parents_file(root)).unwrap()),

            incomplete_commits_ : Mutex::new(HashMap::new()),

            commit_messages_files_ : Mutex::new((
                OpenOptions::new().append(true).open(Self::get_commit_messages_index_file(root)).unwrap(),
                OpenOptions::new().create(true).append(true).open(Self::get_commit_messages_file(root)).unwrap()
            )),

            commit_changes_files_ : Mutex::new((
                OpenOptions::new().append(true).open(Self::get_commit_changes_index_file(root)).unwrap(),
                OpenOptions::new().create(true).append(true).open(Self::get_commit_changes_file(root)).unwrap()
            )),

            path_ids_ : IDMapper::behind_mutex(path_ids,Self::get_path_ids_file(root)), 

            snapshot_ids_ : IDMapper::behind_mutex(snapshot_ids, Self::get_snapshot_ids_file(root)), 

        }
    }

    pub fn root(& self) -> & str {
        return & self.root_;
    }

    // Projects ---------------------------------------------------------------------------------------------------------------------

    /** Returns the number of projects the database contains.
     */
    pub fn num_projects(& self) -> u64 {
        return * self.num_projects_.lock().unwrap();
    }

    /** Creates new project with given url and source.
     
        If the url is new, returns the id assigned to the project, ortherwise returns None. The project log is initialized with init message of the appropriate url and source.  

        Note that the function does not commit the changes to the database. 
     */
    pub fn add_project(& self, url : String, source : Source) -> Option<ProjectId> {
        let mut live_urls = self.live_urls_.lock().unwrap(); // we lock for too long, but not care now
        // don't know how to do this on single lookup in rust yet
        if live_urls.contains(& url) {
            return None;
        }
        // get the project id
        let mut num_projects = self.num_projects_.lock().unwrap();
        let id = *num_projects as ProjectId;
        // get the project folder and create it 
        let project_folder = Self::get_project_log_folder(& self.root_, id);
        std::fs::create_dir_all(& project_folder).unwrap();
        // initialize the log for the project
        {
            let mut project_log = record::ProjectLog::new(self.get_project_log_filename(id));
            project_log.add(record::ProjectLogEntry::init(source, url.clone()));
            project_log.create_and_save();
        }
        // now that the log is ok, increment total number of projects, add the live url and return the id
        *num_projects += 1;
        live_urls.insert(url);
        return Some(id);
    }

    /** Commits the total number of projects which makes them reachable. 
     
        Technically this could happen after each new project is created, but that is too prohibitive so it is the responsibility of the code that adds projects to actually commit the number once the projects are created. 
     */
    pub fn commit_created_projects(& self) {
        let num_projects = self.num_projects_.lock().unwrap();
        let mut f = File::create(format!("{}/num_projects.csv", self.root_)).unwrap();
        write!(& mut f, "numProjects\n{}\n", num_projects).unwrap();
    }

    pub fn get_project_log_filename(& self, id : ProjectId) -> String{
        return Self::get_project_log_file(& self.root_, id);
    }

    /** Goes through all projects and loads their urls in the live urls database. 
     
        TODO this has to change a lot when we support dead urls and project movements as well. 
     */ 
    pub fn load_project_urls(& self) {
        println!("Loading live urls from project logs...");
        for pid in 0 .. self.num_projects() {
            let log = record::ProjectLog::new(self.get_project_log_filename(pid as ProjectId));
            let mut latest_url = String::new();
            log.analyze(|entry| {
                match entry {
                    record::ProjectLogEntry::Init{time : _, source: _, url } => latest_url = url.to_owned(),
                    _ => { }
                };
                return true;
            });
            {
                let mut live_urls = self.live_urls_.lock().unwrap();
                live_urls.insert(latest_url);
            }
        }
        println!("    {} live urls loaded", self.live_urls_.lock().unwrap().len());
    }

    // Users ---------------------------------------------------------------------------------------

    pub fn num_users(& self) -> u64 {
        let user_ids = self.user_ids_.lock().unwrap();
        return user_ids.map.len() as u64;
    }

    pub fn get_user(& self, email : & str) -> Option<UserId> {
        let user_ids = self.user_ids_.lock().unwrap();
        if let Some(id) = user_ids.map.get(email) {
            return Some(*id);
        } else {
            return None;
        }
    }
    /** Returns existing user id, or creates new user from given data.
     
        
     */
    pub fn get_or_create_user(& self, email : & str, name : & str, source: Source) -> UserId {
        let mut user_ids = self.user_ids_.lock().unwrap();
        if let Some(id) = user_ids.map.get(email) {
            return *id;
        } else {
            let id = user_ids.map.len() as UserId;
            user_ids.map.insert(String::from(email), id);
            // first store the email to id mapping
            user_ids.writer.write_record(&[String::from(email), id.to_string()]).unwrap();
            // then store the actual user record
            {
                let mut user_records_file = self.user_records_file_.lock().unwrap();
                record::User::new(id, String::from(name), source).to_csv(& mut user_records_file).unwrap();
            }
            return id;
        }
    }

    // Commits ------------------------------------------------------------------------------------

    pub fn load_incomplete_commits(& self) {
        println!("Loading incomplete commits...");
        let mut incomplete_commits = self.incomplete_commits_.lock().unwrap();
        {
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(true)
                .double_quote(false)
                .escape(Some(b'\\'))
                .from_path(Self::get_commit_records_file(& self.root_)).unwrap();
            for x in reader.records() {
                let record = x.unwrap();
                let id = record[1].parse::<usize>().unwrap() as CommitId;
                let source = Source::from_str(& record[6]);
                // TODO in the future multiple sources can be thought of as incomplete
                if source == Source::GHTorrent {
                    incomplete_commits.insert(id, IncompleteCommit::new(source));
                }
            }
        }
        println!("    {} incomplete commits found", incomplete_commits.len());
        if !incomplete_commits.is_empty() {
            println!("Loading incomplete commits parents...");
            let mut parent_update_times = HashMap::<CommitId, u64>::new();
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(true)
                .double_quote(false)
                .escape(Some(b'\\'))
                .from_path(Self::get_commit_parents_file(& self.root_)).unwrap();
            for x in reader.records() {
                let record = x.unwrap();
                let id = record[1].parse::<usize>().unwrap() as CommitId;
                if let Some(IncompleteCommit{source : _, parents}) = incomplete_commits.get_mut(& id) {
                    let t = record[0].parse::<u64>().unwrap();
                    let x = parent_update_times.entry(id).or_insert(0);
                    if *x != t {
                        *x = t;
                        parents.clear();
                    }
                    parents.insert(record[2].parse::<u64>().unwrap() as CommitId);
                }
            }
        }
    }

    /** Returns true if given existing commit is complete. 
     
        Commit is complete if it has no record in the completed commits, or if the record has source set to Source::NA, which indicates that there is already a worker making sure the commit will be completed.  
     */    
    pub fn is_commit_complete(& self, id : CommitId) -> bool {
        let incomplete_commits = self.incomplete_commits_.lock().unwrap();
        match incomplete_commits.get(& id) {
            Some(IncompleteCommit{source : Source::NA, parents : _}) => {
                return true;
            },
            Some(_) => {
                return false;
            },
            _ => {
                return true;
            }
        }
    }

    /** Returns id for given commit if the commit exists in the database. 
     
        The second value indicates whether the commit is complete, or not. 
     */
    pub fn get_commit_id(& self, hash : git2::Oid) -> Option<(CommitId, bool)> {
        let commit_ids = self.commit_ids_.lock().unwrap();
        match commit_ids.map.get(& hash) {
            Some(id) => {
                return Some((*id, self.is_commit_complete(*id)));
            },
            None => {
                return None;
            }
        }
    }

    /** Returns an id for given commit hash and whether the commit must be analyzed & stored.
     
        The commit should be analyzed / stored if the id had to be created for it, or if the commit is currently incomplete. 
        
        IMPORTANT: If the function returns the commit to be incomplete, the caller *must* complete the commit and further calls will return the commit's state as completed. 
     */  
    pub fn get_or_create_commit_id(& self, hash: git2::Oid) -> (CommitId, RecordState) {
        let mut commit_ids = self.commit_ids_.lock().unwrap();
        let mut incomplete_commits = self.incomplete_commits_.lock().unwrap();
        if let Some(commit_id) = commit_ids.map.get(& hash) {
            match incomplete_commits.get_mut(commit_id) {
                // if the source is NA, it means someone else is already working on the commit so we can treat it as existing for now
                Some(IncompleteCommit{source : Source::NA, parents : _}) => {
                    return (*commit_id, RecordState::Existing);
                },
                Some(IncompleteCommit{source, parents : _}) => {
                    *source = Source::NA;
                    return (*commit_id, RecordState::Incomplete);
                },
                None => {
                    return (*commit_id, RecordState::Existing);
                }
            }
        } else {
            let commit_id = commit_ids.map.len() as CommitId;
            commit_ids.map.insert(hash, commit_id);
            // write the hash to id mapping
            commit_ids.writer.write_record(&[hash.to_string(), commit_id.to_string()]).unwrap();
            return (commit_id, RecordState::New);
        }
    }

    /** Looks at given commit hashes and determines if any of the commits requires update. 
     
        This is either if the commit hash is not known, or if the commit is incomplete. 
     */
    pub fn commits_require_update(&self, iter : & mut dyn std::iter::Iterator<Item = & git2::Oid>) -> bool {
        let commit_ids = self.commit_ids_.lock().unwrap();
        for hash in iter {
            if let Some(commit_id) = commit_ids.map.get(hash) {
                if ! self.is_commit_complete(*commit_id) {
                    return true;
                }
            } else {
                return true;
            }
        }
        return false;
    }

    pub fn append_commit_record(& self, id : CommitId, committer_id : UserId, committer_time : i64, author_id : UserId, author_time : i64, source : Source) {
        let mut commit_records_file = self.commit_records_file_.lock().unwrap();
        record::Commit::new(id, committer_id, committer_time, author_id, author_time, source).to_csv(& mut commit_records_file).unwrap();
    }

    /** Appends parents records for multiple commits. 
     
        Does not check the validity of the records. 
     */
    pub fn append_commit_parents_records(& self, iter : & mut dyn std::iter::Iterator<Item = &(CommitId, CommitId)>) {
        let mut commit_parents_file = self.commit_parents_file_.lock().unwrap();
        let t = helpers::now();
        for (commit_id, parent_id) in iter {
            writeln!(commit_parents_file, "{},{},{}", t, commit_id, parent_id).unwrap();
        }
    }

    pub fn num_paths(& self) -> u64 {
        let mut path_ids = self.path_ids_.lock().unwrap();
        return path_ids.map.len() as u64;
    }

    pub fn num_snapshots(& self) -> u64 {
        let mut snapshot_ids = self.snapshot_ids_.lock().unwrap();
        return snapshot_ids.map.len() as u64;
    }

    /** Translates commits changes represented as path and hash to their respective ids.
     
        For each snapshot, returns whether the snapshot is new, or existing. 
     */
    pub fn translate_commit_changes(& self, changes : & HashMap<String, git2::Oid>) -> HashMap<PathId, (SnapshotId, bool)> {
        let mut path_ids = self.path_ids_.lock().unwrap();
        let mut snapshot_ids = self.snapshot_ids_.lock().unwrap();
        return changes.iter().map(|(path, hash)| {
            (Self::get_or_create_path_id_(& mut * path_ids, path),
             Self::get_or_create_snapshot_id_(& mut * snapshot_ids, *hash)
            ) 
        }).collect();
    }

    pub fn get_or_create_path_id(& self, path : & str) -> PathId {
        let mut path_ids = self.path_ids_.lock().unwrap();
        return Self::get_or_create_path_id_(& mut * path_ids, path);
    }

    fn get_or_create_path_id_(path_ids : & mut IDMapper<HashMap<String, PathId>>, path : & str) -> PathId {
        let new_id = path_ids.map.len() as PathId;
        match path_ids.map.entry(path.to_owned()) {
            Entry::Vacant(entry) => {
                entry.insert(new_id);
                path_ids.writer.write_record(&[path.to_owned(), new_id.to_string()]).unwrap();
                return new_id;
            },
            Entry::Occupied(entry) => {
                return * entry.get();
            }
        }
    }

    pub fn get_or_create_snapshot_id(& self, hash: git2::Oid ) -> (SnapshotId, bool) {
        let mut snapshot_ids = self.snapshot_ids_.lock().unwrap();
        return Self::get_or_create_snapshot_id_(& mut * snapshot_ids, hash);
    }


    fn get_or_create_snapshot_id_(snapshot_ids : & mut IDMapper<HashMap<git2::Oid, SnapshotId>>, hash: git2::Oid) -> (SnapshotId, bool) {
        let new_id = snapshot_ids.map.len() as SnapshotId;
        match snapshot_ids.map.entry(hash) {
            Entry::Vacant(entry) => {
                entry.insert(new_id);
                snapshot_ids.writer.write_record(&[hash.to_string(), new_id.to_string()]).unwrap();
                return (new_id, true);
            },
            Entry::Occupied(entry) => {
                return (* entry.get(), false);
            }
        }
    }

    pub fn append_commit_changes(& self, id : CommitId, changes : & Vec<(PathId, SnapshotId)>, additions : usize, deletions : usize) {
        let (index, changes_file) = & mut * self.commit_changes_files_.lock().unwrap();
        let offset: u64 = changes_file.seek(SeekFrom::Current(0)).unwrap();
        changes_file.write_u64::<LittleEndian>(id).unwrap();// checksum to tell us that next changeset is for commit X - no-one can change path 0
        changes_file.write_u32::<LittleEndian>(changes.len() as u32).unwrap();
        changes_file.write_u64::<LittleEndian>(additions as u64).unwrap(); // additions and deletions 
        changes_file.write_u64::<LittleEndian>(deletions as u64).unwrap();
        for (path_id, snapshot_id) in changes {
            changes_file.write_u64::<LittleEndian>(*path_id as u64).unwrap();
            changes_file.write_u64::<LittleEndian>(*snapshot_id as u64).unwrap();
        }
        writeln!(index, "{},{},{}", helpers::now(), id, offset).unwrap();
    }


    /** Appends commit parents information. 
     
        If the commit is incomplete, first verifies whether the parent information differs and only updates the parents if there is change. 
     */
    pub fn append_commit_parents_record(& self, id : CommitId, parents : & HashSet<CommitId>) {
        if ! self.is_commit_complete(id) {
            let incomplete_commits = self.incomplete_commits_.lock().unwrap();
            let IncompleteCommit{source : _, parents: old_parents} = incomplete_commits.get(& id).unwrap();     
            if parents.symmetric_difference(old_parents).next().is_none() {
                return;
            }
        }
        let mut commit_parents_file = self.commit_parents_file_.lock().unwrap();
        let t = helpers::now();
        for parent_id in parents {
            writeln!(commit_parents_file, "{},{},{}", t, id, parent_id).unwrap();
        }
    }

    pub fn append_commit_message(& self, id : CommitId, msg : & [u8]) {
        let (index, messages) = & mut * self.commit_messages_files_.lock().unwrap();
        let len : u32 = msg.len() as u32;
        let offset: u64 = messages.seek(SeekFrom::Current(0)).unwrap();
        messages.write_u64::<LittleEndian>(id).unwrap(); // serialize commit id and length of message for bookkeeping
        messages.write_u32::<LittleEndian>(len).unwrap();
        messages.write(msg).unwrap();
        writeln!(index, "{},{},{}", helpers::now(), id, offset).unwrap();
    }

    // bookkeeping & stuff
    pub fn get_num_projects_file(root : & str) -> String {
        return format!("{}/num_projects.csv", root);
    }

    pub fn get_user_ids_file(root : & str) -> String {
        return format!("{}/user_ids.csv", root);
    }

    pub fn get_user_records_file(root : & str) -> String {
        return format!("{}/user_records.csv", root);
    }

    pub fn get_commit_ids_file(root : & str) -> String {
        return format!("{}/commit_ids.csv", root);
    }

    pub fn get_commit_records_file(root : & str) -> String {
        return format!("{}/commit_records.csv", root);
    }

    pub fn get_commit_parents_file(root : & str) -> String {
        return format!("{}/commit_parents.csv", root);
    }

    pub fn get_commit_messages_index_file(root : & str) -> String {
        return format!("{}/commit_messages_index.csv", root);
    }

    pub fn get_commit_messages_file(root : & str) -> String {
        return format!("{}/commit_messages.dat", root);
    }

    pub fn get_commit_changes_index_file(root : & str) -> String {
        return format!("{}/commit_changes_index.csv", root);
    }

    pub fn get_commit_changes_file(root : & str) -> String {
        return format!("{}/commit_changes.dat", root);
    }

    pub fn get_path_ids_file(root : & str) -> String {
        return format!("{}/path_ids.csv", root);
    }

    pub fn get_snapshot_ids_file(root : & str) -> String {
        return format!("{}/snapshot_ids.csv", root);
    }

    /** Returns the log file for given project id. 
     
        
     */
    pub fn get_project_log_file(root : & str, id : ProjectId) -> String {
        return format!("{}/projects/{}/{}/{}.csv", root, id / 1000000, id % 1000, id);
    }

    /** Returns only the folder where the project log should exist so that we can ensure its presence. 
     */
    pub fn get_project_log_folder(root : & str, id : ProjectId) -> String {
        return format!("{}/projects/{}/{}", root, id / 1000000, id % 1000);
    }

    pub fn get_num_projects(root : & str) -> u64 {
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(Self::get_num_projects_file(root)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            return record[0].parse::<u64>().unwrap();
        }
        panic!("Invalid number of projects format.");
    }

    pub fn get_user_ids(root : & str) -> HashMap<String, UserId> {
        let mut result = HashMap::<String,UserId>::new();
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(Self::get_user_ids_file(root)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let email = String::from(&record[0]);
            let id = record[1].parse::<u64>().unwrap() as CommitId;
            if result.insert(email, id) != None {
                panic!("Duplucate record {}", id);
            }
        }
        return result;
    }

    pub fn get_commit_ids(root : & str) -> HashMap<git2::Oid, CommitId> {
        let mut result = HashMap::<git2::Oid,CommitId>::new();
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(Self::get_commit_ids_file(root)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let hash = git2::Oid::from_str(& record[0]).unwrap();
            let id = record[1].parse::<u64>().unwrap() as CommitId;
            result.insert(hash, id);
        }
        return result;
    }

    pub fn get_path_ids(root : & str) -> HashMap<String, PathId> {
        let mut result = HashMap::<String, PathId>::new();
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(Self::get_path_ids_file(root)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let path = String::from(& record[0]);
            let id = record[1].parse::<u64>().unwrap() as PathId;
            result.insert(path, id);
        }
        return result;

    }

    pub fn get_snapshot_ids(root : & str) -> HashMap<git2::Oid, SnapshotId> {
        let mut result = HashMap::<git2::Oid, SnapshotId>::new();
        let mut reader = csv::ReaderBuilder::new().has_headers(true).double_quote(false).escape(Some(b'\\')).from_path(Self::get_snapshot_ids_file(root)).unwrap();
        for x in reader.records() {
            let record = x.unwrap();
            let hash = git2::Oid::from_str(& record[0]).unwrap();
            let id = record[1].parse::<u64>().unwrap() as SnapshotId;
            result.insert(hash, id);
        }
        return result;
    }


}

/** Simple struct holding for each incomplete commit its source and current set of parents. 
 */
pub struct IncompleteCommit {
    source: Source, 
    parents : HashSet<CommitId>,
}

impl IncompleteCommit {
    pub fn new(source : Source) -> IncompleteCommit {
        return IncompleteCommit{ source, parents : HashSet::new()};
    }
}

