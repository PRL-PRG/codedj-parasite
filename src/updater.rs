use std::collections::*;
use std::sync::*;
use std::io::{Write, stdout};

use crate::datastore::*;
use crate::helpers;
use crate::repo_updater::*;
use crate::github::*;
use crate::records::*;

struct ThreadInfo {
    running : usize, 
    idle : usize,
    paused : usize,
    pause : bool,
    stop : bool,
    queue : BinaryHeap<std::cmp::Reverse<QueuedProject>>,
}

impl ThreadInfo {
    fn new() -> ThreadInfo {
        return ThreadInfo {
            running : 0, 
            idle : 0,
            paused : 0,
            pause : false, 
            stop : false,
            queue : BinaryHeap::new(),
        }
    }

    pub (crate) fn valid_time(& self) -> i64 {
        if self.queue.is_empty() {
            return 0;
        } else {
            return self.queue.peek().unwrap().0.last_update_time;
        }
    }

}


/** The updater status structure that displays various information about the running updater. 
 
 */
pub struct Updater {
    pub (crate) tmp_folder : String,
    pub (crate) ds : Datastore,
    pub (crate) gh : Github, 
    start : i64,
    // condition variable used to synchronize threads
    cv_threads : Condvar,
    // mutex for the threads synchronization
    threads : Mutex<ThreadInfo>,
    // task information, ordered by start time, equal on task name
    tasks : Mutex<HashMap<String, TaskInfo>>,
}

impl Updater {

    pub fn new(mut datastore : Datastore) -> Updater {
        // prep datastore 
        println!("Creating updater...");
        datastore.fill_mappings();
        // check the tmp directory inside the datastore
        let tmp_folder = format!("{}/tmp", datastore.root());
        let tmp_path = std::path::Path::new(& tmp_folder);
        if tmp_path.exists() {
            std::fs::remove_dir_all(&tmp_path).unwrap();
        }
        std::fs::create_dir_all(&tmp_path).unwrap();
        // create the updater
        return Updater{
            tmp_folder,
            ds : datastore,
            gh : Github::new("/mnt/data/github-tokens.csv"),
            start : helpers::now(),
            cv_threads : Condvar::new(),
            threads : Mutex::new(ThreadInfo::new()),
            tasks : Mutex::new(HashMap::new()),
        };
    }

    pub fn run(& mut self) {
        self.fill_queue();
        let num_workers = 16;
        print!("\x1b[2J"); // clear screen
        print!("\x1b[4;r"); // set scroll region
        print!("\x1b[2;4H"); // set cursor to where it belongs
        stdout().flush().unwrap();
        crossbeam::thread::scope(|s| {
            s.spawn(|_| {
                self.status_printer();
            });
            s.spawn(|_| {
                self.controller();
            });
            // start the worker threads
            for _ in 0..num_workers {
                s.spawn(|_| {
                    self.incremental_update_worker();
                });
            }
        }).unwrap();
        print!("\x1b[2J"); // clear screen
        print!("\x1b[r"); // reset scroll region
        print!("\x1b[H\x1b[0m"); // reset cursor and color
        println!("DCD Downloader done.");
    }

    fn incremental_update_worker(& self) {
        self.thread_start();
        let ru = RepoUpdater::new(& self.ds, & self.gh);
        while self.thread_next() {
            if let Some((id, version)) = self.deque() {
                let t = helpers::now();
                let task = self.new_task(format!("{}", id));
                // because we can't pass updater ref to the catch_unwind closure
                let tmp_folder = self.tmp_folder.clone();
                let result = std::panic::catch_unwind(||{ 
                    return ru.update_project(& tmp_folder, id, version, & task);
                });
                match result {
                    Ok(Ok(true)) => {
                        self.ds.project_last_updates.lock().unwrap().set(id, & UpdateLog::Ok{
                            time : t,
                            version : Datastore::VERSION
                        });
                        task.update().done();
                    },
                    Ok(Ok(false)) => {
                        self.ds.project_last_updates.lock().unwrap().set(id, & UpdateLog::NoChange{
                            time : t,
                            version : Datastore::VERSION
                        });
                        task.update().done();
                    },
                    Ok(Err(cause)) => {
                        self.ds.project_last_updates.lock().unwrap().set(id, & UpdateLog::Error{
                            time : t,
                            version : Datastore::VERSION,
                            error : format!("{:?}", cause)
                        });
                        task.update().error(& format!("{:?}", cause));
                    },
                    Err(cause) => {
                        self.ds.project_last_updates.lock().unwrap().set(id, & UpdateLog::Error{
                            time : t,
                            version : Datastore::VERSION,
                            error : format!("Panic: {:?}", cause)
                        });
                        task.update().error(& format!("Panic: {:?}", cause));
                    }
                }
            }
        }
        self.thread_stop();
    }

    /** Helper function for non task oriented threads to determine whether they should stop. 
     
        A thread stop when the global stop command has been issued and when there are no task oriented threads left (running, idle, or paused). 
     */
    pub fn thread_should_exit(& self) -> bool {
        let threads = self.threads.lock().unwrap();
        return threads.stop && (threads.running + threads.idle + threads.paused == 0);
    }

    pub (crate) fn thread_start(& self) {
        let mut x = self.threads.lock().unwrap();
        x.running += 1;
    }

    pub (crate) fn thread_stop(& self) {
        let mut x = self.threads.lock().unwrap();
        x.running -= 1;
    }

    pub (crate) fn thread_next(& self) -> bool {
        let mut x = self.threads.lock().unwrap();
        while x.pause {
            x.running -= 1;
            x.paused += 1;
            x = self.cv_threads.wait(x).unwrap();
            x.paused -= 1;
            x.running += 1;
        }
        return ! x.stop;
    }

    fn fill_queue(& self) {
        println!("Initializing projects queue...");
        let mut threads = self.threads.lock().unwrap();
        for (id, last_update) in self.ds.project_last_updates.lock().unwrap().latest_iter() {
            if last_update.is_ok() {
                threads.queue.push(std::cmp::Reverse(QueuedProject{
                    id, 
                    last_update_time : last_update.time(),
                    version : last_update.version()
                }));
            }
        }
        println!("    projects queueued: {}", threads.queue.len());
    }

    pub (crate) fn deque(& self) -> Option<(u64, u16)> {
        let mut threads = self.threads.lock().unwrap();
        while threads.queue.is_empty() {
            threads.running -= 1;
            threads.idle += 1;
            threads = self.cv_threads.wait(threads).unwrap();
            threads.idle -= 1;
            threads.running += 1;
            if threads.pause || threads.stop {
                return None;
            }
        }
        let x = threads.queue.pop().unwrap().0;
        return Some((x.id, x.version));
    }

    #[allow(dead_code)]
    pub (crate) fn enqueue(& self, id : u64, last_update_time : i64) {
        let mut threads = self.threads.lock().unwrap();
        threads.queue.push(std::cmp::Reverse(QueuedProject{id, last_update_time, version : Datastore::VERSION }));
        // we have to notify all because paused threads (or otherwise synchronized threads might be blocked on the same cv too)
        self.cv_threads.notify_all();
    }

    pub (crate) fn new_task(&self, name : String) -> Task {
        let mut tasks = self.tasks.lock().unwrap();
        assert_eq!(tasks.contains_key(&name), false);
        tasks.insert(name.clone(), TaskInfo::new());
        return Task{name, tasks : & self.tasks};
    }

    fn controller(& self) {
        {
            // acquire lock for printing and prepare the command area (lines 2 and 3)
            let _ = self.tasks.lock().unwrap();
            print!("\x1b[2;H\x1b[0m > \x1b[K\n");
            print!("\x1b[K\x1b[2;4H");
            stdout().flush().unwrap();
        }
        loop {
            let mut command = String::new();
            match std::io::stdin().read_line(& mut command) {
                Ok(_) => {
                    match command.trim() {
                        "stop" => {
                            let mut threads = self.threads.lock().unwrap();
                            threads.stop = true;
                            print!("\x1b[2;H\x1b[0m -- command interface not available, waiting for threads to stop \x1b[K\n");
                            print!(" \x1b[K\x1b[2;4H");
                            self.cv_threads.notify_all();
                            // exit immediately, there will ne no further input 
                            return;
                        },
                        "pause" => {
                            let mut threads = self.threads.lock().unwrap();
                            threads.pause = true;
                            self.cv_threads.notify_all();
                        },
                        "resume" => {
                            let mut threads = self.threads.lock().unwrap();
                            threads.pause = false;
                            self.cv_threads.notify_all();
                        },
                        "savepoint" => {
                            self.ds.savepoint();                            
                            let mut threads = self.threads.lock().unwrap();
                            threads.pause = false;
                            self.cv_threads.notify_all();
                        }
                        _ => {
                            let _ = self.tasks.lock().unwrap();
                            print!("\x1b[2;H\x1b[0m > \x1b[K\n");
                            print!(" Unknown command: {}\x1b[K\x1b[2;4H", command);
                            stdout().flush().unwrap();
                        }
                    }
                },
                Err(e) => {
                    let _ = self.tasks.lock().unwrap();
                    print!("\x1b[2;H\x1b[0m > \x1b[K\n");
                    print!(" Unexpected error: {:?}\x1b[K\x1b[2;4H", e);
                    stdout().flush().unwrap();
                }
            }
        }
    }

    fn status_printer(& self) {
        while ! self.thread_should_exit() {
            let now = helpers::now();
            {
                let mut tasks = self.tasks.lock().unwrap();
                // acquire the lock so that we can print out stuff
                //let x = self.status.lock().unwrap();
                // print the global status
                let threads = self.threads.lock().unwrap();
                print!("\x1b7"); // save cursor
                print!("\x1b[H\x1b[104;97m");
                print!("DCD - {}, queue: {}, valid_time: {}, workers : {}r, {}i, {}p, datastore : p : {}, c : {}, co: {} {} {}\x1b[K\x1b[4;H",
                    Updater::pretty_time(now - self.start),
                    threads.queue.len(),
                    threads.valid_time(),
                    threads.running, threads.idle, threads.paused,
                    Updater::pretty_value(self.ds.num_projects()),
                    Updater::pretty_value(self.ds.commits.lock().unwrap().loaded_len()),
                    Updater::pretty_value(self.ds.contents.lock().unwrap().loaded_len()),
                    if threads.pause { " <PAUSE>" } else { "" },
                    if threads.stop { " <STOP>" } else { "" },
                );
                for (name, task) in tasks.iter() {
                    if task.error {
                        task.print(name);
                    }
                }
                let mut odd = false;
                for (name, task) in tasks.iter() {
                    if task.end == 0 {
                        odd = ! odd;
                        if odd {
                            print!("\x1b[48;2;0;0;0m\x1b[97m");
                        } else {
                            print!("\x1b[48;2;32;32;32m\x1b[97m");
                        }
                        task.print(name);
                    }
                }
                println!("\x1b[0m\x1b[J");
                // now remove all tasks that are long dead (10s)
                tasks.retain(|_,x| !x.is_done() || (now  - x.end < 10));
                for (_, task) in tasks.iter_mut() {
                    task.ping += 1;
                }
                print!("\x1b8"); // restore cursor
                stdout().flush().unwrap();
            }
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }

    fn pretty_time(mut seconds : i64) -> String {
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

    fn pretty_value(mut value : usize) -> String {
        if value < 1000 {
            return format!("{}", value);
        }
        value = value / 1000;
        if value < 1000 {
            return format!("{}K", value);
        }
        value = value / 1000;
        if value < 1000 {
            return format!("{}M", value);
        }
        value = value / 1000;
        return format!("{}B", value);
    }

}

/** Information about each task the updater works on. 
 
    A task can be updated. 
 */ 
pub struct Task<'a> {
    name : String,
    tasks : &'a Mutex<HashMap<String, TaskInfo>>,
}

impl<'a> Task<'a> {

    pub fn update(& self) -> TaskUpdater {
        return TaskUpdater{g : self.tasks.lock().unwrap(), t : self};
    }
}

pub struct TaskInfo {
    start : i64,
    end : i64,
    ping : i64,
    error : bool, 
    url : String,
    message : String,

}

impl TaskInfo {
    fn new() -> TaskInfo {
        return TaskInfo{
            start : helpers::now(),
            end : 0,
            ping : 0,
            error : false,
            url : String::new(),
            message : String::from("initializing..."),
        };
    }

    pub fn set_url(& mut self, url : & str) -> & mut Self {
        self.url = url.to_owned();
        self.ping = 0;
        return self;
    }

    pub fn set_message(& mut self, msg : & str) -> & mut Self {
        self.message = msg.to_owned();
        self.ping = 0;
        return self;
    }

    pub fn done(& mut self) -> & mut Self {
        self.end = helpers::now();
        self.message = "done".to_owned();
        self.ping = 0;
        return self;
    }

    pub fn error(& mut self, msg : & str) -> & mut Self {
        self.end = helpers::now();
        self.message = format!("Error: {}", msg);
        self.ping = 0;
        self.error = true;
        return self;
    }

    pub fn is_done(& self) -> bool {
        return self.end != 0;
    }

    /** Prints the task. */
    fn print(& self, name : & str) {
        // first determine the status
        let mut status = String::new();
        if self.error {
            print!("\x1b[101;30m");
        } else if self.is_done() {
            print!("\x1b[90m");
        } else {
            if self.ping > 10 {
                status = format!(" - NOT RESPONDING: {}", Updater::pretty_time(self.ping));
                print!("\x1b[48;2;255;165;0m");
            }
        }
        let end = if self.is_done() { self.end } else { helpers::now() };
        println!("{}: {} - {}{}\x1b[K", 
            name, 
            Updater::pretty_time(end - self.start),
            self.url,
            status
        );
        println!("    {}\x1b[K", self.message);
    }

}

pub struct TaskUpdater<'a> {
    g : MutexGuard<'a, HashMap<String, TaskInfo>>,
    t : &'a Task<'a>
}

impl<'a> std::ops::Deref for TaskUpdater<'a> {
    type Target = TaskInfo;

    fn deref(&self) -> &Self::Target {
        return self.g.get(& self.t.name).unwrap();
    }
}

impl<'a> std::ops::DerefMut for TaskUpdater<'a> {

    fn deref_mut(&mut self) -> & mut Self::Target {
        return self.g.get_mut(& self.t.name).unwrap();
    }
}








/** The queued object record. 
  
    The records are ordered by the time of the last update. 
 */
#[derive(Eq)]
struct QueuedProject {
    id : u64, 
    last_update_time : i64,
    version : u16,
}

impl Ord for QueuedProject {
    fn cmp(& self, other : & Self) -> std::cmp::Ordering {
        return self.last_update_time.cmp(& other.last_update_time);
    }
}

impl PartialOrd for QueuedProject {
    fn partial_cmp(& self, other : & Self) -> Option<std::cmp::Ordering> {
        return Some(self.last_update_time.cmp(& other.last_update_time));
    }
}

impl PartialEq for QueuedProject {
    fn eq(& self, other : & Self) -> bool {
        return self.last_update_time == other.last_update_time;
    }
}



