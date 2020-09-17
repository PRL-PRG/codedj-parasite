use std::collections::*;
use std::sync::*;

use crate::datastore::*;
use crate::helpers;
use crate::repo_updater::*;


/** The updater status structure that displays various information about the running updater. 
 
 */
pub struct Updater {
    pub (crate) tmp_folder : String,
    pub (crate) ds : Datastore,
    start : i64,
    tasks : Mutex<HashMap<String, TaskInfo>>,
    thread_status : Mutex<ThreadStatus>,
    qcv_pause : Condvar,
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
            start : helpers::now(),
            tasks : Mutex::new(HashMap::new()),
            thread_status : Mutex::new(ThreadStatus{running : 0, idle : 0, paused : 0, pause : false, stop : false}),
            qcv_pause : Condvar::new(),
        };
    }

    pub fn run(& mut self) {
        println!("Initializing repo updater...");
        let repo_updater = RepoUpdater::new(self);
        let num_workers = 10;

        crossbeam::thread::scope(|s| {
            s.spawn(|_| {
                self.status_printer();
            });
            // start the worker threads
            for _ in 0..num_workers {
                s.spawn(|_| {
                    repo_updater.worker();
                });
            }
        }).unwrap();

    }

    /** Informs the updater that a thread has started. 
     */
    pub (crate) fn thread_start(& self) {
        let mut x = self.thread_status.lock().unwrap();
        x.running += 1;
    }

    /** Should be executed by each thread before new work item is requested. 
     
        Returns true if the thread should continue, false if it should stop immediately. If the thread should pause, pauses the thread in the function. 
     */
    pub (crate) fn thread_next(& self) -> bool {
        let mut x = self.thread_status.lock().unwrap();
        while x.pause {
            x.running -= 1;
            x.paused += 1;
            x = self.qcv_pause.wait(x).unwrap();
            x.paused -= 1;
            x.running += 1;
        }
        return ! x.stop;
    }

    /** Informs the updater that a thread is idle. 
     */
    pub (crate) fn thread_running_to_idle(& self) {
        let mut x = self.thread_status.lock().unwrap();
        x.running -= 1;
        x.idle += 1;
    }

    /** Informs the updater that a thread is in working state again. 
     */
    pub (crate) fn thread_idle_to_running(& self) {
        let mut x = self.thread_status.lock().unwrap();
        x.idle -= 1;
        x.running += 1;
    }

    /** Informs the updater that a thread has finished its execution. 
     */
    pub (crate) fn thread_done(& self) {
        let mut x = self.thread_status.lock().unwrap();
        x.running -= 1;
    }

    pub (crate) fn new_task(& self, name : String) -> Task {
        return Task::new(self, name);
    }

    fn status_printer(& self) {
        println!("\x1b[2J"); // clear screen
        loop {
            let now = helpers::now();
            {
                let mut tasks = self.tasks.lock().unwrap();
                // acquire the lock so that we can print out stuff
                //let x = self.status.lock().unwrap();
                // print the global status
                let ts = self.thread_status.lock().unwrap();
                print!("\x1b[H\x1b[104;97m");
                print!("DCD - {}, workers : {}r, {}i, {}p {} {}, datastore : p : {}, c : {}, co: {}\x1b[K\n",
                    Updater::pretty_time(now - self.start),
                    ts.running, ts.idle, ts.paused,
                    if ts.pause { " <PAUSE>" } else { "" },
                    if ts.stop { " <STOP>" } else { "" },
                    Updater::pretty_value(self.ds.num_projects()),
                    Updater::pretty_value(self.ds.commits.lock().unwrap().loaded_len()),
                    Updater::pretty_value(self.ds.contents.lock().unwrap().loaded_len()),
                );
                println!("");
                for (name, task) in tasks.iter_mut() {
                    if task.error {
                        task.print(name);
                    }
                }
                let mut odd = false;
                for (name, task) in tasks.iter_mut() {
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
                tasks.retain(|&_, x| !x.is_done() || (now  - x.end < 10));
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

/** Thread counts and updater exections state.  
 */
struct ThreadStatus {
    running: u64, 
    idle : u64, 
    paused : u64,
    pause : bool,
    stop : bool,
}

/** Information about each task the updater works on. 
 
    A task can be updated. 
 */ 
pub struct Task<'a> {
    name : String,
    updater : &'a Updater,
}

impl<'a> Task<'a> {
    fn new(updater : & Updater, name : String) -> Task {
        updater.tasks.lock().unwrap().insert(name.clone(), TaskInfo::new());
        return Task{ name, updater };
    } 

    pub fn update(& self) -> TaskUpdater {
        return TaskUpdater{g : self.updater.tasks.lock().unwrap(), t : self};
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
    fn print(& mut self, name : & str) {
        // first determine the status
        let mut status = String::new();
        if self.error {
            print!("\x1b[101;30m");
        } else if self.is_done() {
            print!("\x1b[90m");
        } else {
            self.ping += 1;
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





