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
    status : Mutex<BinaryHeap<std::cmp::Reverse<UpdateRecord>>>,
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
            status : Mutex::new(BinaryHeap::new()),
            thread_status : Mutex::new(ThreadStatus{running : 0, idle : 0, paused : 0, pause : false, stop : false}),
            qcv_pause : Condvar::new(),
        };
    }

    pub fn run(& mut self) {
        println!("Initializing repo updater...");
        let repo_updater = RepoUpdater::new(self);
        let num_workers = 1;

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

    fn status_printer(& self) {
        loop {
            {
                // acquire the lock so that we can print out stuff
                let x = self.status.lock().unwrap();
                // print the global status
                let ts = self.thread_status.lock().unwrap();
                print!("\x1b[H\x1b[104;97m");
                print!("DCD - {}, workers : {}r, {}i, {}p {} {}, datastore : p : {}, c : {}, co: {}",
                    Updater::pretty_time(helpers::now() - self.start),
                    ts.running, ts.idle, ts.paused,
                    if ts.pause { " <PAUSE>" } else { "" },
                    if ts.stop { " <STOP>" } else { "" },
                    Updater::pretty_value(self.ds.num_projects()),
                    Updater::pretty_value(self.ds.commits.lock().unwrap().loaded_len()),
                    Updater::pretty_value(self.ds.contents.lock().unwrap().loaded_len()),
                );
                //println!("DCD - t : {}, ");
                //for rec in x {
                //    rec.print();
                //}
                println!("");
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

    const HOME : &'static str = "\x1b[H";
    const EOL : &'static str = "\x1b[K\r\n";
}

struct ThreadStatus {
    running: u64, 
    idle : u64, 
    paused : u64,
    pause : bool,
    stop : bool,
}

/** Status of the update record. 
 */
#[derive(Eq, PartialEq)]
pub enum UpdateRecordState {
    Running,
    Done,
    Error
}

/** Information about single work item. 
 
    Work items are ordered by their start time so that the oldest appear at the top. 
 */
#[derive(Eq)]
pub struct UpdateRecord {
    pub id : u64,
    pub state : UpdateRecordState,
    pub start : i64, 
    pub progress : u64,
    pub progress_max : u64,
    pub status : String,
    pub url : String,
}

impl UpdateRecord {
    /** Prints the update record. 
     */
    fn print(& self) {
    }
}

impl Ord for UpdateRecord {
    fn cmp(& self, other : & Self) -> std::cmp::Ordering {
        return self.start.cmp(& other.start);
    }
}

impl PartialOrd for UpdateRecord {
    fn partial_cmp(& self, other : & Self) -> Option<std::cmp::Ordering> {
        return Some(self.start.cmp(& other.start));
    }
}

impl PartialEq for UpdateRecord {
    fn eq(& self, other : & Self) -> bool {
        return self.id == other.id;
    }
}
