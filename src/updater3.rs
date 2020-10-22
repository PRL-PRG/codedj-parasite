use std::collections::*;
use std::sync::*;
use std::io::{Write, stdout};
use std::any::*;

use crate::datastore3::*;
use crate::records3::*;
use crate::helpers;

/** The updater. 

    

 */
struct Updater {

    /** The datastore on which the updater operates. 
     */
    ds : Datastore,

    /** Incremental updater
     */
    num_workers : u64, 
    iupdater : Mutex<IncrementalUpdater>,
    cv_workers : Condvar,
}

impl Updater {
    /** Updater is initialized with an existing datastore. 
     */
    pub fn new(ds : Datastore) -> Updater {
        return Updater {
            ds, 

            num_workers : 16,
            iupdater : Mutex::new(IncrementalUpdater::new()),
            cv_workers : Condvar::new(),
        }
    }

    /** Starts the worker threads, responder and status printer.  
     
        Because of the condvars not being able to pass the catch_unwind function barrier we use to execute the tasks, 
     */
    pub fn run(& self) {
        let (tx, rx) = crossbeam_channel::unbounded::<TaskMessage>();
        // TODO alt mode, clear screen and do stuff
        crossbeam::thread::scope(|s| {
            s.spawn(|_| {
                self.status(rx);
            });
            s.spawn(|_| {
                self.controller();
            });
            // start the worker threads
            for _ in 0.. self.num_workers {
                s.spawn(|_| {
                    self.incremental_update_worker(tx.clone());
                });
            }
        }).unwrap();
        // TODO get back from alt and so on...
    }

    /** 
     
        
     */
    fn incremental_update_worker(& self, tx : crossbeam_channel::Sender<TaskMessage>) {
        self.iupdater.lock().unwrap().running_workers += 1;
        while let Some(task) = self.get_next_task() {
            let result = std::panic::catch_unwind(|| {
                // TODO be smarter here, actually get the project, determine its kind, and so on. 
                return self.update_project(task);
            });
        }
        self.iupdater.lock().unwrap().running_workers -= 1;
    }

    /** Returns the next project to be updated. 
     
        Returns None if the updater should stop and blocks if there are no avilable projects, or the updater should pause. 
     */
    fn get_next_task(& self) -> Option<QueuedProject> {
        let mut state = self.iupdater.lock().unwrap();
        loop {
            if state.state == State::Running && !state.queue.is_empty() {
                break;
            }
            if state.state == State::Stopped {
                return None;
            }
            state.running_workers -= 1;
            state.paused_workers += 1;
            state = self.cv_workers.wait(state).unwrap();
            state.running_workers += 1;
            state.paused_workers -= 1;
        }
        return state.queue.pop();
    }

    /** Returns true if the thread should continue, blocks if the updater is being paused and returns false if the updater should stop immediately. 
     
        This function is to be used periodically by workers that are not part of the incremental downloader as it provides the stop/pause/running check outside of the scope of 
     */
    fn can_continue(& self) -> bool {
        let mut state = self.iupdater.lock().unwrap();
        while state.state != State::Running {
            if state.state == State::Stopped {
                return false;
            }
            state.running_workers -= 1;
            state.paused_workers += 1;
            state = self.cv_workers.wait(state).unwrap();
            state.running_workers += 1;
            state.paused_workers -= 1;
        }
        return true;
    }

    /** Prints the status of the update process. 
     */
    fn status(& self, rx : crossbeam_channel::Receiver<TaskMessage>) {
        let mut tasks = HashMap::<String, TaskInfo>::new(); 
        let mut errors = Vec::<(TaskInfo, String)>::new();
        while self.can_continue() {
            // see how many messages are there and process them, otherwise we can just keep processing messages without ever printing anything 
            let mut msgs = rx.len();
            while msgs > 0 {
                match rx.recv() {
                    Ok(TaskMessage::Start{name}) => {
                        assert!(tasks.contains_key(& name) == false, "Task already exists");
                        tasks.insert(name, TaskInfo::new());
                    },
                    Ok(TaskMessage::Done{name}) => {
                        assert!(tasks.contains_key(& name) == true, "Task does not exist");
                        tasks.remove(& name);
                    },
                    Ok(TaskMessage::Error{name, cause}) => {
                        assert!(tasks.contains_key(& name) == true, "Task does not exist");
                        let task = tasks.get_mut(& name).unwrap();    
                        task.ping = 0;
                    },
                    Ok(TaskMessage::Progress{name, progress, max}) => {
                        assert!(tasks.contains_key(& name) == true, "Task does not exist");
                        let task = tasks.get_mut(& name).unwrap();    
                        task.ping = 0;
                        task.progress = progress;
                        task.progress_max = max;

                    },
                    _ => {
                        panic!("Unknown message or channel error");
                    }

                }
                msgs -= 1;
            }
            // now that the messages have been processed, redraw the status information




        }
        print!("\x1b7"); // save cursor
        print!("\x1b[H\x1b[104;97m"); // set cursor to top left corner and do white on blue background


    }

    /** The user interface and controller. 
     */
    fn controller(& self) {

    }

    fn update_project(& self, task : QueuedProject) -> Result<(), std::io::Error> {
        unimplemented!();
    }
}

/** This is required so that we can pass updater across the catch_unwind barrier. 
 
    It's safe too, because the offending conditional variables in the updater are actually never accessed when the thread may panic, or during the unwinding. 
 */
impl std::panic::RefUnwindSafe for Updater { }


/** The queued object record. 
  
    Holds the information about the project to be updated. This consists of the project id, the last time the project was updated and the version of dcd used to update the project. Project records are ordered by their increasing last update time. 
 */
#[derive(Eq)]
struct QueuedProject {
    id : u64, 
    last_update_time : i64,
    version : u16,
}

impl Ord for QueuedProject {
    fn cmp(& self, other : & Self) -> std::cmp::Ordering {
        return self.last_update_time.cmp(& other.last_update_time).reverse();
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

/** Main structure for the incremental updater part of the downloader. 
 
    Grouped together because of how mutexes work in Rust. 
 */
struct IncrementalUpdater {
    state : State,
    running_workers : u64, 
    idle_workers : u64,
    paused_workers : u64,
    queue : BinaryHeap<QueuedProject>,
}

#[derive(Eq, PartialEq)]
enum State {
    Running,
    Paused,
    Stopped,
}

impl IncrementalUpdater {
    fn new() -> IncrementalUpdater {
        return IncrementalUpdater {
            state : State::Running,
            running_workers : 0,
            idle_workers : 0,
            paused_workers : 0,
            queue : BinaryHeap::new()
        };
    }
    fn is_paused(& self) -> bool {
        return self.running_workers == 0 && self.paused_workers == 0;
    }

    fn is_stopped(& self) -> bool {
        return self.running_workers + self.idle_workers + self.paused_workers == 0;
    }

}

/** Messages that communicate to the updater changes about tasks. 
 */
enum TaskMessage {
    Start{name : String},
    Done{name : String},
    Error{name : String, cause : String},
    Progress{name : String, progress : u64, max : u64 },
}

/** Task info as stored on the updater's end. 
 */
struct TaskInfo {
    start_time : i64,
    progress : u64, 
    progress_max : u64, 
    ping : u64, 
}

impl TaskInfo {
    fn new() -> TaskInfo {
        return TaskInfo{
            start_time : helpers::now(),
            progress : 0, 
            progress_max : 100,
            ping : 0,
        };
    }
}







