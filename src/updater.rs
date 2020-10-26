use std::collections::*;
use std::sync::*;
use std::io::{Write, stdout};
use std::any::*;

use crate::datastore::*;
use crate::records::*;
use crate::helpers;

/** The updater. 

    

 */
pub (crate) struct Updater {

    /** The datastore on which the updater operates. 
     */
    ds : Datastore,

    /** Incremental updater
     */
    num_workers : u64, 
    iupdater : Mutex<IncrementalUpdater>,
    cv_workers : Condvar,




    /** Mutex to guard console output.
     */
    cout_lock : Mutex<()>,
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

            cout_lock : Mutex::new(()),
        }
    }

    /** Starts the worker threads, responder and status printer.  
     
        Because of the condvars not being able to pass the catch_unwind function barrier we use to execute the tasks, 
     */
    pub fn run(& self, command : String) {
        println!("Running updater...");
        // prepare status & control screen
        print!("\x1b[?1049h"); // switch to alternate mode
        print!("\x1b[7r"); // enable scroll region
        print!("\x1b[2J"); // clear screen
        stdout().flush().unwrap();
        let (tx, rx) = crossbeam_channel::unbounded::<TaskMessage>();
        // TODO alt mode, clear screen and do stuff
        crossbeam::thread::scope(|s| {
            s.spawn(|_| {
                self.reporter(rx);
            });
            s.spawn(|_| {
                self.controller(command);
            });
            // start the worker threads
            for _ in 0.. self.num_workers {
                s.spawn(|_| {
                    self.incremental_update_worker(tx.clone());
                });
            }
        }).unwrap();
        print!("\x1b[?1049l"); // return to normal mode
        print!("\x1b[r"); // reset scroll region
        println!("Updater terminated.");
    }

    /** 
     
        
     */
    fn incremental_update_worker(& self, tx : crossbeam_channel::Sender<TaskMessage>) {
        self.iupdater.lock().unwrap().running_workers += 1;
        while let Some(task) = self.get_next_task() {
            let result = std::panic::catch_unwind(|| {
                // TODO be smarter here, actually get the project, determine its kind, and so on. 
                //return self.update_project(task);
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
            if state.state == State::Stopped {
                return None;
            } else if state.state == State::Paused {
                state.running_workers -= 1;
                state.paused_workers += 1;
                state = self.cv_workers.wait(state).unwrap();
                state.running_workers += 1;
                state.paused_workers -= 1;
            } else if !state.queue.is_empty() {
                break;
            } else {
                state.running_workers -= 1;
                state.idle_workers += 1;
                state = self.cv_workers.wait(state).unwrap();
                state.running_workers += 1;
                state.idle_workers -= 1;
            }
        }
        return state.queue.pop();
    }

    /** Returns true if the non-worker thread should stop immediately, false otherwise. 
     
        Non worker threads are required to stop immediately after al worker threads are done. 
     */
    fn should_stop(& self) -> bool {
        let state = self.iupdater.lock().unwrap();
        return state.is_stopped();
    }

    /* Returns true if the thread should continue, blocks if the updater is being paused and returns false if the updater should stop immediately. 
     
        This function is to be used periodically by workers that are not part of the incremental downloader as it provides the stop/pause/running check outside of the scope of 
     */
    /*
    fn can_continue(& self) -> bool {
        let mut state = self.iupdater.lock().unwrap();
        while state.state != State::Running {
            if state.state == State::Stopped {
                return false;
            }
            state.paused_workers += 1;
            state = self.cv_workers.wait(state).unwrap();
            state.paused_workers -= 1;
        }
        return true;
    }
    */

    /** Prints the status of the update process. 
     */
    fn reporter(& self, rx : crossbeam_channel::Receiver<TaskMessage>) {
        let mut info = ReporterInfo::new();
        while ! self.should_stop() {
            // see how many messages are there and process them, otherwise we can just keep processing messages without ever printing anything 
            let mut msgs = rx.len();
            while msgs > 0 {
                match rx.recv() {
                    Ok(TaskMessage::Start{name}) => {
                        assert!(info.tasks.contains_key(& name) == false, "Task already exists");
                        info.tasks.insert(name, TaskInfo::new());
                    },
                    Ok(TaskMessage::Done{name}) => {
                        assert!(info.tasks.contains_key(& name) == true, "Task does not exist");
                        info.tasks.remove(& name);
                        info.tick_tasks_done += 1;
                    },
                    Ok(TaskMessage::Error{name, cause}) => {
                        assert!(info.tasks.contains_key(& name) == true, "Task does not exist");
                        info.errors.push_back((info.tasks.remove(& name).unwrap(), cause));
                        info.tick_tasks_error += 1;
                    },
                    Ok(TaskMessage::Progress{name, progress, max}) => {
                        assert!(info.tasks.contains_key(& name) == true, "Task does not exist");
                        let task = info.tasks.get_mut(& name).unwrap();    
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
            self.status(& info);
            // retire errored tasks that are too old
            info.tick();

            // sleep a second or whatever is needed
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }

    fn status(& self, info : & ReporterInfo) {
        let _g = self.cout_lock.lock().unwrap();
        print!("\x1b7"); // save cursor
        print!("\x1b[H"); // set cursor to top left corner
        print!("\x1b[104;97m"); // set white on blue background
        // the header 
        let mut queue_size = 0;
        {
            let threads = self.iupdater.lock().unwrap();
            println!("{} DCD v3 (datastore version {}), uptime [ {} ], threads [ {}r, {}i, {}p ], status: [ {} ] \x1b[K",
                info.get_tick_symbol(), 
                Datastore::VERSION, 
                helpers::pretty_time(helpers::now() - info.start_time), 
                threads.running_workers, threads.idle_workers, threads.paused_workers, 
                threads.status());
            queue_size = threads.queue.len();
        }
        // datastore size header
        println!("  Datastore: \x1b[K");
        // server health
        println!("  Health: \x1b[K");
        // tasks summary
        print!("\x1b[6H\x1b[104m");
        println!(" tick [ {}a, {}d, {}e ] total [ {}d, {}e ] queue [{}]\x1b[K",
            info.tasks.len(), info.tick_tasks_done, info.tick_tasks_error,
            helpers::pretty_value(info.total_tasks_done), helpers::pretty_value(info.total_tasks_error),
            helpers::pretty_value(queue_size)
        );
        // tasks detail
        // TODO
        print!("\x1b[m"); // reset attributes
        print!("\x1b8"); // restore cursor
        stdout().flush().unwrap();
    }

    /** The user interface and controller. 
     */
    fn controller(& self, initial_command : String) {
        if ! initial_command.is_empty() {
            self.process_command(initial_command);
        } else {
            self.display_prompt("ready...");
        }
        loop {
            // the controller breaks immediately after issuing the stop command so that it does not enter into the waiting prompt
            {
                let threads = self.iupdater.lock().unwrap();
                if threads.state == State::Stopped {
                    break;
                }
            }
            let mut command = String::new();
            match std::io::stdin().read_line(& mut command) {
                Ok(_) => {
                    self.process_command(command);
                },
                Err(e) => {
                    self.display_prompt(& format!("Unknown error: {:?}", e));
                }
            }
        }
        self.display_prompt("Controller thread terminated. Command interface not available");
    }

    fn display_prompt(& self, command_output : & str) {
        let _g = self.cout_lock.lock().unwrap();
        print!("\x1b[4;H\x1b[0m > \x1b[K\n");  
        print!("\x1b[90m    {}\x1b[K", command_output);  
        print!("\x1b[m\x1b[4;4H");
        stdout().flush().unwrap();
    }

    fn process_command(& self, command : String) {
        match command.trim() {
            "pause" => {
                let mut threads = self.iupdater.lock().unwrap();
                threads.state = State::Paused;
                self.cv_workers.notify_all();
                self.display_prompt("Pausing threads...");
            },
            "stop" => {
                let mut threads = self.iupdater.lock().unwrap();
                threads.state = State::Stopped;
                self.cv_workers.notify_all();
                self.display_prompt("Stopping threads...");
            },
            "run" => {
                let mut threads = self.iupdater.lock().unwrap();
                threads.state = State::Running;
                self.cv_workers.notify_all();
                self.display_prompt("Resuming worker threads...");
            }
            /* Kill immediately aborts the entire process. 
               
               It goes without saying that this should be used only sparingly and that issuing the command is likely to have dire consequences for the integrity of the datastore. 
             */
            "kill" => {
                print!("\x1b[?1049l"); // return to normal mode
                print!("\x1b[r"); // reset scroll region
                println!("ERROR: kill command issued. Terminating immediately. Datastore might be corrupted !!!");
                std::process::abort();
            }

            _ => {
                self.display_prompt(& format!("Unknown command: {}", command));
            }
        }
    }

    /*
    fn update_project(& self, _task : QueuedProject) -> Result<(), std::io::Error> {
        unimplemented!();
    } */
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
            state : State::Paused,
            running_workers : 0,
            idle_workers : 0,
            paused_workers : 0,
            queue : BinaryHeap::new()
        };
    }
    fn is_paused(& self) -> bool {
        return self.running_workers == 0 && self.idle_workers == 0;
    }

    fn is_stopped(& self) -> bool {
        return (self.running_workers + self.idle_workers + self.paused_workers == 0) && self.state == State::Stopped;
    }

    fn status(& self) -> &'static str {
        match self.state {
            State::Running => {
                return "running";
            },
            State::Paused => {
                if self.is_paused() {
                    return "paused";
                } else {
                    return "pausing";
                }
            },
            State::Stopped => {
                if self.is_stopped() {
                    return "stopped";
                } else {
                    return "stopping";
                }
            }
        }
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

struct ReporterInfo {
    start_time : i64,
    tasks : HashMap<String, TaskInfo>,
    errors : VecDeque<(TaskInfo, String)>,
    tick_num : u8, 
    tick_tasks_done : usize,
    tick_tasks_error : usize,
    total_tasks_done : usize,
    total_tasks_error : usize
}

impl ReporterInfo {
    fn new() -> ReporterInfo {
        return ReporterInfo {
            start_time : helpers::now(),
            tasks : HashMap::new(),
            errors : VecDeque::new(),
            tick_num : 0,
            tick_tasks_done : 0,
            tick_tasks_error : 0,
            total_tasks_done : 0,
            total_tasks_error : 0,
        };
    }

    fn get_tick_symbol(& self) -> &'static str {
        match self.tick_num {
            0 => "-",
            1 => "\\",
            2 => "|",
            3 => "/",
            _ => "X"
        }
    }

    fn tick(& mut self) {
        self.tick_num = ( self.tick_num + 1) % 4;
        self.total_tasks_done += self.tick_tasks_done;
        self.total_tasks_error += self.tick_tasks_error;
        self.tick_tasks_done = 0;
        self.tick_tasks_error = 0;
    }
}







