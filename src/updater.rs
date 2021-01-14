use std::collections::*;
use std::sync::*;
use std::io::{Write, stdout};
//use sysinfo::{SystemExt, ProcessExt};


use crate::datastore::*;
use crate::records::*;
use crate::github::*;
use crate::helpers;
use crate::db::*;

use crate::task_add_projects::*;
use crate::task_update_repo::*;
use crate::task_update_substore::*;
use crate::task_load_substore::*;
use crate::task_drop_substore::*;
use crate::task_verify_substore::*;

use crate::settings::Settings;

pub type Tx = crossbeam_channel::Sender<TaskMessage>;

/** Convenience struct that brings together the tx end of a channel, task name and task itself and exposes the sending of task messages via a simple api. 
 */
pub struct TaskStatus<'a> {
    pub tx : &'a Tx,
    pub name : String,
    pub task : Task,
}

impl<'a> TaskStatus<'a> {
    pub fn new(tx : &'a Tx, task : Task) -> TaskStatus {
        return TaskStatus {
            tx : tx, 
            name : task.name(),
            task : task
        };
    }

    pub fn info<S: Into<String>>(& self, info : S) {
        self.tx.send(TaskMessage::Info{name : self.name.to_owned(), info : info.into() }).unwrap();
    }

    pub fn extra<S: Into<String>>(& self, extra : S) {
        self.tx.send(TaskMessage::Extra{name : self.name.to_owned(), extra : extra.into() }).unwrap();
    }

    pub fn extra_url<S: Into<String>>(& self, extra : S, url : S) {
        self.extra(format!("\x1b]8;;{}\x07{}\x1b]8;;\x07", url.into(), extra.into()));
    }

    pub fn progress(& self, progress : usize, max : usize) {
        self.tx.send(TaskMessage::Progress{name : self.name.to_owned(), progress, max}).unwrap();
    }

    pub fn color(& self, color : & str) {
        self.tx.send(TaskMessage::Color{name : self.name.to_owned(), color : color.to_owned()}).unwrap();
    }

}


/** The updater. 

    

 */
pub (crate) struct Updater {

    /** The datastore on which the updater operates. 
     */
    pub (crate) ds : Datastore,

    pub (crate) github : Github,

    /** Incremental updater
     */
    num_workers : usize, 
    pub (crate) pool : Mutex<Pool>,
    cv_workers : Condvar,

    /** List of all urls of projects in the datastore so that new projects can be checked against duplicates. 
     */
    pub (crate) project_urls : Mutex<HashSet<Project>>,


    /** Mutex to guard console output.
     */
    cout_lock : Mutex<()>,
}

impl Updater {

    pub const NEVER : i64 = 0;

    /** Updater is initialized with an existing datastore. 
     */
    pub fn new(ds : Datastore, settings : & Settings) -> Updater {
        return Updater {
            ds, 
            github : Github::new(& settings.github_tokens),
            num_workers : settings.num_threads,
            pool : Mutex::new(Pool::new()),
            cv_workers : Condvar::new(),

            project_urls : Mutex::new(HashSet::new()),

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
                    self.worker(tx.clone());
                });
            }
        }).unwrap();
        print!("\x1b[?1049l"); // return to normal mode
        print!("\x1b[r"); // reset scroll region
        println!("Updater terminated.");
    }

    /** 
     
        
     */
    fn worker(& self, tx : crossbeam_channel::Sender<TaskMessage>) {
        self.pool.lock().unwrap().running_workers += 1;
        while let Some(task) = self.get_next_task() {
            let task_name = task.name();
            tx.send(TaskMessage::Start{name : task_name.to_owned()}).unwrap();
            let result = std::panic::catch_unwind(|| {
                match task {
                    Task::UpdateRepo{last_update_time : _, id : _ } => {
                        return task_update_repo(self, TaskStatus::new(& tx, task));
                    }
                    Task::AddProjects{ref source} => {
                        return task_add_projects(self, source.to_owned(), TaskStatus::new(& tx, task));
                    },
                    Task::UpdateSubstore{store, mode} => {
                        return task_update_substore(self, store, mode, TaskStatus::new(& tx, task));
                    }, 
                    Task::LoadSubstore{store} => {
                        return task_load_substore(self, store, TaskStatus::new(& tx, task));
                    },
                    Task::DropSubstore{store} => {
                        return task_drop_substore(self, store, TaskStatus::new(& tx, task));
                    }
                    Task::VerifySubstore{store, mode} => {
                        return task_verify_substore(self, store, mode, TaskStatus::new(& tx, task));
                    }
                    Task::VerifyDatastore{} => {
                        return task_verify_datastore(self, TaskStatus::new(& tx, task));
                    }
                }
            });
            match result {
                Ok(Ok(())) => {
                    tx.send(TaskMessage::Done{ name : task_name }).unwrap();
                },
                Ok(Err(cause)) => {
                    tx.send(TaskMessage::Error{ name : task_name, cause : format!("{}", cause).trim().to_owned() }).unwrap();
                },
                Err(cause) => {
                    tx.send(TaskMessage::Error{ name : task_name, cause : format!("PANIC: {:?}", cause) }).unwrap();
                }
            }
        }
        self.pool.lock().unwrap().running_workers -= 1;
    }

    /** Returns the next project to be updated. 
     
        Returns None if the updater should stop and blocks if there are no avilable projects, or the updater should pause. 
     */
    fn get_next_task(& self) -> Option<Task> {
        let mut state = self.pool.lock().unwrap();
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

    pub fn schedule(& self, task : Task) {
        let mut pool = self.pool.lock().unwrap();
        pool.queue.push(task);
        self.cv_workers.notify_one();
    }

    /** Returns true if the non-worker thread should stop immediately, false otherwise. 
     
        Non worker threads are required to stop immediately after al worker threads are done. 
     */
    pub fn should_stop(& self) -> bool {
        let state = self.pool.lock().unwrap();
        return state.is_stopped();
    }

    /** Prints the status of the update process. 
     */
    fn reporter(& self, rx : crossbeam_channel::Receiver<TaskMessage>) {
        let mut rinfo = ReporterInfo::new();
        while ! self.should_stop() {
            // see how many messages are there and process them, otherwise we can just keep processing messages without ever printing anything 
            let mut msgs = rx.len();
            while msgs > 0 {
                match rx.recv() {
                    Ok(TaskMessage::Start{name}) => {
                        assert!(rinfo.tasks.contains_key(& name) == false, "Task already exists");
                        rinfo.tasks.insert(name, TaskInfo::new());
                    },
                    Ok(TaskMessage::Done{name}) => {
                        assert!(rinfo.tasks.contains_key(& name) == true, "Task does not exist");
                        let mut task = rinfo.tasks.remove(& name).unwrap();
                        task.end_time = helpers::now();
                        rinfo.done.push((name, task));
                        rinfo.tick_tasks_done += 1;
                    },
                    Ok(TaskMessage::Error{name, cause}) => {
                        assert!(rinfo.tasks.contains_key(& name) == true, "Task does not exist");
                        let mut task = rinfo.tasks.remove(& name).unwrap();
                        task.end_time = helpers::now();
                        rinfo.errors.push((name, task, cause));
                        rinfo.tick_tasks_error += 1;
                    },
                    Ok(TaskMessage::Progress{name, progress, max}) => {
                        assert!(rinfo.tasks.contains_key(& name) == true, "Task does not exist");
                        let task = rinfo.tasks.get_mut(& name).unwrap();    
                        task.ping = 0;
                        task.progress = progress;
                        task.progress_max = max;
                    },
                    Ok(TaskMessage::Info{name, info}) => {
                        assert!(rinfo.tasks.contains_key(& name) == true, "Task does not exist");
                        let task = rinfo.tasks.get_mut(& name).unwrap();    
                        task.ping = 0;
                        task.info = info;
                    },
                    Ok(TaskMessage::Extra{name, extra}) => {
                        assert!(rinfo.tasks.contains_key(& name) == true, "Task does not exist");
                        let task = rinfo.tasks.get_mut(& name).unwrap();    
                        task.ping = 0;
                        task.extra = extra;
                    },
                    Ok(TaskMessage::Color{name, color}) => {
                        assert!(rinfo.tasks.contains_key(& name) == true, "Task does not exist");
                        let task = rinfo.tasks.get_mut(& name).unwrap();    
                        task.ping = 0;
                        task.color = color;
                    },
                    Err(_) => {
                        panic!("Oh noez, can't receive stuff");
                    }
                }
                msgs -= 1;
            }
            // now that the messages have been processed, redraw the status information
            self.status(& rinfo);
            // retire errored tasks that are too old
            rinfo.tick();

            // sleep a second or whatever is needed
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }

    fn num_projects(& self) -> usize {
        return self.ds.projects.lock().unwrap().len();
    }

    fn status(& self, info : & ReporterInfo) {
        let _g = self.cout_lock.lock().unwrap();
        print!("\x1b7"); // save cursor
        print!("\x1b[H"); // set cursor to top left corner
        print!("\x1b[104;97m"); // set white on blue background
        // the header 
        let queue_size;
        {
            let threads = self.pool.lock().unwrap();
            println!("{} DCD v3 (datastore version {}), uptime [ {} ], threads [ {}r, {}i, {}p ], status: [ {} ] \x1b[K",
                info.get_tick_symbol(), 
                Datastore::VERSION, 
                helpers::pretty_duration(helpers::now() - info.start_time), 
                threads.running_workers, threads.idle_workers, threads.paused_workers, 
                threads.status());
            queue_size = threads.queue.len();
        }
        // datastore header
        let mut loaded = self.ds.project_urls_memory_report();
        for substore in self.ds.substores_iter()  {
            let x = substore.memory_report();
            if ! x.is_empty() {
                loaded = format!("{} {}", loaded, x);
            }
        }
        println!("  Datastore: [{}p], up [ {} ]\x1b[K",
            helpers::pretty_value(self.ds.num_projects()),
            loaded
        );
        // server health
        // TODO get this from the process tables instead
        // add disk info for temp and for datastore
        let (pid, mem, cpu) = helpers::process_resources();
        println!("  Health: pid: {}, [cpu: {}%], [mem:{}%] \x1b[K",
            pid,
            cpu,
            mem,
        );

        // tasks summary
        print!("\x1b[6H\x1b[104m");
        println!(" tick [ {}a, {}d, {}e ] total [ {}d, {}e ] queue [{}]\x1b[K",
            info.tasks.len(), info.tick_tasks_done, info.tick_tasks_error,
            helpers::pretty_value(info.total_tasks_done), helpers::pretty_value(info.total_tasks_error),
            helpers::pretty_value(queue_size)
        );
        // details for running tasks, ordered by their start time
        {
            let mut tasks : Vec<(& String, & TaskInfo)> = info.tasks.iter().collect();
            tasks.sort_by(|a, b| a.1.start_time.cmp(& b.1.start_time));
            let mut odd = true;
            for (name, task) in tasks {
                let mut color = task.color.as_str();
                if task.ping >= 10 {
                    color = "\x1b[48;2;255;165;0m";
                }
                if color.is_empty() {
                    color = if odd { "\x1b[48;2;0;0;0m" } else { "\x1b[48;2;48;48;48m" };
                }
                print!("{}", color);
                odd = ! odd;
                task.print(name);
            }
        }
        // print error one-liners
        if ! info.errors.is_empty() {
            print!("\x1b[48;2;128;0;0mErrors ({}):\x1b[K\x1b[0m\n", info.errors.len());
            for (_, task, cause) in info.errors.iter() {
                print!("{}: {} {},", task.extra, task.info, cause);
            }
            print!("\x1b[K\n");
    
        }
        // print done one-liners
        if ! info.done.is_empty() {
            print!("\x1b[48;2;0;128;0mDone ({}):\x1b[K\x1b[0m\n", info.done.len());
            for (_, task) in info.done.iter() {
                print!("{}{}: {}, ", task.color, task.extra, task.info);
            }
            print!("\x1b[K\n");
        }
        print!("\x1b[m"); // reset attributes
        print!("\x1b[J"); // clear the rest of the screen
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
                let threads = self.pool.lock().unwrap();
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

    fn display_prompt<T: Into<String>>(& self, command_output : T) {
        let _g = self.cout_lock.lock().unwrap();
        print!("\x1b[4;H\x1b[0m > \x1b[K\n");  
        print!("\x1b[90m    {}\x1b[K", command_output.into());  
        print!("\x1b[m\x1b[4;4H");
        stdout().flush().unwrap();
    }

    fn display_error<T: Into<String>>(& self, error : T) {
        self.display_prompt(& format!("ERROR: {}", error.into()));
    }

    fn process_command(& self, command : String) {
        let cmd : Vec<&str> = command.trim().split(" ").collect();
        match cmd[0] {
            "pause" => {
                {
                    let mut threads = self.pool.lock().unwrap();
                    threads.state = State::Paused;
                    self.cv_workers.notify_all();
                }
                self.display_prompt("Pausing threads...");
            },
            "stop" => {
                {
                    let mut threads = self.pool.lock().unwrap();
                    threads.state = State::Stopped;
                    self.cv_workers.notify_all();
                }
                self.display_prompt("Stopping threads...");
            },
            "run" => {
                {
                    let mut threads = self.pool.lock().unwrap();
                    threads.state = State::Running;
                    self.cv_workers.notify_all();
                }
                self.display_prompt("Resuming worker threads...");
            }, 
            /* Updates project belonging to the given substore . 
             */
            "update" => {
                if cmd.len() != 2 {
                    self.display_error("No store to update specified");
                } else if let Some(kind) = StoreKind::from_string(cmd[1]) {
                    self.schedule(Task::UpdateSubstore{store : kind, mode : UpdateMode::Single});
                    self.display_prompt(format!("Updating substore {:?}, see task progress...", kind));
                } else {
                    self.display_error(format!("Unknown store kind {}", cmd[1]));
                }
            },
            /* Updates all projects once substore by substore. 
             */
            "updateall" => {
                if cmd.len() != 1 {
                    self.display_error("Invalid arguments");
                } else {
                    self.schedule(Task::UpdateSubstore{store : StoreKind::from_number(0), mode : UpdateMode::All});
                    self.display_prompt("Updating all substores , see task progress...");
                }
            },
            /* Continuously updates all projects store by store
             */
            "updatecontinuous" => {
                if cmd.len() != 1 {
                    self.display_error("Invalid arguments");
                } else {
                    self.schedule(Task::UpdateSubstore{store : StoreKind::from_number(0), mode : UpdateMode::Continuous});
                    self.display_prompt("Updating all substores , see task progress...");
                }
            },
            /* Adds given project url, or projects from given csv file. 
             */
            "add" => {
                if cmd.len() != 2 {
                    self.display_error("Specify single project url or csv file to load the projects from");
                } else {
                    self.display_prompt("Adding projects to datastore, see task progress...");
                    self.schedule(Task::AddProjects{ source : cmd[1].to_owned() });
                }
            },
            /* Loads given substore in memory. 
             */
            "load" => {
                if cmd.len() != 2 {
                    self.display_error("No store to load specified");
                } else if let Some(kind) = StoreKind::from_string(cmd[1]) {
                    self.schedule(Task::LoadSubstore{store : kind});
                    self.display_prompt(format!("Loading substore {:?}, see task progress...", kind));
                } else {
                    self.display_error(format!("Unknown store kind {}", cmd[1]));
                }
            },
            "drop" => {
                if cmd.len() != 2 {
                    self.display_error("No store to drop specified");
                } else if let Some(kind) = StoreKind::from_string(cmd[1]) {
                    self.schedule(Task::DropSubstore{store : kind});
                    self.display_prompt(format!("Dropping substore {:?}, see task progress...", kind));
                } else {
                    self.display_error(format!("Unknown store kind {}", cmd[1]));
                }
            },
            "loadall" => {
                for kind in SplitKindIter::<StoreKind>::new() {
                    self.display_prompt("Loading all substores, see task progress...");
                    self.schedule(Task::LoadSubstore{store : kind});
                }
            }
            "verify" => {
                if cmd.len() != 2 {
                    self.display_error("No store to verify specified");
                } else if let Some(kind) = StoreKind::from_string(cmd[1]) {
                    self.schedule(Task::VerifySubstore{store : kind, mode : UpdateMode::Single});
                    self.display_prompt(format!("Verifying substore {:?}, see task progress...", kind));
                } else {
                    self.display_error(format!("Unknown store kind {}", cmd[1]));
                }
            },
            "verifyall" => {
                if cmd.len() != 1 {
                    self.display_error("Invalid arguments");
                } else {
                    self.schedule(Task::VerifySubstore{store : StoreKind::from_number(0), mode : UpdateMode::All});
                    self.display_prompt("Verifying all substores, see task progress...");
                }
            },
            "verifyds" => {
                if cmd.len() != 1 {
                    self.display_error("Invalid arguments");
                } else {
                    self.schedule(Task::VerifyDatastore{});
                    self.display_prompt("Verifying main datastore, see task progress...");
                }
            },
            "savepoint" => {
                if cmd.len() != 2 {
                    self.display_error("Invalid arguments");
                } else {
                    let sp = self.ds.create_savepoint(cmd[1].to_owned(), true);
                    self.display_prompt(format!("Created savepoint {}, total size {}", sp.name(), helpers::pretty_size(sp.size())));
                }
            },
            // debug stuffz

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
                self.display_error(& format!("Unknown command: {}", command));
            }
        }
    }

    /** Loads the project urls. 
     */
    pub (crate) fn load_project_urls(& self, task_name : & str, tx : & Tx) {
        let mut urls = self.project_urls.lock().unwrap();
        if urls.is_empty() {
            for (_, p) in self.ds.projects.lock().unwrap().iter_all() {
                if urls.len() % 1000 == 0 {
                    tx.send(TaskMessage::Info{
                        name : task_name.to_owned(),
                        info : format!("loading datastore project urls ({}) ", helpers::pretty_value(urls.len()))
                    }).unwrap();
                }
                urls.insert(p);
            }
        }
    } 

    pub (crate) fn drop_project_urls(& self) {
        self.project_urls.lock().unwrap().clear();
    }

}

/** This is required so that we can pass updater across the catch_unwind barrier. 
 
    It's safe too, because the offending conditional variables in the updater are actually never accessed when the thread may panic, or during the unwinding. 
 */
impl std::panic::RefUnwindSafe for Updater { }


/** Determines the mode of the update. 
 */
#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub enum UpdateMode {
    Single,
    All,
    Continuous,
}

#[derive(Eq, PartialEq, Debug)] 
pub enum Task {
    UpdateRepo{id : ProjectId, last_update_time : i64},
    AddProjects{source : String},
    /** Updates projects that belong to the specific substore. 
     
        Also looks at all unspecified projects and assigns their store, updating those that belong to the provided store. 
     */
    UpdateSubstore{store: StoreKind, mode : UpdateMode},
    /** Loads given substore to memory.
     */
    LoadSubstore{store: StoreKind},
    /** Drops the given substore from memory. 
     */
    DropSubstore{store: StoreKind},
    VerifySubstore{store : StoreKind, mode : UpdateMode},
    VerifyDatastore{},
}

impl Task {
    pub fn priority(& self) -> i64 {
        match self {
            Task::UpdateRepo{last_update_time, id : _} => *last_update_time, 
            _ => -1,
        }
    }

    pub fn name(& self) -> String {
        match self {
            Task::UpdateRepo{id, last_update_time : _} => format!("{:?}", id),
            Task::AddProjects{source : _ } => "add".to_owned(), 
            Task::UpdateSubstore{store, mode} => format!("update {:?} {:?}", store, mode),
            Task::LoadSubstore{store} => format!("load {:?}", store),
            Task::DropSubstore{store} => format!("drop {:?}", store),
            Task::VerifySubstore{store, mode} => format!("verify {:?} {:?}", store, mode),
            Task::VerifyDatastore{} => format!("verify datastore"),
        }
    }
}

impl Ord for Task {
    fn cmp(& self, other : & Self) -> std::cmp::Ordering {
        return self.priority().cmp(& other.priority()).reverse();
    }
}

impl PartialOrd for Task {
    fn partial_cmp(& self, other : & Self) -> Option<std::cmp::Ordering> {
        return Some(self.priority().cmp(& other.priority()));
    }
}





/** Main structure for the incremental updater part of the downloader. 
 
    Grouped together because of how mutexes work in Rust. 
 */
pub (crate) struct Pool {
    pub (crate) state : State,
    pub (crate) running_workers : u64, 
    pub (crate) idle_workers : u64,
    pub (crate) paused_workers : u64,
    pub (crate) queue : BinaryHeap<Task>,
}

#[derive(Eq, PartialEq)]
pub (crate) enum State {
    Running,
    Paused,
    Stopped,
}

impl Pool {
    fn new() -> Pool {
        return Pool {
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
pub enum TaskMessage {
    Start{name : String},
    Done{name : String},
    Error{name : String, cause : String},
    Progress{name : String, progress : usize, max : usize },
    Info{name : String, info : String },
    Extra{name : String, extra : String },
    Color{name : String, color : String },
}

/** Task info as stored on the updater's end. 
 */
struct TaskInfo {
    start_time : i64,
    end_time : i64,
    progress : usize, 
    progress_max : usize, 
    ping : u64, 
    info : String,
    // extra string that can be displayed
    extra : String,
    // color to be printed before the task, if any
    color : String,
}

impl TaskInfo {
    fn new() -> TaskInfo {
        return TaskInfo{
            start_time : helpers::now(),
            end_time : 0,
            progress : 0, 
            progress_max : 0,
            ping : 0,
            info : String::new(),
            extra : String::new(),
            color : String::new(),
        };
    }

    /** Prints the task information. 
     */
    pub fn print(& self, name : & str) {
        println!(" {}: {} elapsed [ {} ], progress [ {}% ({}/{}) ]\x1b[K",
            name,
            self.extra,
            helpers::pretty_duration(helpers::now() - self.start_time),
            helpers::pct(self.progress, self.progress_max),
            self.progress,
            self.progress_max
        );
        if ! self.info.is_empty() {
            println!("    {}\x1b[K", self.info)
        }
    }
}

struct ReporterInfo {
    start_time : i64,
    tasks : HashMap<String, TaskInfo>,
    errors : Vec<(String, TaskInfo, String)>, // name, task, cause
    done : Vec<(String, TaskInfo)>, // name, task
    tick_num : u8, 
    tick_tasks_done : usize,
    tick_tasks_error : usize,
    total_tasks_done : usize,
    total_tasks_error : usize,
}

impl ReporterInfo {
    fn new() -> ReporterInfo {
        return ReporterInfo {
            start_time : helpers::now(),
            tasks : HashMap::new(),
            errors : Vec::new(),
            done : Vec::new(),
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

        // clear old errors and done tasks
        let time_now = helpers::now();
        self.errors.retain(|(_, task, _)| (time_now - task.end_time) < 10);
        self.done.retain(|(_, task)| (time_now - task.end_time) < 10);
        // increase tick for all tasks
        for (_, task) in self.tasks.iter_mut() {
            task.ping += 1;
        }

    }
}
