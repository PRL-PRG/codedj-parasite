use std::collections::HashMap;
use std::sync::Mutex;
use crate::updater::*;

pub type Tx = crossbeam_channel::Sender<TaskMessage>;
pub type Rx = crossbeam_channel::Receiver<TaskMessage>;

/** Task progress reporter tailored to the single task terminal output of the non-interactive mode. 
 
 */
pub struct TerminalReporter {
    tx : Tx,
    rx : Rx,
    tasks : Mutex<HashMap<String, String>>,
}

impl TerminalReporter {

    pub fn report<F : FnMut(& TerminalReporter)>(mut f : F) {  
        let (tx, rx) = crossbeam_channel::unbounded::<TaskMessage>();
        let reporter = TerminalReporter{
            tx,
            rx,
            tasks : Mutex::new(HashMap::new()),
        };
        crossbeam::thread::scope(|s| {
            s.spawn(|_| {
                reporter.reporter();
            });
            f(& reporter);
        }).unwrap();
        println!("\x1b[0m"); // reset the color when done
    }

    pub fn run_task<F : FnMut(TaskStatus) -> Result<(), std::io::Error>>(& self, task : Task, mut f : F) {
        let task_name = task.name();
        if self.tasks.lock().unwrap().insert(task.name(), "\x1b[0m".to_owned()).is_some() {
            panic!("Task {} already exists", task.name());
        }
        let ts = TaskStatus::new(& self.tx, task);
        match f(ts) {
            Ok(()) => {
                self.tx.send(TaskMessage::Done{ name : task_name }).unwrap();
            },
            Err(cause) => {
                self.tx.send(TaskMessage::Error{ name : task_name, cause : format!("{}", cause).trim().to_owned() }).unwrap();
            },
        }
    }

    fn reporter(& self) {
        while let Ok(msg) = self.rx.recv() {
            match msg {
                TaskMessage::Start{name} => {
                    self.report_message(& name, format!("starting..."));
                },
                TaskMessage::Done{name} => {
                    self.report_message(& name, format!("DONE."));
                    let mut tasks = self.tasks.lock().unwrap();
                    tasks.remove(& name);
                    if tasks.is_empty() {
                        break;
                    }
                },
                TaskMessage::Error{name, cause} => {
                    self.report_message(& name, format!("ERROR: {}", cause));
                    let mut tasks = self.tasks.lock().unwrap();
                    tasks.remove(& name);
                    if tasks.is_empty() {
                        break;
                    }
                },
                TaskMessage::Progress{name : _, progress : _, max : _} => {
                    // don't do anything 
                },
                TaskMessage::Info{name, info} => {
                    self.report_message(& name, info);
                },
                TaskMessage::Extra{name, extra} => {
                    self.report_message(& name, extra);
                },
                TaskMessage::Color{name, color} => {
                    let mut tasks = self.tasks.lock().unwrap();
                    *tasks.get_mut(& name).unwrap() = color;
                }
            }
        }
    }

    fn report_message(& self, task_name : & str, what : String) {
        // set the color
        let tasks = self.tasks.lock().unwrap();
        print!("{}", tasks[task_name]);
        if tasks.len() != 1 {
            print!("{}: ", task_name);
        }
        println!("{}", what);
    }
}

