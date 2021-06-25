use crate::updater::*;
use crate::records::*;
use crate::db::*;

/** Task that does an update of a given substore. 
 
    First the substore is loaded, then its own and unspecified projects are scheduled and then the task waits for completion of the scheduled queue and monitor the health of the datastore. 
 */
pub (crate) fn task_update_substore(updater : & Updater, store : StoreKind, mode : UpdateMode, task : TaskStatus) -> Result<(), std::io::Error> {
    // load the substore
    updater.ds.substore(store).load(& task);
    let mut num_projects = 0;
    // schedule all projects
    {
        let total_projects = updater.ds.num_projects();
        task.info("scheduling projects...");
        task.progress(0, total_projects);
        let mut i = 0;
        while i < total_projects {
            let id = ProjectId::from(i as u64);
            let pstore = updater.ds.get_project_substore(id);
            // errors take *all* stores at once, and updates if the store is loaded
            if pstore == store || pstore == StoreKind::None || mode == UpdateMode::Errors {
                // its a possibly valid project, so determine the last time it was updated
                if let Some(last_update) = updater.ds.get_project_last_update(id) {
                    if ! last_update.is_error() || mode == UpdateMode::Errors {
                        updater.schedule(Task::UpdateRepo{id, last_update_time : last_update.time()});
                        num_projects += 1;
                    }
                } else {
                    if mode != UpdateMode::Errors {
                        updater.schedule(Task::UpdateRepo{id, last_update_time : 0});
                        num_projects += 1;
                    }
                }
            }
            i += 1;
            if i % 1000 == 0 {
                task.progress(i, total_projects);
            }
        }
    }
    // observe the update progress and report the state, in the future also observe the datastore & updater health and manage substores. 
    // we determine that the update has finished when the queue is empty and all threads but one are idle
    {
        task.info("Updating projects...");
        task.progress(0, num_projects);
        loop {
            let progress;
            {
                let pool = updater.pool.lock().unwrap();
                if pool.running_workers == 1 && pool.queue.is_empty() {
                    break;
                }
                progress = num_projects - pool.queue.len();
            }
            task.progress(progress, num_projects);
            // and sleep for a second
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }
    // now that we have finished we can start update of other datastore. Technically we can do this earlier too, as long as the queue is empty and there are some idle threads, but that would require the necessity to have two substore mappings loaded in memory which we want to avoid. So this is less efficient but more robust solution
    if mode != UpdateMode::Single {
        let mut next_substore = StoreKind::from_number(store.to_number() + 1);
        if next_substore == StoreKind::None && mode == UpdateMode::Continuous {
            next_substore = StoreKind::from_number(0);
        }
        if next_substore != StoreKind::None && mode != UpdateMode::Errors {
            updater.schedule(Task::UpdateSubstore{store : next_substore, mode});
        }
    }
    return Ok(());
}
