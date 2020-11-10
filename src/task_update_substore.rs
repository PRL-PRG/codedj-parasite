use crate::updater::*;
use crate::records::*;

/** Task that does an update of a given substore. 
 
    First the substore is loaded, then its own and unspecified projects are scheduled and then the task waits for completion of the scheduled queue and monitor the health of the datastore. 
 */
pub (crate) fn task_update_substore(updater : & Updater, store : StoreKind,  task : TaskStatus) -> Result<(), std::io::Error> {
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
            let id = i as u64;
            let pstore = updater.ds.get_project_substore(id);
            if pstore == store || pstore == StoreKind::Unspecified {
                // its a possibly valid project, so determine the last time it was updated
                let mut last_update_time = 0;
                if let Some(last_update) = updater.ds.get_project_last_update(id) {
                    last_update_time = last_update.time();
                }
                updater.schedule(Task::UpdateRepo{id, last_update_time});
                num_projects += 1;
            }
            i += 1;
            if i % 1000 == 0 {
                task.progress(i, total_projects);
            }
        }
    }
    // observe the update progress and report the state, in the future also observe the datastore & updater health and manage substores
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
            // TODO check updater's health and deal with the consequences

            // and sleep for a second
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }
    return Ok(());
}
