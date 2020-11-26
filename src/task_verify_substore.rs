use crate::updater::*;
use crate::records::*;

pub (crate) fn task_verify_substore(updater : & Updater, store : StoreKind,  task : TaskStatus) -> Result<(), std::io::Error> {
    // load the substore
    updater.ds.substore(store).load(& task);
    let substore = updater.ds.substore(store);
    substore.load(& task);
    task.progress(0, 10);
    return Ok(());
}