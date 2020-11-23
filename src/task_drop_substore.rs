use crate::updater::*;
use crate::records::*;

pub (crate) fn task_drop_substore(updater : & Updater, store : StoreKind,  task : TaskStatus) -> Result<(), std::io::Error> {
    updater.ds.substore(store).clear(& task);
    return Ok(());
}