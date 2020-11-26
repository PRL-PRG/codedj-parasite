use crate::updater::*;
use crate::records::*;
use crate::helpers;
use crate::db::*;

pub (crate) fn task_verify_substore(updater : & Updater, store : StoreKind, mode : UpdateMode, task : TaskStatus) -> Result<(), std::io::Error> {
    // load the substore
    let substore = updater.ds.substore(store);
    match substore.verify(& task) {
        Ok(items) => {
            task.info(format!("{}", helpers::pretty_value(items)));
            task.extra(format!("{:?}", store));
            substore.clear(& task);
            verify_next(updater, store, mode);
            return Ok(());
        },
        Err(e) => {
            substore.clear(& task);
            verify_next(updater, store, mode);
            return Err(e);
        }
    }
}

pub (crate) fn task_verify_datastore(updater : & Updater, task : TaskStatus) -> Result<(), std::io::Error> {
    match updater.ds.verify(& task) {
        Ok(items) => {
            task.info(format!("{}", helpers::pretty_value(items)));
            task.extra("datastore");
            return Ok(());
        },
        Err(e) => {
            return Err(e);
        }
    }
}


fn verify_next(updater : & Updater, store : StoreKind, mode : UpdateMode) {
    if mode == UpdateMode::All {
        let next_substore = StoreKind::from_number(store.to_number() + 1);
        if next_substore != StoreKind::Unspecified {
            updater.schedule(Task::VerifySubstore{store : next_substore, mode});
        } else {
            updater.schedule(Task::VerifyDatastore{});
        }
    }
}