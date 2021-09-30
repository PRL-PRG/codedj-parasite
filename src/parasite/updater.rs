use std::sync::atomic::{AtomicBool, Ordering};


use parasite::codedj::*;

/** The incremental updater. 
 
    V3 changes : use local git instance in own process to clone repositories as opposed to libgit so that we can timeout the downloads. 


 
 */
pub struct Updater {

    /** The datastore the updater operates on.
     * 
     */
    cdj : CodeDJ,

    /** Signals the worker threads that the updater is going to die and they should terminate. 
     */
    terminate: AtomicBool, 

}


impl Updater {


    /** Returns truf if the workers should terminate immediately.
     */
    pub fn should_terminate(& self) -> bool { self.terminate.load(Ordering::SeqCst) }

}

