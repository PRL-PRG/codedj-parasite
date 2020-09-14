

/** The updater status structure that displays various information about the running updater. 
 
 */
pub struct Updater {


}

impl Updater {


}

/** Information about single work item. 
 */
pub struct UpdateRecord {
    pub start : i64, 
    pub progress : u64,
    pub progress_max : u64,
    pub status : String
}

impl UpdateRecord {
    /** Prints the update record. 
     */
    fn print(& self) {

    }

}