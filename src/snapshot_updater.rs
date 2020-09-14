use crate::datastore::*;
use crate::records::*;
use crate::helpers::*;

/** Goes over snapshots, and analyzes them. 
 
    This is because this is a completely local thread and therefore does not bother github bandwidth. Furthermore, in the future versions if more is required about snapshot, it's gpoing to be obtained aside from the repository updates.  
 */
pub struct SnapshotUpdater {

}