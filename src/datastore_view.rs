
use crate::serialization::*;
use crate::table_writers::*;
//use crate::table_readers::*;
use crate::savepoints::*;

/** A view into an append-only table as a trait that is presented to the users od the API. 
 
    This is to be as compatible with V3 as possible, but minor changes are inevitable. 
 
 */
pub trait TableView : IntoIterator<Item = (Self::Id, Self::Value)> {
    type Id : Id;
    type Value : Serializable;

    fn get(& mut self, id : & Self::Id) -> Option<Self::Value>;
}

/** Datastore viewer.
 
    Very much different from the datastore itself the viewer provides a multi-threaded read-only access to existing datastore at a given savepoint. The viewer keeps indices for read-only or latest-only information access to the underlying datastore. These indices are created and cached on demand. 

    The datastore view can be copied & moved around in its entirety as long as the absolute path to the original dataset remains the same.
    
    The V3 API we should support-ish:

    projects -> Table
    project_heads -> Iterator
    project_logs -> Iterator

    commits -> Table
    hashes -> Table
    contents -> Table
    paths -> Table
    users -> Table


    Iterator simply iterates, Table allows random access to latest values and can transform itself into an iterator...
 */
struct DatastoreView {

    /** The folder in which the original datastore lives. Read access is required to this folder as the datastore view never modifies the underlying datastore. 
     */
    datastore_root : String, 

    /** The view folder, where the information about the actual view is stored. Write access to this folder is required as the datastore view keeps the generated indices and other cached information in this folder. Once the datastore view is created, this is the only argument required for its reuse. */
    view_root : String, 

    /** The savepoint associated with the view. Stored in the view_root as well. 
     */
    savepoint : Savepoint, 



}

impl DatastoreView {

}
