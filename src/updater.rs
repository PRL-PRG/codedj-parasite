use crate::db::*;
use crate::datastore::*;

/* This is the updater. 

   Manage the workers and the 
 */
struct Updater {
    ds : Datastore,
}

impl Updater {

    pub new(datastore : Datastore) -> Updater {
        return Updater{
            ds : datastore,
        };
    } 

    /** Updates the given project. 
     */
    fn update_project(& self, id : u64) {
        // if there is no url, it means the project is dead and should not be updated
        if let mut Some(url) = self.ds.get_project_url(id) {
            
            let last_heads = self.ds.get_project_heads(id).or(Heads::new()).unwrap();

        }
    }

    fn update_github_project(& self, id : u64, url : & str) {
        if !url.starts_with("https://github.com") {
            return;
        }

    }

}




