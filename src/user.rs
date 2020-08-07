use crate::*;
/** User information
 
    Users are identified by their email. This is not completely precise as single email can have different names in different repositories, but for most purposes, the database is after the identity of a user, not their name. 

    In the future, if names for users are desired, we can add new table to the downloader, add metadata to users, etc. 

 */
#[derive(Clone)]
pub struct User {
    // id of the user
    pub id : UserId,
    // email for the user
    pub email : String,
    // name of the user
    pub name : String, 
}

impl User {

    pub(crate) fn write_to_csv(& self, f : & mut File) {
        writeln!(f, "{},\"{}\",\"{}\"", self.id, self.email, self.name).unwrap();
    }

}
