  use crate::datastore::*;

/** Kinds of datastores that CodeDJ supports. 
 
    This list can be extended in time, each added language means a new folder in the CodeDJ super store that would contain a datastore for the specific kind of projects. 

    The expectation is that the datastor kinds will correspond to major programming languages used by the repositories contained within, but arguably, this does not have to be the case. 
    
    Another expectation is that a project exists in only one datastore within CodeDJ at a time, but technically, this is not necessary, all that codeDJ really should guarantee is that project ids are unique across all datastores. 
 */
pub enum DatastoreKind {
    C,
    Cpp,
    CSharp,
    Clojure,
    CoffeeScript,
    Erlang,
    Go,
    Haskell,
    Html,
    Java,
    Julia,
    JavaScript,
    ObjectiveC,
    Perl,
    Php,
    Python,
    R,
    Ruby,
    Scala,
    Shell,
    TypeScript,    
}

/** CodeDJ manages the set of language-specific datastores and their bookkeeping as well as bookkeeping from other actors, such as the downloader. 
 
    
 */
struct CodeDJ {



}

impl CodeDJ {

    pub fn datastore<'a>(&'a mut self, _kind : DatastoreKind) -> &'a Datastore {
        unimplemented!();
    }


}