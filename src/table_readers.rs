use crate::savepoints::*;

use crate::table_writer::*;



/** An indexer of append only table's contents. 
 
    The indexer facilitates the random access to the stored contents by keeping for each id the last offset at which the id's value is set. Indexed readers only work up to a given savepoint limit. 

    Note that to be usable, the indexer assumes continous ids to be present in the underlying table, otherwise there will be large holes in the index file making it very inefficient.

    TODO the indexfile can be memory mapped for increased speed is my thinking. Arguably so could the actual datastore file? 
 */
struct IndexedReader<RECORD : TableRecord> {

    why_oh_why : std::marker::PhantomData<RECORD>
}

impl<RECORD : TableRecord> IndexedReader<RECORD> {

    pub fn has(& mut self, id : & RECORD::Id) -> bool {
        unimplemented!();
    }

    pub fn get(& mut self, id : & RECORD::Id) -> Option<RECORD::Value> {
        unimplemented!();
    }

    /** Number of indexed entries. 
     */
    pub fn len(& self) -> usize {
        unimplemented!();
    }



    /** Builds the index file for the table.
     */
    fn create(table_root : & str, index_root : & str, savepoint : & Savepoint) -> usize {
        let index_filename = format!("{}/{}.index", index_root, RECORD::TABLE_NAME);
        let mut index = Vec::<u64>::new();
        let mut iter = TableIterator::<RECORD>::for_savepoint(table_root, savepoint);
        let mut unique_ids = 0;
        loop {
            let offset = iter.offset();
            if let Some((id, _)) = iter.next() {
                let idx = id.to_number() as usize;
                while index.len() <= idx {
                    index.push(u64::MAX);
                }
                if index[idx] == u64::MAX {
                    unique_ids += 1;
                }
                index[idx] = offset;
            } else {
                break;
            }
        }
        // we have the index built, time to save it
        // TODO
        unimplemented!();
        return unique_ids;
    }


}
