/** The database support.

    Provides serialization and deserialization of various structures used by the downloader and extra infrastructure for their efficiency, such as indexes and mappings. 

    Indexer = Writes stuff to file, 
    PropertyStore = Writes properties to file, can (but does not have to override)
    Mappings = also contains a hashmap for quick retrieval

 */
use std::fs::*;
use std::io::*;
use byteorder::*;
use std::collections::*;

/** Trait that signifies that given type has a static size, i.e. all its values occupy the same ammount of bytes.
 
    This is important for mappings as statically sized values do not need an extra index file to store the offsets as offset can simply be calculated as the size of the value times the id.
 */
pub trait FileWriterStaticSize {
}

/** File serialization. 
 
    Implements serialization and deserialization of the value to a file. 
 */
pub trait FileWriter<T> {
    fn read(f : & mut File) -> T;
    fn write(f : & mut File, value : & T);
}

/** Serialization for unsigned and unsigned 64 bit integers. 
 */
impl FileWriter<u64> for u64 {
    fn read(f : & mut File) -> u64 {
        return f.read_u64::<LittleEndian>().unwrap();
    }

    fn write(f : & mut File, value : & u64) {
        f.write_u64::<LittleEndian>(*value).unwrap();
    }
}

impl FileWriter<i64> for i64 {
    fn read(f : & mut File) -> i64 {
        return f.read_i64::<LittleEndian>().unwrap();
    }

    fn write(f : & mut File, value : & i64) {
        f.write_i64::<LittleEndian>(*value).unwrap();
    }
}


impl FileWriterStaticSize for u64 { }
impl FileWriterStaticSize for i64 { }

/** Serialization for SHA1 hashes. 
 */
impl FileWriter<git2::Oid> for git2::Oid {
    fn read(f : & mut File) -> git2::Oid {
        let mut buffer = vec![0; 20];
        f.read(& mut buffer).unwrap();
        return git2::Oid::from_bytes(& buffer).unwrap();
    }

    fn write(f : & mut File, value : & git2::Oid) {
        f.write(value.as_bytes()).unwrap();
    }
}

impl FileWriterStaticSize for git2::Oid { }

/** Serialization of strings. 
 */
impl FileWriter<String> for String {
    fn read(f : & mut File) -> String {
        let len = f.read_u32::<LittleEndian>().unwrap();
        let mut buf = vec![0; len as usize];
        if f.read(& mut buf).unwrap() as u32 != len {
            panic!("Corrupted binary format");
        }
        return String::from_utf8(buf).unwrap();
    }

    fn write(f : & mut File, value : & String) {
        f.write_u32::<LittleEndian>(value.len() as u32).unwrap();
        f.write(value.as_bytes()).unwrap();
    }
}

/** Indexing file for statically sized values. 
 
    Consists of a file that can be read and written to, containing the values ordered by their consecutive ids. The values associated with their ids can be read and written (overwritten) by seeking the file. 
    
    This provides cheap and not completely slow random access to the values stored. 
 */
pub struct Indexer<T: FileWriter<T> + FileWriterStaticSize> {
    f : File,
    size : u64,
    why_oh_why : std::marker::PhantomData<T>,
}

impl<T : FileWriter<T> + FileWriterStaticSize> Indexer<T> {
    pub fn new(filename : & str) -> Indexer<T> {
        let mut f = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();
        let size = f.seek(SeekFrom::End(0)).unwrap() / std::mem::size_of::<T>() as u64;
        return Indexer{ f, size, why_oh_why : std::marker::PhantomData{} };
    }

    pub fn get(& mut self, id : u64) -> T {
        assert!(id < self.size);
        self.f.seek(SeekFrom::Start(id * std::mem::size_of::<T>() as u64)).unwrap();
        return T::read(& mut self.f);
    }

    pub fn set(& mut self, id : u64, value : & T) {
        if id < self.size {
            self.f.seek(SeekFrom::Start(id * std::mem::size_of::<T>() as u64)).unwrap();
            T::write(& mut self.f, value);
        } else {
            self.f.seek(SeekFrom::End(0)).unwrap();
            if id > self.size {
                let fill = vec!(255; std::mem::size_of::<T>());
                while id > self.size  {
                    self.f.write(& fill).unwrap();
                    self.size += 1;
                }
            }
            T::write(& mut self.f, value);
            self.size += 1;
        }
    }

    pub fn len(& self) -> usize {
        return self.size as usize;
    }

    pub fn iter(& mut self) -> DirectIndexerIter<T> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return DirectIndexerIter{indexer : self, id : 0};
    }
} 

pub struct DirectIndexerIter<'a, T> where T : FileWriter<T> + FileWriterStaticSize  {
    indexer : &'a mut Indexer<T>,
    id : u64
}

impl<'a, T : FileWriter<T> + FileWriterStaticSize> Iterator for DirectIndexerIter<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<Self::Item> {
        if self.id == self.indexer.size {
            return None;
        } else {
            let id = self.id;
            self.id += 1;
            return Some((id, T::read(& mut self.indexer.f)));
        }
    }
}

/** Special form of indexer that allows both random access from id to value, but also keeps a mapping from values to already assigned ids. 
  
    Mappings do not allow updates, only additions.    
 */
pub trait Mapping<T> {
    fn get(& mut self, key : & T) -> Option<u64>;
    fn get_or_create(& mut self, key : & T) -> (u64, bool);
    fn get_value(& mut self, id : u64) -> T;
}

/** Direct mapping for statically sized values. 
 
    Since the values are statically sized, the mapping does with a single file that contains the values in the order of their ids. The mapping then contains a hashmap from values to ids and the file itself can be used for the reverse mapping from ids to values. 
 */
pub struct DirectMapping<T : FileWriter<T> + std::cmp::Eq + std::hash::Hash + std::clone::Clone + FileWriterStaticSize> {
    mapping : HashMap<T, u64>,
    indexer : Indexer<T>,
}

impl<T: FileWriter<T> + std::cmp::Eq + std::hash::Hash + std::clone::Clone + FileWriterStaticSize> DirectMapping<T> {
    pub fn new(filename : & str) -> DirectMapping<T> {
        return DirectMapping::<T>{
            mapping : HashMap::new(), 
            indexer : Indexer::new(filename),
        };
    }

    pub fn fill(& mut self) {
        self.mapping.clear();
        for (id, value) in self.indexer.iter() {
            self.mapping.insert(value, id);
        }
    }

    pub fn len(& self) -> usize {
        return self.indexer.len();
    }

    pub fn loaded_len(& self) -> usize {
        return self.mapping.len();
    }

}

impl<T: FileWriter<T> + std::cmp::Eq + std::hash::Hash + std::clone::Clone + FileWriterStaticSize> Mapping<T> for DirectMapping<T> {

    fn get(& mut self, key : & T) -> Option<u64> {
        match self.mapping.get(key) {
            Some(id) => return Some(*id),
            _ => return None
        }
    }

    fn get_or_create(& mut self, key : & T) -> (u64, bool) {
        match self.mapping.get(key) {
            Some(id) => return (*id, false),
            _ => {
                let id = self.mapping.len() as u64;
                self.mapping.insert((*key).clone(), id);
                self.indexer.set(id, key);
                return (id, true);
            }
        }
    }

    fn get_value(& mut self, id : u64) -> T {
        return self.indexer.get(id);
    }

}

/** Indirect mapping is suitable for value that do not have static sizes, such as strings. 
 
    The mapping adds a level of indirection, where the indexer points to the file containing the actual values of different sizes, thus allowing random access. 
 */
pub struct IndirectMapping<T : FileWriter<T> + std::cmp::Eq + std::hash::Hash + std::clone::Clone> {
    mapping : HashMap<T, u64>,
    indexer : Indexer<u64>,
    f : File
}

impl<T: FileWriter<T> + std::cmp::Eq + std::hash::Hash + std::clone::Clone> IndirectMapping<T> {
    pub fn new(filename : & str) -> IndirectMapping<T> {
        let f = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();
        let index_filename = format!("{}.index", filename);
        return IndirectMapping::<T>{
            mapping : HashMap::new(), 
            indexer : Indexer::new(& index_filename),
            f : f,
        };
    }

    pub fn fill(& mut self) {
        self.mapping.clear();
        self.f.seek(SeekFrom::Start(0)).unwrap();
        let mut id : u64 = 0;
        while id < self.indexer.size {
            self.mapping.insert(T::read(& mut self.f), id);
            id += 1;
        }
    }

    pub fn len(& self) -> usize {
        return self.indexer.len();
    }

    pub fn loaded_len(& self) -> usize {
        return self.mapping.len();
    }

}

impl<T: FileWriter<T> + std::cmp::Eq + std::hash::Hash + std::clone::Clone> Mapping<T> for IndirectMapping<T> {

    fn get(& mut self, key : & T) -> Option<u64> {
        match self.mapping.get(key) {
            Some(id) => return Some(*id),
            _ => return None
        }
    }

    fn get_or_create(& mut self, key : & T) -> (u64, bool) {
        match self.mapping.get(key) {
            Some(id) => return (*id, false),
            _ => {
                let id = self.mapping.len() as u64;
                let offset = self.f.seek(SeekFrom::Current(0)).unwrap();
                T::write(& mut self.f, key);
                self.mapping.insert((*key).clone(), id);
                self.indexer.set(id, & offset);
                return (id, true);
            }
        }
    }

    fn get_value(& mut self, id : u64) -> T {
        let offset = self.indexer.get(id);
        self.f.seek(SeekFrom::Start(offset)).unwrap();
        return T::read(& mut self.f);
    }

}

/** Indexed Property Store
 
    Property store consists of two files - the actual properties which are stored as consecutive records of id followed by the value, and where updates are recorded as new values for already specified id (i.e. no value is ever deleted) and an index file that contains the offsets into the property file for given ids. 
 */
pub struct PropertyStore<T> {
    indexer : Indexer<u64>,
    f : File,
    why_oh_why : std::marker::PhantomData<T>,
}

impl<T : FileWriter<T>> PropertyStore<T> {
    pub fn new(filename : & str) -> PropertyStore<T> {
        let f = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();
        let index_file = format!("{}.index", filename);
        return PropertyStore::<T>{
            indexer : Indexer::new(& index_file),
            f : f, 
            why_oh_why : std::marker::PhantomData{},
        };
    }

    pub fn get(& mut self, id : u64) -> Option<T> {
        if id >= self.indexer.len() as u64 {
            return None;
        }
        let offset = self.indexer.get(id);
        if offset != std::u64::MAX {
            self.f.seek(SeekFrom::Start(offset)).unwrap();
            let check_id = self.f.read_u64::<LittleEndian>().unwrap();
            assert_eq!(check_id, id);
            return Some(T::read(& mut self.f));
        } else {
            return None;
        }
    }

    pub fn set(& mut self, id : u64, value : & T) {
        let offset = self.f.seek(SeekFrom::End(0)).unwrap();
        self.f.write_u64::<LittleEndian>(id).unwrap();
        T::write(& mut self.f, value);
        self.indexer.set(id, & offset);
    }

    pub fn len(& self) -> usize {
        return self.indexer.len();
    }

    /** Returns the iterator to the property store that returns *all* stored records chronologically, i.e. including the old ones that were later overwritten. 
     */
    pub fn iter(& mut self) -> PropertyStoreIter<T> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return PropertyStoreIter{ps : self};
    }
}

pub struct PropertyStoreIter<'a, T> {
    ps : &'a mut PropertyStore<T>
}

impl<'a, T : FileWriter<T>> Iterator for PropertyStoreIter<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<Self::Item> {
        if let Ok(id) = self.ps.f.read_u64::<LittleEndian>() {
            let value = T::read(& mut self.ps.f);
            return Some((id, value));
        } else {
            return None;
        }
    }
}






// PropertyStore




// Extra types


/*
impl FileWriter<Heads> for PropertyWriter<Heads> {

    fn read(& mut self) -> Heads {
        let mut result = Heads::new();
        let records = self.f.read_u32::<LittleEndian>().unwrap() as usize;
        while result.len() < records {
            let head_name_len = self.f.read_u32::<LittleEndian>().unwrap();
            let mut head_name = vec![0; head_name_len as usize];
            if self.f.read(& mut head_name).unwrap() as u32 != head_name_len {
                panic!("Corrupted binary format");
            }
            let commit_id = self.f.read_u64::<LittleEndian>().unwrap();
            result.insert(head_name, commit_id);
        }
        return result;
    }

    fn write(& mut self, value : & Heads) {
        self.f.write_u32::<LittleEndian>(value.len() as u32).unwrap();
        for (head_name, commit_id) in value {
            self.f.write_u32::<LittleEndian>(head_name.len() as u32).unwrap();
            self.f.write(head_name).unwrap();
            self.f.write_u64::<LittleEndian>(*commit_id).unwrap();
        }
    }
}
*/

