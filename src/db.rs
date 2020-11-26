/** The database support.

    Provides serialization and deserialization of various structures used by the downloader and extra infrastructure for their efficiency, such as indexes and mappings. 



 */
use std::fs::{File, OpenOptions};
use std::io::*;
use byteorder::*;
use std::collections::*;
use crate::records::*;

/** Trait that signifies that given type has a static size, i.e. all its values occupy the same ammount of bytes.
 
    This is important for mappings as statically sized values do not need an extra index file to store the offsets as offset can simply be calculated as the size of the value times the id.
 */
pub trait FileWriterStaticSize<T> {
    fn empty_value() -> T;
    fn is_empty_value(value : & T) -> bool;
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


impl FileWriterStaticSize<u64> for u64 {
    fn empty_value() -> u64 {
        return std::u64::MAX;
    }

    fn is_empty_value(value : & u64) -> bool {
        return *value == std::u64::MAX;
    }
}
impl FileWriterStaticSize<i64> for i64 {
    fn empty_value() -> i64 {
        return std::i64::MAX;
    }

    fn is_empty_value(value : & i64) -> bool {
        return *value == std::i64::MAX;
    }
 }

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

impl FileWriterStaticSize<git2::Oid> for git2::Oid {
    fn empty_value() -> git2::Oid {
        return git2::Oid::from_str("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF").unwrap();
    }

    fn is_empty_value(value : & git2::Oid) -> bool {
        for byte in value.as_bytes() {
            if *byte != 0xff {
                return false;
            }
        }
        return true;
    }
}

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


/** Holds indices for each id.
 
    To a degree the ids don't have to be consecutive.  
 */
pub struct Indexer {
    f : File, 
    pub (crate) size: u64
}

impl Indexer {
    pub fn new(filename : & str) -> Indexer {
        let mut f = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();
        let size = f.seek(SeekFrom::End(0)).unwrap() / 8;
        return Indexer{ f, size };
    } 

    pub fn get(& mut self, id : u64) -> Option<u64> {
        if id < self.size {
            self.f.seek(SeekFrom::Start(8 * id)).unwrap();
            let result = self.f.read_u64::<LittleEndian>().unwrap();
            if result != Indexer::EMPTY {
                return Some(result);
            }
        }
        return None;
    }

    pub fn set(& mut self, id : u64, offset : u64) {
        if id < self.size {
            self.f.seek(SeekFrom::Start(8 * id)).unwrap();
            self.f.write_u64::<LittleEndian>(offset).unwrap();
        } else {
            self.f.seek(SeekFrom::End(0)).unwrap();
            while id > self.size  {
                self.f.write_u64::<LittleEndian>(Indexer::EMPTY).unwrap();
                self.size += 1;
            }
            self.f.write_u64::<LittleEndian>(offset).unwrap();
            self.size += 1;
        }
    }

    pub fn len(& self) -> usize {
        return self.size as usize;
    }

    const EMPTY : u64 = std::u64::MAX;
}

pub struct IndexedWriter<T: FileWriter<T>> {
    indexer : Indexer,
    pub (crate) f : File, 
    why_oh_why : std::marker::PhantomData<T>,
}

impl<T : FileWriter<T>> IndexedWriter<T> {
    pub fn new(filename : & str) -> IndexedWriter<T> {
        let f = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();
        return IndexedWriter{
            indexer : Indexer::new(& 
                format!("{}.index", filename)),
            f,
            why_oh_why : std::marker::PhantomData{}
        }
    }   

    pub fn len(& self) -> usize {
        return self.indexer.len();
    }
    
    pub fn get(& mut self, id : u64) -> Option<T> {
        if let Some(offset) = self.indexer.get(id) {
            self.f.seek(SeekFrom::Start(offset)).unwrap();
            let stored_id = self.f.read_u64::<LittleEndian>().unwrap();
            assert!(stored_id == id);
            return Some(T::read(& mut self.f));
        }
        return None;
    }

    pub fn add(& mut self, id : u64, value : & T) {
        assert!(self.indexer.get(id).is_none(), "Value already exists");
        let offset = self.f.seek(SeekFrom::Current(0)).unwrap();
        self.f.write_u64::<LittleEndian>(id).unwrap();
        T::write(& mut self.f, value);
        self.indexer.set(id, offset);
    }

    pub fn iter(& mut self) -> IndexedWriterIterator<T> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return IndexedWriterIterator{writer : self};
    }
}

pub struct IndexedWriterIterator<'a, T : FileWriter<T>> {
    writer : &'a mut IndexedWriter<T>
}

impl<'a, T: FileWriter<T>> Iterator for IndexedWriterIterator<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<Self::Item> {
        if let Ok(id) = self.writer.f.read_u64::<LittleEndian>() {
            return Some((id, T::read(& mut self.writer.f)));
        } else {
            return None;
        }
    }
} 

/** Indexed writer optimized for record of static size. 
 
    Here we can do without the extra index file as offset for any given index can be easily calculated. This does however pose a problem as it is now not possible to 
 */
pub struct DirectIndexedWriter<T : FileWriter<T> + FileWriterStaticSize<T>> {
    pub (crate) f : File, 
    size : u64, 
    why_oh_why : std::marker::PhantomData<T>,
}

impl<T : FileWriter<T> + FileWriterStaticSize<T>> DirectIndexedWriter<T> {
    pub fn new(filename : & str) -> DirectIndexedWriter<T> {
        let mut f = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();
        let size = f.seek(SeekFrom::End(0)).unwrap() / std::mem::size_of::<T>() as u64;
        return DirectIndexedWriter{
            f,
            size,
            why_oh_why : std::marker::PhantomData{}
        }
    }

    pub fn len(& self) -> usize {
        return self.size as usize;
    }

    pub fn get(& mut self, id : u64) -> Option<T> {
        if id >= self.size {
            return None;
        }
        self.f.seek(SeekFrom::Start(id * std::mem::size_of::<T>() as u64)).unwrap();
        let result = T::read(& mut self.f);
        if T::is_empty_value(& result) {
            return None;
        } else {
            return Some(result);
        }
    }

    pub fn add(& mut self, id : u64, value : & T) {
        assert!(self.get(id).is_none(), "Value already exists");
        if id < self.size {
            self.f.seek(SeekFrom::Start(id * std::mem::size_of::<T>() as u64)).unwrap();
            T::write(& mut self.f, value);
        } else {
            self.f.seek(SeekFrom::End(0)).unwrap();
            if id > self.size {
                let fill = T::empty_value();
                while id > self.size  {
                    T::write(& mut self.f, & fill);
                    self.size += 1;
                }
            }
            T::write(& mut self.f, value);
            self.size += 1;
        }
    }

    pub fn iter(& mut self) -> DirectIndexedWriterIterator<T> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return DirectIndexedWriterIterator{writer : self, id : 0};
    }
}

pub struct DirectIndexedWriterIterator<'a, T : FileWriter<T> + FileWriterStaticSize<T>> {
    writer : &'a mut  DirectIndexedWriter<T>,
    id : u64
}

impl<'a, T: FileWriter<T> + FileWriterStaticSize<T>> Iterator for DirectIndexedWriterIterator<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<Self::Item> {
        loop {
            if self.id == self.writer.size {
                return None;
            }
            let id = self.id;
            let result = T::read(& mut self.writer.f);
            self.id += 1;
            if ! T::is_empty_value(& result) {
                return Some((id, result));
            }
        }
    }
} 

/** An appendable file with hash map in memory that provides translation from T to unique ids. 
 */
pub struct Mapping<T : FileWriter<T> + std::cmp::Eq + std::hash::Hash + std::clone::Clone> {
    mapping : HashMap<T, u64>,
    pub (crate) writer : IndexedWriter<T>
}

impl<T: FileWriter<T> + std::cmp::Eq + std::hash::Hash + std::clone::Clone> Mapping<T> {
    pub fn new(filename : & str) -> Mapping<T> {
        return Mapping{
            mapping : HashMap::new(),
            writer : IndexedWriter::new(filename),
        }
    }

    pub fn fill(& mut self) {
        self.mapping.clear();
        for (id, value) in self.writer.iter() {
            self.mapping.insert(value, id);
        }
    }

    pub fn len(& self) -> usize {
        return self.writer.len();
    }

    pub fn loaded_len(& self) -> usize {
        return self.mapping.len();
    }

    pub fn get(& self, key : &T) -> Option<u64> {
        if let Some(id) = self.mapping.get(key) {
            return Some(*id);
        } else {
            return None;
        }
    }

    pub fn get_or_create(& mut self, key : & T) -> (u64, bool) {
        if let Some(id) = self.get(key) {
            return (id, false);
        } else {
            let id = self.writer.indexer.size;
            self.writer.add(id, key);
            self.mapping.insert(key.clone(), id);
            return (id, true);
        }
    }

    pub fn get_value(& mut self, id : u64) -> T {
        return self.writer.get(id).unwrap();
    }
}


pub struct DirectMapping<T : FileWriter<T> + FileWriterStaticSize<T> + std::cmp::Eq + std::hash::Hash + std::clone::Clone> {
    mapping : HashMap<T, u64>,
    pub (crate) writer : DirectIndexedWriter<T>
}

impl<T : FileWriter<T> + FileWriterStaticSize<T> + std::cmp::Eq + std::hash::Hash + std::clone::Clone> DirectMapping<T> {
    pub fn new(filename : & str) -> DirectMapping<T> {
        return DirectMapping{
            mapping : HashMap::new(),
            writer : DirectIndexedWriter::new(filename),
        }
    }

    pub fn fill(& mut self) {
        self.mapping.clear();
        for (id, value) in self.writer.iter() {
            self.mapping.insert(value, id);
        }
    }

    pub fn len(& self) -> usize {
        return self.writer.len();
    }

    pub fn loaded_len(& self) -> usize {
        return self.mapping.len();
    }

    pub fn get(& self, key : &T) -> Option<u64> {
        if let Some(id) = self.mapping.get(key) {
            return Some(*id);
        } else {
            return None;
        }
    }

    pub fn get_or_create(& mut self, key : & T) -> (u64, bool) {
        if let Some(id) = self.get(key) {
            return (id, false);
        } else {
            let id = self.writer.size;
            self.writer.add(id, key);
            self.mapping.insert(key.clone(), id);
            return (id, true);
        }
    }

    pub fn get_value(& mut self, id : u64) -> T {
        return self.writer.get(id).unwrap();
    }

}

pub struct PropertyStore<T : FileWriter<T>> {
    pub (crate) indexer : Indexer,
    pub (crate) f : File, 
    why_oh_why : std::marker::PhantomData<T>,
}

impl<T: FileWriter<T>> PropertyStore<T> {
    pub fn new(filename : & str) -> PropertyStore<T> {
        let f = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();
        let index_file = format!("{}.index", filename);
        return PropertyStore::<T>{
            indexer : Indexer::new(& index_file),
            f : f, 
            why_oh_why : std::marker::PhantomData{},
        };
    }

    pub fn indices_len(& self) -> usize {
        return self.indexer.len();
    }

    pub fn has(& mut self, id : u64) -> bool {
        return !self.indexer.get(id).is_none();
    }

    pub fn get(& mut self, id : u64) -> Option<T> {
        if let Some(offset) = self.indexer.get(id) {
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
        self.indexer.set(id, offset);
    }

    pub fn latest_iter(& mut self) -> PropertyStoreLatestIterator<T> {
        return PropertyStoreLatestIterator{ps : self, id : 0};
    }

    pub fn all_iter(& mut self) -> PropertyStoreAllIterator<T> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return PropertyStoreAllIterator{ps : self};
    }

}

pub struct PropertyStoreLatestIterator<'a, T : FileWriter<T>> {
    ps : &'a mut PropertyStore<T>,
    id : u64,
}

impl<'a, T: FileWriter<T>> Iterator for PropertyStoreLatestIterator<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<Self::Item> {
        loop {
            if self.id >= self.ps.indexer.size {
                return None;
            }
            if let Some(result) = self.ps.get(self.id) {
                let id = self.id;
                self.id += 1;
                return Some((id, result));
            }
            self.id += 1;
        }
    }
}

pub struct PropertyStoreAllIterator<'a, T : FileWriter<T>> {
    ps : &'a mut PropertyStore<T>
}

impl<'a, T : FileWriter<T>> Iterator for PropertyStoreAllIterator<'a, T> {
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

/** Similar to property store, but each record has also a link to previous record for the same id, so that the records can be traversed. 
 */
pub struct LinkedPropertyStore<T : FileWriter<T>> {
    latest : Indexer, 
    pub (crate) f : File, 
    why_oh_why : std::marker::PhantomData<T>,
}

#[allow(dead_code)]
impl<T: FileWriter<T>> LinkedPropertyStore<T> {
    pub fn new(filename : & str) -> LinkedPropertyStore<T> {
        let f = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();
        let index_file = format!("{}.index", filename);
        return LinkedPropertyStore::<T>{
            latest : Indexer::new(& index_file),
            f : f, 
            why_oh_why : std::marker::PhantomData{},
        };
    }

    pub fn indices_len(& self) -> usize {
        return self.latest.len();
    }

    pub fn has(& mut self, id : u64) -> bool {
        return !self.latest.get(id).is_none();
    }

    pub fn get(& mut self, id : u64) -> Option<T> {
        if let Some(offset) = self.latest.get(id) {
            self.f.seek(SeekFrom::Start(offset)).unwrap();
            let check_id = self.f.read_u64::<LittleEndian>().unwrap();
            assert_eq!(check_id, id);
            return Some(T::read(& mut self.f));
        } else {
            return None;
        }
    }

    pub fn set(& mut self, id : u64, value : & T) {
        let old_offset = self.latest.get(id).or(Some(Indexer::EMPTY)).unwrap();
        let offset = self.f.seek(SeekFrom::End(0)).unwrap();
        self.f.write_u64::<LittleEndian>(id).unwrap();
        T::write(& mut self.f, value);
        self.f.write_u64::<LittleEndian>(old_offset).unwrap();
        self.latest.set(id, offset);
    }

    pub fn latest_iter(& mut self) -> LinkedPropertyStoreLatestIterator<T> {
        return LinkedPropertyStoreLatestIterator{ps : self, id : 0};
    }

    pub fn all_iter(& mut self) -> LinkedPropertyStoreAllIterator<T> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return LinkedPropertyStoreAllIterator{ps : self};
    }

    pub fn id_iter(& mut self, id : u64) -> LinkedPropertyStoreIdIterator<T> {
        let offset = self.latest.get(id).or(Some(Indexer::EMPTY)).unwrap();
        return LinkedPropertyStoreIdIterator{ps : self, offset};
    }

}

pub struct LinkedPropertyStoreLatestIterator<'a, T : FileWriter<T>> {
    ps : &'a mut LinkedPropertyStore<T>,
    id : u64,
}

impl<'a, T: FileWriter<T>> Iterator for LinkedPropertyStoreLatestIterator<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<Self::Item> {
        loop {
            if self.id >= self.ps.latest.size {
                return None;
            }
            if let Some(result) = self.ps.get(self.id) {
                let id = self.id;
                self.id += 1;
                return Some((id, result));
            }
            self.id += 1;
        }
    }
}

pub struct LinkedPropertyStoreAllIterator<'a, T : FileWriter<T>> {
    ps : &'a mut LinkedPropertyStore<T>
}

impl<'a, T : FileWriter<T>> Iterator for LinkedPropertyStoreAllIterator<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<Self::Item> {
        if let Ok(id) = self.ps.f.read_u64::<LittleEndian>() {
            let value = T::read(& mut self.ps.f);
            // read and skip the previous record offset
            self.ps.f.read_u64::<LittleEndian>().unwrap();
            return Some((id, value));
        } else {
            return None;
        }
    }
}

pub struct LinkedPropertyStoreIdIterator<'a, T : FileWriter<T>> {
    ps : &'a mut LinkedPropertyStore<T>,
    offset : u64
}

impl<'a, T : FileWriter<T>> Iterator for LinkedPropertyStoreIdIterator<'a, T> {
    type Item = T;

    fn next(& mut self) -> Option<Self::Item> {
        if self.offset == Indexer::EMPTY {
            return None;
        } else {
            // skip the ID which is the same
            self.ps.f.seek(SeekFrom::Start(self.offset + 8)).unwrap();
            let result = T::read(& mut self.ps.f);
            self.offset = self.ps.f.read_u64::<LittleEndian>().unwrap();
            return Some(result);
        }
    }
}

impl MetadataReader for LinkedPropertyStore<Metadata> {
    fn read_metadata(& mut self, id : u64) -> HashMap<String, String> {
        let mut result = HashMap::<String, String>::new();
        for Metadata{key, value} in self.id_iter(id) {
            result.insert(key, value);
        }
        return result;
    }

    fn get_metadata(& mut self, id : u64, key : & str) -> Option<String> {
        for Metadata{key : k, value} in self.id_iter(id) {
            if k == key {
                return Some(value);
            }
        }
        return None;
    }
}

