/** The database support.

    Provides serialization and deserialization of various structures used by the downloader and extra infrastructure for their efficiency, such as indexes and mappings. 

    # Indexer 

    The indexer is the simplest structre. Provides a mutable file store for given type of fixed serialized size. 

    # Store

    Provides indexed storage for any items (hence the index). Store elements cannot be updated once created, but new records for same id can be added overriding the old ones, although this particular functionality is not expected to be used much. 

    # Linked Store

    Like store, 

 */
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write};
use byteorder::*;
use std::collections::*;
use std::hash::*;
use std::fmt::{Debug};
use std::convert::From;
use crate::helpers;
use crate::settings;
use crate::LOG;

pub (crate) const MAX_BUFFER_LENGTH : u64 = 10 * 1024 * 1024 * 1024; // 10GB

pub trait Id : From<u64> + Into<u64> + std::marker::Copy + std::fmt::Debug + std::cmp::PartialEq + std::cmp::Eq + std::hash::Hash {}

impl Id for u64 {}

/** TODO since at the end I need verify anyways, maybe check if deserialize should behave like verify already and the performance hit of that.
 */
pub trait Serializable {
    type Item;

    fn serialize(f : & mut File, value : & Self::Item);
    fn deserialize(f : & mut File) -> Self::Item;

    fn verify(f : & mut File) -> Result<Self::Item, std::io::Error>;
}

pub trait FixedSizeSerializable : Serializable {
    const SIZE : u64;
}

pub trait Indexable : FixedSizeSerializable + Eq {
    const EMPTY : Self::Item;
}

/* The serializable, fixed size serializable and indexable implementations are provided for u64 used as id in the rest of the file. 
 */
impl Serializable for u64 {
    type Item = u64;
    fn serialize(f : & mut File, value : & u64) {
        f.write_u64::<LittleEndian>(*value).unwrap();
    }
    fn deserialize(f : & mut File) -> u64 {
        return f.read_u64::<LittleEndian>().unwrap();
    }

    fn verify(f : & mut File) -> Result<u64, std::io::Error> {
        return f.read_u64::<LittleEndian>();
    }

}

impl FixedSizeSerializable for u64 {
    const SIZE : u64 = 8;
}

impl Indexable for u64 {
    const EMPTY : u64 = std::u64::MAX;
}

impl Serializable for i64 {
    type Item = i64;
    fn serialize(f : & mut File, value : & i64) {
        f.write_i64::<LittleEndian>(*value).unwrap();
    }
    fn deserialize(f : & mut File) -> i64 {
        return f.read_i64::<LittleEndian>().unwrap();
    }

    fn verify(f : & mut File) -> Result<i64, std::io::Error> {
        return f.read_i64::<LittleEndian>();
    }
}

impl FixedSizeSerializable for i64 {
    const SIZE : u64 = 8;
}

impl Serializable for u32 {
    type Item = u32;
    fn serialize(f : & mut File, value : & u32) {
        f.write_u32::<LittleEndian>(*value).unwrap();
    }
    fn deserialize(f : & mut File) -> u32 {
        return f.read_u32::<LittleEndian>().unwrap();
    }

    fn verify(f : & mut File) -> Result<u32, std::io::Error> {
        return f.read_u32::<LittleEndian>();
    }

}

impl FixedSizeSerializable for u32 {
    const SIZE : u64 = 4;
}

impl Serializable for u16 {
    type Item = u16;
    fn serialize(f : & mut File, value : & u16) {
        f.write_u16::<LittleEndian>(*value).unwrap();
    }
    fn deserialize(f : & mut File) -> u16 {
        return f.read_u16::<LittleEndian>().unwrap();
    }

    fn verify(f : & mut File) -> Result<u16, std::io::Error> {
        return f.read_u16::<LittleEndian>();
    }

}

impl FixedSizeSerializable for u16 {
    const SIZE : u64 = 2;
}

impl Serializable for u8 {
    type Item = u8;
    fn serialize(f : & mut File, value : & u8) {
        f.write_u8(*value).unwrap();
    }
    fn deserialize(f : & mut File) -> u8 {
        return f.read_u8().unwrap();
    }
    fn verify(f : & mut File) -> Result<u8, std::io::Error> {
        return f.read_u8();
    }

}

impl FixedSizeSerializable for u8 {
    const SIZE : u64 = 1;
}

/** Strings are serializable too, very handy:)
 */
impl Serializable for String {
    type Item = String;

    fn serialize(f : & mut File, value : & String) {
        f.write_u32::<LittleEndian>(value.len() as u32).unwrap();
        f.write(value.as_bytes()).unwrap();
    }

    fn deserialize(f : & mut File) -> String {
        let len = f.read_u32::<LittleEndian>().unwrap();
        let mut buf = vec![0; len as usize];
        if f.read(& mut buf).unwrap() as u32 != len {
            panic!("Corrupted binary format");
        }
        return String::from_utf8(buf).unwrap();
    }
    fn verify(f : & mut File) -> Result<String, std::io::Error> {
        let len = u32::verify(f)?;
        if len as u64 > MAX_BUFFER_LENGTH {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid buffer size"));
        }
        let mut buf = vec![0; len as usize];
        if f.read(& mut buf)? as u32 != len {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Corrupted binary format"));
        }
        return Ok(String::from_utf8(buf).unwrap());
    }
}

/** Holds indices for each id.

    The idsn are expected to be mostly consecutive, i.e. if an id `N` is added all ids from `0` to `N-1` either must exist, or will be created. 

    The indexer is usually not used alone, but as part of more complex structures. 
 */
pub struct Indexer<T : Indexable + Serializable<Item = T> = u64, ID : Id = u64 > {
    name : String, 
    f : File, 
    size : u64,
    why_oh_why : std::marker::PhantomData<(T, ID)>
}

impl<T : Indexable + Serializable<Item = T>, ID : Id> Indexer<T, ID> {
    pub fn new(root : & str, name : & str, readonly : bool) -> Indexer<T, ID> {
        let mut f;
        if readonly {
            f = OpenOptions::new().read(true).open(format!("{}/{}.idx", root, name)).unwrap();
        } else {
            f = OpenOptions::new().read(true).write(true).create(true).open(format!("{}/{}.idx", root, name)).unwrap();
        }
        let size = f.seek(SeekFrom::End(0)).unwrap() / T::SIZE;
        return Indexer{ name : name.to_owned(), f, size, why_oh_why : std::marker::PhantomData{} };
    } 

    pub fn get(& mut self, id : ID) -> Option<T> {
        if id.into() < self.size {
            self.f.seek(SeekFrom::Start(T::SIZE * id.into())).unwrap();
            let result = T::deserialize(& mut self.f);
            if result != T::EMPTY {
                return Some(result); 
            } else {
                return None;
            }
        }
        return None;
    }

    pub fn set(& mut self, id : ID, value : & T) {
        if id.into() < self.size {
            self.f.seek(SeekFrom::Start(T::SIZE * id.into())).unwrap();
            T::serialize(& mut self.f, value);
        } else {
            self.f.seek(SeekFrom::End(0)).unwrap();
            while id.into() > self.size  {
                T::serialize(& mut self.f, & T::EMPTY);
                self.size += 1;
            }
            T::serialize(& mut self.f, value);
            self.size += 1;
        }
    }
    
    pub fn len(& self) -> usize {
        return self.size as usize;
    }

    pub fn iter(& mut self) -> IndexerIterator<T, ID> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return IndexerIterator{indexer : self, id : 0, max_offset: u64::MAX};
    }

    pub fn savepoint_iter(& mut self, sp : & Savepoint) -> IndexerIterator<T, ID> {
        let max_offset = sp.limit_for(& self.name);
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return IndexerIterator{indexer : self, id : 0, max_offset };
    }

}

pub struct IndexerIterator<'a, T : Indexable + Serializable<Item = T>, ID : Id = u64> {
    indexer : &'a mut Indexer<T, ID>,
    id : u64,
    max_offset : u64,
}

impl<'a, T : Indexable + Serializable<Item = T>, ID : Id> Iterator for IndexerIterator<'a, T, ID> {
    type Item = (ID, T);

    fn next(& mut self) -> Option<(ID, T)> {
        loop {
            if self.indexer.f.seek(SeekFrom::Current(0)).unwrap() >= self.max_offset {
                return None;
            } else if self.id == self.indexer.len() as u64 { 
                return None;
            } else {
                let id = ID::from(self.id);
                let result = T::deserialize(& mut self.indexer.f);
                self.id += 1;
                return Some((id, result));
            }
        }
    }
}

/** Store implementation. 
 
    Store is an indexed updatable container that keeps history of updates.
 */
pub struct Store<T : Serializable<Item = T>, ID : Id = u64> {
    pub (crate) indexer : Indexer<u64, ID>,
    pub (crate) f : File,
    why_oh_why : std::marker::PhantomData<T>,
}

impl<T: Serializable<Item = T>, ID : Id> Store<T, ID> {

    pub fn new(root : & str, name : & str, readonly : bool) -> Store<T, ID> {
        let f;
        if readonly {
            f = OpenOptions::new().read(true).open(format!("{}/{}.store", root, name)).unwrap();
        } else {
            f = OpenOptions::new().read(true).write(true).create(true).open(format!("{}/{}.store", root, name)).unwrap();
        }
        let mut result = Store{
            indexer : Indexer::new(root, name, readonly),
            f,
            why_oh_why : std::marker::PhantomData{}
        };
        LOG!("    {}: indices {}, size {}", name, result.indexer.len(), result.f.seek(SeekFrom::End(0)).unwrap());
        return result;
    }

    pub fn name<'a>(&'a self) -> &'a str {
        return self.indexer.name.as_str();
    }

    /** Updates the savepoint with own information. 
     */
    pub fn savepoint(& mut self, savepoint : & mut Savepoint) {
        savepoint.add_entry(
            self.name().to_owned(),
            self.f.seek(SeekFrom::End(0)).unwrap()
        );
    }

    /** Verifies the store. 
     
        Checks the following:

        - that every item stored in the store is valid
        - that the indices point to valid starts of the items
        - that these are the latest
        - if there is a missing slot in the index then no id is defined
     */
    pub fn verify(& mut self, checker : & mut dyn FnMut(T) -> Result<(), std::io::Error>) -> Result<(), std::io::Error> {
        let end = self.f.seek(SeekFrom::End(0))?;
        self.f.seek(SeekFrom::Start(0))?;
        // first check all the items in the store, including the old ones
        let mut latest_mappings = HashMap::<u64, u64>::new();
        loop {
            let offset = self.f.seek(SeekFrom::Current(0))?;
            if offset == end {
                break;
            }
            let id = self.f.read_u64::<LittleEndian>()?;
            if id >= self.indexer.size {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Store id {:?}, but only {} ids known at offset {}", ID::from(id), self.indexer.size, offset)));
            }
            latest_mappings.insert(id, offset);
            let item = T::verify(& mut self.f)?;
            checker(item)?;
        }
        // then check the index's integrity
        for (id, offset) in self.indexer.iter() {
            if offset == u64::EMPTY {
                if latest_mappings.contains_key(& id.into()) {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Store index id {:?}, has empty index, but offset {} found in the store", id, latest_mappings[& id.into()])));
                }
            } else {
                match latest_mappings.get(& id.into()) {
                    Some(found_offset) => {
                        if offset != *found_offset {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Store index id {:?}, has indexed offset {}, but offset {} found in store", id, offset, found_offset)));
                        }
                    },
                    None => {
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Store index id {:?}, has indexed offset {} but none found", id, offset)));
                    }
                }
            }
        }
        return Ok(());
    }

    /** Returns true if there is a valid record for sgiven id. 
     */
    pub fn has(& mut self, id : ID) -> bool {
        return self.indexer.get(id).is_some();
    }

    /** Gets the value for given id. 
     */
    pub fn get(& mut self, id : ID) -> Option<T> {
        if let Some(offset) = self.indexer.get(id) {
            self.f.seek(SeekFrom::Start(offset)).unwrap();
            let (record_id, value) = Self::read_record(& mut self.f).unwrap();
            assert_eq!(id, record_id, "Corrupted store or index");
            return Some(value);
        } else {
            return None;
        }
    }

    /** Sets the value for given id. 
     */
    pub fn set(& mut self, id : ID, value : & T) {
        self.indexer.set(id, & Self::write_record(& mut self.f, id, value));
    }

    /** Returns the number of indexed ids. 
     
        The actual values might be smaller as not all ids can have stored values. Actual number of values in the store can also be greater because same id may have multiple value updates. 
     */
    pub fn len(&self) -> usize {
        return self.indexer.len();
    }

    /** Iterates over the stored values. 
     
        Returns the latest stored value for every id. The ids are guaranteed to be increasing. 
     */
    pub fn iter(& mut self) -> StoreIter<T, ID> {
        return StoreIter::new(& mut self. f, & mut self.indexer);
    }

    /** Iterates over all stored values. 
     
        Iterates over *all* stored values, returning them in the order they were added to the store. Multiple values may be returned for single id, the last value returned is the valid one. 
     */
    pub fn iter_all(& mut self) -> StoreIterAll<T, ID> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return StoreIterAll{ store : self, max_offset : u64::MAX };
    }

    pub fn savepoint_iter_all(& mut self, sp : & Savepoint) -> StoreIterAll<T, ID> {
        let max_offset = sp.limit_for(self.name());
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return StoreIterAll{ store : self, max_offset };
    }

    /** Reads the record from a file. 
     
        Returns tuple of the id associated with the record and the value stored. 
     */
    fn read_record(f : & mut File) -> Option<(ID, T)> {
        if let Ok(id) = f.read_u64::<LittleEndian>() {
            return Some((ID::from(id), T::deserialize(f)));
        } else {
            return None;
        }
    }

    fn write_record(f : & mut File, id : ID, value : & T) -> u64 {
        let offset = f.seek(SeekFrom::End(0)).unwrap();
        f.write_u64::<LittleEndian>(id.into()).unwrap();
        T::serialize(f, value);
        return offset;
    }
}

/** Latest store iterator does not support savepoints since the indices can be udpated. 
 */
pub struct StoreIter<'a, T: Serializable<Item = T>, ID : Id> {
    f : &'a mut File,
    iiter : IndexerIterator<'a, u64,ID>,
    why_oh_why : std::marker::PhantomData<T>,
}

impl<'a, T : Serializable<Item = T>, ID : Id> StoreIter<'a, T, ID> {
    fn new(f : &'a mut File, indexer : &'a mut Indexer<u64, ID>) -> StoreIter<'a, T, ID> {
        return StoreIter{
            f : f,
            iiter : indexer.iter(),
            why_oh_why : std::marker::PhantomData{}
        };
    }
}

impl<'a, T : Serializable<Item = T>, ID : Id> Iterator for StoreIter<'a, T, ID> {
    type Item = (ID, T);

    fn next(& mut self) -> Option<(ID, T)> {
        if let Some((id, offset)) = self.iiter.next() {
            self.f.seek(SeekFrom::Start(offset)).unwrap();
            let (store_id, value) = Store::<T, ID>::read_record(self.f).unwrap();
            assert_eq!(id, store_id, "Corrupted store or its indexing");
            return Some((id, value)); 
        } else {
            return None;
        }
    }
}

pub struct StoreIterAll<'a, T : Serializable<Item = T>, ID : Id> {
    store : &'a mut Store<T, ID>,
    max_offset : u64,
}

impl<'a, T : Serializable<Item = T>, ID : Id> Iterator for StoreIterAll<'a, T, ID> {
    type Item = (ID, T);

    fn next(& mut self) -> Option<(ID, T)> {
        if self.store.f.seek(SeekFrom::Current(0)).unwrap() >= self.max_offset {
            return None;
        } else {
            return Store::<T, ID>::read_record(& mut self.store.f); 
        }
    }
}

/** Linked store implementation. 
 
    Store is an indexed updatable container that keeps history of updates.

    TODO add savepoint
 */
pub struct LinkedStore<T : Serializable<Item = T>, ID : Id = u64> {
    pub (crate) indexer : Indexer<u64, ID>,
    pub (crate) f : File,
    why_oh_why : std::marker::PhantomData<T>,
}

impl<T: Serializable<Item = T>, ID : Id> LinkedStore<T, ID> {

    pub fn new(root : & str, name : & str, readonly : bool) -> LinkedStore<T, ID> {
        let f;
        if readonly {
            f = OpenOptions::new().read(true).open(format!("{}/{}.store", root, name)).unwrap();
        } else {
            f = OpenOptions::new().read(true).write(true).create(true).open(format!("{}/{}.store", root, name)).unwrap();
        }
        let mut result = LinkedStore{
            indexer : Indexer::new(root, name, readonly),
            f,
            why_oh_why : std::marker::PhantomData{}
        };
        LOG!("    {}: indices {}, size {}", name, result.indexer.len(), result.f.seek(SeekFrom::End(0)).unwrap());
        return result;
    }

    pub fn name<'a>(&'a self) -> &'a str {
        return self.indexer.name.as_str();
    }

    /** Updates the savepoint with own information. 
     */
    pub fn savepoint(& mut self, savepoint : & mut Savepoint) {
        savepoint.add_entry(
            self.name().to_owned(),
            self.f.seek(SeekFrom::End(0)).unwrap()
        );
    }

    /** Verifies the linked store. 
     
        Checks the following:

        - that every item stored in the store is valid
        - that every update's back link points to correct item
        - that the indices point to valid starts of the items
        - that these are the latest
        - if there is a missing slot in the index then no id is defined
     */
    pub fn verify(& mut self, checker : & mut dyn FnMut(T) -> Result<(), std::io::Error>) -> Result<(), std::io::Error> {
        let end = self.f.seek(SeekFrom::End(0))?;
        self.f.seek(SeekFrom::Start(0))?;
        // first check all the items in the store, including the old ones
        let mut latest_mappings = HashMap::<u64, u64>::new();
        loop {
            let offset = self.f.seek(SeekFrom::Current(0))?;
            if offset == end {
                break;
            }
            let id = self.f.read_u64::<LittleEndian>()?;
            if id >= self.indexer.size {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("LinkedStore id {:?}, but only {} ids known at offset {}", ID::from(id), self.indexer.size, offset)));
            }
            let previous_offset = self.f.read_u64::<LittleEndian>()?;
            if previous_offset == u64::EMPTY {
                if latest_mappings.contains_key(& id) {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("LinkedStore index id {:?} at offset {} has empty backlink, but offset {} found", ID::from(id), offset, latest_mappings[& id])));
                }
            } else {
                match latest_mappings.get(& id) {
                    Some(found_offset) => {
                        if previous_offset != *found_offset {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("LinkedStore index id {:?} at offset {} has previous offset {} but offset {} found in the store", ID::from(id), offset, previous_offset, found_offset)));
                        }
                    },
                    None => {
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("LinkedStore index id {:?} at offset {} has previous offset {} but no offset found in the store", ID::from(id), offset, previous_offset)));
                    }
                }
            }
            latest_mappings.insert(id, offset);
            let item = T::verify(& mut self.f)?;
            checker(item)?;
        }
        // then check the index's integrity
        for (id, offset) in self.indexer.iter() {
            if offset == u64::EMPTY {
                if latest_mappings.contains_key(& id.into()) {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("LinkedStore index id {:?}, has empty index, but offset {} found in the store", id, latest_mappings[& id.into()])));
                }
            } else {
                match latest_mappings.get(& id.into()) {
                    Some(found_offset) => {
                        if offset != *found_offset {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("LinkedStore index id {:?}, has indexed offset {}, but offset {} found in store", id, offset, found_offset)));
                        }
                    },
                    None => {
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("LinkedStore index id {:?}, has indexed offset {} but none found", id, offset)));
                    }
                }
            }
        }
        return Ok(());
    }



    /** Gets the value for given id. 
     */
    pub fn get(& mut self, id : ID) -> Option<T> {
        if let Some(offset) = self.indexer.get(id) {
            self.f.seek(SeekFrom::Start(offset)).unwrap();
            let (record_id, _, value) = Self::read_record(& mut self.f).unwrap();
            assert_eq!(id, record_id, "Corrupted store or index");
            return Some(value);
        } else {
            return None;
        }
    }

    /** Sets the value for given id. 
     */
    pub fn set(& mut self, id : ID, value : & T) {
        let previous_offset = self.indexer.get(id);
        self.indexer.set(id, & Self::write_record(& mut self.f, id, previous_offset, value));
    }

    /** Returns the number of indexed ids. 
     
        The actual values might be smaller as not all ids can have stored values. Actual number of values in the store can also be greater because same id may have multiple value updates. 
     */
    pub fn len(&self) -> usize {
        return self.indexer.len();
    }

    /** Iterates over the stored values. 
     
        Returns the latest stored value for every id. The ids are guaranteed to be increasing. 
     */
    pub fn iter(& mut self) -> LinkedStoreIter<T, ID> {
        return LinkedStoreIter::new(& mut self. f, & mut self.indexer);
    }

    /** Iterates over all stored values. 
     
        Iterates over *all* stored values, returning them in the order they were added to the store. Multiple values may be returned for single id, the last value returned is the valid one. 
     */
    pub fn iter_all(& mut self) -> LinkedStoreIterAll<T, ID> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return LinkedStoreIterAll{ store : self, max_offset : u64::MAX };
    }

    pub fn savepoint_iter_all(& mut self, sp : & Savepoint) -> LinkedStoreIterAll<T, ID> {
        let max_offset = sp.limit_for(self.name());
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return LinkedStoreIterAll{ store : self, max_offset };
    }

    /** Given an id, returns an iterator over all values ever stored for it. 
     
        The values are returned in the reverse order they were added, i.e. latest value first. 
     */
    pub fn iter_id(& mut self, id : ID) -> LinkedStoreIterId<T, ID> {
        let offset = self.indexer.get(id);
        return LinkedStoreIterId{ store : self, offset };
    }

    /** Reads the record from a file. 
     
        Returns tuple of the id associated with the record, offset of the previous record associated with the id and the value stored. 
     */
    fn read_record(f : & mut File) -> Option<(ID, Option<u64>, T)> {
        if let Ok(id) = f.read_u64::<LittleEndian>() {
            let previous_offset = f.read_u64::<LittleEndian>().unwrap();
            return Some((ID::from(id), if previous_offset == u64::EMPTY { None } else { Some(previous_offset) }, T::deserialize(f)));
        } else {
            return None;
        }
    }

    fn write_record(f : & mut File, id : ID, previous_offset : Option<u64>, value : & T) -> u64 {
        let offset = f.seek(SeekFrom::End(0)).unwrap();
        f.write_u64::<LittleEndian>(id.into()).unwrap();
        match previous_offset {
            Some(offset) => f.write_u64::<LittleEndian>(offset).unwrap(),
            None => f.write_u64::<LittleEndian>(u64::EMPTY).unwrap(),
        }
        T::serialize(f, value);
        return offset;
    }
}

pub struct LinkedStoreIter<'a, T: Serializable<Item = T>, ID : Id> {
    f : &'a mut File,
    iiter : IndexerIterator<'a, u64, ID>,
    why_oh_why : std::marker::PhantomData<T>,
}

impl<'a, T : Serializable<Item = T>, ID : Id> LinkedStoreIter<'a, T, ID> {
    fn new(f : &'a mut File, indexer : &'a mut Indexer<u64, ID>) -> LinkedStoreIter<'a, T, ID> {
        return LinkedStoreIter{
            f : f,
            iiter : indexer.iter(),
            why_oh_why : std::marker::PhantomData{}
        };
    }
}

impl<'a, T : Serializable<Item = T>, ID : Id> Iterator for LinkedStoreIter<'a, T, ID> {
    type Item = (ID, T);

    fn next(& mut self) -> Option<(ID, T)> {
        if let Some((id, offset)) = self.iiter.next() {
            self.f.seek(SeekFrom::Start(offset)).unwrap();
            let (store_id, _, value) = LinkedStore::<T, ID>::read_record(self.f).unwrap();
            assert_eq!(id, store_id, "Corrupted store or its indexing");
            return Some((id, value)); 
        } else {
            return None;
        }
    }
}

pub struct LinkedStoreIterAll<'a, T : Serializable<Item = T>, ID : Id> {
    store : &'a mut LinkedStore<T, ID>,
    max_offset : u64,
}

impl<'a, T : Serializable<Item = T>, ID : Id> Iterator for LinkedStoreIterAll<'a, T, ID> {
    type Item = (ID, T);

    fn next(& mut self) -> Option<(ID, T)> {
        if self.store.f.seek(SeekFrom::Current(0)).unwrap() >= self.max_offset {
            return None;
        } else {
            match LinkedStore::<T, ID>::read_record(& mut self.store.f) {
                Some((id, _, value)) => Some((id, value)),
                None => None
            }
        }
    }

}

pub struct LinkedStoreIterId<'a, T : Serializable<Item = T>, ID : Id> {
    store : &'a mut LinkedStore<T, ID>,
    offset : Option<u64>,
}

impl<'a, T : Serializable<Item = T>, ID : Id> Iterator for LinkedStoreIterId<'a, T, ID> {
    type Item = T;

    fn next(& mut self) -> Option<T> {
        match self.offset {
            Some(offset) => {
                self.store.f.seek(SeekFrom::Start(offset)).unwrap();
                let (_, previous_offset, value) = LinkedStore::<T, ID>::read_record(& mut self.store.f).unwrap(); 
                self.offset = previous_offset;
                return Some(value);
            }, 
            None => None
        }
    }
}

/** Mapping from values to ids. 
 
    Unlike store, mapping does not allow updates to added values. 
 */
pub struct Mapping<T : FixedSizeSerializable<Item = T> + Eq + Hash + Clone, ID : Id = u64> {
    name : String,
    f : File,
    mapping : HashMap<T, ID>,
    size : u64
}

impl<T : FixedSizeSerializable<Item = T> + Eq + Hash + Clone, ID : Id> Mapping<T, ID> {

    pub fn new(root : & str, name : & str, readonly : bool) -> Mapping<T, ID> {
        let mut f;
        if readonly {
            f = OpenOptions::new().read(true).open(format!("{}/{}.mapping", root, name)).unwrap();
        } else {
            f = OpenOptions::new().read(true).write(true).create(true).open(format!("{}/{}.mapping", root, name)).unwrap();
        }
        let size = f.seek(SeekFrom::End(0)).unwrap() / T::SIZE;
        let mut result = Mapping{
            name : name.to_owned(),
            f, 
            mapping : HashMap::new(),
            size
        };
        LOG!("    {}: indices {}, size {}", name, result.size, result.f.seek(SeekFrom::End(0)).unwrap());
        return result;
    }

    pub fn name<'a>(&'a self) -> &'a str {
        return self.name.as_str();
    }

    /** Updates the savepoint with own information. 
     */
    pub fn savepoint(& mut self, savepoint : & mut Savepoint) {
        savepoint.add_entry(
            self.name().to_owned(),
            self.f.seek(SeekFrom::End(0)).unwrap()
        );
    }

    /** Verifies the mapping's integrity. 

        Checking mapping is simple and simply the verification function is called on all items stored in the mapping. 
     */
    pub fn verify(& mut self, checker : & mut dyn FnMut(T) -> Result<(), std::io::Error>) -> Result<(), std::io::Error> {
        let end = self.f.seek(SeekFrom::End(0))?;
        self.f.seek(SeekFrom::Start(0))?;
        loop {
            let offset = self.f.seek(SeekFrom::Current(0))?;
            if offset == end {
                break;
            }
            let item = T::verify(& mut self.f)?;
            checker(item)?;
        }
        return Ok(());
    }

    /** Loads the mapping into from disk to the hashmap. 
     */
    pub fn load(& mut self) {
        // we have to create the iterator ourselves here otherwise rust would complain of double mutable borrow
        self.f.seek(SeekFrom::Start(0)).unwrap();
        let iter = MappingIter{f : & mut self.f, index : 0, size : self.size, why_oh_why : std::marker::PhantomData{} };
        self.mapping.clear();
        for (id, value) in iter {
            self.mapping.insert(value, id);
        }
    }

    /** Clears the loaded mapping and shrinks the hashmap to free up as much memory as possible. 
     */
    pub fn clear(& mut self) {
        self.mapping.clear();
        self.mapping.shrink_to_fit();
    }

    pub fn get(& mut self, value : & T) -> Option<ID> {
        match self.mapping.get(value) {
            Some(id) => Some(*id),
            None => None
        }
    }

    pub fn get_or_create(& mut self, value : & T) -> (ID, bool) {
        match self.mapping.get(value) {
            Some(id) => (*id, false),
            None => {
                let next_id = ID::from(self.mapping.len() as u64);
                self.mapping.insert(value.to_owned(), next_id);
                // serialize the value and increase size
                T::serialize(& mut self.f, value);
                self.size += 1;
                return (next_id, true);
            }
        }
    }

    pub fn get_value(& mut self, id : ID) -> Option<T> {
        if id.into() >= self.size {
            return None;
        }
        let offset = T::SIZE * id.into();
        self.f.seek(SeekFrom::Start(offset)).unwrap();
        let result = T::deserialize(& mut self.f);
        self.f.seek(SeekFrom::End(0)).unwrap();
        return Some(result);
    }

    /** Updates the already stored mapping. 
     */
    pub fn update(& mut self, id : ID, value : & T) {
        assert!(id.into() < self.size);
        let offset = T::SIZE * id.into();
        self.f.seek(SeekFrom::Start(offset)).unwrap();
        T::serialize(& mut self.f, value);
        self.f.seek(SeekFrom::End(0)).unwrap();
        // now that the file has been changed, update the mapping
        self.mapping.remove(value);
        self.mapping.insert(value.to_owned(), id);
    }

    pub fn len(& self) -> usize {
        return self.size as usize;
    }

    pub fn mapping_len(& self) -> usize {
        return self.mapping.len();
    }

    pub fn iter(& mut self) -> MappingIter<T, ID> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return MappingIter{f : & mut self.f, index : 0, size : self.size, why_oh_why : std::marker::PhantomData{} };
    }

    pub fn savepoint_iter(& mut self, sp : & Savepoint) -> MappingIter<T, ID> {
        let max_offset = sp.limit_for(self.name());
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return MappingIter{f : & mut self.f, index : 0, size : max_offset / (std::mem::size_of::<T>() as u64), why_oh_why : std::marker::PhantomData{} };
    }
}

pub struct MappingIter<'a, T : FixedSizeSerializable<Item = T> + Eq + Hash + Clone, ID : Id = u64> {
    f : &'a mut File,
    index : u64,
    size : u64,
    why_oh_why : std::marker::PhantomData<(T, ID)>
}

impl<'a, T : FixedSizeSerializable<Item = T> + Eq + Hash + Clone, ID : Id> Iterator for MappingIter<'a, T, ID> {
    type Item = (ID, T);

    fn next(& mut self) -> Option<(ID, T)> {
        if self.index == self.size {
            return None;
        } else {
            let value = T::deserialize(self.f);
            let id = ID::from(self.index);
            self.index += 1;
            return Some((id, value));
        }
    }
}

/** Mapping from values to ids where the values require indexing. 
 */
pub struct IndirectMapping<T : Serializable<Item = T> + Eq + Hash + Clone, ID : Id = u64> {
    pub (crate) store : Store<T, ID>,
    mapping : HashMap<T, ID>
}

impl<T : Serializable<Item = T> + Eq + Hash + Clone, ID : Id> IndirectMapping<T, ID> {

    /** Creates new mapping. 
     */
    pub fn new(root : & str, name : & str, readonly : bool) -> IndirectMapping<T, ID> {
        return IndirectMapping{
            store : Store::new(root, & format!("{}.mapping", name), readonly),
            mapping : HashMap::new(),
        }
    }

    pub fn name<'a>(&'a self) -> &'a str {
        return self.store.name();
    }

    /** Updates the savepoint with own information. 
     */
    pub fn savepoint(& mut self, savepoint : & mut Savepoint) {
        self.store.savepoint(savepoint);
    }


    /** Verifies the mapping's integrity. 

        Simply verifies the integrity of the store as mapping is just a hashmap and a store.
     */
    pub fn verify(& mut self, checker : & mut dyn FnMut(T) -> Result<(), std::io::Error>) -> Result<(), std::io::Error> {
        return self.store.verify(checker);
    }

    pub fn load(& mut self) {
        self.mapping.clear();
        for (id, value) in self.store.iter() {
            self.mapping.insert(value, id);
        }
    }

    pub fn clear(& mut self) {
        self.mapping.clear();
        self.mapping.shrink_to_fit();
    }

    pub fn get(& mut self, value : & T) -> Option<ID> {
        match self.mapping.get(value) {
            Some(id) => Some(*id),
            None => None
        }
    }

    pub fn get_or_create(& mut self, value : & T) -> (ID, bool) {
        match self.mapping.get(value) {
            Some(id) => (*id, false),
            None => {
                let next_id = ID::from(self.mapping.len() as u64);
                self.store.set(next_id, value);
                self.mapping.insert(value.to_owned(), next_id);
                return (next_id, true);
            }
        }
    }

    pub fn get_value(& mut self, id : ID) -> Option<T> {
        return self.store.get(id);
    }

    pub fn len(& self) -> usize {
        return self.store.len();
    }

    pub fn mapping_len(& self) -> usize {
        return self.mapping.len();
    }

    pub fn iter(& mut self) -> StoreIterAll<T, ID> {
        return self.store.iter_all();
    }

    pub fn savepoint_iter(& mut self, sp : & Savepoint) -> StoreIterAll<T, ID> {
        return self.store.savepoint_iter_all(sp);
    }

}

/** Requirements for a type that can be used to split storage of its elements. 
 
    This is expected to be an enum-like type that satisfies the following properties: the SplitKind must allow to be created from u64 and be convertible to it. These values must be sequential, starting at zero and the number of valid kinds must be stored in the COUNT field. This is important so that the vectors can be used for splits instead of more expensive hash maps. 

    Finally, kind that should be used for empty indices must be provided. This can be a non-valid kind number, but can also be valid one offset part of the empty index is always set to u64::EMPTY so there is no ambiguity. 

    Note that at most 65535 valid categories are supported (this can in theory be easily increased, at the moment I just feel that 2 bytes for storage are sufficient and more would be a waste of disk space).
 */
pub trait SplitKind : FixedSizeSerializable + Eq + Debug {
    const COUNT : u64;
    const EMPTY : Self;    
    fn to_number(& self) -> u64;
    fn from_number(value : u64) -> Self;

}

/** Iterator for all values of a SplitKind. 
 
    This is ugly, but works. Instead of having the iter() function part of the SplitKind, where I ran to all kinds of trait specific issues because they just won't behave like templates, using the iterator as its own class works. 
 */
pub struct SplitKindIter<T : SplitKind> {
    i : u64,
    why_oh_why : std::marker::PhantomData<T>,
}

impl<T : SplitKind> SplitKindIter<T> {
    pub fn new() -> SplitKindIter<T> {
        return SplitKindIter{i : 0, why_oh_why : std::marker::PhantomData{} };
    }
}

impl<T : SplitKind> Iterator for SplitKindIter<T> {
    type Item = T;

    fn next(& mut self) -> Option<T> {
        if self.i == T::COUNT {
            return None;
        } else {
            let result = T::from_number(self.i);
            self.i += 1;
            return Some(result);
        }
    }
} 

/** Offset record that for each id stores its kind and the actual offset. 
 
    Each kind has its own store. The SplitOffset is serializable with fixed size calculated from the offset (8 bytes) and serialized kind size. 
 */
#[derive(Eq, PartialEq)]
pub (crate) struct SplitOffset<KIND : SplitKind<Item = KIND>> {
    pub (crate) offset : u64,
    pub (crate) kind : KIND, 
}

impl<KIND : SplitKind<Item = KIND>> Serializable for SplitOffset<KIND> {
    type Item = SplitOffset<KIND>;

    fn serialize(f : & mut File, value : & SplitOffset<KIND>) {
        u64::serialize(f, & value.offset);
        KIND::serialize(f, & value.kind);
    }

    fn deserialize(f : & mut File) -> SplitOffset<KIND> {
        return SplitOffset{
            offset : u64::deserialize(f),
            kind : KIND::deserialize(f)
        };
    }

    fn verify(f : & mut File) -> Result<SplitOffset<KIND>, std::io::Error> {
        return Ok(SplitOffset{
            offset : u64::verify(f)?,
            kind : KIND::verify(f)?
        });  
    }
}

impl<KIND : SplitKind<Item = KIND>> FixedSizeSerializable for SplitOffset<KIND> {
    const SIZE : u64 = 8 + KIND::SIZE;
}

impl<KIND : SplitKind<Item = KIND>> Indexable for SplitOffset<KIND> {
    const EMPTY : SplitOffset<KIND> = SplitOffset{offset : u64::EMPTY, kind : KIND::EMPTY};
}

/** Split store contains single index, but multiple files that store the data based on its kind. 
 */
pub struct SplitStore<T : Serializable<Item = T>, KIND : SplitKind<Item = KIND>, ID : Id = u64> {
    name : String,
    pub (crate) indexer : Indexer<SplitOffset<KIND>, ID>,
    pub (crate) files : Vec<File>,
    why_oh_why : std::marker::PhantomData<T>
}

impl<T : Serializable<Item = T>, KIND: SplitKind<Item = KIND>, ID : Id> SplitStore<T, KIND, ID> {
    pub fn new(root : & str, name : & str, readonly : bool) -> SplitStore<T, KIND, ID> {
        let mut files = Vec::<File>::new();
        for i in 0..KIND::COUNT {
            let path = format!("{}/{}-{:?}.splitstore", root, name, KIND::from_number(i));
            let f;
            if readonly {
                f = OpenOptions::new().read(true).open(path).unwrap();
            } else {
                f = OpenOptions::new().read(true).write(true).create(true).open(path).unwrap();
            }
            files.push(f);
        }
        let result = SplitStore{
            name : name.to_owned(),
            indexer : Indexer::new(root, name, readonly),
            files, 
            why_oh_why : std::marker::PhantomData{}
        };
        LOG!("    {}: indices {}, splits {}", name, result.indexer.len(), result.files.len());
        return result;
    }

    pub fn name<'a>(&'a self) -> &'a str {
        return self.name.as_str();
    }

    /** Updates the savepoint with own information. 
     */
    pub fn savepoint(& mut self, savepoint : & mut Savepoint) {
        let mut i = 0;
        for f in self.files.iter_mut() {
            savepoint.add_entry(
                format!("{}-{}", self.name, i),
                f.seek(SeekFrom::End(0)).unwrap()
            );
            i += 1;
        }
    }

    /** Verifies the split store's integrity
     
        For a split store, this means:

        - look at all files and verify that the things stored in them are valid


     */
    pub fn verify(& mut self, checker : & mut dyn FnMut(T) -> Result<(), std::io::Error>) -> Result<(), std::io::Error> {
        let mut latest_mappings = Vec::new();
        let mut i = 0;
        for f in self.files.iter_mut() {
            latest_mappings.push(HashMap::<u64, u64>::new());
            let end = f.seek(SeekFrom::End(0))?;
            f.seek(SeekFrom::Start(0))?;
            loop {
                let offset = f.seek(SeekFrom::Current(0))?;
                if offset == end {
                    break;
                }
                let id = f.read_u64::<LittleEndian>()?;
                if id >= self.indexer.size {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("SplitStore id {:?}, but only {} ids known at offset {} in split {:?}", ID::from(id), self.indexer.size, offset, KIND::from_number(i))));
                }
                let item = T::verify(f)?;
                checker(item)?;
                // now we need to add this to the mappings, but only to those valid for current id
                latest_mappings.get_mut(i as usize).unwrap().insert(id, offset);
            }
            i += 1;
        }
        // then check the index's integrity
        for (id, offset) in self.indexer.iter() {
            if offset == SplitOffset::<KIND>::EMPTY {
                let mut i = 0;
                for mapping in latest_mappings.iter() {
                    if mapping.contains_key(& id.into()) {
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("SplitStore index id {:?}, has empty index, but offset {} found in the split {:?}", id, mapping[& id.into()], KIND::from_number(i))));
                    }
                    i += 1;
                }
            } else {
                match latest_mappings[offset.kind.to_number() as usize].get(& id.into()) {
                    Some(found_offset) => {
                        if offset.offset != *found_offset {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Store index id {:?}, has indexed offset {} in split {:?}, but offset {} found", id, offset.offset, offset.kind, found_offset)));
                        }
                    },
                    None => {
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Store index id {:?}, has indexed offset {} in split {:?} but none found", id, offset.offset, offset.kind)));
                    }
                }
            }
        }
        return Ok(());
    }


    /** Determines the file that holds value for given id and returns the stored value. If the value has not been stored, returns None. 
     */
    pub fn get(& mut self, id : ID) -> Option<T> {
        match self.indexer.get(id) {
            Some(offset) => {
                let f = self.files.get_mut(offset.kind.to_number() as usize).unwrap();
                f.seek(SeekFrom::Start(offset.offset)).unwrap();
                // we can use default store reader
                let (record_id, value) = Store::<T, ID>::read_record(f).unwrap();
                assert_eq!(id, record_id, "Corrupted store or index");
                return Some(value);
            },
            None => None
        }
    }

    /** Sets the value for given id in a file specified by given kind.  
     
        If this is an update, then the kind specified must be the same as the kind the value has already been stored under. In other words, the split store allows updates of the values, but value cannot change its kind. 
     */
    pub fn set(& mut self, id : ID, kind : KIND, value : & T) {
        match self.indexer.get(id) {
            Some(offset) => {
                assert_eq!(kind, offset.kind, "Cannot change kind of already stored value");
            },
            None => {}
        }
        let f = self.files.get_mut(kind.to_number() as usize).unwrap();
        self.indexer.set(id, & SplitOffset{
            offset : Store::<T, ID>::write_record(f, id, value),
            kind
        });
    }

    pub fn len(&self) -> usize {
        return self.indexer.len();
    }

    pub fn savepoint_iter(& mut self, sp : & Savepoint) -> SplitStoreIterAll<T,KIND,ID> {
        let mut max_offsets = Vec::new();
        let mut i = 0;
        for _f in self.files.iter_mut() {
            max_offsets.push(sp.limit_for(& format!("{}-{}", self.name, i)));
            i += 1;
        }
        self.files[0].seek(SeekFrom::Start(0)).unwrap();
        return SplitStoreIterAll{ store : self, max_offsets, split : 0 }
    }

    // TODO add iterators

}

pub struct SplitStoreIterAll<'a, T : Serializable<Item = T>, KIND: SplitKind<Item = KIND>, ID : Id> {
    store: &'a mut SplitStore<T, KIND, ID>,
    max_offsets : Vec<u64>,
    split : usize,
}

impl<'a, T : Serializable<Item = T>, KIND: SplitKind<Item = KIND>, ID : Id> Iterator for SplitStoreIterAll<'a, T, KIND, ID> {
    type Item = (ID, KIND, T);

    fn next(& mut self) -> Option<(ID, KIND, T)> {
        loop {
            if self.store.files[self.split].seek(SeekFrom::Current(0)).unwrap() >= self.max_offsets[self.split] {
                self.split += 1;
                if self.split >= self.max_offsets.len() {
                    return None;
                }
                self.store.files[self.split].seek(SeekFrom::Start(0)).unwrap();
            } 
            // there might be empty splits too
            if let Some((id, value)) = Store::<T, ID>::read_record(self.store.files.get_mut(self.split).unwrap()) {
                return Some((id, KIND::from_number(self.split as u64), value));
            }
        }
    }
}

/** Savepoint for the entire datastore. 
 
 */
pub struct Savepoint {
    name : String,
    time : i64,
    sizes : HashMap<String, u64>,
}

impl Savepoint {
    pub fn new(name : String) -> Savepoint {
        let time = helpers::now();
        return Savepoint{
            name, 
            time, 
            sizes : HashMap::new(),
        };
    }

    pub fn add_entry(& mut self, name : String, size : u64) {
        assert!(! self.sizes.contains_key(&name), "weird  {}", name);
        self.sizes.insert(name, size);
    }

    pub fn limit_for(& self, name : & str) -> u64 {
        match self.sizes.get(name) {
            Some(size) => return *size,
            None => return 0,
        }
    }

    pub fn name(& self) -> & str {
        return self.name.as_str();
    }

    pub fn size(& self) -> u64 {
        return self.sizes.iter().map(|(_, size)| size).sum();
    }

    /** Returns the time at which the savepoint has been created. 
     */    
    pub fn time(& self) -> i64 {
        return self.time;
    }
}

/** Simple formatter for a savepoint. 
 
    Displays the name and time as well as the sizes for all stored files
 */
impl std::fmt::Display for Savepoint {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Savepoint: {}, time: {} ({})", self.name, helpers::pretty_timestamp(self.time), self.time)?;
        writeln!(f, "    files: {}", self.sizes.len())?;
        for (file, size) in self.sizes.iter() {
            writeln!(f, "    {} {}", size, file)?;
        }
        return Ok(());
    }
}

impl Serializable for Savepoint {
    type Item = Savepoint;

    fn serialize(f : & mut File, value : & Savepoint) {
        i64::serialize(f, & value.time);
        String::serialize(f, & value.name);
        u32::serialize(f, & (value.sizes.len() as u32));
        for (name, size) in value.sizes.iter() {
            String::serialize(f, name);
            u64::serialize(f, size);
        }
    }

    fn deserialize(f : & mut File) -> Savepoint {
        let time = i64::deserialize(f);
        let name = String::deserialize(f);
        let mut records = u32::deserialize(f);
        let mut result = Savepoint{
            name, 
            time, 
            sizes : HashMap::new(),
        };
        while records > 0 {
            let name = String::deserialize(f);
            let size = u64::deserialize(f);
            result.sizes.insert(name, size);
            records -= 1;
        }
        return result;
    }

    fn verify(f : & mut File) -> Result<Savepoint, std::io::Error> {
        let time = i64::verify(f)?;
        let name = String::verify(f)?;
        let mut records = u32::verify(f)?;
        if records as u64 > MAX_BUFFER_LENGTH {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid length of savepoint records"));
        }
        let mut result = Savepoint{
            name, 
            time, 
            sizes : HashMap::new(),
        };
        while records > 0 {
            let name = String::verify(f)?;
            let size = u64::verify(f)?;
            result.sizes.insert(name, size);
            records -= 1;
        }
        return Ok(result);
    }

}


