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
use std::io::*;
use byteorder::*;
use std::collections::*;
use std::hash::*;
use std::fmt::*;
//use crate::records::*;


pub trait Serializable {
    fn serialize(f : & mut File, value : & Self);
    fn deserialize(f : & mut File) -> Self;
}

pub trait FixedSizeSerializable : Serializable {
    const SIZE : u64;
}

pub trait Indexable : FixedSizeSerializable + Eq {
    const EMPTY : Self;
}

/* The serializable, fixed size serializable and indexable implementations are provided for u64 used as id in the rest of the file. 
 */
impl Serializable for u64 {
    fn serialize(f : & mut File, value : & u64) {
        f.write_u64::<LittleEndian>(*value).unwrap();
    }
    fn deserialize(f : & mut File) -> u64 {
        return f.read_u64::<LittleEndian>().unwrap();
    }
}

impl FixedSizeSerializable for u64 {
    const SIZE : u64 = 8;
}

impl Indexable for u64 {
    const EMPTY : u64 = std::u64::MAX;
}

impl Serializable for u32 {
    fn serialize(f : & mut File, value : & u32) {
        f.write_u32::<LittleEndian>(*value).unwrap();
    }
    fn deserialize(f : & mut File) -> u32 {
        return f.read_u32::<LittleEndian>().unwrap();
    }
}

impl FixedSizeSerializable for u32 {
    const SIZE : u64 = 4;
}

impl Serializable for u16 {
    fn serialize(f : & mut File, value : & u16) {
        f.write_u16::<LittleEndian>(*value).unwrap();
    }
    fn deserialize(f : & mut File) -> u16 {
        return f.read_u16::<LittleEndian>().unwrap();
    }
}

impl FixedSizeSerializable for u16 {
    const SIZE : u64 = 2;
}
impl Serializable for u8 {
    fn serialize(f : & mut File, value : & u8) {
        f.write_u8(*value).unwrap();
    }
    fn deserialize(f : & mut File) -> u8 {
        return f.read_u8().unwrap();
    }
}

impl FixedSizeSerializable for u8 {
    const SIZE : u64 = 1;
}

/** Strings are serializable too, very handy:)
 */
impl Serializable for String {

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
}

/** Holds indices for each id.

    The idsn are expected to be mostly consecutive, i.e. if an id `N` is added all ids from `0` to `N-1` either must exist, or will be created. 

    The indexer is usually not used alone, but as part of more complex structures. 
 */
pub struct Indexer<T : Indexable = u64> {
    name : String, 
    f : File, 
    size : u64,
    why_oh_why : std::marker::PhantomData<T>
}

impl<T : Indexable> Indexer<T> {
    pub fn new(root : & str, name : & str) -> Indexer<T> {
        let mut f = OpenOptions::new().read(true).write(true).create(true).open(format!("{}/{}.idx", root, name)).unwrap();
        let size = f.seek(SeekFrom::End(0)).unwrap() / T::SIZE;
        return Indexer{ name : name.to_owned(), f, size, why_oh_why : std::marker::PhantomData{} };
    } 

    pub fn get(& mut self, id : u64) -> Option<T> {
        if id < self.size {
            self.f.seek(SeekFrom::Start(T::SIZE * id)).unwrap();
            let result = T::deserialize(& mut self.f);
            if result != T::EMPTY {
                return Some(result); 
            } else {
                return None;
            }
        }
        return None;
    }

    pub fn set(& mut self, id : u64, value : & T) {
        if id < self.size {
            self.f.seek(SeekFrom::Start(T::SIZE * id)).unwrap();
            T::serialize(& mut self.f, value);
        } else {
            self.f.seek(SeekFrom::End(0)).unwrap();
            while id > self.size  {
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

    pub fn iter(& mut self) -> IndexerIterator<T> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return IndexerIterator{indexer : self, id : 0};
    }

}

pub struct IndexerIterator<'a, T : Indexable> {
    indexer : &'a mut Indexer<T>,
    id : u64
}

impl<'a, T : Indexable> Iterator for IndexerIterator<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<(u64, T)> {
        loop {
            if self.id == self.indexer.len() as u64 { 
                return None;
            } else {
                let id = self.id;
                let result = T::deserialize(& mut self.indexer.f);
                self.id += 1;
                return Some((id, result));
            }
        }
    }
}
/*
pub trait StoreTrait {
    type Value;
    type Iterator;
    fn get_x(& mut self, id : u64) -> Option<Self::Value>;
    fn iter_x(& mut self) -> Self::Iterator;
}

impl<'a, T: Serializable> StoreTrait for &'a mut Store<T> {
    type Value = T;
    type Iterator = StoreIter<'a, T>;

    fn get_x(& mut self, id : u64) -> Option<Self::Value> {
        return self.get(id);
    }
    fn iter_x(& mut self) -> Self::Iterator {
        return StoreIter::new(& mut self.f, & mut self.indexer);
    }
}

*/

/** Store implementation. 
 
    Store is an indexed updatable container that keeps history of updates.

    TODO add savepoint
 */
pub struct Store<T : Serializable> {
    indexer : Indexer,
    f : File,
    why_oh_why : std::marker::PhantomData<T>,
}

impl<T: Serializable> Store<T> {

    pub fn new(root : & str, name : & str) -> Store<T> {
        let f = OpenOptions::new().read(true).write(true).create(true).open(format!("{}/{}.store", root, name)).unwrap();
        let mut result = Store{
            indexer : Indexer::new(root, name),
            f,
            why_oh_why : std::marker::PhantomData{}
        };
        println!("    {}: indices {}, size {}", name, result.indexer.len(), result.f.seek(SeekFrom::End(0)).unwrap());
        return result;
    }

    /** Gets the value for given id. 
     */
    pub fn get(& mut self, id : u64) -> Option<T> {
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
    pub fn set(& mut self, id : u64, value : & T) {
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
    pub fn iter(& mut self) -> StoreIter<T> {
        return StoreIter::new(& mut self. f, & mut self.indexer);
    }

    /** Iterates over all stored values. 
     
        Iterates over *all* stored values, returning them in the order they were added to the store. Multiple values may be returned for single id, the last value returned is the valid one. 
     */
    pub fn iter_all(& mut self) -> StoreIterAll<T> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return StoreIterAll{ store : self };
    }

    /** Reads the record from a file. 
     
        Returns tuple of the id associated with the record and the value stored. 
     */
    fn read_record(f : & mut File) -> Option<(u64, T)> {
        if let Ok(id) = f.read_u64::<LittleEndian>() {
            return Some((id, T::deserialize(f)));
        } else {
            return None;
        }
    }

    fn write_record(f : & mut File, id : u64, value : & T) -> u64 {
        let offset = f.seek(SeekFrom::End(0)).unwrap();
        f.write_u64::<LittleEndian>(id).unwrap();
        T::serialize(f, value);
        return offset;
    }
}

pub struct StoreIter<'a, T: Serializable> {
    f : &'a mut File,
    iiter : IndexerIterator<'a, u64>,
    why_oh_why : std::marker::PhantomData<T>,
}

impl<'a, T : Serializable> StoreIter<'a, T> {
    fn new(f : &'a mut File, indexer : &'a mut Indexer) -> StoreIter<'a, T> {
        return StoreIter{
            f : f,
            iiter : indexer.iter(),
            why_oh_why : std::marker::PhantomData{}
        };
    }
}

impl<'a, T : Serializable> Iterator for StoreIter<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<(u64, T)> {
        if let Some((id, offset)) = self.iiter.next() {
            self.f.seek(SeekFrom::Start(offset)).unwrap();
            let (store_id, value) = Store::<T>::read_record(self.f).unwrap();
            assert_eq!(id, store_id, "Corrupted store or its indexing");
            return Some((id, value)); 
        } else {
            return None;
        }
    }
}

pub struct StoreIterAll<'a, T : Serializable> {
    store : &'a mut Store<T>
}

impl<'a, T : Serializable> Iterator for StoreIterAll<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<(u64, T)> {
        return Store::<T>::read_record(& mut self.store.f); 
    }
}

/** Linked store implementation. 
 
    Store is an indexed updatable container that keeps history of updates.

    TODO add savepoint
 */
pub struct LinkedStore<T : Serializable> {
    indexer : Indexer,
    f : File,
    why_oh_why : std::marker::PhantomData<T>,
}

impl<T: Serializable> LinkedStore<T> {

    pub fn new(root : & str, name : & str) -> LinkedStore<T> {
        let f = OpenOptions::new().read(true).write(true).create(true).open(format!("{}/{}.store", root, name)).unwrap();
        let mut result = LinkedStore{
            indexer : Indexer::new(root, name),
            f,
            why_oh_why : std::marker::PhantomData{}
        };
        println!("    {}: indices {}, size {}", name, result.indexer.len(), result.f.seek(SeekFrom::End(0)).unwrap());
        return result;

    }

    /** Gets the value for given id. 
     */
    pub fn get(& mut self, id : u64) -> Option<T> {
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
    pub fn set(& mut self, id : u64, value : & T) {
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
    pub fn iter(& mut self) -> LinkedStoreIter<T> {
        return LinkedStoreIter::new(& mut self. f, & mut self.indexer);
    }

    /** Iterates over all stored values. 
     
        Iterates over *all* stored values, returning them in the order they were added to the store. Multiple values may be returned for single id, the last value returned is the valid one. 
     */
    pub fn iter_all(& mut self) -> LinkedStoreIterAll<T> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return LinkedStoreIterAll{ store : self };
    }

    /** Given an id, returns an iterator over all values ever stored for it. 
     
        The values are returned in the reverse order they were added, i.e. latest value first. 
     */
    pub fn iter_id(& mut self, id : u64) -> LinkedStoreIterId<T> {
        let offset = self.indexer.get(id);
        return LinkedStoreIterId{ store : self, offset };
    }

    /** Reads the record from a file. 
     
        Returns tuple of the id associated with the record, offset of the previous record associated with the id and the value stored. 
     */
    fn read_record(f : & mut File) -> Option<(u64, Option<u64>, T)> {
        if let Ok(id) = f.read_u64::<LittleEndian>() {
            let previous_offset = f.read_u64::<LittleEndian>().unwrap();
            return Some((id, if previous_offset == u64::EMPTY { None } else { Some(previous_offset) }, T::deserialize(f)));
        } else {
            return None;
        }
    }

    fn write_record(f : & mut File, id : u64, previous_offset : Option<u64>, value : & T) -> u64 {
        let offset = f.seek(SeekFrom::End(0)).unwrap();
        f.write_u64::<LittleEndian>(id).unwrap();
        match previous_offset {
            Some(offset) => f.write_u64::<LittleEndian>(offset).unwrap(),
            None => f.write_u64::<LittleEndian>(u64::EMPTY).unwrap(),
        }
        T::serialize(f, value);
        return offset;
    }
}

pub struct LinkedStoreIter<'a, T: Serializable> {
    f : &'a mut File,
    iiter : IndexerIterator<'a, u64>,
    why_oh_why : std::marker::PhantomData<T>,
}

impl<'a, T : Serializable> LinkedStoreIter<'a, T> {
    fn new(f : &'a mut File, indexer : &'a mut Indexer) -> LinkedStoreIter<'a, T> {
        return LinkedStoreIter{
            f : f,
            iiter : indexer.iter(),
            why_oh_why : std::marker::PhantomData{}
        };
    }
}

impl<'a, T : Serializable> Iterator for LinkedStoreIter<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<(u64, T)> {
        if let Some((id, offset)) = self.iiter.next() {
            self.f.seek(SeekFrom::Start(offset)).unwrap();
            let (store_id, _, value) = LinkedStore::<T>::read_record(self.f).unwrap();
            assert_eq!(id, store_id, "Corrupted store or its indexing");
            return Some((id, value)); 
        } else {
            return None;
        }
    }
}

pub struct LinkedStoreIterAll<'a, T : Serializable> {
    store : &'a mut LinkedStore<T>
}

impl<'a, T : Serializable> Iterator for LinkedStoreIterAll<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<(u64, T)> {
        match LinkedStore::<T>::read_record(& mut self.store.f) {
            Some((id, _, value)) => Some((id, value)),
            None => None
        }
    }

}

pub struct LinkedStoreIterId<'a, T : Serializable> {
    store : &'a mut LinkedStore<T>,
    offset : Option<u64>,
}

impl<'a, T : Serializable> Iterator for LinkedStoreIterId<'a, T> {
    type Item = T;

    fn next(& mut self) -> Option<T> {
        match self.offset {
            Some(offset) => {
                self.store.f.seek(SeekFrom::Start(offset)).unwrap();
                let (_, previous_offset, value) = LinkedStore::<T>::read_record(& mut self.store.f).unwrap(); 
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
pub struct Mapping<T : FixedSizeSerializable + Eq + Hash + Clone> {
    name : String,
    f : File,
    mapping : HashMap<T, u64>,
    size : u64
}

impl<T : FixedSizeSerializable + Eq + Hash + Clone> Mapping<T> {

    pub fn new(root : & str, name : & str) -> Mapping<T> {
        let mut f = OpenOptions::new().read(true).write(true).create(true).open(format!("{}/{}.mapping", root, name)).unwrap();
        let size = f.seek(SeekFrom::End(0)).unwrap() / T::SIZE;
        let mut result = Mapping{
            name : name.to_owned(),
            f, 
            mapping : HashMap::new(),
            size
        };
        println!("    {}: indices {}, size {}", name, result.size, result.f.seek(SeekFrom::End(0)).unwrap());
        return result;
    }

    /** Loads the mapping into from disk to the hashmap. 
     */
    pub fn load(& mut self) {
        unimplemented!();
    }

    /** Clears the loaded mapping and shrinks the hashmap to free up as much memory as possible. 
     */
    pub fn clear(& mut self) {
        self.mapping.clear();
        self.mapping.shrink_to_fit();
    }

    pub fn get(& mut self, value : & T) -> Option<u64> {
        match self.mapping.get(value) {
            Some(id) => Some(*id),
            None => None
        }
    }

    pub fn get_or_create(& mut self, value : & T) -> (u64, bool) {
        match self.mapping.get(value) {
            Some(id) => (*id, false),
            None => {
                let next_id = self.mapping.len() as u64;
                self.mapping.insert(value.to_owned(), next_id);
                // serialize the value and increase size
                T::serialize(& mut self.f, value);
                self.size += 1;
                return (next_id, true);
            }
        }
    }

    pub fn get_value(& mut self, id : u64) -> Option<T> {
        if id >= self.size {
            return None;
        }
        let offset = T::SIZE * id;
        self.f.seek(SeekFrom::Start(offset)).unwrap();
        let result = T::deserialize(& mut self.f);
        self.f.seek(SeekFrom::End(0)).unwrap();
        return Some(result);
    }

    /** Updates the already stored mapping. 
     */
    pub fn update(& mut self, id : u64, value : & T) {
        assert!(id < self.size);
        let offset = T::SIZE * id;
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

    pub fn iter(& mut self) -> MappingIter<T> {
        self.f.seek(SeekFrom::Start(0)).unwrap();
        return MappingIter{mapping : self, index : 0};
    }
}

pub struct MappingIter<'a, T : FixedSizeSerializable + Eq + Hash + Clone> {
    mapping : &'a mut Mapping<T>,
    index : u64
}

impl<'a, T : FixedSizeSerializable + Eq + Hash + Clone> Iterator for MappingIter<'a, T> {
    type Item = (u64, T);

    fn next(& mut self) -> Option<(u64, T)> {
        if self.index == self.mapping.size {
            return None;
        } else {
            let value = T::deserialize(& mut self.mapping.f);
            let id = self.index;
            self.index += 1;
            return Some((id, value));
        }
    }
}

/** Mapping from values to ids where the values require indexing. 
 */
struct IndirectMapping<T> {
    indexer : Indexer,
    f : File,
    mapping : HashMap<T, u64>
}

impl<T> IndirectMapping<T> {

    pub fn new(_root : & str, _name : & str) -> IndirectMapping<T> {
        unimplemented!();
    }

    pub fn get(& mut self, _value : & T) -> Option<u64> {
        unimplemented!();
    }

    pub fn get_or_create(& mut self, _value : & T) -> (u64, bool) {
        unimplemented!();

    }

    pub fn get_value(& mut self, _id : u64) -> Option<T> {
        unimplemented!();
    }

    pub fn len(& self) -> usize {
        unimplemented!();
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
struct SplitOffset<KIND : SplitKind> {
    offset : u64,
    kind : KIND, 
}

impl<KIND : SplitKind> Serializable for SplitOffset<KIND> {
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
}

impl<KIND : SplitKind> FixedSizeSerializable for SplitOffset<KIND> {
    const SIZE : u64 = 8 + KIND::SIZE;
}

impl<KIND : SplitKind> Indexable for SplitOffset<KIND> {
    const EMPTY : SplitOffset<KIND> = SplitOffset{offset : u64::EMPTY, kind : KIND::EMPTY};
}

/** Split store contains single index, but multiple files that store the data based on its kind. 
 */
pub struct SplitStore<T : Serializable, KIND : SplitKind> {
    name : String,
    indexer : Indexer<SplitOffset<KIND>>,
    files : Vec<File>,
    why_oh_why : std::marker::PhantomData<T>
}

impl<T : Serializable, KIND: SplitKind> SplitStore<T, KIND> {
    pub fn new(root : & str, name : & str) -> SplitStore<T, KIND> {
        let mut files = Vec::<File>::new();
        for i in 0..KIND::COUNT {
            let path = format!("{}/{}-{:?}.splitstore", root, name, KIND::from_number(i));
            let f = OpenOptions::new().read(true).write(true).create(true).open(path).unwrap();
            files.push(f);
        }
        let result = SplitStore{
            name : name.to_owned(),
            indexer : Indexer::new(root, name),
            files, 
            why_oh_why : std::marker::PhantomData{}
        };
        println!("    {}: indices {}, splits {}", name, result.indexer.len(), result.files.len());
        return result;

    }

    /** Determines the file that holds value for given id and returns the stored value. If the value has not been stored, returns None. 
     */
    pub fn get(& mut self, id : u64) -> Option<T> {
        match self.indexer.get(id) {
            Some(offset) => {
                let f = self.files.get_mut(offset.kind.to_number() as usize).unwrap();
                f.seek(SeekFrom::Start(offset.offset)).unwrap();
                // we can use default store reader
                let (record_id, value) = Store::<T>::read_record(f).unwrap();
                assert_eq!(id, record_id, "Corrupted store or index");
                return Some(value);
            },
            None => None
        }
    }

    /** Sets the value for given id in a file specified by given kind.  
     
        If this is an update, then the kind specified must be the same as the kind the value has already been stored under. In other words, the split store allows updates of the values, but value cannot change its kind. 
     */
    pub fn set(& mut self, id : u64, value : & T, kind : KIND) {
        match self.indexer.get(id) {
            Some(offset) => {
                assert_eq!(kind, offset.kind, "Cannot change kind of already stored value");
            },
            None => {}
        }
        let f = self.files.get_mut(kind.to_number() as usize).unwrap();
        self.indexer.set(id, & SplitOffset{
            offset : Store::<T>::write_record(f, id, value),
            kind
        });
    }

    pub fn len(&self) -> usize {
        return self.indexer.len();
    }

    // TODO add iterators

}

/** Split store contains single index, but multiple files that store the data based on its kind. 
 */
pub struct SplitLinkedStore<T : Serializable, KIND : SplitKind> {
    name : String,
    indexer : Indexer<SplitOffset<KIND>>,
    files : Vec<File>,
    why_oh_why : std::marker::PhantomData<T>
}

impl<T : Serializable, KIND: SplitKind> SplitLinkedStore<T, KIND> {
    pub fn new(root : & str, name : & str) -> SplitLinkedStore<T, KIND> {
        let mut files = Vec::<File>::new();
        for i in 0..KIND::COUNT {
            let path = format!("{}/{}-{:?}.splitstore", root, name, KIND::from_number(i));
            let f = OpenOptions::new().read(true).write(true).create(true).open(path).unwrap();
            files.push(f);
        }
        return SplitLinkedStore{
            name : name.to_owned(),
            indexer : Indexer::new(root, name),
            files, 
            why_oh_why : std::marker::PhantomData{}
        };
    }

    /** Determines the file that holds value for given id and returns the stored value. If the value has not been stored, returns None. 
     */
    pub fn get(& mut self, id : u64) -> Option<T> {
        match self.indexer.get(id) {
            Some(offset) => {
                let f = self.files.get_mut(offset.kind.to_number() as usize).unwrap();
                f.seek(SeekFrom::Start(offset.offset)).unwrap();
                // we can use default store reader
                let (record_id, _, value) = LinkedStore::<T>::read_record(f).unwrap();
                assert_eq!(id, record_id, "Corrupted store or index");
                return Some(value);
            },
            None => None
        }
    }

    /** Sets the value for given id in a file specified by given kind.  
     
        If this is an update, then the kind specified must be the same as the kind the value has already been stored under. In other words, the split store allows updates of the values, but value cannot change its kind. 
     */
    pub fn set(& mut self, id : u64, value : & T, kind : KIND) {
        let f = self.files.get_mut(kind.to_number() as usize).unwrap();
        match self.indexer.get(id) {
            Some(previous_offset) => {
                assert_eq!(kind, previous_offset.kind, "Cannot change kind of already stored value");
                self.indexer.set(id, & SplitOffset{
                    offset : LinkedStore::<T>::write_record(f, id, Some(previous_offset.offset), value),
                    kind
                });
            },
            None => {
                self.indexer.set(id, & SplitOffset{
                    offset : LinkedStore::<T>::write_record(f, id, None, value),
                    kind
                });
            }
        }
    }

    pub fn len(&self) -> usize {
        return self.indexer.len();
    }

    // TODO add iterators

}






/** ID Prefix trait. 
 
    The idea is that a sequential id can have its prefix, which determines the kind of the id, some category on which we can split the underlying objects responsible for storing objects. These underlying objects are not aware of the prefix part of the id and only use the sequential part for their dealings. 
 */
pub trait IDPrefix {
    fn prefix(id : u64) -> Self;
    fn sequential_part(id : u64) -> u64;

    fn augment(& self, sequential_part : u64) -> u64;
}



pub struct SplitIterator<T : IDPrefix, W : Serializable, ITER : Iterator<Item = (u64, W)>> {
    pub (crate) iter : ITER,
    pub (crate) prefix : T,
} 


impl<T : IDPrefix, W : Serializable, ITER: Iterator<Item = (u64, W)>> Iterator for SplitIterator<T, W, ITER> {
    type Item = (u64, W);

    fn next(& mut self) -> Option<(u64, W)> {
        match self.iter.next() {
            Some((id, value)) => Some((self.prefix.augment(id), value)),
            None => None,
        }
    }
} 

