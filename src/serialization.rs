use std::io;
use std::io::{Read, Write};
use std::collections::{HashMap};
use std::hash::Hash;
use byteorder::*;
use zstd;

/** A trait for serializable items.
 
    Allows reading and writing the items to a file. The serializable's contract is a bit specific as it also keeps track of the position in the file it writes to/reads from as this is rather expensive in Rust I was told, even when using buffers.
 */
pub trait Serializable {
    // can't use Self because of unknown compile-time size, so hiding behind the typedef
    type Item;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item>;
    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()>;

    fn just_read_from(f : & mut dyn Read) -> io::Result<Self::Item> {
        let mut offset = 0; // fake offset
        return Self::read_from(f, & mut offset);
    }

    fn just_write_to(f : & mut dyn Write, item : & Self::Item) -> io::Result<()> {
        let mut offset = 0; // fake offset
        return Self::write_to(f, item, & mut offset);
    }
}

/** A trait for serializable items with fixed size.
    
 */
pub trait FixedSize : Serializable {

    /** Returns the serialized size of the item.
     */
    fn size_of() -> usize;

}

// ------------------------------------------------------------------------------------------------

/* Serializable and FixedSize implementations for primitive types. 
 */
impl Serializable for u64 {
    type Item = u64;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        *offset += 8;
        return f.read_u64::<LittleEndian>();
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        *offset += 8;
        return f.write_u64::<LittleEndian>(*item);
    }
}

impl FixedSize for u64 {
    fn size_of() -> usize { 8 }
}

impl Serializable for u32 {
    type Item = u32;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        *offset += 4;
        return f.read_u32::<LittleEndian>();
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        *offset += 4;
        return f.write_u32::<LittleEndian>(*item);
    }
}

impl FixedSize for u32 {
    fn size_of() -> usize { 4 }
}

impl Serializable for u16 {
    type Item = u16;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        *offset += 2;
        return f.read_u16::<LittleEndian>();
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        *offset += 2;
        return f.write_u16::<LittleEndian>(*item);
    }
}

impl FixedSize for u16 {
    fn size_of() -> usize { 2 }
}

/* FIXME: Can't use this because rust does not support trait impl specialization yet:( and therefore this would conflict with the Vec<u8> specialization later
    
impl Serializable for u8 {
    type Item = u8;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        *offset += 1;
        return f.read_u8();
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        *offset += 1;
        return f.write_u8(*item);
    }
}

impl FixedSize for u8 {
    fn size_of() -> usize { 1 }
}
*/

impl Serializable for i64 {
    type Item = i64;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        *offset += 8;
        return f.read_i64::<LittleEndian>();
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        *offset += 8;
        return f.write_i64::<LittleEndian>(*item);
    }
}

impl FixedSize for i64 {
    fn size_of() -> usize { 8 }
}


/* Serializable implementation for String. 

   Unlike vec<u8>, strings are *not* compressed and are stored in their raw form. 
 */
impl Serializable for String {
    type Item = String;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        let len = u32::read_from(f, offset)?;
        let mut buf = vec![0; len as usize];
        if f.read(& mut buf)? as u32 != len {
            panic!("Corrupted binary format, expected size {} around offset {}", len, offset);
        }
        *offset += len as u64;
        return Ok(String::from_utf8(buf).unwrap());
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        u32::write_to(f, & (item.len() as u32), offset)?;
        f.write(item.as_bytes())?;
        *offset += item.len() as u64;
        return Ok(());
    }
}

/* Serializable implementations for containers of serializable elements.
 */
impl<T : Serializable<Item = T>> Serializable for Vec<T> {
    type Item = Vec<T>;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        let mut len = u64::read_from(f, offset)?;
        let mut result : Vec<T> = Vec::new();
        while len > 0 {
            result.push(T::read_from(f, offset)?);
            len -= 1;
        }
        return Ok(result);
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        u64::write_to(f, & (item.len() as u64), offset)?;
        for i in item.iter() {
            T::write_to(f, i, offset)?;
        }
        return Ok(());
    }
}

impl<KEY : Serializable<Item = KEY> + Eq + Hash, VALUE : Serializable<Item = VALUE>> Serializable for HashMap<KEY, VALUE> {
    type Item = HashMap<KEY, VALUE>;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
        let mut len = u64::read_from(f, offset)?;
        let mut result : HashMap<KEY, VALUE> = HashMap::new();
        while len > 0 {
            let key = KEY::read_from(f, offset)?;
            let value = VALUE::read_from(f, offset)?;
            result.insert(key, value);
            len -= 1;
        }
        return Ok(result);
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        u64::write_to(f, & (item.len() as u64), offset)?;
        for (k, v) in item.iter() {
            KEY::write_to(f, k, offset)?;
            VALUE::write_to(f, v, offset)?;
        }
        return Ok(());
    }
}

/* Serializable implementations for tuples of serializable elements.
 */

 impl<T : Serializable<Item = T>, W: Serializable<Item = W>> Serializable for (T, W) {
     type Item = (T, W);

     fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Self::Item> {
         let t = T::read_from(f, offset)?;
         let w = W::read_from(f, offset)?;
         return Ok((t, w));
    }

    fn write_to(f : & mut dyn Write, item : & Self::Item, offset : & mut u64) -> io::Result<()> {
        T::write_to(f, & item.0, offset)?;
        W::write_to(f, & item.1, offset)?;
        return Ok(());
    }
}

impl<T : FixedSize + Serializable<Item = T>, W: FixedSize + Serializable<Item = W>> FixedSize for (T, W) {
    fn size_of() -> usize { T::size_of() + W::size_of() }
}
 

/** Special case for vector of u8, which we compress.
  
    Looked at https://blog.logrocket.com/rust-compression-libraries/ and then chose the ZStandard compression as it seems to be both faster and achieving better compression ratios than the original flate2 package used. 
 */
impl Serializable for Vec<u8> {
    type Item = Vec<u8>;

    fn read_from(f : & mut dyn Read, offset : & mut u64) -> io::Result<Vec<u8>> {
        let compressed_length = u64::read_from(f, offset)? as usize;
        let mut compressed = vec![0; compressed_length];
        f.read(& mut compressed)?;
        *offset += compressed_length as u64;
        return Ok(zstd::block::decompress(& compressed, compressed_length).unwrap());
    }

    fn write_to(f : & mut dyn Write, item : & Vec<u8>, offset : & mut u64) -> io::Result<()> {
        let compressed = zstd::block::compress(item , /* level */ 3).unwrap();
        u64::write_to(f, & (compressed.len() as u64), offset)?;
        f.write(& compressed)?;
        *offset += compressed.len() as u64;
        return Ok(());
    }
}

#[cfg(test)]
mod tests{
    /// to run tests over this module you can either run:
    /// with println! statements: cargo test serialization:: -- --nocapture 
    /// without println! statements: cargo test serialization::
    use super::*;
    use std::io::Cursor;
    use std::num;

    /// Tests to see if a datatype returns the correct size (FixedSize implementation)
    #[test]
    fn fixed_size_test(){
        assert_eq!(u64::size_of(), 8);
        assert_eq!(u32::size_of(), 4);
        assert_eq!(u16::size_of(), 2);
        assert_eq!(i64::size_of(), 8);
    }

    /// tests for Serializable::read_from and Serializable::write_to for numeric datatypes
    macro_rules! serializable_test {
        ($type:ty, $size:literal) => {{        
            let mut current_offset : u64 = 0;
            let mut buffer = Vec::new();
    
            for value in 1..100 {
                let _a = <$type>::write_to(&mut buffer, &value, &mut current_offset).unwrap();
                assert_eq!(current_offset, (value*$size) as u64);    
            }
    
            let mut cursor = Cursor::new(buffer);
            
            current_offset = 0;
            
            for value in 1..100 {
                let result = <$type>::read_from(&mut cursor, &mut current_offset).unwrap();
                assert_eq!(current_offset as u64, (value*$size) as u64 );  
                assert_eq!(result as u64,value as u64);
            }

        }};
    }

    #[test]
    fn serializable_tests_numeric() {
        serializable_test!(u64, 8);
        serializable_test!(u32, 4);
        serializable_test!(u16, 2);
        serializable_test!(i64, 8);
        serializable_test!(i64, 8)
    }

    #[test]
    fn serializable_test_string() {
        let mut current_offset : u64 = 0;
        let mut buffer = Vec::new();
        
        let mut current_string = String::from("");
        let mut offset_comparison : u64 = 0;
        for value in 'a'..'z' {
            current_string.push(value);
            println!("{}", current_string);
            let _a = String::write_to(&mut buffer, &current_string, &mut current_offset).unwrap();
            offset_comparison += current_string.len() as u64;
            println!("offset: {} {}",current_offset, offset_comparison+1);
            assert_eq!(current_offset, offset_comparison+1);    
        }

        let mut cursor = Cursor::new(buffer);
            
        current_offset = 0;
        
        for value in 'a'..'z' {
            let result = String::read_from(&mut cursor, &mut current_offset).unwrap();
            println!("asd: {}", result);
        }
    }

    #[test]
    fn serializable_test_vec() {
        
    }

}