use dcd::*;

fn main() {
    let mut idx = db::Indexer::new("/home/peta/foobar.dat");
    println!("{}", idx.len());
    idx.set(100, 65535);
    println!("{}", idx.len());
}