use dcd::*;

fn main() {
    let dc_reader = DCD::new("/dejavuii/dejacode/dataset-peta-x".to_owned());

    println!("{:?}", dc_reader.get_project(0).unwrap());
    println!("{:?}", dc_reader.get_user(0).unwrap());
    let commit = dc_reader.get_commit(0).unwrap();
    println!("{:?}", commit);
    println!("message: {}", String::from_utf8_lossy(& commit.message.unwrap()));

}