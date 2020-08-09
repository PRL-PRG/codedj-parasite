use dcd::*;

fn main() {
    let dc_reader = DCD::new("/dejavuii/dejacode/dataset-small".to_owned());

    println!("{:?}", dc_reader.get_project(0).unwrap());
    println!("{:?}", dc_reader.get_user(0).unwrap());
    println!("{:?}", dc_reader.get_commit(0).unwrap());


}