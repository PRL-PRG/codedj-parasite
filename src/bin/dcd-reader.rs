use dcd::*;

fn main() {
    let args : Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        panic!{"Invalid usage - dcd PATH_TO_DATABASE"}
    }
   let dc_reader = DCD::new(args[1].to_owned());

    println!("{:?}", dc_reader.get_project(0).unwrap());
    println!("{:?}", dc_reader.get_user(0).unwrap());
    let commit = dc_reader.get_commit(0).unwrap();
    println!("{:?}", commit);
    println!("message: {}", String::from_utf8_lossy(& commit.message.unwrap()));

}