use std::env::args;
use std::fs::File;
use csv::WriterBuilder;
use solution::execute_transactions;

const USAGE: &'static str = "Usage: ./solution [input file]";

fn main() {
    let file_path = match args().skip(1).next() {
        Some(f) => f,
        None => panic!("{}", USAGE)
    };
    let file = File::open(file_path).unwrap();
    let (result, errors) = execute_transactions(&file, num_cpus::get());
    for e in errors {
        eprintln!("{}", e.to_string());
    }
    let mut wtr = WriterBuilder::new().has_headers(true).from_writer(vec![]);
    wtr.write_record(&["client","available","held","total","locked"]).unwrap();
    for client in result {
        wtr.write_record(&client.to_record()).unwrap();
    }
    let data = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
    println!("{}", data);
}
