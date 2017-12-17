extern crate csv;

use std::error::Error;
use std::process;
use std::fs::File;

// use time crate
extern crate time;
use time::PreciseTime;

const LEO_DIRECTORY: &'static str = "/home/yinghua/Documents/FYP1/FYP-data/subject-data/institution-subject-data.csv";
const COMPANY_DIRECTORY: &'static str = "/home/yinghua/Documents/FYP1/FYP-data/company-data/company-data-full.csv";
const NSPL_DIRECTORY: &'static str = "/home/yinghua/Documents/FYP1/FYP-data/postcode-data/UK-NSPL.csv";

fn retrieve_data(directory: &'static str) -> Result<(), Box<Error>> {

    // Parse the CSV reader and iterate over each record.
    let csv_file = File::open(directory).expect("Error open LEO file");

    let start = PreciseTime::now();
    let mut rdr = csv::Reader::from_reader(csv_file);

    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the error here.
        let record = result?;
        //        println!("{:?}", record);
    }

    let end = PreciseTime::now();
    let duration = start.to(end);

    println!("{} seconds on retrieve {}. ", duration, directory);
    Ok(())
}

fn main() {

    // retrieve all the data sequentially
    if let Err(err) = retrieve_data(LEO_DIRECTORY) {
        println!("error running retreive_leo: {}", err);
        process::exit(1);
    }

    if let Err(err) = retrieve_data(COMPANY_DIRECTORY) {
        println!("error running retreive_company: {}", err);
        process::exit(1);
    }

    if let Err(err) = retrieve_data(NSPL_DIRECTORY) {
        println!("error running retreive_postcode: {}", err);
        process::exit(1);
    }

}
