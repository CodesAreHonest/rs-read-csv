extern crate csv;

use std::error::Error;
use std::process;
use std::fs::File;


// multiple producer, single consumer.
use std::sync::mpsc;

// for multithreading
use std::thread;

// use time crate
extern crate time;
use time::PreciseTime;

const LEO_INDICATOR: &'static str = "subject";
const COMPANY_INDICATOR: &'static str = "company";
const NSPL_INDICATOR: &'static str = "postcode";
const LEO_DIRECTORY: &'static str = "/home/yinghua/Documents/FYP1/FYP-data/subject-data/institution-subject-data.csv";
const COMPANY_DIRECTORY: &'static str = "/home/yinghua/Documents/FYP1/FYP-data/company-data/company-data-full.csv";
const NSPL_DIRECTORY: &'static str = "/home/yinghua/Documents/FYP1/FYP-data/postcode-data/UK-NSPL.csv";

//fn retrieve_data(directory: &'static str) -> Result<(), Box<Error>> {
//
//    // Parse the CSV reader and iterate over each record.
//    let csv_file = File::open(directory).expect("Error open LEO file");
//
//    let start = PreciseTime::now();
//    let mut rdr = csv::Reader::from_reader(csv_file);
//
//    for result in rdr.records() {
//        // The iterator yields Result<StringRecord, Error>, so we check the error here.
//        let record = result?;
//        //        println!("{:?}", record);
//    }
//
//    let end = PreciseTime::now();
//    let duration = start.to(end);
//
//    println!("{} seconds on retrieve {}. ", duration, directory);
//    Ok(())
//}

fn retrieve_data(directory: &'static str, indicator: &'static str) -> u32 {

    println!("BEGIN retrieve data from {} files. ", indicator);

    // Parse the CSV reader and iterate over each record.
    let csv_file = File::open(directory).expect("Error open LEO file");

    let start = PreciseTime::now();
    let mut rdr = csv::Reader::from_reader(csv_file);

    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the error here.
        let record = result;
        //        println!("{:?}", record);
    }

    let end = PreciseTime::now();
    let duration = start.to(end);

    println!(
        "FINISH retrieve all rows of data from {} files with {} seconds.",
        indicator,
        duration
    );

    return 1;
}

fn sequential_read() {

    let start = PreciseTime::now();

    let leo = retrieve_data(LEO_DIRECTORY, LEO_INDICATOR);
    let company = retrieve_data(COMPANY_DIRECTORY, COMPANY_INDICATOR);
    let nspl = retrieve_data(NSPL_DIRECTORY, NSPL_INDICATOR);

    //    // retrieve all the data sequentially
    //    if let Err(err) = retrieve_data(LEO_DIRECTORY) {
    //        println!("error running retreive_leo: {}", err);
    //        process::exit(1);
    //    }
    //
    //    if let Err(err) = retrieve_data(COMPANY_DIRECTORY) {
    //        println!("error running retreive_company: {}", err);
    //        process::exit(1);
    //    }
    //
    //    if let Err(err) = retrieve_data(NSPL_DIRECTORY) {
    //        println!("error running retreive_postcode: {}", err);
    //        process::exit(1);
    //    }

    let end = PreciseTime::now();
    let duration = start.to(end);

    println!(
        " {} seconds on retrieve all the data SEQUENTIALLY. ",
        duration
    );

}

fn concurrent_read() {

    let start = PreciseTime::now();

    // transmitter and receiver over the channel
    let (leo_tx, leo_rx) = mpsc::channel();
    let (company_tx, company_rx) = mpsc::channel();
    let (nspl_tx, nspl_rx) = mpsc::channel();

    thread::spawn(move || {

        let leo = retrieve_data(LEO_DIRECTORY, LEO_INDICATOR);
        leo_tx.send(leo).unwrap();
    });

    thread::spawn(move || {

        let company = retrieve_data(COMPANY_DIRECTORY, COMPANY_INDICATOR);
        company_tx.send(company).unwrap();
    });

    thread::spawn(move || {

        let nspl = retrieve_data(NSPL_DIRECTORY, NSPL_INDICATOR);
        nspl_tx.send(nspl).unwrap();
    });

    let leo_channel = leo_rx.recv().unwrap();
    let company_channel = company_rx.recv().unwrap();
    let nspl_channel = nspl_rx.recv().unwrap();

    let end = PreciseTime::now();
    let duration = start.to(end);

    println!(
        " {} seconds on retrieve all the data CONCURRENTLY. ",
        duration
    );

}

fn main() {
    sequential_read();
    concurrent_read();
}
