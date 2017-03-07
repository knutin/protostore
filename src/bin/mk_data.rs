// cargo run --bin mk_data -- --path=/mnt/data/ --num-cookies=1000000000

extern crate rand;
extern crate uuid;
extern crate byteorder;
extern crate clap;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::mem;

use clap::{Arg, App};
use byteorder::{BigEndian, ByteOrder};

fn main() {

    let matches = App::new("mk_data")
        .arg(Arg::with_name("path")
             .long("path")
             .takes_value(true)
             .required(true)
             .help("Directory to write datafiles"))
        .arg(Arg::with_name("cookies")
             .long("num-cookies")
             .takes_value(true)
             .required(true)
             .help("Number of cookies to write in datafiles"))
        .get_matches();


    let path = matches.value_of("path").unwrap();

    let mut toc_path = PathBuf::from(path);
    let mut data_path = PathBuf::from(path);

    toc_path.push("protostore.toc");
    data_path.push("protostore.data");


    let num_cookies = matches.value_of("cookies").unwrap().parse::<u64>().expect("Could not parse cookies into u64");

    let mut opts = OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    let mut toc_file = opts.open(toc_path).unwrap();
    let mut data_file = opts.open(data_path).unwrap();


    use rand::distributions::{Range, IndependentSample};
    let range = Range::new(100, 10000);
    let mut rng = rand::thread_rng();


    println!("Creating toc");
    let mut toc_buf: Vec<u8> = Vec::with_capacity(20*num_cookies as usize);
    let mut lens = Vec::with_capacity(num_cookies as usize);
    for _ in 0..num_cookies {
        let uuid = *uuid::Uuid::new_v4().as_bytes();

        let num = range.ind_sample(&mut rng);
        lens.push(num);

        let mut encoded_num = [0; 4];
        BigEndian::write_u32(&mut encoded_num, num as u32);

        toc_buf.write_all(&uuid).expect("Could not write to toc_buf");
        toc_buf.write_all(&encoded_num).expect("Could not write to toc_buf");
        println!("uuid {:?} len {}", uuid, num);
    }

    println!("Writing toc buffer to disk");
    toc_file.write_all(&toc_buf).expect("Could not write toc_buf to toc_file");


    let total_entries: usize = lens.iter().sum();
    let total_bytes = total_entries * 8;
    let total_gb = total_bytes / 1024 / 1024 / 1024;
    println!("Creating data file. Need to write {} bytes, {} GB", total_bytes, total_gb);
    let mut n = 0;
    for chunk in lens.chunks(10_000) {
        println!("{} of {}", n, lens.len());
        n += chunk.len();


        let data_buf: Vec<u64> = chunk.iter().flat_map(|len| (0..*len).map(|j| j as u64)).collect();
        let data_buf: Vec<u8> = unsafe { mem::transmute(data_buf) };

        data_file.write_all(&data_buf).expect("Could not write data_buf to data_file");
    }
}
