// cargo run --bin mk_data -- --path=/mnt/data/ --num-cookies=1000000000

extern crate rand;
extern crate uuid;
extern crate byteorder;
extern crate clap;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

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


    use rand::{XorShiftRng, Rng};
    let mut rng: XorShiftRng = rand::random();


    println!("Creating toc");
    let mut toc_buf: Vec<u8> = Vec::with_capacity(20*num_cookies as usize);
    let mut lens = Vec::with_capacity(num_cookies as usize);
    for _ in 0..num_cookies {
        let uuid = *uuid::Uuid::new_v4().as_bytes();
        let len = rng.gen_range(100, 10_000);
        lens.push(len);

        let mut encoded_len = [0; 4];
        BigEndian::write_u32(&mut encoded_len, len as u32);

        toc_buf.write_all(&uuid).expect("Could not write to toc_buf");
        toc_buf.write_all(&encoded_len).expect("Could not write to toc_buf");
    }

    println!("Writing toc buffer to disk");
    toc_file.write_all(&toc_buf).expect("Could not write toc_buf to toc_file");





    let total_entries: usize = lens.iter().sum();
    let total_bytes = total_entries;
    let total_gb = total_bytes / 1024 / 1024 / 1024;
    println!("Creating data file. Need to write {} bytes, {} GB", total_bytes, total_gb);


    let max_len = lens.iter().max().unwrap();
    let mut dummies: Vec<u8> = vec![];

    for i in 0..*max_len {
        let mut encoded_len = [0; 8];
        BigEndian::write_u64(&mut encoded_len, i as u64);
        dummies.extend(encoded_len.iter());
    }
    println!("max {}, dummies {:?}", max_len, &dummies[0..24]);


    let mut n = 0;
    for chunk in lens.chunks(10_000) {
        println!("{} of {}", n, lens.len());
        n += chunk.len();

        let chunk_sum = chunk.iter().sum();
        let mut data_buf = Vec::with_capacity(chunk_sum);
        for len in chunk {
            data_buf.extend_from_slice(&dummies[0..(*len)]);
        }
        data_file.write_all(&data_buf).expect("Could not write data_buf to data_file");
    }
}
