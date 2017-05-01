// time cargo run --bin mk_data -- --path=/mnt/data/ --num-cookies=250000000 --min-size 1024 --max-size 4

extern crate rand;
extern crate uuid;
extern crate byteorder;
extern crate clap;
extern crate rayon;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use clap::{Arg, App};
use byteorder::{LittleEndian, ByteOrder};

use rayon::prelude::*;

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
        .arg(Arg::with_name("min_size")
             .long("min-size")
             .takes_value(true)
             .required(true)
             .help("Minimum number of data per cookie, in bytes"))
        .arg(Arg::with_name("max_size")
             .long("max-size")
             .takes_value(true)
             .required(true)
             .help("Maximum number of data per cookie, in bytes. \
                   Values will be randomly distributed between min and max"))
        .get_matches();


    let path = matches.value_of("path").unwrap();

    let mut toc_uuids_path = PathBuf::from(path);
    let mut toc_offsets_path = PathBuf::from(path);
    let mut toc_lens_path = PathBuf::from(path);
    let mut data_path = PathBuf::from(path);

    toc_uuids_path.push("protostore.toc.uuids");
    toc_offsets_path.push("protostore.toc.offsets");
    toc_lens_path.push("protostore.toc.lengths");
    data_path.push("protostore.data");


    let num_cookies = matches.value_of("cookies").unwrap().parse::<u64>().expect("Could not parse cookies into u64");
    let min_size = matches.value_of("min_size").unwrap().parse::<u16>().expect("Could not parse min-size");
    let max_size = matches.value_of("max_size").unwrap().parse::<u16>().expect("Could not parse max-size");

    let mut opts = OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    
    let mut toc_uuids_file = opts.open(toc_uuids_path).unwrap();
    let mut toc_offsets_file = opts.open(toc_offsets_path).unwrap();
    let mut toc_lens_file = opts.open(toc_lens_path).unwrap();
    let mut data_file = opts.open(data_path).unwrap();


    use rand::{XorShiftRng, Rng};
    let mut rng: XorShiftRng = rand::random();


    println!("Creating Table of Contents with {} uuids, \
              with sizes randomly distributed from {} to {} bytes",
             num_cookies, min_size, max_size);

    let mut toc_uuids_buf: Vec<u8> = Vec::with_capacity(16*num_cookies as usize);
    let mut toc_offsets_buf: Vec<u8> = Vec::with_capacity(8*num_cookies as usize);
    let mut toc_lens_buf: Vec<u8> = Vec::with_capacity(2*num_cookies as usize);

    let mut lens: Vec<u64> = Vec::with_capacity(num_cookies as usize);


    let mut uuids = (0..num_cookies)
        .into_par_iter()
        .map(|_| *uuid::Uuid::new_v4().as_bytes())
        .collect::<Vec<[u8; 16]>>();

    println!("Sorting uuids");
    uuids.sort();

    println!("Writing Table of Contents to in-memory buffers");
    let mut offset: u64 = 0;
    for uuid in uuids {
        let len = rng.gen_range(min_size as u16, max_size as u16);
        lens.push(len as u64);

        let mut encoded_offset = [0; 8];
        let mut encoded_len = [0; 2];
        LittleEndian::write_u64(&mut encoded_offset, offset);
        LittleEndian::write_u16(&mut encoded_len, len as u16);

        toc_uuids_buf.write_all(&uuid).expect("Could not write to toc_buf");
        toc_offsets_buf.write_all(&encoded_offset).expect("Could not write to toc_buf");
        toc_lens_buf.write_all(&encoded_len).expect("Could not write to toc_buf");

        offset += len as u64;
    }

    println!("Writing Table of Contents to disk");
    toc_uuids_file.write_all(&toc_uuids_buf).expect("Could not write buffer to file");
    toc_offsets_file.write_all(&toc_offsets_buf).expect("Could not write buffer to file");
    toc_lens_file.write_all(&toc_lens_buf).expect("Could not write buffer to file");



    let total_entries: u64 = lens.iter().sum();
    let total_bytes = total_entries;
    let total_gb = total_bytes / 1024 / 1024 / 1024;
    println!("Creating data file. Need to write {} bytes, {} GB", total_bytes, total_gb);


    let max_len = lens.iter().max().unwrap();
    let mut dummies: Vec<u8> = vec![];

    for i in 0..*max_len {
        let mut encoded_len = [0; 8];
        LittleEndian::write_u64(&mut encoded_len, i as u64);
        dummies.extend(encoded_len.iter());
    }

    let mut n = 0;
    let mut written_bytes = 0;
    for chunk in lens.chunks(100_000) {
        println!("{} of {}, {} GB of {} GB", n, lens.len(), written_bytes / 1024 / 1024 / 1024, total_gb);
        n += chunk.len();

        let chunk_sum: u64 = chunk.iter().sum();
        let mut data_buf = Vec::with_capacity(chunk_sum as usize);
        for len in chunk {
            data_buf.extend_from_slice(&dummies[0..(*len as usize)]);
        }
        data_file.write_all(&data_buf).expect("Could not write data_buf to data_file");
        written_bytes += chunk_sum;
    }
}
