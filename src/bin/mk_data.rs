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

    {
        println!("Generating dummy data in {}. {} cookies", path, num_cookies);

        use rand::distributions::{Range, IndependentSample};
        let range = Range::new(4, 64);

        let mut rng = rand::thread_rng();
        let mut opts = OpenOptions::new();
        opts.write(true).create(true).truncate(true);

        let mut toc = opts.open(toc_path).unwrap();
        let mut data = opts.open(data_path).unwrap();

        for _ in 0..num_cookies {
            let uuid = *uuid::Uuid::new_v4().as_bytes();

            let num = range.ind_sample(&mut rng);
            let mut encoded_num = [0; 4];
            BigEndian::write_u32(&mut encoded_num, num as u32);

            toc.write_all(&uuid).expect("Could not write to toc");
            toc.write_all(&encoded_num).expect("Could not write to toc");

            let mut encoded_value = [0; 8];
            for j in 0..num {
                BigEndian::write_u64(&mut encoded_value, j as u64);
                data.write_all(&encoded_value).expect("Could not write to data");
            }
        }
    }
}
