#[macro_use]
extern crate log;
extern crate env_logger;
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_io;
extern crate libaio;
extern crate mio;
extern crate eventfd;
extern crate slab;
extern crate byteorder;
extern crate uuid;
extern crate bytes;
extern crate clap;
extern crate rayon;
extern crate memmap;
extern crate hwloc;
extern crate libc;
extern crate rand;

use std::io;
use std::str;
use std::path::Path;
use std::sync::{Arc, mpsc as std_mpsc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, Duration};
use std::thread;
use std::collections::HashMap;

use futures::{future, Future, BoxFuture, Sink};
use futures::stream::Stream;


use std::os::unix::io::AsRawFd;
use libaio::directio::{DirectFile, Mode, FileAccess};

use tokio_core::reactor::{Core, Remote};
use tokio_core::net::{TcpStream, TcpListener};
use tokio_io::AsyncRead;
use tokio_io::codec::{Decoder, Encoder};

use bytes::{Buf, BufMut, BytesMut, Bytes, IntoBuf};
use byteorder::{BigEndian};

use hwloc::{Topology, ObjectType, CPUBIND_THREAD, CpuSet};

use clap::{App, Arg};

use libaio::FD;

use toc::TableOfContents;

mod aio;
mod toc;



fn main() {

    env_logger::init().unwrap();

    //
    // PARSE ARGUMENTS
    //

    let matches = App::new("mk_data")
        .arg(Arg::with_name("data-dir")
             .long("data-dir")
             .takes_value(true)
             .required(true)
             .help("Path to directory containing protostore.data and protostore.toc.* files"))
        .arg(Arg::with_name("num-aio-threads")
             .long("num-aio-threads")
             .takes_value(true)
             .help("Number of threads handling AIO communication with the kernel"))
        .arg(Arg::with_name("num-tcp-thread")
             .long("num-tcp-threads")
             .takes_value(true)
             .help("Number of threads to use for handling TCP communication with clients.
                   (In addition to this number, there is a separate thread for accepting socket connections"))
        .arg(Arg::with_name("max-io-depth")
             .long("max-io-depth")
             .takes_value(true)
             .help("Max kernel IO queue depth"))
        .arg(Arg::with_name("short-circuit-reads")
             .long("short-circuit-reads")
             .help("If set, reads will not hit disk but return a default response.
                   (Useful for testing network throughput)"))
        .get_matches();

    let data_dir = Path::new(matches.value_of("data-dir").unwrap());
    let num_aio_threads = matches.value_of("num-aio-threads").unwrap_or("1").parse::<usize>().expect("Could not parse 'num-aio-threads'");
    let num_tcp_threads = matches.value_of("num-tcp-threads").unwrap_or("1").parse::<usize>().expect("Could not parse 'num-tcp-threads'");
    let max_io_depth = matches.value_of("max-io-depth").unwrap_or("64").parse::<usize>().expect("Could not parse 'max-io-depth'");
    let short_circuit_reads = matches.is_present("short-circuit-reads");


    let datafile_path = data_dir.join("protostore.data".to_owned());
    let datafile_path = datafile_path.as_path();
    info!("Will read datafile at {:?}", datafile_path);


    let cores = hwloc_cores();
    let processing_units = hwloc_processing_units();
    let mut pu_index = 1; // 0 is reserved for main thread
    info!("Found total of {} cores, total of {} processing units", cores.len(), processing_units.len());


    // Start metrics reporter
    //let metrics_handle = start_metrics_reporter().clone();

    //
    // Read Table of Contents
    //

    let toc = Arc::new(TableOfContents::from_path(data_dir).expect("Could not open table of contents"));
    let max_value_len = toc.max_len();


    //
    // Create threads for handling AIO requests
    //
    let mut aio_sessions = vec![];
    let aio_sessions_index = AtomicUsize::new(0);

    for _ in 0..num_aio_threads {
        info!("Creating event loop for receiving AIO responses from the kernel");

        let session = aio::Session::new(max_io_depth).expect("Could not create aio::Session");
        bind_thread_to_processing_unit(session.thread_id(), pu_index);
        //pu_index += 1;

        aio_sessions.push(session);
    }




    //
    // Create threads for handling client communications
    //

    let (remote_tx, remote_rx) = std_mpsc::channel();

    let mut tcp_threads = vec![];
    for _ in 0..num_tcp_threads {
        info!("Creating event loop for handling TCP communications with clients");

        let remote_tx = remote_tx.clone();
        let tid = thread::spawn(move || {
            bind_thread_to_processing_unit(unsafe { libc::pthread_self() }, pu_index);
            //pu_index += 1;

            let mut core = Core::new().unwrap();
            remote_tx.send(core.remote()).unwrap();

            loop {
                core.turn(None)
            }

        });
        tcp_threads.push(tid);
    }

    let tcp_handles: Vec<Remote> = remote_rx.into_iter().take(num_tcp_threads).collect();
    let tcp_handles_index = AtomicUsize::new(0);

    //
    // Create TCP listener which will accept new client connections
    // and spawn a new future that handles requests on that
    // connection.
    //

    info!("Creating event loop for accepting TCP connections");
    let addr = "0.0.0.0:12345".parse().unwrap();
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let listener = TcpListener::bind(&addr, &handle).unwrap();


    // Bind the thread that accepts new connections to a dedicated
    // processing unit where we also run other low-intensity tasks
    bind_thread_to_processing_unit(unsafe { libc::pthread_self() }, 0);


    // For each new client connection
    let fut = listener.incoming().for_each(move |(socket, addr)| {
        info!("Got new connection from {}", addr);

        let datafile = DirectFile::open(datafile_path.clone(), Mode::Open, FileAccess::Read, 4096)
            .expect("Could not open data file with O_DIRECT");


        // Pick a thread for running futures
        let tcp_idx = tcp_handles_index.fetch_add(1, Ordering::SeqCst) % num_tcp_threads;
        let aio_idx = aio_sessions_index.fetch_add(1, Ordering::SeqCst) % num_aio_threads;
        let ref tcp_remote = tcp_handles[tcp_idx];
        let ref aio_session = aio_sessions[aio_idx];

        let result = handle_client(toc.clone(), datafile, socket, max_value_len, aio_session)
            .map(move |_| {
                info!("Connection to {} closed", addr);
                ()
            })
            .map_err(move |_| {
                warn!("Server error in connection to {}", addr);
                ()
            });
        tcp_remote.spawn(|_| result);

        Ok(())
    });

    info!("Now accepting connections on {}", addr);
    core.run(fut).unwrap();
}





#[derive(Debug)]
struct Request {
    id: u32,
    uuid: [u8; 16]
}

#[derive(Debug)]
struct Response {
    id: u32,
    len: usize,
    body: Bytes
}

struct Protocol;

impl Decoder for Protocol {
    type Item = Request;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Request>> {
        if buf.len() < 20 {
            return Ok(None)
        }

        let id = buf.split_to(4).into_buf().get_u32::<BigEndian>();
        let mut uuid: [u8; 16] = [0; 16];
        buf.split_to(16).into_buf().copy_to_slice(&mut uuid);

        Ok(Some(Request { id: id, uuid: uuid }))
    }
}

impl Encoder for Protocol {
    type Item = Response;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(20 + item.len);
        dst.put_u32::<BigEndian>(item.id);
        dst.put_u32::<BigEndian>(item.len as u32);
        dst.put_slice(&item.body.slice(0, item.len));
        Ok(())
    }
}


// Handle a single client. Receive request for an uuid, look up offset
// and length in table of contents, read the value from disk, send
// response to client.
fn handle_client(toc: Arc<TableOfContents>,
                 file: DirectFile,
                 socket: TcpStream,
                 max_value_len: usize,
                 aio: &aio::Session) -> BoxFuture<(), io::Error> {

    // Create a buffer to hold values read from disk
    let aligned_max_len = max_value_len + (max_value_len % 512);
    let mut buf = BytesMut::with_capacity(aligned_max_len*2);
    unsafe { buf.set_len(aligned_max_len*2) };

    let buf = buf.clone();
    let aio_channel = aio.inner.clone();

    let framed = socket.framed(Protocol);
    let (writer, reader) = framed.split();

    let responses = reader.and_then(move |req| {
        let mybuf = buf.clone(); // BytesMut.clone()
        let aio_channel = aio_channel.clone();

        // Poor-man-clone of DirectFile
        let fd = file.as_raw_fd();
        let file = DirectFile { fd: FD::new(fd), alignment: file.alignment};

        if let Some((offset, len)) = toc.offset_and_len(&req.uuid) {
            let aligned_offset = offset - (offset % 512);
            let aligned_len = len + 512 - (len % 512);

            let (tx, rx) = futures::oneshot();
            trace!("creating msg");
            aio_channel.send(aio::Message::PRead(file, 0, 512, mybuf, tx)).wait();
            rx.then(move |res| {
                match res {
                    Ok(Ok((buf, err))) => {
                        let pad_start = offset - aligned_offset;
                        let buflen = if err.is_none() { len as usize } else { 0 };
                        let body = buf.freeze().slice(pad_start as usize, pad_start as usize + buflen);
                        let res = Response {
                            id: req.id,
                            len: buflen,
                            body: body
                        };

                        future::ok(res)
                    },
                    Ok(Err(e)) => {
                        panic!("aio failed: {:?}", e)
                    },
                    Err(e) => {
                        panic!("aio failed: {:?}", e)
                    }
                }
            }).boxed()

        } else {
            let res = Response {
                id: req.id,
                len: 0,
                body: Bytes::new()
            };

            future::ok(res).boxed()

            //Err(io::Error::new(io::ErrorKind::Other, "Uuid not found"))
        }
    });


            // aio_channel.send(aio::Message::PRead(file, 0, 512, mybuf, tx)).boxed()


    //let responses = aio_channel.clone().send_all(reads.map_err(|_| ()));

    //let responses = reads
    //    .map(|msg| aio_channel.clone().send(msg));




    //let responses = reads.map(|res| future::ok(res)).buffer_unordered(1000);
    //let responses = reads;

    writer.send_all(responses).then(move |result| {
        match result {
            Ok(_) => {
                future::ok(())
            },
            Err(e) => {
                println!("Error {}", e);
                future::ok(())
            }
        }
    }).boxed()
}



fn cpuset_for_core(topology: &Topology, idx: usize) -> CpuSet {
    let cores = (*topology).objects_with_type(&ObjectType::PU).unwrap();
    match cores.get(idx) {
        Some(val) => val.cpuset().unwrap(),
        None => panic!("No Core found with id {}", idx),
    }
}

fn hwloc_processing_units() -> Vec<CpuSet> {
    let topo = Topology::new();
    let cores = topo.objects_with_type(&ObjectType::PU).unwrap();
    cores.iter().map(|c| c.cpuset().unwrap()).collect::<Vec<CpuSet>>()
}

fn hwloc_cores() -> Vec<CpuSet> {
    let topo = Topology::new();
    let cores = topo.objects_with_type(&ObjectType::Core).unwrap();
    cores.iter().map(|c| c.cpuset().unwrap()).collect::<Vec<CpuSet>>()
}

fn bind_thread_to_processing_unit(thread: libc::pthread_t, idx: usize) {
    let mut topo = Topology::new();
    let bind_to = match topo.objects_with_type(&ObjectType::PU).unwrap().get(idx) {
        Some(val) => val.cpuset().unwrap(),
        None => panic!("No processing unit found for idx {}", idx)
    };
    info!("Binding thread to {:?}", bind_to);
    topo.set_cpubind_for_thread(thread, bind_to, CPUBIND_THREAD).expect("Could not set cpubind for thread");
}






#[derive(Debug)]
enum Metric {
    Incr(String),
    Timing(String, u64)
}

type MetricHandle = Arc<std_mpsc::Sender<Metric>>;

fn start_metrics_reporter() -> MetricHandle {

    //let (channel_tx, channel_rx) = mpsc::channel();
    let (metrics_tx, metrics_rx) = std_mpsc::channel();

    thread::Builder::new().name("metrics_reporter".to_string()).spawn(move || {
        info!("Creating thread for reporting metrics");

        let timeout = Duration::new(1, 0);

        let mut previous_counters: HashMap<String, u64> = HashMap::new();
        let mut current_counters: HashMap<String, u64> = HashMap::new();
        let default_count: u64 = 0;


        let last_report_ts = SystemTime::now();

        loop {
            if let Ok(msg) = metrics_rx.recv_timeout(timeout) {
                match msg {
                    Metric::Incr(name) => {
                        let entry = current_counters.entry(name).or_insert(0);
                        *entry += 1;
                    }
                    Metric::Timing(name, time) => {
                        println!("time {} {}", name, time);
                    }
                }
            };

            let elapsed = last_report_ts.elapsed().expect("Time drift!");
            let elapsed_ms = ((elapsed.as_secs() * 1_000_000_000) + elapsed.subsec_nanos() as u64) / 1000000;

            if elapsed_ms >= 1000 {
                let report = current_counters.len() > 0;

                for (name, current) in current_counters.iter_mut() {
                    let previous = previous_counters.entry(name.clone()).or_insert(default_count);
                    if *current > 0 {
                        let rate: f64 = (*current - *previous) as f64 / elapsed_ms as f64;
                        print!("{}:{}", name, rate);

                        *previous = *current;
                        *current = 0;
                    }
                };
                if report {
                    print!("\n");
                }

                current_counters = current_counters.into_iter()
                    .filter(|&(_, count)| count > 0)
                    .collect();

            }
        }
    });

    Arc::new(metrics_tx)
}
