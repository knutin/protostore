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
use std::thread;
use std::cmp;

use futures::{future, Future, BoxFuture, Sink};
use futures::stream::Stream;


use std::os::unix::io::AsRawFd;
use libaio::directio::{DirectFile, Mode, FileAccess};

use tokio_core::reactor::{Core, Remote};
use tokio_core::net::{TcpStream, TcpListener};
use tokio_io::AsyncRead;

use bytes::{Buf, BytesMut, Bytes, IntoBuf};

use hwloc::{Topology, ObjectType, CPUBIND_THREAD, CpuSet};

use clap::{App, Arg};

use libaio::FD;

use toc::TableOfContents;
use protocol::{Protocol, RequestType, Response};



mod aio;
mod toc;
mod protocol;



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

    for i in 0..num_aio_threads {
        let session = aio::Session::new(max_io_depth).expect("Could not create aio::Session");

        let pu = cmp::min(pu_index, processing_units.len()-1);
        bind_thread_to_processing_unit(session.thread_id(), pu);
        pu_index += 1;

        info!("aio_loop id:{} processing_unit:{}", i, pu);

        aio_sessions.push(session);
    }




    //
    // Create threads for handling client communications
    //

    let (remote_tx, remote_rx) = std_mpsc::channel();

    let mut tcp_threads = vec![];
    for i in 0..num_tcp_threads {
        let pu = cmp::min(pu_index, processing_units.len()-1);
        pu_index += 1;
        info!("tcp_loop id:{} processing_unit:{}", i, pu);

        let remote_tx = remote_tx.clone();
        let tid = thread::spawn(move || {
            bind_thread_to_processing_unit(unsafe { libc::pthread_self() }, pu);

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

        let datafile = DirectFile::open(datafile_path.clone(), Mode::Open, FileAccess::ReadWrite, 4096)
            .expect("Could not open data file with O_DIRECT");


        // Pick a thread for running futures
        let tcp_idx = tcp_handles_index.fetch_add(1, Ordering::SeqCst) % num_tcp_threads;
        let aio_idx = aio_sessions_index.fetch_add(1, Ordering::SeqCst) % num_aio_threads;
        let ref tcp_remote = tcp_handles[tcp_idx];
        let ref aio_session = aio_sessions[aio_idx];

        let result = handle_client(toc.clone(), datafile, socket, max_value_len, aio_session, short_circuit_reads)
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





// Handle a single client. Receive request for an uuid, look up offset
// and length in table of contents, read the value from disk, send
// response to client.
fn handle_client(toc: Arc<TableOfContents>,
                 file: DirectFile,
                 socket: TcpStream,
                 max_value_len: usize,
                 aio: &aio::Session,
                 short_circuit_reads: bool) -> BoxFuture<(), io::Error> {

    // Create a buffer to hold values read from disk or the client
    let aligned_max_len = cmp::max(512, max_value_len + (max_value_len % 512));
    let mut buf = BytesMut::with_capacity(aligned_max_len*2);
    unsafe { buf.set_len(aligned_max_len*2) };

    let buf = buf.clone();
    let aio_channel = aio.inner.clone();

    let framed = socket.framed(Protocol { len: None });
    let (writer, reader) = framed.split();

    let responses = reader.and_then(move |req| {
        let mybuf = buf.clone(); // BytesMut.clone()
        let aio_channel = aio_channel.clone();

        // Poor-man-clone of DirectFile
        let fd = file.as_raw_fd();
        let file_alignment = file.alignment;
        let file = DirectFile { fd: FD::new(fd), alignment: file_alignment};

        trace!("reqtype:{:?} uuid:{:?}", req.reqtype, req.uuid);

        match req.reqtype {
            RequestType::Read => {
                if let Some((offset, len)) = toc.offset_and_len(&req.uuid) {
                    let aligned_offset = offset - (offset % 512);
                    let pad_left = offset - aligned_offset;
                    let padded = pad_left + len as u64;
                    let aligned_len = cmp::max(512, padded + 512 - (padded as u64 % 512));

                    if !short_circuit_reads {
                        let (tx, rx) = futures::oneshot();
                        aio_channel.send(aio::Message::PRead(file, aligned_offset as usize, aligned_len as usize, mybuf, tx)).wait();
                        rx.then(move |res| {
                            match res {
                                Ok(Ok((buf, _))) => {
                                    let body = buf.freeze().slice(pad_left as usize, pad_left as usize + len as usize);
                                    let res = Response { id: req.id, body: body };
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
                        let res = Response { id: req.id, body: Bytes::from(vec![0,1,2,3]) };
                        future::ok(res).boxed()
                    }
                } else {
                    let res = Response { id: req.id, body: Bytes::new() };
                    future::ok(res).boxed()
                }
            },
            RequestType::Write => {
                let reqid = req.id;
                let body = req.body.unwrap();

                if let Some((offset, len)) = toc.offset_and_len(&req.uuid) {
                    // For the sake of simplicity in this prototype,
                    // we only allow overwriting the full body already
                    // stored. It must be exactly the same length.

                    if body.len() != len as usize {
                        panic!("unexpected length of body: {}, expected: {}", body.len(), len);
                    }


                    // Since we must do aligned writes, we first need
                    // to read the existing value to get the bytes
                    // surrounding it. We can then place these
                    // surrounding bytes into our aligned buffer and
                    // write it back.

                    let aligned_offset = offset - (offset % 512);
                    let pad_left = offset - aligned_offset;
                    let padded = pad_left + len as u64;
                    let aligned_len = cmp::max(512, padded + 512 - (padded as u64 % 512));

                    let (tx, rx) = futures::oneshot();
                    aio_channel.clone().send(aio::Message::PRead(file, aligned_offset as usize, aligned_len as usize, mybuf, tx)).wait();
                    rx.then(move |res| {
                        match res {
                            Ok(Ok((buf, _))) => {
                                let existing = buf.freeze();
                                let left = existing.slice(0, pad_left as usize);
                                let right = existing.slice((pad_left + len as u64) as usize, aligned_len as usize);

                                let new: Bytes = left.into_buf().chain(body.into_buf().chain(right)).collect();
                                let new: BytesMut = new.try_mut().expect("Could not convert 'new' into BytesMut");

                                // Poor-man-clone of DirectFile
                                let file = DirectFile { fd: FD::new(fd), alignment: file_alignment};
                                let (tx, rx) = futures::oneshot();
                                aio_channel.clone().send(aio::Message::PWrite(file, aligned_offset as usize, new, tx)).wait();
                                rx.then(move |res| {
                                   match res {
                                       Ok(Ok(_)) => {
                                           let res = Response { id: reqid, body: Bytes::new() };
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
                    let res = Response { id: reqid, body: Bytes::new() };
                    future::ok(res).boxed()
                }
            }
        }
    });

    writer.send_all(responses).then(move |result| {
        match result {
            Ok(_) => {
                future::ok(())
            },
            Err(e) => {
                warn!("Connection closed with error: {}", e);
                future::ok(())
            }
        }
    }).boxed()
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
    topo.set_cpubind_for_thread(thread, bind_to, CPUBIND_THREAD).expect("Could not set cpubind for thread");
}
