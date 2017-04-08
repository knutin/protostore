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

use std::io;
use std::str;
use std::path::Path;
use std::ops::Drop;
use std::sync::{Arc, Mutex, mpsc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::RefCell;
use std::time::SystemTime;
use std::thread;

use futures::{future, Future, Complete, Oneshot, BoxFuture, Async, Poll, Sink};
use futures::stream::{Stream, Fuse};
use futures::sync::mpsc::{unbounded, UnboundedSender, UnboundedReceiver};

use eventfd::EventFD;
use std::os::unix::io::AsRawFd;
use libaio::raw::{Iocontext, IoOp};
use libaio::directio::{DirectFile, Mode, FileAccess};

use slab::Slab;

use tokio_core::reactor::{Core, Handle, Remote, PollEvented};
use tokio_core::net::{TcpStream, TcpListener};
use tokio_io::AsyncRead;
use tokio_io::codec::{Decoder, Encoder};

use bytes::{Buf, BufMut, BytesMut, Bytes, IntoBuf};
use byteorder::{BigEndian, ByteOrder};
use rayon::prelude::*;

use hwloc::{Topology, ObjectType, CPUBIND_THREAD, CpuSet};

use clap::{App, Arg};


fn main() {

    env_logger::init().unwrap();

    //
    // PARSE ARGUMENTS
    //

    let matches = App::new("mk_data")
        .arg(Arg::with_name("num-threads")
             .long("num-threads")
             .takes_value(true)
             .help("Number of AIO threads"))
        .get_matches();


    let num_threads = matches.value_of("num-threads").unwrap_or("4").parse::<usize>().expect("Could not parse 'num-threads'");

    let path_toc = Path::new("/mnt/data/protostore.toc");
    let path_data = Path::new("/mnt/data/protostore.data");


    //
    // Figure out the cpu topology available
    //
    let topo = Arc::new(Mutex::new(Topology::new()));
    let num_pu = topo.lock().unwrap().objects_with_type(&ObjectType::PU).unwrap().len();
    let num_cores = topo.lock().unwrap().objects_with_type(&ObjectType::Core).unwrap().len();
    println!("Found {} cores, {} processing units", num_cores, num_pu);


    //
    // Read Table of Contents
    //
    println!("Parsing Table of Contents file at {:?}", path_toc);
    let meta = path_toc.metadata().expect("Could not read metadata for toc file");
    let toc_size: usize = meta.len() as usize;
    let num_entries = (toc_size / 20) as u64;

    let toc_mmap = memmap::Mmap::open_path(path_toc.clone(), memmap::Protection::Read).expect("Could not read toc file");
    let toc_buf: &[u8] = unsafe { toc_mmap.as_slice() };

    let offsets: Vec<usize> = (0..num_entries).map(|i| (i*20) as usize).collect();
    let toc_uuids: Vec<Vec<u8>> = offsets
        .par_iter()
        .cloned()
        .map(|offset| toc_buf[offset .. offset+16].to_vec())
        .collect();
    let toc_uuids = Arc::new(toc_uuids);

    let toc_lens: Vec<usize> = offsets
        .par_iter()
        .cloned()
        .map(|offset| BigEndian::read_u32(&toc_buf[offset+16 .. offset+20]) as usize)
        .collect();
    let toc_lens = Arc::new(toc_lens);

    let mut toc_offsets = Vec::with_capacity(num_entries as usize);
    let mut offset = 0;
    for i in 0..num_entries {
        toc_offsets.push(offset);
        offset += toc_lens[i as usize];
    }
    let toc_offsets = Arc::new(toc_offsets);

    let max_len = toc_lens.iter().cloned().max().unwrap();
    println!("ToC has {} entries. Max length {}", toc_uuids.len(), max_len);



    //
    // Create TCP listener
    //

    println!("Creating event loop for accepting TCP connections");
    let addr = "0.0.0.0:12345".parse().unwrap();
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let listener = TcpListener::bind(&addr, &handle).unwrap();

    let (session_tx, session_rx) = mpsc::channel();

    for thread_idx in 0..num_threads {
        let path_data = path_data.clone();
        let session_tx = session_tx.clone();
        let topo = topo.clone();
        thread::spawn(move || {
            println!("Creating event loop for receiving AIO responses from the kernel, thread index {}", thread_idx);
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let datafile = DirectFile::open(path_data, Mode::Open, FileAccess::Read, 4096)
                .expect("Could not open data file with O_DIRECT");
            let session = Arc::new(AioSession::new(handle, datafile).unwrap());
            session_tx.send((session.handle(), core.remote()));

            {
                let tid = unsafe { libc::pthread_self() };
                let mut locked_topo = topo.lock().unwrap();
                let bind_to = cpuset_for_core(&*locked_topo, thread_idx);
                locked_topo.set_cpubind_for_thread(tid, bind_to, CPUBIND_THREAD).unwrap();
                let after = locked_topo.get_cpubind_for_thread(tid, CPUBIND_THREAD);
                println!("Bound thread {} to {:?}", thread_idx, after);
            }

            loop {
                core.turn(None)
            };
        });
    }

    let handles: Vec<(Arc<SessionHandle>, Remote)> = session_rx.into_iter().take(num_threads).collect();
    let handle_index = AtomicUsize::new(0);


    let s = listener.incoming().for_each(move |(socket, addr)| {
        let aligned_max_len = max_len + (max_len % 512);
        let mut buf = BytesMut::with_capacity(aligned_max_len*2);
        unsafe { buf.set_len(aligned_max_len*2) };

        let next = handle_index.fetch_add(1, Ordering::SeqCst);
        let idx = next % num_threads;
        let (ref session, ref remote) = handles[idx];

        let server = Server {
            session: session.clone(),
            toc_uuids: toc_uuids.clone(),
            toc_offsets: toc_offsets.clone(),
            toc_lens: toc_lens.clone(),
        };

        println!("Got new connection from {}", addr);

        let result = server
            .serve(socket, buf)
            .map(|_|  println!("Connection closed"))
            .map_err(|_| println!("Serve error"));
        remote.spawn(|_| result);

        Ok(())
    });


    // Bind main thread to the last core
    {
        let tid = unsafe { libc::pthread_self() };
        let mut locked_topo = topo.lock().unwrap();
        let bind_to = last_core(&*locked_topo);
        locked_topo.set_cpubind_for_thread(tid, bind_to, CPUBIND_THREAD).unwrap();
        let after = locked_topo.get_cpubind_for_thread(tid, CPUBIND_THREAD);
        println!("Bound main thread to {:?}", after);
    }

    println!("Now accepting connections on {}", addr);
    core.run(s).unwrap();
}



enum Message {
    Execute(usize, BytesMut, usize, Complete<io::Result<(BytesMut, Option<io::Error>)>>)
}

struct ReadRequest {
    inner: Oneshot<io::Result<(BytesMut, Option<io::Error>)>>
}

struct AioReader {
    rx: Fuse<UnboundedReceiver<Message>>,
    ctx: Iocontext<usize, BytesMut, BytesMut>,
    stream: PollEvented<AioEventFd>,
    file: DirectFile,

    // Handles to outstanding requests
    handles: Slab<HandleEntry>
}

struct HandleEntry {
    timestamp: SystemTime,
    complete: Complete<io::Result<(BytesMut, Option<io::Error>)>>
}



#[derive(Debug)]
struct Request {
    id: u32,
    uuid: Vec<u8>
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
        let mut uuid = vec![0; 16];
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


struct Server {
    session: Arc<SessionHandle>,
    toc_uuids: Arc<Vec<Vec<u8>>>,
    toc_offsets: Arc<Vec<usize>>,
    toc_lens: Arc<Vec<usize>>,
}

impl Server {
    fn serve(self, socket: TcpStream, buf: BytesMut) -> BoxFuture<(), io::Error> {
        let framed = socket.framed(Protocol);
        let (writer, reader) = framed.split();
        let buf = RefCell::new(buf);

        let responses = reader.and_then(move |req| {
            let mybuf = buf.clone().into_inner();

            match self.toc_uuids.binary_search(&req.uuid) {
                Ok(index) => {
                    let offset = self.toc_offsets[index];
                    let len = self.toc_lens[index];

                    let aligned_offset = offset - (offset % 512);
                    let aligned_len = len + 512 - (len % 512);

                    self.session.read(aligned_offset, mybuf, aligned_len).and_then(move |(buf, err)| {
                        let pad_start = offset - aligned_offset;
                        let buflen = if err.is_none() { len } else { 0 };
                        let body = buf.freeze().slice(pad_start, pad_start+buflen);
                        let res = Response {
                            id: req.id,
                            len: buflen,
                            body: body
                        };
                        future::ok(res)
                    }).boxed()
                },
                Err(_) => {
                    let res = Response {
                        id: req.id,
                        len: 0,
                        body: Bytes::new()
                    };
                    future::ok(res).boxed()
                }
            }
        });

        writer.send_all(responses).then(|result| {
            match result {
                Ok(_) => future::ok(()),
                Err(e) => {
                    println!("Error {}", e);
                    future::ok(())
                }
            }
        }).boxed()
    }
}


struct AioSession {
    tx: Arc<UnboundedSender<Message>>
}

struct SessionHandle {
    inner: Arc<UnboundedSender<Message>>
}


impl AioSession {
   pub fn new(handle: Handle, file: DirectFile) -> io::Result<AioSession> {
       let mut ctx = match Iocontext::new(100) {
           Err(e) => panic!("iocontext new {:?}", e),
           Ok(ctx) => ctx
       };
       try!(ctx.ensure_evfd());
       let evfd = ctx.evfd.as_ref().unwrap().clone();
       let source = AioEventFd { inner: evfd };
       let stream = PollEvented::new(source, &handle).unwrap();
       let (tx, rx) = unbounded(); // TODO: bounded?

       handle.clone().spawn(AioReader {
           rx: rx.fuse(),
           ctx: ctx,
           stream: stream,
           file: file,
           handles: Slab::with_capacity(512)
       }.map_err(|e| {
           panic!("error while processing request: {:?}", e);
       }));

       Ok(AioSession { tx: Arc::new(tx) })
    }


    pub fn handle(&self) -> Arc<SessionHandle> {
        Arc::new(SessionHandle { inner: self.tx.clone() })
    }
}

impl SessionHandle {
    pub fn read(&self, offset: usize, buf: BytesMut, len: usize) -> ReadRequest {
        let (tx, rx) = futures::oneshot();
        //self.inner.send(Message::Execute(file, offset, buf, len, tx));
        UnboundedSender::send(&self.inner, Message::Execute(offset, buf, len, tx));
        ReadRequest { inner: rx }
    }
}




impl Future for ReadRequest {
    type Item = (BytesMut, Option<io::Error>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.inner.poll().expect("complete canceled") {
            Async::Ready(Ok(res)) => Ok(res.into()),
            Async::Ready(Err(res)) => panic!("readrequest.poll failed, {:?}", res),
            Async::NotReady =>  Ok(Async::NotReady)
        }
    }
}


impl Future for AioReader {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        trace!("AioReader.poll");

        // Enqueue incoming requests
        loop {
            let msg = match self.rx.poll().expect("cannot fail") {
                Async::Ready(Some(msg)) => msg,
                Async::Ready(None) => break,
                Async::NotReady => break
            };

            let (offset, buf, len, tx) = match msg {
                Message::Execute(offset, buf, len, tx) => (offset, buf, len, tx)
            };

            let entry = self.handles.vacant_entry().expect("No more free handles!");
            let index = entry.index();
            trace!("calling pread. offset:{} len:{}", offset, len);
            match self.ctx.pread(&self.file, buf, offset as u64, len, index) {
                Ok(()) => {
                    entry.insert(HandleEntry { complete: tx, timestamp: SystemTime::now() });
                },
                Err((buf, _token)) => {
                    tx.send(Ok((buf, Some(io::Error::new(io::ErrorKind::Other, "pread failed")))));
                    continue
                }
            };
        };

        trace!("submitting batch. len {}", self.ctx.batched());
        while self.ctx.batched() > 0 {
            if let Err(e) = self.ctx.submit() {
                panic!("batch submit failed {:?}", e);
            }
        };

        if self.stream.poll_read().is_ready() {
            trace!("eventfd is ready, calling io_getevents");
            match self.ctx.results(0, 100, None) {
                Ok(res) => {
                    trace!("ctx.results len {}", res.len());
                    for (op, result) in res.into_iter() {
                        match result {
                            Ok(_) => {
                                match op {
                                    IoOp::Pread(retbuf, token) => {
                                        let entry = self.handles.remove(token).unwrap();
                                        let elapsed = entry.timestamp.elapsed().expect("Time drift!");
                                        trace!("pread returned in {} us", ((elapsed.as_secs() * 1_000_000_000) + elapsed.subsec_nanos() as u64) / 1000);

                                        entry.complete.send(Ok((retbuf, None)));
                                    },
                                    _ => ()
                                }
                            },
                            Err(e) => panic!("error in pread: {:?}", e)
                        }
                    };
                },

                Err(e) => panic!("ctx.results failed: {:?}", e),
            }
        };

        if self.handles.len() > 0 {
            trace!("outstanding handles {}, stream.need_read()", self.handles.len());
            self.stream.need_read();
        }

        // Keep polling forever
        Ok(Async::NotReady)
    }
}


impl Drop for AioReader {
    fn drop(&mut self) {
        println!("dropping aioreader");
    }
}



struct AioEventFd {
    inner: EventFD
}


// TODO: Since we're just wrapping, could we just use the Evented
// trait directly on AioEventFd?
impl mio::Evented for AioEventFd {
    fn register(&self,
                poll: &mio::Poll,
                token: mio::Token,
                interest: mio::Ready,
                opts: mio::PollOpt) -> io::Result<()> {
        trace!("AioEventFd.register");
        mio::unix::EventedFd(&self.inner.as_raw_fd()).register(poll, token, interest, opts)
    }

    fn reregister(&self,
                  poll: &mio::Poll,
                  token: mio::Token,
                  interest: mio::Ready,
                  opts: mio::PollOpt) -> io::Result<()> {
        trace!("AioEventFd.reregister");
        mio::unix::EventedFd(&self.inner.as_raw_fd()).reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &mio::Poll) -> io::Result<()> {
        trace!("AioEventFd.deregister");
        mio::unix::EventedFd(&self.inner.as_raw_fd()).deregister(poll)
    }
}

fn cpuset_for_core(topology: &Topology, idx: usize) -> CpuSet {
    let cores = (*topology).objects_with_type(&ObjectType::PU).unwrap();
    //let cores = (*topology).objects_with_type(&ObjectType::Core).unwrap();
    match cores.get(idx) {
        Some(val) => val.cpuset().unwrap(),
        None => panic!("No Core found with id {}", idx),
    }
}

fn last_core(topology: &Topology) -> CpuSet {
    let cores = (*topology).objects_with_type(&ObjectType::PU).unwrap();
    //let cores = (*topology).objects_with_type(&ObjectType::Core).unwrap();
    cores.last().unwrap().cpuset().unwrap()
}
