#[macro_use]
extern crate log;
extern crate env_logger;
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_io;
extern crate tokio_pool;
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
extern crate fnv;

use std::io;
use std::str;
use std::cmp;
use std::fs::metadata;
use std::collections::HashMap;
use std::path::PathBuf;
use std::ops::Drop;
use std::sync::Arc;
use std::cell::RefCell;
use std::time::SystemTime;

use futures::{future, Future, Complete, Oneshot, BoxFuture, Async, Poll, Sink};
use futures::stream::{Stream, Fuse};
use futures::sync::mpsc::{unbounded, UnboundedSender, UnboundedReceiver};

use eventfd::EventFD;
use std::os::unix::io::AsRawFd;
use libaio::raw::{Iocontext, IoOp};
use libaio::directio::{DirectFile, Mode, FileAccess};

use slab::Slab;

use tokio_core::reactor::{Core, Handle, PollEvented};
use tokio_core::net::{TcpStream, TcpListener};
use tokio_io::AsyncRead;
use tokio_io::codec::{Decoder, Encoder};
use tokio_pool::TokioPool;

use bytes::{Buf, BufMut, BytesMut, Bytes, IntoBuf};
use byteorder::{BigEndian, ByteOrder};
use rayon::prelude::*;

use fnv::FnvHashMap;

use clap::{App, Arg};


fn main() {

    env_logger::init().unwrap();

    //
    // PARSE ARGUMENTS
    //

    let matches = App::new("mk_data")
        .arg(Arg::with_name("path")
             .long("path")
             .takes_value(true)
             .required(true)
             .help("Directory to write datafiles"))
        .get_matches();


    let path = matches.value_of("path").unwrap();

    let mut toc_path = PathBuf::from(path);
    let mut data_path = PathBuf::from(path);

    toc_path.push("protostore.toc");
    data_path.push("protostore.data");


    //
    // Read Table of Contents
    //
    println!("Reading Table of Contents file at {:?}", toc_path);
    let meta = metadata(toc_path.clone()).expect("Could not read metadata for toc file");
    let toc_size: usize = meta.len() as usize;


    let toc_mmap = memmap::Mmap::open_path(toc_path, memmap::Protection::Read).expect("Could not read toc file");
    let toc_buf: &[u8] = unsafe { toc_mmap.as_slice() };


    let num_entries = (toc_size / 20) as u64;
    let toc_entries = (0..num_entries)
        .into_par_iter()
        .map(|i| {
            let offset = (i*20) as usize;
            let uuid = toc_buf[offset .. offset+16].to_vec();
            let len = BigEndian::read_u32(&toc_buf[offset+16 .. offset+20]) as usize;
            (uuid, len)
        })
        .collect::<Vec<(Vec<u8>, usize)>>();

    println!("Read toc. Creating HashMap");
    let mut toc: FnvHashMap<Vec<u8>, (usize, usize)> = FnvHashMap::with_capacity_and_hasher(num_entries as usize, Default::default());
    let mut offset = 0;
    let mut max_len = 0;
    for (uuid, len) in toc_entries {
        toc.insert(uuid, (offset, len));
        offset += len;
        max_len = cmp::max(max_len, len);
    }
    let toc = Arc::new(toc);

    println!("Toc has {} entries. Max length {}", toc.len(), max_len);



    //
    // Create TCP listener
    //

    println!("Creating tcp event loop");
    let addr = "0.0.0.0:12345".parse().unwrap();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let datafile = DirectFile::open(data_path.clone(), Mode::Open, FileAccess::Read, 4096)
        .expect("Could not open data file with O_DIRECT");
    let session = Arc::new(AioSession::new(handle.clone(), datafile).unwrap());
    let session_handle = session.handle();
    let listener = TcpListener::bind(&addr, &handle).unwrap();

    let (pool, join) = TokioPool::new(1).unwrap();
    let pool = Arc::new(pool);


    let s = listener.incoming().for_each(move |(socket, addr)| {
        let aligned_max_len = max_len + (max_len % 512);
        let mut buf = BytesMut::with_capacity(aligned_max_len*2);
        unsafe { buf.set_len(aligned_max_len*2) };

        let server = Server {
            session: session_handle.clone(),
            toc: toc.clone()
        };

        println!("Got new connection from {}", addr);

        let result = server
            .serve(socket, buf)
            .map(|_|  println!("Connection closed"))
            .map_err(|_| println!("Serve error"));
        pool.next_worker().spawn(move |h| result);

        Ok(())
    });

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
    toc: Arc<FnvHashMap<Vec<u8>, (usize, usize)>>,
}

impl Server {
    fn serve(self, socket: TcpStream, buf: BytesMut) -> BoxFuture<(), io::Error> {
        let framed = socket.framed(Protocol);
        let (writer, reader) = framed.split();
        let buf = RefCell::new(buf);

        let responses = reader.and_then(move |req| {
            let mybuf = buf.clone().into_inner();

            match self.toc.clone().get(req.uuid.as_slice()) {
                Some(&(offset, len)) => {
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
                None => {
                    let res = Response {
                        id: req.id,
                        len: 0,
                        body: Bytes::new()
                    };
                    future::ok(res).boxed()
                }
            }
        });

        writer.send_all(responses).and_then(|_| future::ok(())).boxed()
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
            trace!("calling results");
            match self.ctx.results(1, 100, None) {
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

        trace!("outstanding handles {}", self.handles.len());

        if self.handles.len() > 0 {
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
