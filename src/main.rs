extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_io;
extern crate libaio;
extern crate chrono;
extern crate mio;
extern crate eventfd;
extern crate slab;
extern crate byteorder;
extern crate uuid;
extern crate bytes;
extern crate clap;
extern crate rayon;
extern crate memmap;

use std::io;
use std::mem;
use std::str;
use std::iter;
use std::cmp;
use std::fs::{File, OpenOptions, metadata};
use std::io::Read;
use std::collections::HashMap;
use std::path::PathBuf;
use std::ops::Drop;
use std::sync::Arc;
use std::cell::RefCell;
use std::rc::Rc;

use futures::{future, Future, Complete, Oneshot, BoxFuture, Async, Poll, Sink, IntoFuture};
use futures::stream::{self, Stream, Fuse};
use futures::sync::mpsc::{unbounded, UnboundedSender, UnboundedReceiver};


use eventfd::EventFD;
use std::os::unix::io::AsRawFd;
use libaio::raw::{Iocontext, IoOp};
use libaio::{WrBuf, RdBuf};

use slab::Slab;

use tokio_core::reactor::{Core, Handle, PollEvented};
use tokio_core::net::{TcpStream, TcpListener};
use tokio_io::io::{read_exact, write_all};
use tokio_io::AsyncRead;
use tokio_io::codec::{FramedRead, Decoder, Encoder};

use bytes::{Buf, BufMut, BytesMut, Bytes, IntoBuf};
use byteorder::{BigEndian, ByteOrder};
use rayon::prelude::*;

use clap::{App, Arg};


fn main() {

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
    let mut toc:  HashMap<Vec<u8>, (usize, usize)> = HashMap::with_capacity(num_entries as usize);
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

    let session = Arc::new(AioSession::new(handle.clone()).unwrap());
    let session_handle = session.handle();
    let listener = TcpListener::bind(&addr, &handle).unwrap();

    let s = listener.incoming().for_each(move |(socket, addr)| {
        let data = OpenOptions::new().read(true).open(data_path.clone()).unwrap();
        let mut buf = BytesMut::with_capacity(max_len);
        unsafe { buf.set_len(max_len) };

        let server = Server {
            session: session_handle.clone(),
            toc: toc.clone(),
            data: data
        };

        println!("Got new connection from {}", addr);

        let result = server
            .serve(socket, buf)
            .map(|_|  println!("Connection closed"))
            .map_err(|_| println!("Serve error"));
        handle.spawn(result);

        Ok(())
    });

    core.run(s).unwrap();
}




enum Message {
    Execute(File, usize, BytesMut, usize, Complete<io::Result<(BytesMut, Option<io::Error>)>>)
}

struct ReadRequest {
    inner: Oneshot<io::Result<(BytesMut, Option<io::Error>)>>
}

struct AioReader {
    rx: Fuse<UnboundedReceiver<Message>>,
    handle: Handle,
    ctx: Iocontext<usize, BytesMut, BytesMut>,
    stream: PollEvented<AioEventFd>,

    // Handles to outstanding requests
    handles: Slab<HandleEntry>
}

struct HandleEntry {
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
    toc: Arc<HashMap<Vec<u8>, (usize, usize)>>,
    data: File
}

impl Server {
    fn serve(mut self, socket: TcpStream, mut buf: BytesMut) -> Box<Future<Item=(), Error=io::Error>> {
        let framed = socket.framed(Protocol);
        let (writer, reader) = framed.split();
        let buf = RefCell::new(buf);

        let responses = reader.and_then(move |req| {
            //println!("Parsed request {:?}", req);

            let mybuf = buf.clone().into_inner();

            match self.toc.clone().get(req.uuid.as_slice()) {
                Some(&(offset, num)) => {
                    let file = self.data.try_clone().unwrap();

                    self.session.read(file, offset, mybuf, num).boxed().and_then(move |(buf, err)| {
                        let buflen = if err.is_none() { num } else { 0 };

                        let res = Response {
                            id: req.id,
                            len: buflen,
                            body: buf.freeze()
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
   pub fn new(handle: Handle) -> io::Result<AioSession> {
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
           handle: handle,
           ctx: ctx,
           stream: stream,
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
    pub fn read(&self, file: File, offset: usize, buf: BytesMut, len: usize) -> ReadRequest {
        let (tx, rx) = futures::oneshot();
        //self.inner.send(Message::Execute(file, offset, buf, len, tx));
        UnboundedSender::send(&self.inner, Message::Execute(file, offset, buf, len, tx));
        ReadRequest { inner: rx }
    }
}




impl Future for ReadRequest {
    type Item = (BytesMut, Option<io::Error>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.inner.poll().expect("complete canceled") {
            Async::Ready(Ok(res)) => Ok(res.into()),
            Async::Ready(Err(res)) => panic!("readrequest.poll failed"),
            Async::NotReady =>  Ok(Async::NotReady)
        }
    }
}


impl Future for AioReader {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        // TODO: Batch up requests and submit in one call. Only allow
        // a certain queue depth, otherwise callers should block.

        // Enqueue incoming requests
        loop {
            let msg = match self.rx.poll().expect("cannot fail") {
                Async::Ready(Some(msg)) => msg,
                Async::Ready(None) => break,
                Async::NotReady => break
            };

            let (file, offset, buf, len, tx) = match msg {
                Message::Execute(file, offset, buf, len, tx) => (file, offset, buf, len, tx)
            };

            let entry = self.handles.vacant_entry().expect("No more free handles!");
            let index = entry.index();
            match self.ctx.pread(&file, buf, offset as u64, len, index) {
                Ok(()) => {
                    entry.insert(HandleEntry {
                        complete: tx
                    });

                    while self.ctx.batched() > 0 {
                        match self.ctx.submit() {
                            Ok(num_submitted) => { assert!(num_submitted == 1); },
                            Err(e) => panic!("submit failed {:?}", e)
                        };
                    };
                },
                Err((buf, _token)) => {
                    println!("pread failed");
                    tx.complete(Ok((buf, Some(io::Error::new(io::ErrorKind::Other, "pread failed, possibly full")))));
                    continue
                }
            };
        };

        if self.stream.poll_read().is_ready() {

            match self.ctx.results(1, 10, None) {
                Ok(res) => {
                    for (op, result) in res.into_iter() {

                        match result {
                            Ok(ret) => {
                                match op {
                                    IoOp::Pread(retbuf, token) => {
                                        let entry = self.handles.remove(token).unwrap();
                                        entry.complete.complete(Ok((retbuf, None)));
                                    },
                                    _ => ()
                                }
                            },
                            Err(e) => panic!("ctx.results failed {:?}", e)
                        }
                    }
                },

                Err(e) => panic!("results failed {:?}", e),
            }
        };

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
        mio::unix::EventedFd(&self.inner.as_raw_fd()).register(poll, token, interest, opts)
    }

    fn reregister(&self,
                  poll: &mio::Poll,
                  token: mio::Token,
                  interest: mio::Ready,
                  opts: mio::PollOpt) -> io::Result<()> {
        mio::unix::EventedFd(&self.inner.as_raw_fd()).reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &mio::Poll) -> io::Result<()> {
        mio::unix::EventedFd(&self.inner.as_raw_fd()).deregister(poll)
    }
}
