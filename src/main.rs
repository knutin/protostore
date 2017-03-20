// cargo run --bin run --  --path=/mnt/data/


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

use std::io;
use std::mem;
use std::str;
use std::iter;
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

use slab::Slab;

use tokio_core::reactor::{Core, Handle, PollEvented};
use tokio_core::net::{TcpStream, TcpListener};
use tokio_io::io::{read_exact, write_all};
use tokio_io::AsyncRead;
use tokio_io::codec::{FramedRead, Decoder, Encoder};

use bytes::{Buf, BufMut, BytesMut, IntoBuf};


use byteorder::{BigEndian, ByteOrder};

use clap::{App, Arg};

mod tcp;
use tcp::LineProto;




fn main() {

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


    println!("Reading toc file at {:?}", toc_path);
    let meta = metadata(toc_path.clone()).expect("Could not read metadata for toc file");
    let toc_size: usize = meta.len() as usize;




    // Read the entire file at once
    let mut toc_file = OpenOptions::new().read(true).open(toc_path).unwrap();
    //let mut toc_buf: Vec<u8> = iter::repeat(0 as u8).take(toc_size).collect();
    let mut toc_buf: Vec<u8> = vec![0; toc_size];
    toc_file.read_exact(&mut toc_buf).expect("Could not read toc file");


    let mut toc: HashMap<Vec<u8>, (usize, usize)> = HashMap::new();
    let mut offset = 0;
    let mut i: usize = 0;
    while i < toc_size {
        let uuid = toc_buf[i .. i+16].to_vec();
        let num = BigEndian::read_u32(&toc_buf[i+16 .. i+20]) as usize;
        let len = num * mem::size_of::<u64>();
        toc.insert(uuid, (offset, num));
        offset += len;
        i += 20;
    }
    let toc = Arc::new(toc);
    println!("Toc has {} entries", toc.len());




    println!("Creating tcp event loop");
    let addr = "0.0.0.0:12345".parse().unwrap();

    let mut lp = Core::new().unwrap();
    let handle = lp.handle();

    let session = Arc::new(AioSession::new(handle.clone()).unwrap());
    let listener = TcpListener::bind(&addr, &handle).unwrap();

    let s = listener.incoming().for_each(move |(socket, addr)| {
        let data = OpenOptions::new().read(true).open(data_path.clone()).unwrap();
        let len = 10_000 * mem::size_of::<u64>();
        let buf = vec![0u8; len];

        let server = Server {
            session: session.clone(),
            toc: toc.clone(),
            data: data,
            buf: buf
        };


        println!("Got connection");

        server.serve(socket)

        // let (writer, reader) = socket.framed(Protocol).split();
        // reader.for_each(|req| {
        //     println!("req {:?}", req);
        //     future::ok(())
        // })



        //handle.spawn(reader.map(|req| {
        //    server.call(req).map(|res| writer.send(res))
        //}).then(|_| Ok(())))

        //reader.and_then(|req| future::ok(())).boxed()
        // reader.for_each(move |req| {
        //     println!("req {:?}", req);
        //     //let res = server.call(req);

        //     future::ok(())
        // }).then(|_| Ok(())).into_future()

        //future::ok(())

        //reader.map(move |req| {
        //    let res = server.call(req);
        //    res.and_then(move |r| writer.send(r))
        //})
    });


    lp.run(s).unwrap();

    // let server = TcpServer::new(LineProto, addr);

    // let data_path = data_path;
    // server.with_handle(move |handle| {
    //     let session = Arc::new(AioSession::new(handle.clone()).unwrap());

    //     let data = OpenOptions::new().read(true).open(data_path.clone()).unwrap();
    //     let toc = toc.clone();

    //     let len = 10_000 * mem::size_of::<u64>();
    //     let buf = Arc::new(vec![0u8; len]);

    //     println!("factory");

    //     move || {
    //         println!("new connection");
    //         Ok(Protocol { session: session.clone(),
    //                       toc: toc.clone(),
    //                       data: data.try_clone().unwrap(),
    //                       buf: buf.clone() })
    //     }
    // });

}








enum Message {
    Execute(File, usize, Vec<u8>, usize, Complete<io::Result<(Vec<u8>, Option<io::Error>)>>)
}


struct ReadRequest {
    inner: Oneshot<io::Result<(Vec<u8>, Option<io::Error>)>>
}


struct AioReader {
    rx: Fuse<UnboundedReceiver<Message>>,
    handle: Handle,
    //ctx: Iocontext<Complete<io::Result<Vec<u8>>>, Vec<u8>, Vec<u8>>,
    ctx: Iocontext<usize, Vec<u8>, Vec<u8>>,
    stream: PollEvented<AioEventFd>,

    // Handles to outstanding requests
    handles: Slab<HandleEntry>
}

struct HandleEntry {
    complete: Complete<io::Result<(Vec<u8>, Option<io::Error>)>>
}


#[derive(Clone)]
struct AioSession {
    pub tx: UnboundedSender<Message>
}





//struct Protocol {
//    session: Arc<AioSession>,
//    toc: Arc<HashMap<Vec<u8>, (usize, usize)>>,
//    data: File,
//    buf: Arc<Vec<u8>>
//}

// impl Service for Protocol {
//     type Request = Vec<u8>;
//     type Response = Vec<u8>;
//     type Error = io::Error;
//     type Future = BoxFuture<Self::Response, Self::Error>;

//     fn call(&self, req: Self::Request) -> Self::Future {
//         match self.toc.as_ref().get(&req) {
//             Some(&(offset, num)) => {

//                 let file = self.data.try_clone().expect("Could not clone fd");
//                 let len = num * mem::size_of::<u64>();
//                 let buf: Vec<u8> = vec![0; len];
//                 //let buf = mem::replace(&mut self.buf, vec![]);
//                 //let buf = Arc::get_mut(&mut self.buf).unwrap();
//                 //let buf = Arc::try_unwrap(self.buf).unwrap();

//                 self.session.read(file, offset, buf, num).and_then(move |(res, err)| {
//                     //mem::replace(&mut self.buf, res);
//                     match err {
//                         Some(_) => future::ok(vec![0]).boxed(),
//                         None => future::ok(res).boxed()
//                     }
//                 }).boxed()
//             },
//             None => future::ok(vec![0]).boxed()
//         }
//     }
// }

#[derive(Debug)]
struct Request {
    id: u32,
    uuid: Vec<u8>
}

#[derive(Debug)]
struct Response {
    id: u32,
    len: u32,
    body: Vec<u8>
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
        dst.reserve(20 + item.len as usize);
        dst.put_u32::<BigEndian>(item.id);
        dst.put_u32::<BigEndian>(item.len);
        dst.put_slice(&item.body);
        Ok(())
    }
}


struct Server {
    session: Arc<AioSession>,
    toc: Arc<HashMap<Vec<u8>, (usize, usize)>>,
    data: File,
    buf: Vec<u8>
}

impl Server {

    //fn call(mut self, socket: TcpStream) -> Box<Future<Item=Response, Error=io::Error>> {
    fn serve(mut self, socket: TcpStream) -> Box<Future<Item=(), Error=io::Error>> {
        let framed = socket.framed(Protocol);
        let (writer, reader) = framed.split();

        // println!("serving");
        // reader.for_each(|f| {
        //     println!("f {:?}", f);
        //     future::ok(())
        // }).boxed()

        //future::ok(()).boxed()
        //future::ok(Ok(())).boxed()
        //future::err(io::Error::new(io::ErrorKind::Other, "foo")).boxed()
            //future::ok(()).boxed()

            //future::ok(()).boxed()


        // reader.for_each(move |req| {
        //     println!("req {:?}", req);
        //     //let res = server.call(req);

        //     future::ok(())
        // }).then(|_| Ok(())).into_future()

        //future::ok(())

        let responses = reader.and_then(move |req| {
            println!("got {:?}", req);

            match self.toc.get(req.uuid.as_slice()) {
                Some(&(offset, num)) => {
                    let file = self.data.try_clone().unwrap();
                    let buf = mem::replace(&mut self.buf, vec![]);

                    self.session.read(file, offset, buf, num).and_then(move |(buf, err)| {
                        //mem::replace(&mut self.buf, buf);

                        let buflen = if err.is_none() { num } else { 0 };
                        //let body: Vec<u8> = self.buf[0..buflen].to_vec();
                        let body: Vec<u8> = buf[0..buflen].to_vec();
                        let res = Response {
                            id: req.id,
                            len: buflen as u32,
                            body: body
                        };
                        future::ok(res)
                    }).boxed()
                },
                None => {
                    let res = Response {
                        id: req.id,
                        len: 0,
                        body: vec![]
                    };
                    future::ok(res).boxed()
                }
            }

            // let res = Response {
            //     id: 123,
            //     len: 0,
            //     body: vec![]
            // };
            // println!("response {:?}", res);
            // future::ok(res)
            //stream::iter(vec![Ok(res)].into_iter()).into_future()
                //writer.send(res).and_then(|_| future::ok(()))
                //framed.send(res).and_then(|_| future::ok(()))
        });

        writer.send_all(responses).and_then(|_| future::ok(())).boxed()
        //responses.for_each(|res| writer.send(res).and_then(|_| future::ok(()))).boxed()


        // reader.map(move |req| {
        //     match self.toc.get(req.uuid.as_slice()) {
        //         Some(&(offset, num)) => {
        //             let file = self.data.try_clone().unwrap();
        //             let buf = mem::replace(&mut self.buf, vec![]);

        //             self.session.read(file, offset, buf, num).and_then(move |(buf, err)| {
        //                 mem::replace(&mut self.buf, buf);

        //                 let buflen = if err.is_none() { num } else { 0 };
        //                 let body: Vec<u8> = self.buf[0..buflen].to_vec();
        //                 let res = Response {
        //                     id: req.id,
        //                     len: buflen as u32,
        //                     body: body
        //                 };

        //                 writer.send(res).boxed()
        //             }).boxed()
        //         },

        //         None => {
        //             let res = Response {
        //                 id: req.id,
        //                 len: 0,
        //                 body: vec![]
        //             };
        //             writer.send(res).boxed()
        //         }
        //     }
        // }).boxed()
    }
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

       Ok(AioSession { tx: tx })
    }


    pub fn read(&self, file: File, offset: usize, buf: Vec<u8>, len: usize) -> ReadRequest {
        let (tx, rx) = futures::oneshot();
        self.tx
            .clone()
            .send(Message::Execute(file, offset, buf, len, tx))
            .map_err(|e| panic!("driver task has gone away"));

        ReadRequest { inner: rx }
    }
}


impl Future for ReadRequest {
    type Item = (Vec<u8>, Option<io::Error>);
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

        //let event = UnparkEvent::new(
        // task::with_unpark_event(event, || { })
        //println!("polling stream");

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

        // if self.handles.is_empty() {
        //     println!("ain't got no more handles");
        //     Ok(().into())
        // } else {
        //     Ok(Async::NotReady)
        // }
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


// TODO: Since we're just wrapping, could we just use the Evented trait directly on AioEventFd
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








#[cfg(test)]
mod tests {
}
