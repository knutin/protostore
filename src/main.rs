extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate libaio;
extern crate chrono;
extern crate mio;
extern crate eventfd;
extern crate slab;



use std::io;
use std::str;
use std::iter;
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::collections::HashMap;

use std::ops::Drop;

use futures::{future, Future, Complete, Oneshot, BoxFuture, Async, Poll};
use futures::stream::{Stream, Fuse};
use futures::sync::mpsc::{unbounded, UnboundedSender, UnboundedReceiver};
use tokio_core::reactor::{Core, Handle, PollEvented};

use eventfd::EventFD;

use std::os::unix::io::AsRawFd;


use tokio_core::io::{Codec, EasyBuf};
use tokio_proto::pipeline::ServerProto;
use tokio_core::io::{Io, Framed};
use tokio_service::Service;

use tokio_proto::TcpServer;

use libaio::raw::{Iocontext, IoOp};
use chrono::Duration;

use slab::Slab;

pub struct LineCodec;
pub struct LineProto;
pub struct Echo;




impl Codec for LineCodec {
    type In = String;
    type Out = String;

    fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Self::In>> {
        if let Some(i) = buf.as_slice().iter().position(|&b| b == b'\n') {
            // remove the serialized frame from the buffer.
            let line = buf.drain_to(i);

            // Also remove the '\n'
            buf.drain_to(1);

            // Turn this data into a UTF string and return it in a Frame.
            match str::from_utf8(line.as_slice()) {
                Ok(s) => Ok(Some(s.to_string())),
                Err(_) => Err(io::Error::new(io::ErrorKind::Other,
                                             "invalid UTF-8")),
            }
        } else {
            Ok(None)
        }
    }

    fn encode(&mut self, msg: String, buf: &mut Vec<u8>)
              -> io::Result<()>
    {
        buf.extend(msg.as_bytes());
        buf.push(b'\n');
        Ok(())
    }

}



impl<T: Io + 'static> ServerProto<T> for LineProto {
    /// For this protocol style, `Request` matches the codec `In` type
    type Request = String;

    /// For this protocol style, `Response` matches the coded `Out` type
    type Response = String;

    /// A bit of boilerplate to hook in the codec:
    type Transport = Framed<T, LineCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;
    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(LineCodec))
    }
}


impl Service for Echo {
    // These types must match the corresponding protocol types:
    type Request = String;
    type Response = String;

    // For non-streaming protocols, service errors are always io::Error
    type Error = io::Error;

    // The future for computing the response; box it for simplicity.
    type Future = BoxFuture<Self::Response, Self::Error>;

    // Produce a future for computing a response from a request.
    fn call(&self, req: Self::Request) -> Self::Future {
        // In this case, the response is immediate.
        let response = "dummy".to_string();

        future::ok(response).boxed()
    }
}



enum Message {
    Execute(File, usize, Vec<u8>, Complete<io::Result<(Vec<u8>, Option<io::Error>)>>)
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



struct AioSession {
    tx: RefCell<UnboundedSender<Message>>
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

       Ok(AioSession { tx: RefCell::new(tx) })
    }


    pub fn read(&self, file: File, offset: usize, buf: Vec<u8>) -> ReadRequest {
        let (tx, rx) = futures::oneshot();
        println!("creating oneshot");
        self.tx
            .borrow_mut()
            .send(Message::Execute(file, offset, buf, tx))
            .expect("driver task has gone away");
        ReadRequest { inner: rx }
    }
}


impl Future for ReadRequest {
    type Item = (Vec<u8>, Option<io::Error>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        println!("polling readrequest");
        match self.inner.poll().expect("complete canceled") {
            Async::Ready(Ok(res)) => Ok(res.into()),
            Async::Ready(Err(res)) => panic!("readrequest.poll failed"),
            Async::NotReady => {
                println!("readrequest not ready");
                Ok(Async::NotReady)
            }
        }
    }
}


impl Future for AioReader {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        println!("AioReader.poll");

        // TODO: Batch up requests and submit in one call. Only allow
        // a certain queue depth, otherwise callers should block.

        // Enqueue incoming requests
        loop {
            let msg = match self.rx.poll().expect("cannot fail") {
                Async::Ready(Some(msg)) => msg,
                Async::Ready(None) => break,
                Async::NotReady => break
            };


            let (file, offset, buf, tx) = match msg {
                Message::Execute(file, offset, buf, tx) => (file, offset, buf, tx)
            };
            println!("executing request, file {:?}, buf {:?}", file, buf);


            let entry = self.handles.vacant_entry().expect("No more free handles!");
            let index = entry.index();
            println!("pread index {}", index);

            match self.ctx.pread(&file, buf, offset as u64, index) {
                Ok(()) => {
                    println!("pread success, adding handler for index {}", index);
                    entry.insert(HandleEntry {
                        complete: tx
                    });

                    println!("submitting");
                    match self.ctx.submit() {
                        Ok(_) => (),
                        Err(e) => panic!("submit failed {:?}", e)
                    }
                },
                Err((buf, token)) => {
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
            println!("poll evented is ready");


            match self.ctx.results(1, 10, None) {
                Ok(res) => {
                    println!("got results");
                    
                    for (op, result) in res.into_iter() {

                        match result {
                            Ok(_) => {
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

        if self.handles.is_empty() {
            Ok(().into())
        } else {
            Ok(Async::NotReady)
        }
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




#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Uuid(pub [u8; 16]);

struct BigFile {
    toc: HashMap<Uuid, usize>,
    ctx: Iocontext<usize, Vec<u8>, Vec<u8>>,
    datafd: File
}


impl BigFile {

    pub fn new(path: &str) -> io::Result<BigFile> {
        // TODO: open toc from separate file, insert into hashmap

        let mut io = match Iocontext::new(100) {
            Err(e) => panic!("iocontext new {:?}", e),
            Ok(io) => io
        };
        try!(io.ensure_evfd());

        let toc = HashMap::new();
        let datafd = OpenOptions::new()
            .read(true)
            .open(path).unwrap();

        Ok(BigFile { toc: toc, ctx: io, datafd: datafd })
    }

    pub fn read(&mut self, index: usize, len: usize) {
        let mut rbuf: Vec<_> = iter::repeat(0 as u8).take(len).collect();

        let token = 123;

        // Enqueue
        match self.ctx.pread(&self.datafd, rbuf, index as u64, token) {
            Ok(()) => {

                match self.ctx.submit() {
                    Err(e) => panic!("submit failed {:?}", e),
                    Ok(_) => ()
                };

                match self.ctx.results(1, 10, Some(Duration::seconds(1))) {
                    Err(e) => panic!("results failed {:?}", e),
                    Ok(res) => {
                        for &(ref op, ref r) in res.iter() {
                            match r {
                                &Err(ref e) => panic!("{:?} failed {:?}", op, e),
                                &Ok(res) => {
                                    match op {
                                        &IoOp::Pread(ref retbuf, tok) => {
                                            println!("complete {:?}, res {:?}, buf {:?}", tok, res, retbuf);
                                        },
                                        _ => panic!("unexpected {:?}", op)
                                    }
                                }
                            }
                        }
                    }
                }
            },
            Err(e) => { panic!("pread error {:?}", e); }
        }
    }
}


//pub struct ReadRequest {
//    inner: OneShot<io::Result<Vec<u8>>>
//}


//impl mio::Evented for BigFile {

//}


fn main() {



    //let addr = "0.0.0.0:12345".parse().unwrap();
    //let server = TcpServer::new(LineProto, addr);
    //server.serve(|| Ok(Echo));

    let path = "/tmp/protostore.data";

    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path).unwrap();

        file.write_all(&vec![97, 98, 99, 100]).expect("Could not write to file");
    }

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let aio_session = AioSession::new(handle).unwrap();

    let file = OpenOptions::new().read(true).open(path).unwrap();


    let buf: Vec<_> = iter::repeat(0 as u8).take(2).collect();
    let req = aio_session.read(file, 0, buf).map(|res| {
        println!("res {:?}", res);
        1
    });

    core.run(req).unwrap();

}

#[cfg(test)]
mod tests {

    extern crate tokio_core;

    use std::fs::{File, OpenOptions};
    use std::io::Write;

    use futures::Future;
    use tokio_core::reactor::Core;

    use super::AioSession;

    // #[test]
    // fn bigfile() {
    //     let path = "/tmp/protostore.data";

    //     {
    //         let mut file = OpenOptions::new()
    //             .write(true)
    //             .create(true)
    //             .truncate(true)
    //             .open(path).unwrap();

    //         file.write_all(&vec![97, 98, 99, 100]).expect("Could not write to file");
    //     }


    //     let mut f = BigFile::new(path).unwrap();

    //     //f.read(0, 2);


    //     let mut core = Core::new().unwrap();
    //     let handle = core.handle();
    // }

    #[test]
    fn session() {
        let path = "/tmp/protostore.data";

        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path).unwrap();

            file.write_all(&vec![97, 98, 99, 100]).expect("Could not write to file");
        }

        let mut core = Core::new().unwrap();
        let handle = core.handle();

        let aio_session = AioSession::new(handle).unwrap();

        let file = OpenOptions::new().read(true).open(path).unwrap();


        let buf = Vec::with_capacity(2);
        let req = aio_session.read(file, 0, buf).then(|(b, e)| {
            println!("b {:?}", b);
        });

        core.run(req).unwrap();



    }

}
