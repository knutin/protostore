extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate libaio;
extern crate chrono;
extern crate mio;
extern crate eventfd;
extern crate slab;
extern crate byteorder;
extern crate uuid;
extern crate rand;


use std::io;
use std::mem;
use std::str;
use std::iter;
use std::cell::{Cell, RefCell};
use std::fs::{File, OpenOptions};
use std::io::{Write, Read};
use std::collections::HashMap;

use std::ops::Drop;

use std::sync::Arc;

use futures::{future, Future, Complete, Oneshot, BoxFuture, Async, Poll};
use futures::stream::{Stream, Fuse};
use futures::sync::mpsc::{unbounded, UnboundedSender, UnboundedReceiver};
use tokio_core::reactor::{Core, Handle, PollEvented};

use eventfd::EventFD;
use std::os::unix::io::AsRawFd;
use libaio::raw::{Iocontext, IoOp};
use chrono::Duration;

use slab::Slab;

use tokio_service::Service;
use tokio_proto::TcpServer;


use byteorder::{BigEndian, ByteOrder};

mod tcp;
use tcp::LineProto;


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


#[derive(Clone)]
struct AioSession {
    pub tx: UnboundedSender<Message>
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


    pub fn read(&self, file: File, offset: usize, buf: Vec<u8>) -> ReadRequest {
        let (tx, rx) = futures::oneshot();
        println!("creating oneshot");
        self.tx
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





//type Uuid = [u8; 16];



struct Protocol {
    session: Arc<AioSession>,
    toc: Arc<HashMap<Vec<u8>, (usize, usize)>>,
    data: File
}

impl Service for Protocol {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Error = io::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        match self.toc.as_ref().get(&req) {
            Some(&(offset, len)) => {

                println!("found uuid in toc, offset {}, len {}", offset, len);

                let file = self.data.try_clone().expect("Could not clone fd");
                let buf: Vec<u8> = iter::repeat(0 as u8).take(len).collect();

                self.session.read(file, offset, buf).and_then(|res| {
                    println!("got aio response {:?}", res);
                    future::ok(vec![1,2,3])
                }).boxed()
            },
            None => future::ok(vec![0]).boxed()
        }
    }
}





fn main() {

    let toc_path = "/tmp/protostore.toc".to_owned();
    let data_path = "/tmp/protostore.data".to_owned();

    let num_cookies = 10;


    // Generate dummy data
    {
        println!("Generating dummy data in /tmp/protostore.data");

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
            let len = num * mem::size_of::<u64>();
            let mut encoded_len = [0; 4];
            BigEndian::write_u32(&mut encoded_len, num as u32);

            toc.write_all(&uuid).expect("Could not write to toc");
            toc.write_all(&encoded_len).expect("Could not write to toc");

            let mut encoded_value = [0; 8];
            for j in 0..num {
                BigEndian::write_u64(&mut encoded_value, j as u64);
                data.write_all(&encoded_value).expect("Could not write to data");
            }
        }
    }


    println!("Creating toc hashmap");
    let mut toc_file = OpenOptions::new().read(true).open("/tmp/protostore.toc".to_owned()).unwrap();

    let mut toc: HashMap<Vec<u8>, (usize, usize)> = HashMap::new();

    let mut toc_buf = [0; 20];
    let mut offset = 0;
    while let Ok(()) = toc_file.read_exact(&mut toc_buf) {
        let uuid = toc_buf.as_ref()[0..16].to_vec();
        let len = BigEndian::read_u32(&toc_buf.as_ref()[16..20]) as usize;
        toc.insert(uuid, (offset, len));
        offset += len;
    }



    let toc = Arc::new(toc);
    println!("toc has {} entries", toc.len());




    println!("Creating tcp event loop");
    let addr = "0.0.0.0:12345".parse().unwrap();
    let server = TcpServer::new(LineProto, addr);


    server.with_handle(move |handle| {
        let session = Arc::new(AioSession::new(handle.clone()).unwrap());
        let tx = session.tx.clone();

        let data = OpenOptions::new().read(true).open("/tmp/protostore.data".to_owned()).unwrap();
        let toc = toc.clone();

        move || {

            Ok(Protocol { session: session.clone(),
                          toc: toc.clone(),
                          data: data.try_clone().unwrap() })
    }

    });


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

}
