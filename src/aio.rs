use futures::sync::mpsc::{channel};
use std::thread::{self, JoinHandle};

use std::io;

use mio;

use libc;

use futures::{Future, Async, Poll};
use futures::{oneshot, Complete};
use futures::stream::{Stream, Fuse};
use futures::sync::mpsc;

use tokio_core::reactor::{Core, PollEvented};

use bytes::BytesMut;
use eventfd::EventFD;
use std::os::unix::io::AsRawFd;
use libaio::raw::{Iocontext, IoOp};
use libaio::directio::DirectFile;

use slab::Slab;

#[derive(Debug)]
pub enum Message {
    PRead(DirectFile, usize, usize, BytesMut, Complete<io::Result<(BytesMut, Option<io::Error>)>>),
    Ping(Complete<io::Result<u8>>)
}



#[derive(Debug)]
pub struct Session {
    pub inner: mpsc::Sender<Message>,
    thread: JoinHandle<()>,
    pthread: libc::pthread_t
}

#[derive(Debug, Clone)]
struct SessionHandle {
    inner: mpsc::Sender<Message>
}


impl Session {
    pub fn new(max_queue_depth: usize) -> io::Result<Session> {

        // Users of session interact with us by sending messages.
        let (tx, rx) = channel::<Message>(max_queue_depth);

        let (tid_tx, tid_rx) = oneshot();

        // Spawn a thread with it's own event loop dedicated to AIO
        let t = thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();

            // Return the pthread id so the main thread can bind this
            // thread to a specific core
            tid_tx.send(unsafe { libc::pthread_self() }).unwrap();

            let mut ctx = match Iocontext::<usize, BytesMut, BytesMut>::new(max_queue_depth) {
                Ok(ctx) => ctx,
                Err(e) => panic!("could not create Iocontext: {}", e)
            };

            // Using an eventfd, the kernel can notify us when there's
            // one or more AIO results ready. See 'man eventfd'
            match ctx.ensure_evfd() {
                Ok(_) => (),
                Err(e) => panic!("ensure_evfd failed: {}", e)
            };
            let evfd = ctx.evfd.as_ref().unwrap().clone();


            // Add the eventfd to the file descriptors we are
            // interested in. This will use epoll under the hood.
            let source = AioEventFd { inner: evfd };
            let stream = PollEvented::new(source, &handle).unwrap();

            let fut = AioThread {
                rx: rx.fuse(),
                ctx: ctx,
                stream: stream,
                handles_pread: Slab::with_capacity(max_queue_depth),
                handles_pwrite: Slab::with_capacity(max_queue_depth),
            };

            core.run(fut).unwrap();
        });

        let tid = tid_rx.wait().unwrap();

        Ok(Session { inner: tx, thread: t, pthread: tid })
    }

    pub fn thread_id(&self) -> libc::pthread_t {
        self.pthread
    }
}


struct AioThread {
    rx: Fuse<mpsc::Receiver<Message>>,
    ctx: Iocontext<usize, BytesMut, BytesMut>,
    stream: PollEvented<AioEventFd>,

    // Handles to outstanding requests
    handles_pread: Slab<HandleEntry>,
    handles_pwrite: Slab<HandleEntry>
}

struct HandleEntry {
    complete: Complete<io::Result<(BytesMut, Option<io::Error>)>>
}



impl Future for AioThread {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        trace!("AioThread.poll");

        // If there are any responses from the kernel available, read
        // as many as we can without blocking.
        if self.stream.poll_read().is_ready() {
            trace!("eventfd is ready, calling io_getevents. {} inflight pread", self.handles_pread.len());
            match self.ctx.results(0, 100, None) {
                Ok(res) => {
                    trace!("got {} AIO responses", res.len());
                    for (op, result) in res.into_iter() {
                        match result {
                            Ok(_) => {
                                match op {
                                    IoOp::Pread(retbuf, token) => {
                                        let entry = self.handles_pread.remove(token).unwrap();
                                        //let elapsed = entry.timestamp.elapsed().expect("Time drift!");
                                        //trace!("pread returned in {} us", ((elapsed.as_secs() * 1_000_000_000) + elapsed.subsec_nanos() as u64) / 1000);

                                        //entry.complete.send(Ok((retbuf, None))).expect("Could not send AioSession response");
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


        // Read all available incoming requests, enqueue in AIO batch
        loop {
            let msg = match self.rx.poll().expect("cannot fail") {
                Async::Ready(Some(msg)) => msg,
                Async::Ready(None) => break,
                Async::NotReady => break // AioThread.poll is automatically scheduled
            };

            trace!("AioThread.poll received msg");

            match msg {
                Message::PRead(file, offset, len, buf, complete) => {
                    let entry = self.handles_pread.vacant_entry().expect("No more free pread handles!");
                    let index = entry.index();
                    match self.ctx.pread(&file, buf, offset as u64, len, index) {
                        Ok(()) => {
                            entry.insert(HandleEntry { complete: complete });
                        },
                        Err((buf, _token)) => {
                            complete.send(Ok((buf, Some(io::Error::new(io::ErrorKind::Other, "pread failed")))))
                                .expect("Could not send AioThread error response");
                        }
                    }
                },

                Message::Ping(complete) => {
                    complete.send(Ok(42)).expect("Could not send AioThread Ping response");
                }
            }

            // TODO: If max queue depth is reached, do not receive any
            // more messages, will cause clients to block
        }

        trace!("ctx.batched() {}", self.ctx.batched());

        // TODO: Need busywait for submit timeout


        while self.ctx.batched() > 0 {
            if let Err(e) = self.ctx.submit() {
                panic!("batch submit failed {:?}", e);
            }
        }


        if self.handles_pread.len() > 0 {
            trace!("inflight {}, calling stream.need_read()", self.handles_pread.len());
            self.stream.need_read();
        }


        // Run forever
        Ok(Async::NotReady)
    }
}


// Register the eventfd with mio
struct AioEventFd {
    inner: EventFD
}

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


#[cfg(test)]
mod tests {
    extern crate env_logger;
    extern crate tempdir;
    extern crate uuid;

    use std::io;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use self::tempdir::TempDir;
    use byteorder::{BigEndian, ByteOrder};

    use bytes::{Buf, BytesMut, BufMut, IntoBuf};
    use libaio::directio::{DirectFile, Mode, FileAccess};
    use aio::{Session, Message};

    use futures::{Future, Sink, Stream, oneshot, stream};



    #[test]
    fn test_init() {
        let session = Session::new(512);
        assert!(session.is_ok());
    }

    // TODO: Test max queue depth



    #[test]
    fn test_pread() {
        env_logger::init().unwrap();

        let path = new_file_with_sequential_u64("pread", 1024);
        let file = DirectFile::open(path, Mode::Open, FileAccess::Read, 4096).unwrap();


        let session = Session::new(2).unwrap();

        let mut buf = BytesMut::with_capacity(512);
        unsafe { buf.set_len(512) };
        let (tx, rx) = oneshot();
        let fut = session.inner.send(Message::PRead(file, 0, 512, buf, tx));
        fut.wait();

        let res = rx.wait();
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_ok());
        let (mut buf, err) = res.unwrap();
        assert!(err.is_none());

        for i in 0..(512/8) {
            assert_eq!(i, buf.split_to(8).into_buf().get_u64::<BigEndian>());
        }
        assert_eq!(0, buf.len());
    }


    #[test]
    fn test_pread_many() {
        //env_logger::init().unwrap();

        let path = new_file_with_sequential_u64("pread", 10240);



        let session = Session::new(4).unwrap();

        let handle1 = session.handle();
        let handle2 = session.handle();

        // let reads = (0..5).map(|_| {
        //     println!("foo");
        //     let file = DirectFile::open(path.clone(), Mode::Open, FileAccess::Read, 4096).unwrap();
        //     let mut buf = BytesMut::with_capacity(512);
        //     unsafe { buf.set_len(512) };
        //     let (tx, rx) = oneshot();
        //     session.inner.send(Message::PRead(file, 0, 512, buf, tx))
        // });

        // let file1 = DirectFile::open(path.clone(), Mode::Open, FileAccess::Read, 4096).unwrap();
        // let file2 = DirectFile::open(path.clone(), Mode::Open, FileAccess::Read, 4096).unwrap();
        // let mut buf1 = BytesMut::with_capacity(512);
        // let mut buf2 = BytesMut::with_capacity(512);
        // unsafe { buf1.set_len(512) };
        // unsafe { buf2.set_len(512) };
        // let req1 = handle1.pread(file1, 0, 512, buf1);
        // let req2 = handle2.pread(file2, 0, 512, buf2);
        // //session.inner.clone().send(Message::PRead(file2, 0, 512, buf2, tx2));


        // let res = req1.wait();



        //let stream: Stream<Item=Message, Error=io::Error> = stream::iter(reads);

        //let stream: Stream<Item=Message, Error=io::Error> = stream::iter((0..5).map(Ok));

        //let responses = session.inner.send_all(stream);

    }







    fn new_file_with_sequential_u64(name: &str, num: usize) -> String {
        let tmp = TempDir::new("test").unwrap();
        let mut path = tmp.into_path();
        path.push(name);

        let mut data = BytesMut::with_capacity(num * 8);
        for i in 0..num {
            data.put_u64::<BigEndian>(i as u64);
        }
        let data = data.freeze();

        let mut file = File::create(path.clone()).expect("Could not create dummy_clustermap");
        file.write_all(data.as_ref()).unwrap();

        path.to_str().unwrap().to_owned()
    }
}
