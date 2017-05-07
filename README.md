# `protostore`

I built this little project to learn Rust and understand the
performance of Rust futures and `libaio` on machines with NVMe SSD
drives. Rust is an amazing language with great tooling and a great
community. NVMe is pretty cool too!


## Architecture

This is a very simple key-value store with just one large datafile on
disk. There's some metadata files that map a key to an offset and
length in the datafile. I did it this way so I could just issue a
single `pread` for each read.

A real system with a similar architecture would probably use multiple
datafiles that are regenerated periodically and a separate buffer for
storing incoming writes. Reads could hit both the datafile an the
buffer.

When the server starts up I `mmap` the metadata files which contains
the uuids, offsets and lengths. Obviously this needs to fit in memory,
but it's not a huge problem since even at 250M keys it would be ~8GB
(32*250M)

The server is multi-threaded and has the following threads:

 * Main thread runs the tcp accept loop, which receives each new
   connection, sets up the state needed to handle the client, then
   runs `handle_client` on one of the tcp event loops.

 * The tcp event loops runs the `handle_client` future. It receives
   requests from client and asks the AIO threads to do the `pread` and
   `pwrite`. The number of threads can be controlled with
   `--num-tcp-threads`. The `future` crate makes it possible to write
   code that looks like it's blocking, while in fact under the hood
   it's using `epoll`.

 * The AIO event loops receives `pread` and `pwrite` requests, batches
   them up and calls `io_submit`. Using `eventfd`, the thread gets
   notified when the reply is ready (usually <1ms) Replies are also
   read in batch and sent back to any client that might be waiting.

I implemented a my own `Future` to run the AIO
threads
[aio.rs][https://github.com/knutin/protostore/blob/master/src/aio.rs#L134] Here's
how it works:

 1. Read results from the kernel with `io_getevents`. Reply to all
    outstanding requests by completing the future they are waiting on
    (just a channel receive currently, this could be made nicer and
    probably more efficient)

 1. Receive as many requests from tcp threads on a channel as we can
    without blocking. This drains the queue of outstanding requests
    that piled up while we we're doing other work. The future will
    automatically run again when there's a new message on the channel.

 1. Submit the current batch to the kernel with `io_submit`.

 1. Print some stats, such as how many times we poll per second, how
    many requests are handled per poll, etc.

One very interesting aspect of this implementation is that as the
system gets busier, the batch size will go up and the number of
syscalls goes down. In practice this means that if we have one client
issuing a single request, it will be submitted to the kernel
immediately, paying the full price of the syscall. If we have many
clients issuing many requests, all those requests that arrive at the
same time will be submitted together, sharing the cost of the
syscall. Since we always submit as fast as we can, the latency stays
very low. This is a perfect example of the benefits of public
transportation vs. everybody having their own car. Read
about
[Tarantool][https://medium.com/@denisanikin/asynchronous-processing-with-in-memory-databases-or-how-to-handle-one-million-transactions-per-36a4c01fc4e4] to
learn more.


Some interesting projects to check out:

 * https://github.com/tokio-rs/tokio-core
 * https://github.com/knutin/libaio-rust.git
 * https://github.com/knutin/eventfd-rust.git


## How to run the benchmarks

Install Rust, Erlang and `libaio1`.

Generate 1000 keys, where the size of each item is randomly
distributed between 1024 and 4096:

```
cargo run --release --bin mk_data -- --num-cookies=1000 --path=/mnt/data/ --min-size 1024 --max-size 4096
```

Run the server. It will listen on port 12345:
```
cargo run --release --bin run --  --path /mnt/data/ --num-tcp-threads 2 --num-aio-threads 1 --max-io-depth 512  ```
```

To run the benchmarking client, first copy `/mnt/data/protostore.toc.*`
from the server to to your client machine, then:

```
erlc bin/bear.erl

bin/benchmark.erl 127.0.0.1 12345 10 60 1 /mnt/data/protostore
```

`10 60 1` means run 10 clients for 60 seconds with maximum `1`
inflight request at a time. Obviously you have to use the IP of the
actual server if you're running it on a different machine. There's
some benefit to having multiple outstanding requests, but the server
does not currently take full advantage of this opportunity for
parallelism.

There's also a client which sends write requests. It writes random
data of same size as the value currently stored. The parameters are
the same as above.

```
bin/insert.erl 127.0.0.1 12345 50 300 /mnt/data/
```
