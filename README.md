# `protostore`

I built this little project to learn Rust and understand the
performance of Rust futures and `libaio`. While there's still
performance improvements to be implemented, the performance is very
interesting.


## Architecture

Keys are 16-byte opaque binaries, values can have any length. When the
server starts up it reads a table of contents file which maps a key to
an offset in the data file. This ToC ist stored in Rusts standard
`HashMap`.

When a read request comes in, we find the offset and key length in the
table, then do an asynchronous read using `libaio-rust`. When the read
is complete we can send the response to the client. While the client
can pipeline requests, we currently do not take advantage of possible
parallelism.

This is all run in a single thread using `tokio` and `futures`. The
server can be parallelised by introducing more event loops. There's
also a bit more copying than is strictly necessary that could easily
be eliminated.

Interesting projects to check out:

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
cargo run --release --bin run --  --path=/mnt/data/```
```

To run the benchmarking client, first copy `/mnt/data/protostore.toc`
from the server to to your client machine, then:

```
erlc bin/bear.erl

bin/benchmark.erl 127.0.0.1 12345 10 60 /mnt/data/protostore.toc
```

`10 60` means run 10 clients for 60 seconds. Obviously you have to use
the IP of the actual server if you're running it on a different
machine.

