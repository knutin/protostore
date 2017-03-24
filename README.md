# `protostore`

I built this little project to learn Rust and understand the
performance of Rust futures and `libaio`. While there's still
performance improvements to be implemented, the performance is very
interesting.


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

