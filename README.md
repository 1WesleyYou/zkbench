# zkbench

ZooKeeper benchmark in Go

### Build

```bash
go mod tidy
go build
```

On success, it will produce the `zkbench` binary.

### Run

Change `bench.conf` to reflect your settings. There are several
example configs `bench_x` in the directory for reference.

```bash
./zkbench -conf bench.conf
```
