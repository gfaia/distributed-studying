# mit6.824

## mr

[assignment link](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

First, make sure the word-count plugin is freshly built:

```sh
go build -race -buildmode=plugin ../../pkg/mrapps/wc.go
```

Start a coordinator:

```sh
go run -race mrcoordinator.go input/pg-*.txt
```

Start a worker:

```sh
go build -race -buildmode=plugin ../../pkg/mrapps/wc.go && go run -race mrworker.go wc.so
```