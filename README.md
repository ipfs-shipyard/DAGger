🗡️ stream-DAGger
=======================

> an elegant slicer for a more civilized age

# ALPHA - REPOSITORY WILL MOVE BEFORE MAY 1ST 2020

`stream-dagger` is a command line tool designed to:

- receive a single `STDIN` input stream
- efficiently split it based on a wide array of tunable criteria
- aggregate and emit detailed statistics about the resulting chunks
- possibly form a Directed Acyclic Graph (`DAG`) from the individual stream parts
- possibly stream the DAG on `STDOUT` in format understood by e.g. [`ipfs`][1]

Further documentation for individual use cases will be available in several days (https://github.com/ipfs/DAGger/issues/5). Until then some basic example snippets:

```
go get -v -u github.com/ipfs-shipyard/DAGger/cmd/stream-dagger
```
```
cat {{somefile}} | stream-dagger --ipfs-add-compatible-command="--cid-version=1"
```
```
find {{somedirectory}} -type f -exec sh -c '
perl -e "print( pack( q{q>}, -s \$ARGV[0]))" {} && cat {}
' \; | stream-dagger --multipart --ipfs-add-compatible-command="--cid-version=1"
```

## Lead Maintainer

[Peter 'ribasushi' Rabbitson](https://github.com/ribasushi)

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)

[1]: https://docs.ipfs.io/introduction/overview/
