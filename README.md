# sqltee

[![Build Status](https://cloud.drone.io/api/badges/danil/sqltee/status.svg)](https://cloud.drone.io/danil/sqltee)
[![Go Reference](https://pkg.go.dev/badge/github.com/danil/sqltee.svg)](https://pkg.go.dev/github.com/danil/sqltee)

SQL [database/sql/driver][] wrapper, execution time logger,
query interpolator and logger, arguments logger (values, named values,
transaction options) for Go.

Source files are distributed under the BSD-style license.

[database/sql/driver]: https://golang.org/pkg/database/sql/driver

## About

The software is considered to be at a alpha level of readiness -
its extremely slow and allocates a lots of memory)

## Benchmark

```sh
$ go test -run ^NOTHING -bench BenchmarkGob\$
goos: linux
goarch: amd64
pkg: github.com/danil/sqltee/examples/teegob
cpu: 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz
BenchmarkGob/gob_test.go:53/query_from_existing_table-8                 2379        510046 ns/op
PASS
ok      github.com/danil/sqltee/examples/teegob 1.270s
```
