// Copyright 2021 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlteegob

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/gob"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/danil/sqltee"
	"github.com/danil/sqltee/sqlteescan"
)

type Gob struct {
	Writer      io.Writer           // destination for output
	Topic       string              // prefix for all logs
	Placeholder string              // if not blank then used as explicit placeholder instead of placeholder from parameters
	NewTimer    func() sqltee.Timer // retrurs a timer that measures a query execution time
}

func (g Gob) DriverOpen(d time.Duration, derr error) {
	g.error("driver-open", d, derr)
}

func (g Gob) ConnPrepare(d time.Duration, query string, derr error) {
	g.query("conn-prepare", d, query, derr)
}

func (g Gob) ConnClose(d time.Duration, derr error) {
	g.error("conn-close", d, derr)
}

func (g Gob) ConnBegin(d time.Duration, derr error) {
	g.error("conn-begin", d, derr)
}

var bufPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

func (g Gob) ConnBeginTx(_ context.Context, d time.Duration, opts driver.TxOptions, derr error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	defer func() { io.Copy(g.Writer, newReader(d, buf.Bytes())) }()

	_, err := buf.Write([]byte(fmt.Sprintf("%s %s %s", g.Topic, "conn-begin-tx", d)))
	if err != nil {
		return
	}

	if derr != nil { // && derr != driver.ErrSkip {
		_, err = buf.Write([]byte(fmt.Sprintf(" error: %v", derr)))
		if err != nil {
			return
		}
	}

	if (opts != driver.TxOptions{}) {
		_, err = buf.Write([]byte(fmt.Sprintf(" opts: %+v", opts)))
		if err != nil {
			return
		}
	}
}

func (g Gob) ConnPrepareContext(_ context.Context, d time.Duration, query string, derr error) {
	g.query("conn-prepare-context", d, query, derr)
}

func (g Gob) ConnExec(d time.Duration, query string, dargs []driver.Value, res driver.Result, derr error) {
	g.interpolation("conn-exec", d, query, dargs, nil, res, derr)
}

func (g Gob) ConnExecContext(_ context.Context, d time.Duration, query string, nvdargs []driver.NamedValue, res driver.Result, derr error) {
	g.interpolation("conn-exec-context", d, query, nil, nvdargs, res, derr)
}

func (g Gob) ConnPing(d time.Duration, derr error) {
	// g.error("conn-ping", d, derr)
}

func (g Gob) ConnQuery(d time.Duration, query string, dargs []driver.Value, derr error) {
	g.interpolation("conn-query", d, query, dargs, nil, nil, derr)
}

func (g Gob) ConnQueryContext(_ context.Context, d time.Duration, query string, nvdargs []driver.NamedValue, derr error) {
	g.interpolation("conn-query-context", d, query, nil, nvdargs, nil, derr)
}

func (g Gob) StmtClose(d time.Duration, derr error) {
	g.error("stmt-close", d, derr)
}

func (g Gob) StmtExec(d time.Duration, query string, dargs []driver.Value, res driver.Result, derr error) {
	g.interpolation("stmt-exec", d, query, dargs, nil, res, derr)
}

func (g Gob) StmtExecContext(_ context.Context, d time.Duration, query string, nvdargs []driver.NamedValue, res driver.Result, derr error) {
	g.interpolation("stmt-exec-context", d, query, nil, nvdargs, res, derr)
}

func (g Gob) StmtQuery(d time.Duration, query string, dargs []driver.Value, derr error) {
	g.interpolation("stmt-query", d, query, dargs, nil, nil, derr)
}

func (g Gob) StmtQueryContext(_ context.Context, d time.Duration, query string, nvdargs []driver.NamedValue, derr error) {
	g.interpolation("stmt-query-context", d, query, nil, nvdargs, nil, derr)
}

func (g Gob) RowsNext(d time.Duration, dest []driver.Value, derr error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	defer func() { io.Copy(g.Writer, newReader(d, buf.Bytes())) }()

	_, err := buf.Write([]byte(fmt.Sprintf("%s %s %s", g.Topic, "rows-next", d)))
	if err != nil {
		return
	}

	if derr != nil { // && derr != driver.ErrSkip {
		_, err = buf.Write([]byte(fmt.Sprintf(" error: %v", derr)))
		if err != nil {
			return
		}
	}

	if len(dest) != 0 {
		_, err = buf.Write([]byte(fmt.Sprintf(" dest: %+v", dest)))
		if err != nil {
			return
		}
	}
}

func (g Gob) TxCommit(d time.Duration, derr error) {
	g.error("tx-commit", d, derr)
}

func (g Gob) TxRollback(d time.Duration, derr error) {
	g.error("tx-rollback", d, derr)
}

func (g Gob) Timer() sqltee.Timer {
	return g.NewTimer()
}

// error is a log function of the sql driver errors.
func (g Gob) error(topic string, d time.Duration, derr error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	defer func() { io.Copy(g.Writer, newReader(d, buf.Bytes())) }()

	_, err := buf.Write([]byte(fmt.Sprintf("%s %s %s", g.Topic, topic, d)))
	if err != nil {
		return
	}

	if derr != nil { // && derr != driver.ErrSkip {
		_, err = buf.Write([]byte(fmt.Sprintf(" error: %v", derr)))
		if err != nil {
			return
		}
	}
}

// query is a log function of the sql queries without parameters.
func (g Gob) query(topic string, d time.Duration, query string, derr error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	defer func() { io.Copy(g.Writer, newReader(d, buf.Bytes())) }()

	_, err := buf.Write([]byte(fmt.Sprintf("%s %s %s", g.Topic, topic, d)))
	if err != nil {
		return
	}

	if derr != nil { // && derr != driver.ErrSkip {
		_, err = buf.Write([]byte(fmt.Sprintf(" error: %v", derr)))
		if err != nil {
			return
		}
	}

	if query != "" {
		_, err = buf.Write([]byte(fmt.Sprintf(" query: %s", query)))
		if err != nil {
			return
		}
	}
}

// interpolation is a log function of the sql query interpolations or queries with parameters.
func (g Gob) interpolation(topic string, d time.Duration, query string, dargs []driver.Value, nvdargs []driver.NamedValue, res driver.Result, derr error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	defer func() { io.Copy(g.Writer, newReader(d, buf.Bytes())) }()

	_, err := buf.Write([]byte(fmt.Sprintf("%s %s %s", g.Topic, topic, d)))
	if err != nil {
		return
	}

	if derr != nil { // && derr != driver.ErrSkip {
		_, err = buf.Write([]byte(fmt.Sprintf(" error: %v", derr)))
		if err != nil {
			return
		}
	}

	var interpolation string

	scan := sqlteescan.GetScanner()
	scan.Values = dargs
	scan.NamedValues = nvdargs
	scan.Reverse = true
	defer sqlteescan.PutScanner(scan)

	for scan.Scan() {
		if interpolation == "" {
			interpolation = query
		}

		placeholder, ordinal, value := scan.Param()
		if placeholder == "" && ordinal != 0 {
			placeholder = fmt.Sprintf("$%d", ordinal)
		}

		if g.Placeholder == "" && placeholder != "" {
			interpolation = strings.Replace(interpolation, placeholder, value, -1)

		} else {
			if g.Placeholder != "" {
				placeholder = g.Placeholder
			} else if placeholder == "" {
				placeholder = "?"
			}

			i := strings.LastIndex(interpolation, placeholder)
			if i != -1 {
				interpolation = interpolation[:i] + string(value) + interpolation[i+1:]
			}
		}

		if interpolation == query {
			interpolation = ""
			break
		}
	}

	err = scan.Err()
	if err != nil {
		interpolation = ""
		_, err = buf.Write([]byte(fmt.Sprintf(" parameters scan error: %s", err)))
		if err != nil {
			return
		}
	}

	if interpolation != "" {
		_, err = buf.Write([]byte(fmt.Sprintf(" query interpolation: %s", interpolation)))
		if err != nil {
			return
		}
	} else if query != "" {
		_, err = buf.Write([]byte(fmt.Sprintf(" query: %s", query)))
		if err != nil {
			return
		}
	}

	if interpolation == "" {
		if len(dargs) != 0 {
			_, err = buf.Write([]byte(fmt.Sprintf(" args: %+v", dargs)))
			if err != nil {
				return
			}
		} else if len(nvdargs) != 0 {
			_, err = buf.Write([]byte(fmt.Sprintf(" args: %+v", nvdargs)))
			if err != nil {
				return
			}
		}
	}

	if res != nil {
		if id, err := res.LastInsertId(); err == nil && id != 0 {
			_, err = buf.Write([]byte(fmt.Sprintf(" last-insert-id: %s", strconv.FormatInt(id, 10))))
			if err != nil {
				return
			}
		}

		if n, err := res.RowsAffected(); err == nil && n != 0 {
			_, err = buf.Write([]byte(fmt.Sprintf(" rows-affected: %s", strconv.FormatInt(n, 10))))
			if err != nil {
				return
			}
		}
	}
}

type bin struct {
	Duration    time.Duration
	Description []byte
}

var binPool = sync.Pool{New: func() interface{} { return new(bin) }}

func newReader(d time.Duration, desc []byte) io.Reader {
	b := binPool.Get().(*bin)
	b.Duration = d
	b.Description = append(b.Description[:0], desc...)
	return reader{binary: b}
}

type reader struct {
	buf    *bytes.Buffer // Buffer for reading.
	binary *bin          // Source for reading.
	done   bool          // Read has finished.
}

func (r reader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF

	} else if r.buf == nil {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		enc := gob.NewEncoder(buf)

		err := enc.Encode(*r.binary)
		binPool.Put(r.binary)
		if err != nil {
			return 0, err
		}

		r.buf = buf
	}

	n, err := r.buf.Read(p)
	if err == io.EOF {
		r.done = true
		bufPool.Put(r.buf)
		r.buf = nil
	}

	return n, err
}
