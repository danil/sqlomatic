// Copyright 2021 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltee

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"
)

type Logger interface {
	DriverOpen(d time.Duration, err error)
	ConnPrepare(d time.Duration, query string, err error)
	ConnClose(d time.Duration, err error)
	ConnBegin(d time.Duration, err error)
	ConnBeginTx(ctx context.Context, d time.Duration, opts driver.TxOptions, err error)
	ConnPrepareContext(ctx context.Context, d time.Duration, query string, err error)
	ConnExec(d time.Duration, query string, dargs []driver.Value, res driver.Result, err error)
	ConnExecContext(ctx context.Context, d time.Duration, query string, nvdargs []driver.NamedValue, res driver.Result, err error)
	ConnPing(d time.Duration, err error)
	ConnQuery(d time.Duration, query string, dargs []driver.Value, err error)
	ConnQueryContext(ctx context.Context, d time.Duration, query string, nvdargs []driver.NamedValue, err error)
	StmtClose(d time.Duration, err error)
	StmtExec(d time.Duration, query string, dargs []driver.Value, res driver.Result, err error)
	StmtExecContext(ctx context.Context, d time.Duration, query string, nvdargs []driver.NamedValue, res driver.Result, err error)
	StmtQuery(d time.Duration, query string, dargs []driver.Value, err error)
	StmtQueryContext(cxt context.Context, d time.Duration, query string, nvdargs []driver.NamedValue, err error)
	RowsNext(d time.Duration, dest []driver.Value, err error)
	TxCommit(d time.Duration, err error)
	TxRollback(d time.Duration, err error)
	Timer() Timer
}

type Driver struct {
	Driver driver.Driver
	Logger Logger
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	t := d.Logger.Timer()
	var err error

	defer func() { d.Logger.DriverOpen(t.Stop(), err) }()

	var conn driver.Conn
	conn, err = d.Driver.Open(name)
	if err != nil {
		return nil, err
	}

	return connLog{Logger: d.Logger, conn: conn}, nil
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return Connector{driver: d, name: name}, nil
}

type Connector struct {
	driver *Driver
	name   string
}

func (c Connector) Connect(_ context.Context) (driver.Conn, error) {
	return c.driver.Open(c.name)
}

func (c Connector) Driver() driver.Driver {
	return c.driver
}

type connLog struct {
	Logger
	conn driver.Conn
}

func (c connLog) Prepare(query string) (driver.Stmt, error) {
	t := c.Logger.Timer()
	var err error

	defer func() { c.Logger.ConnPrepare(t.Stop(), query, err) }()

	var stmt driver.Stmt
	stmt, err = c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	return stmtLog{Logger: c.Logger, query: query, stmt: stmt}, nil
}

func (c connLog) Close() error {
	t := c.Logger.Timer()
	err := c.conn.Close()
	c.Logger.ConnClose(t.Stop(), err)
	return err
}

func (c connLog) Begin() (driver.Tx, error) {
	t := c.Logger.Timer()
	var err error

	defer func() { c.Logger.ConnBegin(t.Stop(), err) }()

	var tx driver.Tx
	tx, err = c.conn.Begin()
	if err != nil {
		return nil, err
	}

	return txLog{Logger: c.Logger, tx: tx}, nil
}

func (c connLog) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var (
		tx  driver.Tx
		t   = c.Logger.Timer()
		err error
	)

	defer func() { c.Logger.ConnBeginTx(ctx, t.Stop(), opts, err) }()

	if connBeginTx, ok := c.conn.(driver.ConnBeginTx); ok {
		tx, err = connBeginTx.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}

		return txLog{Logger: c.Logger, ctx: ctx, tx: tx}, nil
	}

	tx, err = c.conn.Begin()
	if err != nil {
		return nil, err
	}

	return txLog{Logger: c.Logger, ctx: ctx, tx: tx}, nil
}

func (c connLog) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	t := c.Logger.Timer()
	var err error

	defer func() { c.Logger.ConnPrepareContext(ctx, t.Stop(), query, err) }()

	if connPrepareCtx, ok := c.conn.(driver.ConnPrepareContext); ok {
		var stmt driver.Stmt
		stmt, err = connPrepareCtx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}

		return stmtLog{Logger: c.Logger, ctx: ctx, stmt: stmt}, nil
	}

	return c.Prepare(query)
}

func (c connLog) Exec(query string, dargs []driver.Value) (driver.Result, error) {
	var (
		t   = c.Logger.Timer()
		res driver.Result
		err error
	)

	defer func() { c.Logger.ConnExec(t.Stop(), query, dargs, res, err) }()

	if execer, ok := c.conn.(driver.Execer); ok {
		res, err = execer.Exec(query, dargs)
		if err != nil {
			return nil, err
		}

		return resultLog{Logger: c.Logger, result: res}, nil
	}

	return nil, driver.ErrSkip
}

func (c connLog) ExecContext(ctx context.Context, query string, nvdargs []driver.NamedValue) (driver.Result, error) {
	var (
		t   = c.Logger.Timer()
		res driver.Result
		err error
	)

	defer func() { c.Logger.ConnExecContext(ctx, t.Stop(), query, nvdargs, res, err) }()

	if execContext, ok := c.conn.(driver.ExecerContext); ok {

		res, err = execContext.ExecContext(ctx, query, nvdargs)
		if err != nil {
			return nil, err
		}

		return resultLog{Logger: c.Logger, ctx: ctx, result: res}, nil
	}

	var dargs []driver.Value
	dargs, err = namedValueToValue(nvdargs)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.Exec(query, dargs)
}

func (c connLog) Ping(ctx context.Context) error {
	t := c.Logger.Timer()
	var err error

	defer func() { c.Logger.ConnPing(t.Stop(), err) }()

	if pinger, ok := c.conn.(driver.Pinger); ok {
		err = pinger.Ping(ctx)
		return err
	}

	return nil
}

func (c connLog) Query(query string, dargs []driver.Value) (driver.Rows, error) {
	t := c.Logger.Timer()
	var err error

	defer func() { c.Logger.ConnQuery(t.Stop(), query, dargs, err) }()

	if queryer, ok := c.conn.(driver.Queryer); ok {
		var rows driver.Rows
		rows, err = queryer.Query(query, dargs)
		if err != nil {
			return nil, err
		}

		return rowsLog{Logger: c.Logger, rows: rows}, nil
	}

	return nil, driver.ErrSkip
}

func (c connLog) QueryContext(ctx context.Context, query string, nvdargs []driver.NamedValue) (driver.Rows, error) {
	t := c.Logger.Timer()
	var err error

	defer func() { c.Logger.ConnQueryContext(ctx, t.Stop(), query, nvdargs, err) }()

	if queryerContext, ok := c.conn.(driver.QueryerContext); ok {
		var rows driver.Rows
		rows, err = queryerContext.QueryContext(ctx, query, nvdargs)
		if err != nil {
			return nil, err
		}

		return rowsLog{Logger: c.Logger, ctx: ctx, rows: rows}, nil
	}

	var dargs []driver.Value
	dargs, err = namedValueToValue(nvdargs)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.Query(query, dargs)
}

func (c connLog) ResetSession(ctx context.Context) error {
	if sessionResetter, ok := c.conn.(driver.SessionResetter); ok {
		return sessionResetter.ResetSession(ctx)
	}

	return driver.ErrSkip
}

type resultLog struct {
	Logger
	ctx    context.Context
	result driver.Result
}

func (r resultLog) LastInsertId() (int64, error) {
	return r.result.LastInsertId()
}

func (r resultLog) RowsAffected() (int64, error) {
	return r.result.RowsAffected()
}

type stmtLog struct {
	Logger
	ctx   context.Context
	query string
	stmt  driver.Stmt
}

func (s stmtLog) Close() error {
	t := s.Logger.Timer()
	err := s.stmt.Close()
	s.Logger.StmtClose(t.Stop(), err)
	return err
}

func (s stmtLog) NumInput() int {
	return s.stmt.NumInput()
}

func (s stmtLog) Exec(dargs []driver.Value) (driver.Result, error) {
	var (
		t   = s.Logger.Timer()
		res driver.Result
		err error
	)

	defer func() { s.Logger.StmtExec(t.Stop(), s.query, dargs, res, err) }()

	res, err = s.stmt.Exec(dargs)
	if err != nil {
		return nil, err
	}

	return resultLog{Logger: s.Logger, ctx: s.ctx, result: res}, nil
}

func (s stmtLog) ExecContext(ctx context.Context, nvdargs []driver.NamedValue) (driver.Result, error) {
	var (
		t   = s.Logger.Timer()
		res driver.Result
		err error
	)

	defer func() { s.Logger.StmtExecContext(ctx, t.Stop(), s.query, nvdargs, res, err) }()

	if stmtExecContext, ok := s.stmt.(driver.StmtExecContext); ok {
		res, err = stmtExecContext.ExecContext(ctx, nvdargs)
		if err != nil {
			return nil, err
		}

		return resultLog{Logger: s.Logger, ctx: ctx, result: res}, nil
	}

	var dargs []driver.Value
	dargs, err = namedValueToValue(nvdargs)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.Exec(dargs)
}

func (s stmtLog) Query(dargs []driver.Value) (driver.Rows, error) {
	t := s.Logger.Timer()
	var err error

	defer func() { s.Logger.StmtQuery(t.Stop(), s.query, dargs, err) }()

	var rows driver.Rows
	rows, err = s.stmt.Query(dargs)
	if err != nil {
		return nil, err
	}

	return rowsLog{Logger: s.Logger, ctx: s.ctx, rows: rows}, nil
}

func (s stmtLog) QueryContext(ctx context.Context, nvdargs []driver.NamedValue) (driver.Rows, error) {
	t := s.Logger.Timer()
	var err error

	defer func() { s.Logger.StmtQueryContext(ctx, t.Stop(), s.query, nvdargs, err) }()

	if stmtQueryContext, ok := s.stmt.(driver.StmtQueryContext); ok {
		var rows driver.Rows
		rows, err = stmtQueryContext.QueryContext(ctx, nvdargs)
		if err != nil {
			return nil, err
		}

		return rowsLog{Logger: s.Logger, ctx: ctx, rows: rows}, nil
	}

	var dargs []driver.Value
	dargs, err = namedValueToValue(nvdargs)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.Query(dargs)
}

type rowsLog struct {
	Logger
	ctx  context.Context
	rows driver.Rows
}

func (r rowsLog) Columns() []string {
	return r.rows.Columns()
}

func (r rowsLog) Close() error {
	return r.rows.Close()
}

func (r rowsLog) Next(dest []driver.Value) error {
	t := r.Logger.Timer()
	err := r.rows.Next(dest)
	r.Logger.RowsNext(t.Stop(), dest, err)
	return err
}

type txLog struct {
	Logger
	ctx context.Context
	tx  driver.Tx
}

func (tx txLog) Commit() error {
	t := tx.Logger.Timer()
	err := tx.tx.Commit()
	tx.Logger.TxCommit(t.Stop(), err)
	return err
}

func (tx txLog) Rollback() error {
	t := tx.Logger.Timer()
	err := tx.tx.Rollback()
	tx.Logger.TxRollback(t.Stop(), err)
	return err
}

// namedValueToValue is a helper function copied from the database/sql package
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}

type Timer interface {
	Stop() time.Duration
}
