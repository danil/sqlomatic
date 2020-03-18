package logsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"
)

type Logger interface {
	Log(ctx context.Context, topic string, d time.Duration, query string, dargs []driver.Value, nvdargs []driver.NamedValue, opts driver.TxOptions, res driver.Result, err error)
}

type LogFunc func(ctx context.Context, topic string, d time.Duration, query string, dargs []driver.Value, nvdargs []driver.NamedValue, opts driver.TxOptions, res driver.Result, err error)

func (f LogFunc) Log(ctx context.Context, topic string, d time.Duration, query string, dargs []driver.Value, nvdargs []driver.NamedValue, opts driver.TxOptions, res driver.Result, err error) {
	f(ctx, topic, d, query, dargs, nvdargs, opts, res, err)
}

type Driver struct {
	Logger
	Driver driver.Driver
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	t := time.Now()
	var err error

	defer func() {
		d.Log(context.Background(), "driver-open", time.Since(t), "", nil, nil, driver.TxOptions{}, nil, err)
	}()

	var conn driver.Conn
	conn, err = d.Driver.Open(name)
	if err != nil {
		return nil, err
	}

	return logConn{Logger: d.Logger, conn: conn}, nil
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

type logConn struct {
	Logger
	conn driver.Conn
}

func (c logConn) Prepare(query string) (driver.Stmt, error) {
	t := time.Now()
	var err error

	defer func() {
		c.Log(context.Background(), "conn-prepare", time.Since(t), query, nil, nil, driver.TxOptions{}, nil, err)
	}()

	var stmt driver.Stmt
	stmt, err = c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	return logStmt{Logger: c.Logger, query: query, stmt: stmt}, nil
}

func (c logConn) Close() error {
	t := time.Now()
	err := c.conn.Close()
	c.Log(context.Background(), "conn-close", time.Since(t), "", nil, nil, driver.TxOptions{}, nil, err)
	return err
}

func (c logConn) Begin() (driver.Tx, error) {
	t := time.Now()
	var err error

	defer func() {
		c.Log(context.Background(), "conn-begin", time.Since(t), "", nil, nil, driver.TxOptions{}, nil, err)
	}()

	var tx driver.Tx
	tx, err = c.conn.Begin()
	if err != nil {
		return nil, err
	}

	return logTx{Logger: c.Logger, tx: tx}, nil
}

func (c logConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var (
		tx  driver.Tx
		t   = time.Now()
		err error
	)

	defer func() { c.Log(ctx, "conn-begin-tx", time.Since(t), "", nil, nil, opts, nil, err) }()

	if connBeginTx, ok := c.conn.(driver.ConnBeginTx); ok {
		tx, err = connBeginTx.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}

		return logTx{Logger: c.Logger, ctx: ctx, tx: tx}, nil
	}

	tx, err = c.conn.Begin()
	if err != nil {
		return nil, err
	}

	return logTx{Logger: c.Logger, ctx: ctx, tx: tx}, nil
}

func (c logConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	t := time.Now()
	var err error

	defer func() {
		c.Log(ctx, "conn-prepare-context", time.Since(t), query, nil, nil, driver.TxOptions{}, nil, err)
	}()

	if connPrepareCtx, ok := c.conn.(driver.ConnPrepareContext); ok {
		var stmt driver.Stmt
		stmt, err = connPrepareCtx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}

		return logStmt{Logger: c.Logger, ctx: ctx, stmt: stmt}, nil
	}

	return c.Prepare(query)
}

func (c logConn) Exec(query string, dargs []driver.Value) (driver.Result, error) {
	var (
		t   = time.Now()
		res driver.Result
		err error
	)

	defer func() {
		c.Log(context.Background(), "conn-exec", time.Since(t), query, dargs, nil, driver.TxOptions{}, res, err)
	}()

	if execer, ok := c.conn.(driver.Execer); ok {
		res, err = execer.Exec(query, dargs)
		if err != nil {
			return nil, err
		}

		return logResult{Logger: c.Logger, result: res}, nil
	}

	return nil, driver.ErrSkip
}

func (c logConn) ExecContext(ctx context.Context, query string, nvdargs []driver.NamedValue) (driver.Result, error) {
	var (
		t   = time.Now()
		res driver.Result
		err error
	)

	defer func() {
		c.Log(ctx, "conn-exec-context", time.Since(t), query, nil, nvdargs, driver.TxOptions{}, res, err)
	}()

	if execContext, ok := c.conn.(driver.ExecerContext); ok {

		res, err = execContext.ExecContext(ctx, query, nvdargs)
		if err != nil {
			return nil, err
		}

		return logResult{Logger: c.Logger, ctx: ctx, result: res}, nil
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

func (c logConn) Ping(ctx context.Context) error {
	t := time.Now()
	var err error

	defer func() { c.Log(ctx, "conn-ping", time.Since(t), "", nil, nil, driver.TxOptions{}, nil, err) }()

	if pinger, ok := c.conn.(driver.Pinger); ok {
		err = pinger.Ping(ctx)
		return err
	}

	return nil
}

func (c logConn) Query(query string, dargs []driver.Value) (driver.Rows, error) {
	t := time.Now()
	var err error

	defer func() {
		c.Log(context.Background(), "conn-query", time.Since(t), query, dargs, nil, driver.TxOptions{}, nil, err)
	}()

	if queryer, ok := c.conn.(driver.Queryer); ok {
		var rows driver.Rows
		rows, err = queryer.Query(query, dargs)
		if err != nil {
			return nil, err
		}

		return logRows{Logger: c.Logger, rows: rows}, nil
	}

	return nil, driver.ErrSkip
}

func (c logConn) QueryContext(ctx context.Context, query string, nvdargs []driver.NamedValue) (driver.Rows, error) {
	t := time.Now()
	var err error

	defer func() {
		c.Log(ctx, "conn-query-context", time.Since(t), query, nil, nvdargs, driver.TxOptions{}, nil, err)
	}()

	if queryerContext, ok := c.conn.(driver.QueryerContext); ok {
		var rows driver.Rows
		rows, err = queryerContext.QueryContext(ctx, query, nvdargs)
		if err != nil {
			return nil, err
		}

		return logRows{Logger: c.Logger, ctx: ctx, rows: rows}, nil
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

func (c logConn) ResetSession(ctx context.Context) error {
	if sessionResetter, ok := c.conn.(driver.SessionResetter); ok {
		return sessionResetter.ResetSession(ctx)
	}

	return driver.ErrSkip
}

type logTx struct {
	Logger
	ctx context.Context
	tx  driver.Tx
}

func (tx logTx) Commit() error {
	t := time.Now()
	err := tx.tx.Commit()
	tx.Log(tx.ctx, "tx-commit", time.Since(t), "", nil, nil, driver.TxOptions{}, nil, err)
	return err
}

func (tx logTx) Rollback() error {
	t := time.Now()
	err := tx.tx.Rollback()
	tx.Log(tx.ctx, "tx-rollback", time.Since(t), "", nil, nil, driver.TxOptions{}, nil, err)
	return err
}

type logStmt struct {
	Logger
	ctx   context.Context
	query string
	stmt  driver.Stmt
}

func (s logStmt) Close() error {
	t := time.Now()
	err := s.stmt.Close()
	s.Log(s.ctx, "stmt-close", time.Since(t), "", nil, nil, driver.TxOptions{}, nil, err)
	return err
}

func (s logStmt) NumInput() int {
	return s.stmt.NumInput()
}

func (s logStmt) Exec(dargs []driver.Value) (driver.Result, error) {
	var (
		t   = time.Now()
		res driver.Result
		err error
	)

	defer func() {
		s.Log(s.ctx, "stmt-exec", time.Since(t), s.query, dargs, nil, driver.TxOptions{}, res, err)
	}()

	res, err = s.stmt.Exec(dargs)
	if err != nil {
		return nil, err
	}

	return logResult{Logger: s.Logger, ctx: s.ctx, result: res}, nil
}

func (s logStmt) ExecContext(ctx context.Context, nvdargs []driver.NamedValue) (driver.Result, error) {
	var (
		t   = time.Now()
		res driver.Result
		err error
	)

	defer func() {
		s.Log(ctx, "stmt-exec-context", time.Since(t), s.query, nil, nvdargs, driver.TxOptions{}, res, err)
	}()

	if stmtExecContext, ok := s.stmt.(driver.StmtExecContext); ok {
		res, err = stmtExecContext.ExecContext(ctx, nvdargs)
		if err != nil {
			return nil, err
		}

		return logResult{Logger: s.Logger, ctx: ctx, result: res}, nil
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

func (s logStmt) Query(dargs []driver.Value) (driver.Rows, error) {
	t := time.Now()
	var err error

	defer func() {
		s.Log(s.ctx, "stmt-query", time.Since(t), s.query, dargs, nil, driver.TxOptions{}, nil, err)
	}()

	var rows driver.Rows
	rows, err = s.stmt.Query(dargs)
	if err != nil {
		return nil, err
	}

	return logRows{Logger: s.Logger, ctx: s.ctx, rows: rows}, nil
}

func (s logStmt) QueryContext(ctx context.Context, nvdargs []driver.NamedValue) (driver.Rows, error) {
	t := time.Now()
	var err error

	defer func() {
		s.Log(ctx, "stmt-query-context", time.Since(t), s.query, nil, nvdargs, driver.TxOptions{}, nil, err)
	}()

	if stmtQueryContext, ok := s.stmt.(driver.StmtQueryContext); ok {
		var rows driver.Rows
		rows, err = stmtQueryContext.QueryContext(ctx, nvdargs)
		if err != nil {
			return nil, err
		}

		return logRows{Logger: s.Logger, ctx: ctx, rows: rows}, nil
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

// TODO: implement ColumnConverter()

type logResult struct {
	Logger
	ctx    context.Context
	result driver.Result
}

func (r logResult) LastInsertId() (int64, error) {
	return r.result.LastInsertId()
}

func (r logResult) RowsAffected() (int64, error) {
	return r.result.RowsAffected()
}

type logRows struct {
	Logger
	ctx  context.Context
	rows driver.Rows
}

func (r logRows) Columns() []string {
	return r.rows.Columns()
}

func (r logRows) Close() error {
	return r.rows.Close()
}

func (r logRows) Next(dest []driver.Value) error {
	t := time.Now()
	err := r.rows.Next(dest)
	r.Log(r.ctx, "rows-next", time.Since(t), "", dest, nil, driver.TxOptions{}, nil, err)
	return err
}

// TODO: implement ColumnTypeScanType()
// TODO: implement HasNextResultSet()
// TODO: implement NextResultSet()

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
