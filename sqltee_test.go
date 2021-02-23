// Copyright 2021 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltee

import (
	"database/sql/driver"
	"testing"
)

func TestLogFuncSQLOpenDB(_ *testing.T) {
	var (
		// Test sqltee.Driver implements the driver.Driver interface
		_ driver.Driver = &Driver{}
		// Test sqltee.Driver implements the driver.DriverContext interface
		_ driver.DriverContext = &Driver{}
		// Test sqltee.Connector implements the driver.Connector interface

		_ driver.Connector = &Connector{}
		// Test sqltee.connLog implements the driver.Pinger interface

		_ driver.Pinger = &connLog{}
		// Test sqltee.connLog implements the driver.Execer interface
		_ driver.Execer = &connLog{}
		// Test sqltee.connLog implements the driver.ExecerContext interface
		_ driver.ExecerContext = &connLog{}
		// Test sqltee.connLog implements the driver.Queryer interface
		_ driver.Queryer = &connLog{}
		// Test sqltee.connLog implements the driver.QueryerContext interface
		_ driver.QueryerContext = &connLog{}
		// Test sqltee.connLog implements the driver.Conn interface
		_ driver.Conn = &connLog{}

		// Test sqltee.connLog implements the driver.ConnPrepareContext interface
		_ driver.ConnPrepareContext = &connLog{}

		// Test sqltee.connLog implements the driver.ConnBeginTx interface
		_ driver.ConnBeginTx = &connLog{}

		// Test sqltee.connLog implements the driver.SessionResetter interface
		_ driver.SessionResetter = &connLog{}

		// Test sqltee.logResult implements the driver.Result interface
		_ driver.Result = &resultLog{}

		// Test sqltee.stmtLog implements the driver.Stmt interface
		_ driver.Stmt = &stmtLog{}
		// Test sqltee.stmtLog implements the driver.StmtExecContext interface
		_ driver.StmtExecContext = &stmtLog{}
		// Test sqltee.stmtLog implements the driver.StmtQueryContext interface
		_ driver.StmtQueryContext = &stmtLog{}

		// FIXME: driver.NamedValueChecker
		// FIXME: driver.ColumnConverter

		// Test sqltee.logRows implements the driver.Rows interface
		_ driver.Rows = &rowsLog{}

		// FIXME: driver.RowsNextResultSet
		// FIXME: driver.RowsColumnTypeScanType
		// FIXME: driver.RowsColumnTypeDatabaseTypeName
		// FIXME: driver.RowsColumnTypeLength
		// FIXME: driver.RowsColumnTypeNullable
		// FIXME: driver.RowsColumnTypePrecisionScale

		// Test sqltee.logTx implements the driver.Tx interface
		_ driver.Tx = &txLog{}
	)
}
