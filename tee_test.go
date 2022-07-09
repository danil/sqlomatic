// Copyright 2022 The Go Authors. All rights reserved.
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

		_ driver.Pinger = &connection{}
		// Test sqltee.connection implements the driver.Execer interface
		_ driver.Execer = &connection{}
		// Test sqltee.connection implements the driver.ExecerContext interface
		_ driver.ExecerContext = &connection{}
		// Test sqltee.connection implements the driver.Queryer interface
		_ driver.Queryer = &connection{}
		// Test sqltee.connection implements the driver.QueryerContext interface
		_ driver.QueryerContext = &connection{}
		// Test sqltee.connection implements the driver.Conn interface
		_ driver.Conn = &connection{}

		// Test sqltee.connection implements the driver.ConnPrepareContext interface
		_ driver.ConnPrepareContext = &connection{}

		// Test sqltee.connection implements the driver.ConnBeginTx interface
		_ driver.ConnBeginTx = &connection{}

		// Test sqltee.connection implements the driver.SessionResetter interface
		_ driver.SessionResetter = &connection{}

		// Test sqltee.logResult implements the driver.Result interface
		_ driver.Result = &result{}

		// Test sqltee.stmtLog implements the driver.Stmt interface
		_ driver.Stmt = &statement{}
		// Test sqltee.statement implements the driver.StmtExecContext interface
		_ driver.StmtExecContext = &statement{}
		// Test sqltee.statement implements the driver.StmtQueryContext interface
		_ driver.StmtQueryContext = &statement{}

		// FIXME: driver.NamedValueChecker
		// FIXME: driver.ColumnConverter

		// Test sqltee.logRows implements the driver.Rows interface
		_ driver.Rows = &rowsIterator{}

		// FIXME: driver.RowsNextResultSet
		// FIXME: driver.RowsColumnTypeScanType
		// FIXME: driver.RowsColumnTypeDatabaseTypeName
		// FIXME: driver.RowsColumnTypeLength
		// FIXME: driver.RowsColumnTypeNullable
		// FIXME: driver.RowsColumnTypePrecisionScale

		// Test sqltee.logTx implements the driver.Tx interface
		_ driver.Tx = &transaction{}
	)
}
