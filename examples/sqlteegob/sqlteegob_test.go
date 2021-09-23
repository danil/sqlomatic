// Copyright 2021 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlteegob_test

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danil/sqltee"
	"github.com/danil/sqltee/examples/sqlteegob"
	"github.com/danil/sqltee/internal/fakedb"
)

var gobTests = []struct {
	name      string
	line      string
	expected  string
	fetch     func(*sql.DB) error
	benchmark bool
}{
	{
		name: "wipe (truncate)",
		line: line(),
		expected: `{"Duration":42,"Description":"fakedb driver-open 42ns"}
{"Duration":42,"Description":"fakedb conn-exec-context 42ns error: driver: skip fast-path; continue as if unimplemented query: WIPE"}
{"Duration":42,"Description":"fakedb conn-prepare-context 42ns query: WIPE"}
{"Duration":42,"Description":"fakedb stmt-exec-context 42ns"}
{"Duration":42,"Description":"fakedb stmt-close 42ns"}
{"Duration":42,"Description":"fakedb conn-close 42ns"}
`,
		fetch: func(db *sql.DB) error {
			if _, err := db.Exec(`WIPE`); err != nil {
				return fmt.Errorf("%#v %s", err, line())
			}
			return nil
		},
	},
	{
		name: "query from existing table",
		line: line(),
		expected: `{"Duration":42,"Description":"fakedb driver-open 42ns"}
{"Duration":42,"Description":"fakedb conn-exec-context 42ns error: driver: skip fast-path; continue as if unimplemented query: CREATE|tbl|id=int64,name=string"}
{"Duration":42,"Description":"fakedb conn-prepare-context 42ns query: CREATE|tbl|id=int64,name=string"}
{"Duration":42,"Description":"fakedb stmt-exec-context 42ns"}
{"Duration":42,"Description":"fakedb stmt-close 42ns"}
{"Duration":42,"Description":"fakedb conn-exec-context 42ns error: driver: skip fast-path; continue as if unimplemented query interpolation: INSERT|tbl|id=42,name='foo'"}
{"Duration":42,"Description":"fakedb conn-prepare-context 42ns query: INSERT|tbl|id=?,name=?"}
{"Duration":42,"Description":"fakedb stmt-exec-context 42ns args: [{Name: Ordinal:1 Value:42} {Name: Ordinal:2 Value:foo}] rows-affected: 1"}
{"Duration":42,"Description":"fakedb stmt-close 42ns"}
{"Duration":42,"Description":"fakedb conn-query-context 42ns error: driver: skip fast-path; continue as if unimplemented query interpolation: SELECT|tbl|id|name='foo'"}
{"Duration":42,"Description":"fakedb conn-prepare-context 42ns query: SELECT|tbl|id|name=?"}
{"Duration":42,"Description":"fakedb stmt-query-context 42ns args: [{Name: Ordinal:1 Value:foo}]"}
{"Duration":42,"Description":"fakedb rows-next 42ns dest: [42]"}
{"Duration":42,"Description":"fakedb rows-next 42ns error: EOF dest: [42]"}
{"Duration":42,"Description":"fakedb stmt-close 42ns"}
{"Duration":42,"Description":"fakedb conn-exec-context 42ns error: driver: skip fast-path; continue as if unimplemented query: WIPE"}
{"Duration":42,"Description":"fakedb conn-prepare-context 42ns query: WIPE"}
{"Duration":42,"Description":"fakedb stmt-exec-context 42ns"}
{"Duration":42,"Description":"fakedb stmt-close 42ns"}
{"Duration":42,"Description":"fakedb conn-close 42ns"}
`,
		fetch: func(db *sql.DB) error {
			if _, err := db.Exec(`CREATE|tbl|id=int64,name=string`); err != nil {
				return fmt.Errorf("%#v %s", err, line())
			}
			_, err := db.Exec("INSERT|tbl|id=?,name=?", 42, "foo")
			if err != nil {
				return fmt.Errorf("%#v %s", err, line())
			}
			rows, err := db.Query(`SELECT|tbl|id|name=?`, "foo")
			if err != nil {
				return fmt.Errorf("%#v %s", err, line())
			}
			defer rows.Close()
			var ids []int64
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					return fmt.Errorf("%#v %s", err, line())
				}
				ids = append(ids, id)
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("%#v %s", err, line())
			}
			if len(ids) == 0 {
				return sql.ErrNoRows
			} else if len(ids) > 1 {
				return fmt.Errorf("unexpected count, expected: 1, recieved: %d %s", len(ids), line())
			}
			if ids[0] != 42 {
				return fmt.Errorf("unexpected id, expected: 42, recieved: %d %s", ids[0], line())
			}
			_, err = db.Exec("WIPE")
			if err != nil {
				return fmt.Errorf("%#v %s", err, line())
			}
			return nil
		},
		benchmark: true,
	},
	{
		name: "query non existing table",
		line: line(),
		expected: `{"Duration":42,"Description":"fakedb driver-open 42ns"}
{"Duration":42,"Description":"fakedb conn-query-context 42ns error: driver: skip fast-path; continue as if unimplemented query: SELECT|nonexistent_table|nonexistent_column|nonexistent_column=42"}
{"Duration":42,"Description":"fakedb conn-prepare-context 42ns error: fakedb: SELECT on table \"nonexistent_table\" references non-existent column \"nonexistent_column\" query: SELECT|nonexistent_table|nonexistent_column|nonexistent_column=42"}
{"Duration":42,"Description":"fakedb conn-close 42ns"}
`,
		fetch: func(db *sql.DB) error {
			var x int64
			err := db.QueryRow(`SELECT|nonexistent_table|nonexistent_column|nonexistent_column=42`).Scan(&x)
			if fmt.Sprint(errors.New(`fakedb: SELECT on table "nonexistent_table" references non-existent column "nonexistent_column"`)) != fmt.Sprint(err) {
				return fmt.Errorf("%#v %s", err, line())
			}
			return nil
		},
	},
}

func TestGob(t *testing.T) {
	for _, tt := range gobTests {
		tt := tt
		t.Run(tt.name+"/"+tt.line, func(t *testing.T) {
			t.Parallel()

			buf := buffer{}
			tmr := func() sqltee.Timer { return timer{duration: 42 * time.Nanosecond} }
			g := sqlteegob.Gob{Writer: &buf, Topic: "fakedb", Placeholder: "?", NewTimer: tmr}
			drv := &sqltee.Driver{Driver: fakedb.Driver, Logger: g}
			connstr := strings.ReplaceAll(fmt.Sprintf("application_name=TestLog_%s", tt.line), ":", "_")

			c, err := drv.OpenConnector(connstr)
			if err != nil {
				t.Fatalf("driver open connector error: %#v %s", err, tt.line)
			}

			db := sql.OpenDB(c)
			if db.Driver() != drv {
				t.Fatalf("unexpected database sql driver, expected: %#v, received: %#v %s", drv, db.Driver(), tt.line)
			}
			defer db.Close()

			err = tt.fetch(db)
			if err != nil {
				t.Fatalf("test case fetch error: %#v %s", err, tt.line)
			}

			db.Close()

			if buf.String() != tt.expected {
				t.Errorf("unexpected log, expected: %v, recieved: %v %s", tt.expected, buf.String(), tt.line)
			}
		})
	}
}

func BenchmarkGob(b *testing.B) {
	for _, tt := range gobTests {
		if !tt.benchmark {
			continue
		}
		b.Run(tt.line, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				buf := buffer{}

				tmr := func() sqltee.Timer { return timer{duration: 42 * time.Nanosecond} }
				g := sqlteegob.Gob{Writer: &buf, Topic: "fakedb", Placeholder: "?", NewTimer: tmr}
				drv := &sqltee.Driver{Driver: fakedb.Driver, Logger: g}
				connstr := strings.ReplaceAll(fmt.Sprintf("application_name=TestLog_%s", tt.line), ":", "_")

				c, err := drv.OpenConnector(connstr)
				if err != nil {
					fmt.Println(err)
				}

				db := sql.OpenDB(c)
				if db.Driver() != drv {
					fmt.Println(err)
				}
				defer db.Close()

				err = tt.fetch(db)
				if err != nil {
					fmt.Println(err)
				}
			}
		})
	}
}

type buffer struct{ buf bytes.Buffer }

func (buf *buffer) String() string {
	return buf.buf.String()
}

type bin struct {
	Duration    time.Duration
	Description []byte
}

func (b bin) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			Duration    time.Duration
			Description string
		}{
			Duration:    b.Duration,
			Description: string(b.Description),
		},
	)
}

var pool = sync.Pool{New: func() interface{} { return new(bin) }}

func (buf *buffer) Write(p []byte) (int, error) {
	b := pool.Get().(*bin)
	b.Duration = 0
	b.Description = b.Description[:0]
	defer pool.Put(b)

	r := bytes.NewReader(p)
	dec := gob.NewDecoder(r)

	err := dec.Decode(b)
	if err != nil {
		return 0, err
	}

	j, err := json.Marshal(b)
	if err != nil {
		return 0, err
	}

	j = append(j, '\n')

	return buf.buf.Write(j)
}

type timer struct {
	duration time.Duration
}

func (s timer) Stop() time.Duration {
	return s.duration
}

func TestGobSQLOpen(t *testing.T) {
	buf := buffer{}
	tmr := func() sqltee.Timer { return timer{duration: 42 * time.Nanosecond} }
	g := sqlteegob.Gob{Writer: &buf, Topic: "fakedb", Placeholder: "?", NewTimer: tmr}
	drv := &sqltee.Driver{Driver: fakedb.Driver, Logger: g}
	name := `"test log sql open" driver name`

	sql.Register(name, drv)

	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql open error: %#v", err)
	}
	defer db.Close()

	_, err = db.Exec(`WIPE`)
	if err != nil {
		t.Fatalf("db exec error: %#v", err)
	}

	expected := `{"Duration":[0-9]+,"Description":"fakedb driver-open [0-9.nµms]+"}
{"Duration":[0-9]+,"Description":"fakedb conn-exec-context [0-9.nµms]+ error: driver: skip fast-path; continue as if unimplemented query: WIPE"}
{"Duration":[0-9]+,"Description":"fakedb conn-prepare-context [0-9.nµms]+ query: WIPE"}
{"Duration":[0-9]+,"Description":"fakedb stmt-exec-context [0-9.nµms]+"}
{"Duration":[0-9]+,"Description":"fakedb stmt-close [0-9.nµms]+"}
$`

	r, err := regexp.Compile(expected)
	if err != nil {
		t.Fatalf("regexp compile error: %#v", err)
	}
	if !r.MatchString(buf.String()) {
		t.Errorf("unexpected log, expected: %v, recieved: %v", expected, buf.String())
	}
}

func TestGobSQLOpenDB(t *testing.T) {
	buf := buffer{}
	tmr := func() sqltee.Timer { return timer{duration: 42 * time.Nanosecond} }
	g := sqlteegob.Gob{Writer: &buf, Topic: "fakedb", Placeholder: "?", NewTimer: tmr}
	drv := &sqltee.Driver{Driver: fakedb.Driver, Logger: g}

	c, err := drv.OpenConnector("fakedb_sqltee_test_open_db")
	if err != nil {
		t.Fatalf("driver open connector error: %#v", err)
	}

	db := sql.OpenDB(c)
	if db.Driver() != drv {
		t.Fatalf("unexpected database sql driver.Driver, expected: %#v, received: %#v", drv, db.Driver())
	}
	defer db.Close()

	_, err = db.Exec(`WIPE`)
	if err != nil {
		t.Fatalf("db exec error: %#v", err)
	}

	expected := `{"Duration":[0-9]+,"Description":"fakedb driver-open [0-9.nµms]+"}
{"Duration":[0-9]+,"Description":"fakedb conn-exec-context [0-9.nµms]+ error: driver: skip fast-path; continue as if unimplemented query: WIPE"}
{"Duration":[0-9]+,"Description":"fakedb conn-prepare-context [0-9.nµms]+ query: WIPE"}
{"Duration":[0-9]+,"Description":"fakedb stmt-exec-context [0-9.nµms]+"}
{"Duration":[0-9]+,"Description":"fakedb stmt-close [0-9.nµms]+"}
$`

	r, err := regexp.Compile(expected)
	if err != nil {
		t.Fatalf("regexp compile error: %#v", err)
	}
	if !r.MatchString(buf.String()) {
		t.Errorf("unexpected log, expected: %v, recieved: %v", expected, buf.String())
	}
}

// New reports file and line number information about function invocations.
func line() string {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		return fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}

	return "It was not possible to recover file and line number information about function invocations!"
}
