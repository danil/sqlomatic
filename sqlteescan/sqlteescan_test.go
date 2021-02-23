// Copyright 2021 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlteescan_test

import (
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/danil/sqltee/sqlteescan"
)

var testFile = func() string { _, f, _, _ := runtime.Caller(0); return f }()

func line() int { _, _, l, _ := runtime.Caller(1); return l }

var ValueStringTestCases = []struct {
	name      string
	in        interface{}
	expected  string
	line      int
	benchmark bool
}{
	{
		name:     "int",
		line:     line(),
		in:       int(1),
		expected: "1",
	},
	{
		name:     "int32",
		line:     line(),
		in:       int32(2),
		expected: "2",
	},
	{
		name:     "int64",
		line:     line(),
		in:       int64(3),
		expected: "3",
	},
	{
		name:     "float32",
		line:     line(),
		in:       float32(4.1),
		expected: "4.1",
	},
	{
		name:     "float64",
		line:     line(),
		in:       float64(5.2),
		expected: "5.2",
	},
	{
		name:     "int pointer",
		line:     line(),
		in:       func() *int { i := 6; return &i }(),
		expected: "6",
	},
	{
		name:     "int nil pointer",
		line:     line(),
		in:       func() *int { return nil }(),
		expected: "NULL",
	},
	{
		name:     "int32 pointer",
		line:     line(),
		in:       func() *int32 { var i int32 = 7; return &i }(),
		expected: "7",
	},
	{
		name:     "int32 nil pointer",
		line:     line(),
		in:       func() *int32 { return nil }(),
		expected: "NULL",
	},
	{
		name:     "float32 pointer",
		line:     line(),
		in:       func() *float32 { var i float32 = 8.3; return &i }(),
		expected: "8.3",
	},
	{
		name:     "float32 nil pointer",
		line:     line(),
		in:       func() *float32 { return nil }(),
		expected: "NULL",
	},
	{
		name:     "float64 pointer",
		line:     line(),
		in:       func() *float64 { var i float64 = 9.4; return &i }(),
		expected: "9.4",
	},
	{
		name:     "float64 nil pointer",
		line:     line(),
		in:       func() *float64 { return nil }(),
		expected: "NULL",
	},
	{
		name:     "boolean",
		line:     line(),
		in:       true,
		expected: "TRUE",
	},
	{
		name:     "boolean pointer",
		line:     line(),
		in:       func() *bool { return nil }(),
		expected: "NULL",
	},
	{
		name:     "byte slice",
		line:     line(),
		in:       []byte("foo"),
		expected: "E'\\\\x666f6f'",
	},
	{
		name:     "byte slice pointer",
		line:     line(),
		in:       func() *[]byte { return nil }(),
		expected: "NULL",
	},
	{
		name:     "string",
		line:     line(),
		in:       "foo",
		expected: "'foo'",
	},
	{
		name:     "string pointer",
		line:     line(),
		in:       func() *string { return nil }(),
		expected: "NULL",
	},
	{
		name:     "time",
		line:     line(),
		in:       time.Date(2020, time.November, 21, 13, 56, 42, 0, time.UTC),
		expected: "'2020-11-21T13:56:42Z'",
	},
	{
		name:     "time pointer",
		line:     line(),
		in:       func() *time.Time { t := time.Date(2020, time.November, 21, 13, 56, 42, 0, time.UTC); return &t }(),
		expected: "'2020-11-21T13:56:42Z'",
	},
	{
		name:     "time nil pointer",
		line:     line(),
		in:       func() *time.Time { return nil }(),
		expected: "NULL",
	},
}

func TestValueString(t *testing.T) {
	for _, tc := range ValueStringTestCases {
		tc := tc
		t.Run(fmt.Sprintf("%s:%s", tc.name, strconv.Itoa(tc.line)), func(t *testing.T) {
			t.Parallel()
			linkToExample := fmt.Sprintf("%s:%d", testFile, tc.line)

			s, err := sqlteescan.ValueString(tc.in)
			if err != nil {
				t.Fatalf("unexpected error: %s %s", err, linkToExample)
			}

			if s != tc.expected {
				t.Errorf("unexpected interpolation, expected: %q, recieved: %q %s", tc.expected, s, linkToExample)
			}
		})
	}
}
