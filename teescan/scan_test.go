// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package teescan_test

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/pfmt/sqltee/teescan"
)

func TestValueString(t *testing.T) {
	var tests = []struct {
		name  string
		in    interface{}
		want  string
		line  string
		bench bool // TODO: load testing ~~~~<danil@kutkevich.org>
	}{
		{
			name: "int",
			line: testline(),
			in:   int(1),
			want: "1",
		}, {
			name: "int32",
			line: testline(),
			in:   int32(2),
			want: "2",
		}, {
			name: "int64",
			line: testline(),
			in:   int64(3),
			want: "3",
		}, {
			name: "float32",
			line: testline(),
			in:   float32(4.1),
			want: "4.1",
		}, {
			name: "float64",
			line: testline(),
			in:   float64(5.2),
			want: "5.2",
		}, {
			name: "int pointer",
			line: testline(),
			in:   func() *int { i := 6; return &i }(),
			want: "6",
		}, {
			name: "int nil pointer",
			line: testline(),
			in:   func() *int { return nil }(),
			want: "NULL",
		}, {
			name: "int32 pointer",
			line: testline(),
			in:   func() *int32 { var i int32 = 7; return &i }(),
			want: "7",
		}, {
			name: "int32 nil pointer",
			line: testline(),
			in:   func() *int32 { return nil }(),
			want: "NULL",
		}, {
			name: "float32 pointer",
			line: testline(),
			in:   func() *float32 { var i float32 = 8.3; return &i }(),
			want: "8.3",
		}, {
			name: "float32 nil pointer",
			line: testline(),
			in:   func() *float32 { return nil }(),
			want: "NULL",
		}, {
			name: "float64 pointer",
			line: testline(),
			in:   func() *float64 { var i float64 = 9.4; return &i }(),
			want: "9.4",
		}, {
			name: "float64 nil pointer",
			line: testline(),
			in:   func() *float64 { return nil }(),
			want: "NULL",
		}, {
			name: "boolean",
			line: testline(),
			in:   true,
			want: "TRUE",
		}, {
			name: "boolean pointer",
			line: testline(),
			in:   func() *bool { return nil }(),
			want: "NULL",
		}, {
			name: "byte slice",
			line: testline(),
			in:   []byte("foo"),
			want: "E'\\\\x666f6f'",
		}, {
			name: "byte slice pointer",
			line: testline(),
			in:   func() *[]byte { return nil }(),
			want: "NULL",
		}, {
			name: "string",
			line: testline(),
			in:   "foo",
			want: "'foo'",
		}, {
			name: "string pointer",
			line: testline(),
			in:   func() *string { return nil }(),
			want: "NULL",
		}, {
			name: "time",
			line: testline(),
			in:   time.Date(2020, time.November, 21, 13, 56, 42, 0, time.UTC),
			want: "'2020-11-21T13:56:42Z'",
		}, {
			name: "time pointer",
			line: testline(),
			in:   func() *time.Time { t := time.Date(2020, time.November, 21, 13, 56, 42, 0, time.UTC); return &t }(),
			want: "'2020-11-21T13:56:42Z'",
		}, {
			name: "time nil pointer",
			line: testline(),
			in:   func() *time.Time { return nil }(),
			want: "NULL",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name+"/"+tt.line, func(t *testing.T) {
			t.Parallel()

			s, err := teescan.ValueString(tt.in)
			if err != nil {
				t.Fatalf("\nscan: %s\n test: %s", err, tt.line)
			}

			if s != tt.want {
				t.Errorf("\nwant interpolation %q\n got interpolation: %q\ntest: %s", tt.want, s, tt.line)
			}
		})
	}
}

func testline() string {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		return fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
	return "It was not possible to recover file and line number information about function invocations!"
}
