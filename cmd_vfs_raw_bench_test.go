//go:build benchmark

package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/negrel/assert"
	"github.com/pierrec/lz4/v4"
	"github.com/rs/zerolog/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tidwall/wal"
)

const rows = int64(321039)
const walDir = "vfs-raw-bench.wal"
const rocksName = "rocks-bench"
const rawFileName = "raw-file-bench"

type walAppender struct {
	w *wal.Log
	n uint64
}

func appendString(buffer []byte, s string) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(len(s)))
	return append(append(buffer, b...), s...)
}

func makeRowBuffer(e *vfsEvent) []byte {
	buffer := make([]byte, 6*(64/8)+2*(32/4))

	// uint64/int64
	binary.LittleEndian.PutUint64(buffer[0:8], e.Timestamp)
	binary.LittleEndian.PutUint64(buffer[8:16], e.Tid)
	binary.LittleEndian.PutUint64(buffer[16:24], uint64(e.ReturnValue))
	binary.LittleEndian.PutUint64(buffer[24:32], e.Inode)
	binary.LittleEndian.PutUint64(buffer[32:40], e.Offset)
	binary.LittleEndian.PutUint64(buffer[40:48], e.Length)

	// string
	buffer = appendString(buffer, e.Probe)
	buffer = appendString(buffer, e.Path)
	return buffer
}

func (a *walAppender) AppendRow(e *vfsEvent) (err error) {
	a.n++
	buffer := makeRowBuffer(e)
	err = a.w.Write(a.n, buffer)
	return
}

func BenchmarkSimpleParseWithWal(b *testing.B) {
	buffer, err := os.ReadFile(testDataFile)
	if err != nil {
		b.Fatal(err)
	}

	_ = os.RemoveAll(walDir)
	w, err := wal.Open(walDir, &wal.Options{NoSync: true})
	if err != nil {
		b.Fatal(err)
	}
	appender := &walAppender{w: w}

	now := time.Now()
	for n := 0; n < b.N; n++ {
		r := bytes.NewReader(buffer)
		err = simpleParseThenAppend(r, appender.AppendRow)
		if err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(now)
	log.Info().
		Int("n", b.N).
		Int64("elapsed_ns", elapsed.Nanoseconds()).
		Int64("ops", 1000000000/(elapsed.Nanoseconds()/int64(b.N)/rows)).
		Msg("Done in time")
}

type levelAppender struct {
	db *leveldb.DB
	n  uint64
}

var writeOptions = &opt.WriteOptions{}

func (a *levelAppender) AppendRow(e *vfsEvent) (err error) {
	a.n++
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, a.n)
	buffer := makeRowBuffer(e)
	err = a.db.Put(key, buffer, writeOptions)
	return
}

func (a *levelAppender) Close() (err error) {
	err = a.db.Close()
	return
}

type fileAppender struct {
	file *bufio.Writer
}

func (a *fileAppender) AppendRow(e *vfsEvent) (err error) {
	buffer := makeRowBuffer(e)
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(buffer)))
	var n int
	n, err = a.file.Write(append(lenBuf, buffer...))
	assert.True(n == len(buffer)+len(lenBuf))
	return
}

func (a *fileAppender) Close() (err error) {
	err = a.file.Flush()
	return
}

// ops=4385964
// ops=3787878 with lz4
func BenchmarkSimpleParseWithFile(b *testing.B) {
	buffer, err := os.ReadFile(testDataFile)
	if err != nil {
		b.Fatal(err)
	}

	file, err := os.Create(rawFileName)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	w := bufio.NewWriterSize(lz4.NewWriter(file), 128*1024*1024)

	appender := &fileAppender{file: w}
	defer func() { _ = appender.Close() }()

	now := time.Now()
	for n := 0; n < b.N; n++ {
		r := bytes.NewReader(buffer)
		err = simpleParseThenAppend(r, appender.AppendRow)
		if err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(now)
	log.Info().
		Int("n", b.N).
		Int64("elapsed_ns", elapsed.Nanoseconds()).
		Int64("ops", 1000000000/(elapsed.Nanoseconds()/int64(b.N)/rows)).
		Msg("Done in time")
}

func BenchmarkSimpleParseWithLevels(b *testing.B) {
	buffer, err := os.ReadFile(testDataFile)
	if err != nil {
		b.Fatal(err)
	}

	db, err := leveldb.OpenFile(rocksName, nil)
	if err != nil {
		b.Fatal(err)
	}
	appender := &levelAppender{db: db}

	now := time.Now()
	for n := 0; n < b.N; n++ {
		r := bytes.NewReader(buffer)
		err = simpleParseThenAppend(r, appender.AppendRow)
		if err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(now)
	log.Info().
		Int("n", b.N).
		Int64("elapsed_ns", elapsed.Nanoseconds()).
		Int64("ops", 1000000000/(elapsed.Nanoseconds()/int64(b.N)/rows)).
		Msg("Done in time")

	_ = appender.Close()
}

// ops=5076142
func BenchmarkSimpleParseOnly(b *testing.B) {
	buffer, err := os.ReadFile(testDataFile)
	if err != nil {
		b.Fatal(err)
	}

	now := time.Now()

	for n := 0; n < b.N; n++ {
		r := bytes.NewReader(buffer)
		err = simpleParseThenAppend(r, func(*vfsEvent) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}

	elapsed := time.Since(now)
	log.Info().
		Int("n", b.N).
		Int64("elapsed_ns", elapsed.Nanoseconds()).
		Int64("ops", 1000000000/(elapsed.Nanoseconds()/int64(b.N)/rows)).
		Msg("Done in time")
}

// Read from memory and parse only
// ops=3378378
func BenchmarkJsonParseOnly(b *testing.B) {
	buffer, err := os.ReadFile(testDataFile)
	if err != nil {
		b.Fatal(err)
	}

	now := time.Now()

	for n := 0; n < b.N; n++ {
		r := bytes.NewReader(buffer)
		err = jsonParseThenAppend(r, func(*vfsEvent) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}

	elapsed := time.Since(now)
	log.Info().
		Int("n", b.N).
		Int64("elapsed_ns", elapsed.Nanoseconds()).
		Int64("ops", 1000000000/(elapsed.Nanoseconds()/int64(b.N)/rows)).
		Msg("Done in time")
}

// Read from memory and write to DuckDB
// ops=1126126
func BenchmarkReadMemoryWriteDuckDB(b *testing.B) {
	ctx := context.Background()
	dsn := "bench.ddb"
	tableName := "append_bench"

	_ = os.Remove(dsn)
	_ = os.Remove(dsn + ".wal")

	connector, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		b.Fatal(err)
	}

	log.Info().Str("dsn", dsn).Msg("Connecting to db")
	conn, err := connector.Connect(ctx)
	if err != nil {
		b.Fatal(err)
	}

	db := sql.OpenDB(connector)
	log.Info().Msg("DB created")

	_, err = db.Exec(fmt.Sprintf(dropTableSql, tableName))
	if err != nil {
		b.Fatal(err)
	}

	_, err = db.Exec(fmt.Sprintf(createTableSql, tableName))
	if err != nil {
		b.Fatal(err)
	}

	appender, err := duckdb.NewAppenderFromConn(conn, "", tableName)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = appender.Close() }()

	buffer, err := os.ReadFile(testDataFile)
	if err != nil {
		b.Fatal(err)
	}

	now := time.Now()

	for n := 0; n < b.N; n++ {
		r := bytes.NewReader(buffer)
		err = jsonParseThenAppend(r, func(e *vfsEvent) error {
			return appender.AppendRow(e.Timestamp, e.Probe, e.Tid, e.ReturnValue,
				e.Path, e.Inode, e.Offset, e.Length)
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = appender.Flush()
	}

	elapsed := time.Since(now)
	log.Info().
		Int("n", b.N).
		Int64("elapsed_ns", elapsed.Nanoseconds()).
		Int64("ops", 1000000000/(elapsed.Nanoseconds()/int64(b.N)/rows)).
		Msg("Done in time")
}

// ops=1164144
func BenchmarkImportFromBpf(b *testing.B) {
	ctx := context.Background()
	dsn := "bench.ddb"
	tableName := "append_bench"

	_ = os.Remove(dsn)
	_ = os.Remove(dsn + ".wal")

	connector, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		b.Fatal(err)
	}

	log.Info().Str("dsn", dsn).Msg("Connecting to db")
	conn, err := connector.Connect(ctx)
	if err != nil {
		b.Fatal(err)
	}

	db := sql.OpenDB(connector)
	log.Info().Msg("DB created")

	_, err = db.Exec(fmt.Sprintf(dropTableSql, tableName))
	if err != nil {
		b.Fatal(err)
	}

	_, err = db.Exec(fmt.Sprintf(createTableSql, tableName))
	if err != nil {
		b.Fatal(err)
	}

	appender, err := duckdb.NewAppenderFromConn(conn, "", tableName)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = appender.Close() }()

	now := time.Now()

	for n := 0; n < b.N; n++ {
		var r *os.File
		r, err = os.Open("testdata/vfs-raw.ndjson")
		if err != nil {
			b.Fatal(err)
		}

		err = jsonParseThenAppend(r, func(e *vfsEvent) error {
			return appender.AppendRow(e.Timestamp, e.Probe, e.Tid, e.ReturnValue,
				e.Path, e.Inode, e.Offset, e.Length)
		})
		if err != nil {
			b.Fatal(err)
		}

		_ = appender.Flush()
		_ = r.Close()
	}

	elapsed := time.Since(now)
	log.Info().
		Int("n", b.N).
		Int64("elapsed_ns", elapsed.Nanoseconds()).
		Int64("ops", 1000000000/(elapsed.Nanoseconds()/int64(b.N)/rows)).
		Msg("Done in time")
}
