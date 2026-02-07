package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/kr/logfmt"
	"github.com/marcboeker/go-duckdb/v2"
	"github.com/minio/simdjson-go"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v3"
)

var syscallCmd = &cli.Command{
	Name:  "syscall",
	Usage: "System call tracing commands",
	Commands: []*cli.Command{
		syscallCountCmd,
		syscallRawCmd,
	},
}

// SyscallCountEvent holds aggregated syscall counts by name.
type SyscallCountEvent struct {
	Counts map[string]int64
}

// NewSyscallCountEvent creates a new SyscallCountEvent.
func NewSyscallCountEvent() *SyscallCountEvent {
	return &SyscallCountEvent{Counts: make(map[string]int64)}
}

// Add accumulates counts from another SyscallCountEvent.
func (e *SyscallCountEvent) Add(other *SyscallCountEvent) {
	for k, v := range other.Counts {
		e.Counts[k] += v
	}
}

// Total returns the sum of all operation counts.
func (e *SyscallCountEvent) Total() int64 {
	var total int64
	for _, v := range e.Counts {
		total += v
	}
	return total
}

// Fill populates the event from simdjson data.
func (e *SyscallCountEvent) Fill(el *simdjson.Element) error {
	var err error
	var rootEl *simdjson.Element
	rootEl, err = el.Iter.FindElement(rootEl, "@")
	if err != nil {
		return errors.Wrap(err, "failed to find '@' element")
	}
	var obj *simdjson.Object
	obj, err = rootEl.Iter.Object(obj)
	if err != nil {
		return errors.Wrap(err, "failed to get object from '@' element")
	}
	elements, err := obj.Parse(nil)
	if err != nil {
		return errors.Wrap(err, "failed to parse object elements")
	}
	for _, m := range elements.Elements {
		var value int64
		value, err = m.Iter.Int()
		if err != nil {
			log.Warn().Str("field", m.Name).Err(err).Msg("Failed to parse field as int, skipping")
			continue
		}
		e.Counts[m.Name] = value
	}
	return nil
}

// SortedKeys returns syscall names sorted by count descending.
func (e *SyscallCountEvent) SortedKeys() []string {
	keys := make([]string, 0, len(e.Counts))
	for k := range e.Counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return e.Counts[keys[i]] > e.Counts[keys[j]]
	})
	return keys
}

func printSyscallEvent(e *SyscallCountEvent, format string, intervalCount int) {
	switch format {
	case "json":
		output := struct {
			Counts    map[string]int64 `json:"counts"`
			Intervals int              `json:"intervals"`
			Total     int64            `json:"total"`
		}{
			Counts:    e.Counts,
			Intervals: intervalCount,
			Total:     e.Total(),
		}
		data, _ := json.Marshal(output)
		fmt.Println(string(data))

	case "csv":
		w := csv.NewWriter(os.Stdout)
		_ = w.Write([]string{"Syscall", "Count"})
		for _, name := range e.SortedKeys() {
			_ = w.Write([]string{name, fmt.Sprintf("%d", e.Counts[name])})
		}
		_ = w.Write([]string{"total", fmt.Sprintf("%d", e.Total())})
		w.Flush()

	default: // table
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "Syscall\tCount")
		_, _ = fmt.Fprintln(tw, "-------\t-----")
		for _, name := range e.SortedKeys() {
			_, _ = fmt.Fprintf(tw, "%s\t%d\n", name, e.Counts[name])
		}
		_, _ = fmt.Fprintln(tw, "-------\t-----")
		_, _ = fmt.Fprintf(tw, "Total\t%d\n", e.Total())
		_, _ = fmt.Fprintf(tw, "Intervals\t%d\n", intervalCount)
		_ = tw.Flush()
	}
}

var syscallCountCmd = &cli.Command{
	Name:  "count",
	Usage: "Aggregate system call counts",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "input",
			Aliases: []string{"i"},
			Value:   "-",
			Usage:   "input file (- for stdin)",
		},
		&cli.StringFlag{
			Name:  "format",
			Value: "table",
			Usage: "output format: table, json, csv",
		},
		&cli.BoolFlag{
			Name:  "live",
			Usage: "live mode: print each interval as it arrives",
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		var r io.Reader
		input := command.String("input")
		if input == "-" {
			r = os.Stdin
		} else {
			f, err := os.Open(input)
			if err != nil {
				return errors.Wrap(err, "open input")
			}
			defer func() { _ = f.Close() }()
			r = f
		}

		format := command.String("format")
		live := command.Bool("live")

		if err := ValidateFormat(format); err != nil {
			return err
		}

		totalEvent := NewSyscallCountEvent()
		var intervalCount int

		parser := &NDJSONParser{}
		err := parser.ParseStream(r, func(msgType string, data *simdjson.Element) error {
			switch msgType {
			case "map":
				event := NewSyscallCountEvent()
				if err := event.Fill(data); err != nil {
					return errors.Wrap(err, "failed to fill event from map data")
				}
				intervalCount++
				totalEvent.Add(event)

				if live {
					printSyscallEvent(event, format, intervalCount)
				}
			default:
				log.Warn().Str("type", msgType).Msg("Unknown message type, skipping")
			}
			return nil
		})
		if err != nil {
			return err
		}

		if !live {
			printSyscallEvent(totalEvent, format, intervalCount)
		} else if intervalCount > 1 {
			fmt.Println("\n--- Total ---")
			printSyscallEvent(totalEvent, format, intervalCount)
		}

		return nil
	},
}

// syscallRawEvent holds a single syscall event.
type syscallRawEvent struct {
	Timestamp   uint64
	Pid         uint64
	Tid         uint64
	Comm        string
	SyscallNr   uint64
	SyscallName string
	Arg0        uint64
	Arg1        uint64
	Arg2        uint64
	Arg3        uint64
	Arg4        uint64
	Arg5        uint64
	ReturnValue int64
}

var ErrUnknownSyscallField = errors.New("unknown syscall field")

func (e *syscallRawEvent) HandleLogfmt(key []byte, val []byte) (err error) {
	k := string(key)
	v := string(val)
	switch k {
	case "ts":
		e.Timestamp, err = strconv.ParseUint(v, 10, 64)
	case "pid":
		e.Pid, err = strconv.ParseUint(v, 10, 64)
	case "tid":
		e.Tid, err = strconv.ParseUint(v, 10, 64)
	case "comm":
		e.Comm = strings.Trim(v, "'\"")
	case "nr":
		e.SyscallNr, err = strconv.ParseUint(v, 10, 64)
	case "name":
		e.SyscallName = v
	case "arg0":
		e.Arg0, err = strconv.ParseUint(v, 0, 64)
	case "arg1":
		e.Arg1, err = strconv.ParseUint(v, 0, 64)
	case "arg2":
		e.Arg2, err = strconv.ParseUint(v, 0, 64)
	case "arg3":
		e.Arg3, err = strconv.ParseUint(v, 0, 64)
	case "arg4":
		e.Arg4, err = strconv.ParseUint(v, 0, 64)
	case "arg5":
		e.Arg5, err = strconv.ParseUint(v, 0, 64)
	case "ret":
		e.ReturnValue, err = strconv.ParseInt(v, 10, 64)
	default:
		err = ErrUnknownSyscallField
	}
	return
}

const createSyscallTableSQL = `CREATE TABLE IF NOT EXISTS %s (
	Ts UBIGINT,
	Pid UBIGINT,
	Tid UBIGINT,
	Comm STRING,
	SyscallNr UBIGINT,
	SyscallName STRING,
	Arg0 UBIGINT,
	Arg1 UBIGINT,
	Arg2 UBIGINT,
	Arg3 UBIGINT,
	Arg4 UBIGINT,
	Arg5 UBIGINT,
	ReturnValue BIGINT)`

const dropSyscallTableSQL = `DROP TABLE IF EXISTS %s`

type syscallAppendRowFn = func(e *syscallRawEvent) error

func syscallJSONParseThenAppend(r io.Reader, appendRow syscallAppendRowFn) error {
	parser := &NDJSONParser{}
	return parser.ParseStream(r, func(msgType string, data *simdjson.Element) error {
		switch msgType {
		case "printf":
			buf, err := data.Iter.StringBytes()
			if err != nil {
				return errors.Wrap(err, "failed to get 'printf' data as string")
			}
			var e syscallRawEvent
			err = logfmt.Unmarshal(buf, &e)
			if err != nil {
				return errors.Wrap(err, "failed to unmarshal logfmt data")
			}
			return appendRow(&e)
		default:
			log.Warn().Str("type", msgType).Msg("Unknown message type, skipping")
		}
		return nil
	})
}

var syscallRawCmd = &cli.Command{
	Name:  "raw",
	Usage: "Write raw syscall events to DuckDB",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "input",
			Aliases: []string{"i"},
			Value:   "-",
			Usage:   "input file (- for stdin)",
		},
		&cli.StringFlag{
			Name:     "dsn",
			Required: true,
			Usage:    "DuckDB connection string",
		},
		&cli.StringFlag{
			Name:     "table",
			Required: true,
			Usage:    "target table name",
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		dsn := command.String("dsn")
		tableName := command.String("table")

		connector, err := duckdb.NewConnector(dsn, nil)
		if err != nil {
			return err
		}

		conn, err := connector.Connect(ctx)
		if err != nil {
			return err
		}

		db := sql.OpenDB(connector)
		defer func() { _ = db.Close() }()

		_, err = db.Exec(fmt.Sprintf(dropSyscallTableSQL, tableName))
		if err != nil {
			return err
		}

		_, err = db.Exec(fmt.Sprintf(createSyscallTableSQL, tableName))
		if err != nil {
			return err
		}

		appender, err := duckdb.NewAppenderFromConn(conn, "", tableName)
		if err != nil {
			return err
		}
		defer func() { _ = appender.Close() }()

		var r io.Reader
		input := command.String("input")
		if input == "-" {
			r = os.Stdin
		} else {
			f, err := os.Open(input)
			if err != nil {
				return errors.Wrap(err, "open input")
			}
			defer func() { _ = f.Close() }()
			r = f
		}

		return syscallJSONParseThenAppend(r, func(e *syscallRawEvent) error {
			return appender.AppendRow(e.Timestamp, e.Pid, e.Tid, e.Comm,
				e.SyscallNr, e.SyscallName, e.Arg0, e.Arg1, e.Arg2,
				e.Arg3, e.Arg4, e.Arg5, e.ReturnValue)
		})
	},
}
