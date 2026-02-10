package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/kr/logfmt"
	"github.com/minio/simdjson-go"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v3"
)

var procCmd = &cli.Command{
	Name:  "proc",
	Usage: "Process tracing commands",
	Commands: []*cli.Command{
		procCountCmd,
		procRawCmd,
	},
}

// ProcCountEvent holds aggregated process operation counts.
type ProcCountEvent struct {
	Exec  int64 `json:"exec"`
	Fork  int64 `json:"fork"`
	Exit  int64 `json:"exit"`
	Clone int64 `json:"clone"`
}

// Add accumulates counts from another ProcCountEvent.
func (e *ProcCountEvent) Add(other *ProcCountEvent) {
	e.Exec += other.Exec
	e.Fork += other.Fork
	e.Exit += other.Exit
	e.Clone += other.Clone
}

// Total returns the sum of all operation counts.
func (e *ProcCountEvent) Total() int64 {
	return e.Exec + e.Fork + e.Exit + e.Clone
}

// Fill populates the event from simdjson data.
func (e *ProcCountEvent) Fill(el *simdjson.Element) error {
	var err error
	var rootEl *simdjson.Element
	rootEl, err = el.Iter.FindElement(rootEl, "@")
	if err != nil {
		return fmt.Errorf("failed to find '@' element: %w", err)
	}
	var obj *simdjson.Object
	obj, err = rootEl.Iter.Object(obj)
	if err != nil {
		return fmt.Errorf("failed to get object from '@' element: %w", err)
	}
	elements, err := obj.Parse(nil)
	if err != nil {
		return fmt.Errorf("failed to parse object elements: %w", err)
	}
	for _, m := range elements.Elements {
		var value int64
		value, err = m.Iter.Int()
		if err != nil {
			log.Warn().Str("field", m.Name).Err(err).Msg("Failed to parse field as int, skipping")
			continue
		}
		switch m.Name {
		case "exec", "sched_process_exec":
			e.Exec = value
		case "fork", "sched_process_fork":
			e.Fork = value
		case "exit", "sched_process_exit":
			e.Exit = value
		case "clone":
			e.Clone = value
		default:
			log.Debug().Str("field", m.Name).Int64("value", value).Msg("Unknown field in map data")
		}
	}
	return nil
}

func printProcEvent(e *ProcCountEvent, format string, intervalCount int) {
	switch format {
	case "json":
		output := struct {
			ProcCountEvent
			Intervals int   `json:"intervals"`
			Total     int64 `json:"total"`
		}{
			ProcCountEvent: *e,
			Intervals:      intervalCount,
			Total:          e.Total(),
		}
		data, _ := json.Marshal(output)
		fmt.Println(string(data))

	case "csv":
		w := csv.NewWriter(os.Stdout)
		_ = w.Write([]string{"Operation", "Count"})
		_ = w.Write([]string{"exec", fmt.Sprintf("%d", e.Exec)})
		_ = w.Write([]string{"fork", fmt.Sprintf("%d", e.Fork)})
		_ = w.Write([]string{"exit", fmt.Sprintf("%d", e.Exit)})
		_ = w.Write([]string{"clone", fmt.Sprintf("%d", e.Clone)})
		_ = w.Write([]string{"total", fmt.Sprintf("%d", e.Total())})
		w.Flush()

	default: // table
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "Operation\tCount")
		_, _ = fmt.Fprintln(tw, "---------\t-----")
		_, _ = fmt.Fprintf(tw, "exec\t%d\n", e.Exec)
		_, _ = fmt.Fprintf(tw, "fork\t%d\n", e.Fork)
		_, _ = fmt.Fprintf(tw, "exit\t%d\n", e.Exit)
		_, _ = fmt.Fprintf(tw, "clone\t%d\n", e.Clone)
		_, _ = fmt.Fprintln(tw, "---------\t-----")
		_, _ = fmt.Fprintf(tw, "Total\t%d\n", e.Total())
		_, _ = fmt.Fprintf(tw, "Intervals\t%d\n", intervalCount)
		_ = tw.Flush()
	}
}

var procCountCmd = &cli.Command{
	Name:  "count",
	Usage: "Aggregate process operation counts",
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
				return fmt.Errorf("open input: %w", err)
			}
			defer func() { _ = f.Close() }()
			r = f
		}

		format := command.String("format")
		live := command.Bool("live")

		if err := ValidateFormat(format); err != nil {
			return err
		}

		var totalEvent ProcCountEvent
		var intervalCount int

		parser := &NDJSONParser{}
		err := parser.ParseStream(r, func(msgType string, data *simdjson.Element) error {
			switch msgType {
			case "map":
				var event ProcCountEvent
				if err := event.Fill(data); err != nil {
					return fmt.Errorf("failed to fill event from map data: %w", err)
				}
				intervalCount++
				totalEvent.Add(&event)

				if live {
					printProcEvent(&event, format, intervalCount)
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
			printProcEvent(&totalEvent, format, intervalCount)
		} else if intervalCount > 1 {
			fmt.Println("\n--- Total ---")
			printProcEvent(&totalEvent, format, intervalCount)
		}

		return nil
	},
}

// procRawEvent holds a single process event.
type procRawEvent struct {
	Timestamp uint64
	Probe     string
	Pid       uint64
	Ppid      uint64
	Tid       uint64
	Comm      string
	Cmdline   string
	ExitCode  int64
}

var procRawEventPool = sync.Pool{
	New: func() any {
		return &procRawEvent{}
	},
}

var ErrUnknownProcField = errors.New("unknown proc field")

func (e *procRawEvent) HandleLogfmt(key []byte, val []byte) (err error) {
	k := string(key)
	v := string(val)
	switch k {
	case "ts":
		e.Timestamp, err = strconv.ParseUint(v, 10, 64)
	case "fn":
		e.Probe = v
	case "pid":
		e.Pid, err = strconv.ParseUint(v, 10, 64)
	case "ppid":
		e.Ppid, err = strconv.ParseUint(v, 10, 64)
	case "tid":
		e.Tid, err = strconv.ParseUint(v, 10, 64)
	case "comm":
		e.Comm = strings.Trim(v, "'\"")
	case "cmdline":
		e.Cmdline = strings.Trim(v, "'\"")
	case "exit_code":
		e.ExitCode, err = strconv.ParseInt(v, 10, 64)
	default:
		err = ErrUnknownProcField
	}
	return
}

const createProcTableSQL = `CREATE TABLE IF NOT EXISTS %s (
	Ts UBIGINT,
	Probe STRING,
	Pid UBIGINT,
	Ppid UBIGINT,
	Tid UBIGINT,
	Comm STRING,
	Cmdline STRING,
	ExitCode BIGINT)`

const dropProcTableSQL = `DROP TABLE IF EXISTS %S`

type procAppendRowFn = func(e *procRawEvent) error

func procJSONParseThenAppend(r io.Reader, appendRow procAppendRowFn) error {
	parser := &NDJSONParser{}
	return parser.ParseStream(r, func(msgType string, data *simdjson.Element) error {
		switch msgType {
		case "printf":
			buf, err := data.Iter.StringBytes()
			if err != nil {
				return fmt.Errorf("failed to get 'printf' data as string: %w", err)
			}
			e := procRawEventPool.Get().(*procRawEvent)
			*e = procRawEvent{}
			err = logfmt.Unmarshal(buf, e)
			if err != nil {
				procRawEventPool.Put(e)
				return fmt.Errorf("failed to unmarshal logfmt data: %w", err)
			}
			err = appendRow(e)
			procRawEventPool.Put(e)
			return err
		default:
			log.Warn().Str("type", msgType).Msg("Unknown message type, skipping")
		}
		return nil
	})
}

var procRawCmd = &cli.Command{
	Name:  "raw",
	Usage: "Write raw process events to DuckDB",
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

		_, err = db.Exec(fmt.Sprintf(dropProcTableSQL, tableName))
		if err != nil {
			return err
		}

		_, err = db.Exec(fmt.Sprintf(createProcTableSQL, tableName))
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
				return fmt.Errorf("open input: %w", err)
			}
			defer func() { _ = f.Close() }()
			r = f
		}

		return procJSONParseThenAppend(r, func(e *procRawEvent) error {
			return appender.AppendRow(e.Timestamp, e.Probe, e.Pid, e.Ppid,
				e.Tid, e.Comm, e.Cmdline, e.ExitCode)
		})
	},
}
