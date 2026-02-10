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

var memCmd = &cli.Command{
	Name:  "mem",
	Usage: "Memory tracing commands",
	Commands: []*cli.Command{
		memCountCmd,
		memRawCmd,
	},
}

// MemCountEvent holds aggregated memory operation counts.
type MemCountEvent struct {
	Mmap      int64 `json:"mmap"`
	Munmap    int64 `json:"munmap"`
	Brk       int64 `json:"brk"`
	PageFault int64 `json:"page_fault"`
}

// Add accumulates counts from another MemCountEvent.
func (e *MemCountEvent) Add(other *MemCountEvent) {
	e.Mmap += other.Mmap
	e.Munmap += other.Munmap
	e.Brk += other.Brk
	e.PageFault += other.PageFault
}

// Total returns the sum of all operation counts.
func (e *MemCountEvent) Total() int64 {
	return e.Mmap + e.Munmap + e.Brk + e.PageFault
}

// Fill populates the event from simdjson data.
func (e *MemCountEvent) Fill(el *simdjson.Element) error {
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
		case "mmap", "do_mmap":
			e.Mmap = value
		case "munmap", "do_munmap":
			e.Munmap = value
		case "brk", "do_brk":
			e.Brk = value
		case "page_fault", "handle_mm_fault":
			e.PageFault = value
		default:
			log.Debug().Str("field", m.Name).Int64("value", value).Msg("Unknown field in map data")
		}
	}
	return nil
}

func printMemEvent(e *MemCountEvent, format string, intervalCount int) {
	switch format {
	case "json":
		output := struct {
			MemCountEvent
			Intervals int   `json:"intervals"`
			Total     int64 `json:"total"`
		}{
			MemCountEvent: *e,
			Intervals:     intervalCount,
			Total:         e.Total(),
		}
		data, _ := json.Marshal(output)
		fmt.Println(string(data))

	case "csv":
		w := csv.NewWriter(os.Stdout)
		_ = w.Write([]string{"Operation", "Count"})
		_ = w.Write([]string{"mmap", fmt.Sprintf("%d", e.Mmap)})
		_ = w.Write([]string{"munmap", fmt.Sprintf("%d", e.Munmap)})
		_ = w.Write([]string{"brk", fmt.Sprintf("%d", e.Brk)})
		_ = w.Write([]string{"page_fault", fmt.Sprintf("%d", e.PageFault)})
		_ = w.Write([]string{"total", fmt.Sprintf("%d", e.Total())})
		w.Flush()

	default: // table
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "Operation\tCount")
		_, _ = fmt.Fprintln(tw, "---------\t-----")
		_, _ = fmt.Fprintf(tw, "mmap\t%d\n", e.Mmap)
		_, _ = fmt.Fprintf(tw, "munmap\t%d\n", e.Munmap)
		_, _ = fmt.Fprintf(tw, "brk\t%d\n", e.Brk)
		_, _ = fmt.Fprintf(tw, "page_fault\t%d\n", e.PageFault)
		_, _ = fmt.Fprintln(tw, "---------\t-----")
		_, _ = fmt.Fprintf(tw, "Total\t%d\n", e.Total())
		_, _ = fmt.Fprintf(tw, "Intervals\t%d\n", intervalCount)
		_ = tw.Flush()
	}
}

var memCountCmd = &cli.Command{
	Name:  "count",
	Usage: "Aggregate memory operation counts",
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

		var totalEvent MemCountEvent
		var intervalCount int

		parser := &NDJSONParser{}
		err := parser.ParseStream(r, func(msgType string, data *simdjson.Element) error {
			switch msgType {
			case "map":
				var event MemCountEvent
				if err := event.Fill(data); err != nil {
					return fmt.Errorf("failed to fill event from map data: %w", err)
				}
				intervalCount++
				totalEvent.Add(&event)

				if live {
					printMemEvent(&event, format, intervalCount)
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
			printMemEvent(&totalEvent, format, intervalCount)
		} else if intervalCount > 1 {
			fmt.Println("\n--- Total ---")
			printMemEvent(&totalEvent, format, intervalCount)
		}

		return nil
	},
}

// memRawEvent holds a single memory event.
type memRawEvent struct {
	Timestamp uint64
	Probe     string
	Pid       uint64
	Tid       uint64
	Comm      string
	Address   uint64
	Size      uint64
	Type      string
}

var memRawEventPool = sync.Pool{
	New: func() any {
		return &memRawEvent{}
	},
}

var ErrUnknownMemField = errors.New("unknown mem field")

func (e *memRawEvent) HandleLogfmt(key []byte, val []byte) (err error) {
	k := string(key)
	v := string(val)
	switch k {
	case "ts":
		e.Timestamp, err = strconv.ParseUint(v, 10, 64)
	case "fn":
		e.Probe = v
	case "pid":
		e.Pid, err = strconv.ParseUint(v, 10, 64)
	case "tid":
		e.Tid, err = strconv.ParseUint(v, 10, 64)
	case "comm":
		e.Comm = strings.Trim(v, "'\"")
	case "addr":
		e.Address, err = strconv.ParseUint(v, 0, 64)
	case "size":
		e.Size, err = strconv.ParseUint(v, 10, 64)
	case "type":
		e.Type = v
	default:
		err = ErrUnknownMemField
	}
	return
}

const createMemTableSQL = `CREATE TABLE IF NOT EXISTS %s (
	Ts UBIGINT,
	Probe STRING,
	Pid UBIGINT,
	Tid UBIGINT,
	Comm STRING,
	Address UBIGINT,
	Size UBIGINT,
	Type STRING)`

const dropMemTableSQL = `DROP TABLE IF EXISTS %S`

type memAppendRowFn = func(e *memRawEvent) error

func memJSONParseThenAppend(r io.Reader, appendRow memAppendRowFn) error {
	parser := &NDJSONParser{}
	return parser.ParseStream(r, func(msgType string, data *simdjson.Element) error {
		switch msgType {
		case "printf":
			buf, err := data.Iter.StringBytes()
			if err != nil {
				return fmt.Errorf("failed to get 'printf' data as string: %w", err)
			}
			e := memRawEventPool.Get().(*memRawEvent)
			*e = memRawEvent{}
			err = logfmt.Unmarshal(buf, e)
			if err != nil {
				memRawEventPool.Put(e)
				return fmt.Errorf("failed to unmarshal logfmt data: %w", err)
			}
			err = appendRow(e)
			memRawEventPool.Put(e)
			return err
		default:
			log.Warn().Str("type", msgType).Msg("Unknown message type, skipping")
		}
		return nil
	})
}

var memRawCmd = &cli.Command{
	Name:  "raw",
	Usage: "Write raw memory events to DuckDB",
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

		_, err = db.Exec(fmt.Sprintf(dropMemTableSQL, tableName))
		if err != nil {
			return err
		}

		_, err = db.Exec(fmt.Sprintf(createMemTableSQL, tableName))
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

		return memJSONParseThenAppend(r, func(e *memRawEvent) error {
			return appender.AppendRow(e.Timestamp, e.Probe, e.Pid, e.Tid,
				e.Comm, e.Address, e.Size, e.Type)
		})
	},
}
