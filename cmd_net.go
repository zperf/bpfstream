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

	"github.com/kr/logfmt"
	"github.com/marcboeker/go-duckdb/v2"
	"github.com/minio/simdjson-go"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v3"
)

var netCmd = &cli.Command{
	Name:  "net",
	Usage: "Network tracing commands",
	Commands: []*cli.Command{
		netCountCmd,
		netRawCmd,
	},
}

// NetCountEvent holds aggregated network operation counts.
type NetCountEvent struct {
	TCPConnect int64 `json:"tcp_connect"`
	TCPAccept  int64 `json:"tcp_accept"`
	TCPClose   int64 `json:"tcp_close"`
	UDPSend    int64 `json:"udp_send"`
	UDPRecv    int64 `json:"udp_recv"`
	SockCreate int64 `json:"sock_create"`
	SockClose  int64 `json:"sock_close"`
}

// Add accumulates counts from another NetCountEvent.
func (e *NetCountEvent) Add(other *NetCountEvent) {
	e.TCPConnect += other.TCPConnect
	e.TCPAccept += other.TCPAccept
	e.TCPClose += other.TCPClose
	e.UDPSend += other.UDPSend
	e.UDPRecv += other.UDPRecv
	e.SockCreate += other.SockCreate
	e.SockClose += other.SockClose
}

// Total returns the sum of all operation counts.
func (e *NetCountEvent) Total() int64 {
	return e.TCPConnect + e.TCPAccept + e.TCPClose + e.UDPSend + e.UDPRecv + e.SockCreate + e.SockClose
}

// Fill populates the event from simdjson data.
func (e *NetCountEvent) Fill(el *simdjson.Element) error {
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
		case "tcp_connect":
			e.TCPConnect = value
		case "tcp_accept":
			e.TCPAccept = value
		case "tcp_close":
			e.TCPClose = value
		case "udp_send":
			e.UDPSend = value
		case "udp_recv":
			e.UDPRecv = value
		case "sock_create":
			e.SockCreate = value
		case "sock_close":
			e.SockClose = value
		default:
			log.Debug().Str("field", m.Name).Int64("value", value).Msg("Unknown field in map data")
		}
	}
	return nil
}

func printNetEvent(e *NetCountEvent, format string, intervalCount int) {
	switch format {
	case "json":
		output := struct {
			NetCountEvent
			Intervals int   `json:"intervals"`
			Total     int64 `json:"total"`
		}{
			NetCountEvent: *e,
			Intervals:     intervalCount,
			Total:         e.Total(),
		}
		data, _ := json.Marshal(output)
		fmt.Println(string(data))

	case "csv":
		w := csv.NewWriter(os.Stdout)
		_ = w.Write([]string{"Operation", "Count"})
		_ = w.Write([]string{"tcp_connect", fmt.Sprintf("%d", e.TCPConnect)})
		_ = w.Write([]string{"tcp_accept", fmt.Sprintf("%d", e.TCPAccept)})
		_ = w.Write([]string{"tcp_close", fmt.Sprintf("%d", e.TCPClose)})
		_ = w.Write([]string{"udp_send", fmt.Sprintf("%d", e.UDPSend)})
		_ = w.Write([]string{"udp_recv", fmt.Sprintf("%d", e.UDPRecv)})
		_ = w.Write([]string{"sock_create", fmt.Sprintf("%d", e.SockCreate)})
		_ = w.Write([]string{"sock_close", fmt.Sprintf("%d", e.SockClose)})
		_ = w.Write([]string{"total", fmt.Sprintf("%d", e.Total())})
		w.Flush()

	default: // table
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "Operation\tCount")
		_, _ = fmt.Fprintln(tw, "---------\t-----")
		_, _ = fmt.Fprintf(tw, "tcp_connect\t%d\n", e.TCPConnect)
		_, _ = fmt.Fprintf(tw, "tcp_accept\t%d\n", e.TCPAccept)
		_, _ = fmt.Fprintf(tw, "tcp_close\t%d\n", e.TCPClose)
		_, _ = fmt.Fprintf(tw, "udp_send\t%d\n", e.UDPSend)
		_, _ = fmt.Fprintf(tw, "udp_recv\t%d\n", e.UDPRecv)
		_, _ = fmt.Fprintf(tw, "sock_create\t%d\n", e.SockCreate)
		_, _ = fmt.Fprintf(tw, "sock_close\t%d\n", e.SockClose)
		_, _ = fmt.Fprintln(tw, "---------\t-----")
		_, _ = fmt.Fprintf(tw, "Total\t%d\n", e.Total())
		_, _ = fmt.Fprintf(tw, "Intervals\t%d\n", intervalCount)
		_ = tw.Flush()
	}
}

var netCountCmd = &cli.Command{
	Name:  "count",
	Usage: "Aggregate network operation counts",
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

		var totalEvent NetCountEvent
		var intervalCount int

		parser := &NDJSONParser{}
		err := parser.ParseStream(r, func(msgType string, data *simdjson.Element) error {
			switch msgType {
			case "map":
				var event NetCountEvent
				if err := event.Fill(data); err != nil {
					return fmt.Errorf("failed to fill event from map data: %w", err)
				}
				intervalCount++
				totalEvent.Add(&event)

				if live {
					printNetEvent(&event, format, intervalCount)
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
			printNetEvent(&totalEvent, format, intervalCount)
		} else if intervalCount > 1 {
			fmt.Println("\n--- Total ---")
			printNetEvent(&totalEvent, format, intervalCount)
		}

		return nil
	},
}

// netRawEvent holds a single network event.
type netRawEvent struct {
	Timestamp uint64
	Probe     string
	Tid       uint64
	Comm      string
	SrcAddr   string
	SrcPort   uint16
	DstAddr   string
	DstPort   uint16
	Bytes     uint64
	Protocol  string
}

var netRawEventPool = sync.Pool{
	New: func() any {
		return &netRawEvent{}
	},
}

var ErrUnknownNetField = errors.New("unknown net field")

func (e *netRawEvent) HandleLogfmt(key []byte, val []byte) (err error) {
	k := string(key)
	v := string(val)
	switch k {
	case "ts":
		e.Timestamp, err = strconv.ParseUint(v, 10, 64)
	case "fn":
		e.Probe = v
	case "tid":
		e.Tid, err = strconv.ParseUint(v, 10, 64)
	case "comm":
		e.Comm = strings.Trim(v, "'\"")
	case "saddr":
		e.SrcAddr = strings.Trim(v, "'\"")
	case "sport":
		var port uint64
		port, err = strconv.ParseUint(v, 10, 16)
		e.SrcPort = uint16(port)
	case "daddr":
		e.DstAddr = strings.Trim(v, "'\"")
	case "dport":
		var port uint64
		port, err = strconv.ParseUint(v, 10, 16)
		e.DstPort = uint16(port)
	case "bytes":
		e.Bytes, err = strconv.ParseUint(v, 10, 64)
	case "proto":
		e.Protocol = v
	default:
		err = ErrUnknownNetField
	}
	return
}

const createNetTableSQL = `CREATE TABLE IF NOT EXISTS %s (
	Ts UBIGINT,
	Probe STRING,
	Tid UBIGINT,
	Comm STRING,
	SrcAddr STRING,
	SrcPort USMALLINT,
	DstAddr STRING,
	DstPort USMALLINT,
	Bytes UBIGINT,
	Protocol STRING)`

const dropNetTableSQL = `DROP TABLE IF EXISTS %s`

type netAppendRowFn = func(e *netRawEvent) error

func netJSONParseThenAppend(r io.Reader, appendRow netAppendRowFn) error {
	parser := &NDJSONParser{}
	return parser.ParseStream(r, func(msgType string, data *simdjson.Element) error {
		switch msgType {
		case "printf":
			buf, err := data.Iter.StringBytes()
			if err != nil {
				return fmt.Errorf("failed to get 'printf' data as string: %w", err)
			}
			e := netRawEventPool.Get().(*netRawEvent)
			*e = netRawEvent{}
			err = logfmt.Unmarshal(buf, e)
			if err != nil {
				netRawEventPool.Put(e)
				return fmt.Errorf("failed to unmarshal logfmt data: %w", err)
			}
			err = appendRow(e)
			netRawEventPool.Put(e)
			return err
		default:
			log.Warn().Str("type", msgType).Msg("Unknown message type, skipping")
		}
		return nil
	})
}

var netRawCmd = &cli.Command{
	Name:  "raw",
	Usage: "Write raw network events to DuckDB",
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

		_, err = db.Exec(fmt.Sprintf(dropNetTableSQL, tableName))
		if err != nil {
			return err
		}

		_, err = db.Exec(fmt.Sprintf(createNetTableSQL, tableName))
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

		return netJSONParseThenAppend(r, func(e *netRawEvent) error {
			return appender.AppendRow(e.Timestamp, e.Probe, e.Tid, e.Comm,
				e.SrcAddr, e.SrcPort, e.DstAddr, e.DstPort, e.Bytes, e.Protocol)
		})
	},
}
