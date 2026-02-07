package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kr/logfmt"
	"github.com/marcboeker/go-duckdb/v2"
	"github.com/minio/simdjson-go"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v3"
)

const createTableSql = `CREATE TABLE IF NOT EXISTS %s (
	Ts UBIGINT,
	Probe STRING,
	Tid UBIGINT,
	RC  BIGINT,
	Path STRING,
	Inode UBIGINT,
	"Offset" UBIGINT,
	Length UBIGINT)`

const dropTableSql = `DROP TABLE IF EXISTS %s`

type vfsEvent struct {
	Timestamp   uint64
	Probe       string
	Tid         uint64
	ReturnValue int64
	Path        string
	Inode       uint64
	Offset      uint64
	Length      uint64
}

var ErrUnknownField = errors.New("unknown field")

func (e *vfsEvent) HandleLogfmt(key []byte, val []byte) (err error) {
	k := string(key)
	v := string(val)
	switch k {
	case "ts":
		e.Timestamp, err = strconv.ParseUint(v, 10, 64)
	case "fn":
		e.Probe = v
	case "tid":
		e.Tid, err = strconv.ParseUint(v, 10, 64)
	case "rc":
		e.ReturnValue, err = strconv.ParseInt(v, 10, 64)
	case "path":
		e.Path = v[1 : len(v)-1]
	case "inode":
		e.Inode, err = strconv.ParseUint(v, 10, 64)
	case "offset":
		e.Offset, err = strconv.ParseUint(v, 10, 64)
	case "len":
		e.Length, err = strconv.ParseUint(v, 10, 64)
	default:
		err = ErrUnknownField
	}
	return
}

type appendRowFn = func(e *vfsEvent) error

// Ad-hoc parse for the best performance
func simpleParseThenAppend(r io.Reader, appendRow appendRowFn) error {
	const attachedProbeKeyword = `{"type": "attached_probes", "data": {"probes": `
	const startTimeKeyword = `{"type": "time", "data": "`
	const printfKeyword = `{"type": "printf", "data": "`
	const lostEventsKeyword = `{"type": "lost_events", "data": {"events": `

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, attachedProbeKeyword) {
			p := len(attachedProbeKeyword)
			data := line[p : len(line)-len("}}")]
			probes, err := strconv.ParseUint(data, 10, 64)
			if err != nil {
				return err
			}
			if probes <= 0 {
				return errors.New("probes not attached")
			}
		} else if strings.HasPrefix(line, startTimeKeyword) {
			p := len(startTimeKeyword)
			data := line[p : len(line)-len(`\n"}`)]
			startTime, err := time.Parse(time.TimeOnly, data)
			if err != nil {
				return err
			}
			log.Info().Str("start_time", startTime.Format(time.TimeOnly)).Msg("Record start from")
		} else if strings.HasPrefix(line, printfKeyword) {
			p := len(printfKeyword)
			data := line[p : len(line)-len(`"}`)]
			_ = data
			var e vfsEvent
			err := logfmt.Unmarshal([]byte(data), &e)
			if err != nil {
				return err
			}
			err = appendRow(&e)
			if err != nil {
				return err
			}
		} else if strings.HasPrefix(line, lostEventsKeyword) {
			p := len(lostEventsKeyword)
			data := line[p : len(line)-len(`}}`)]
			lostEvents, err := strconv.ParseUint(data, 10, 64)
			if err != nil {
				return err
			}
			log.Info().Uint64("lost_events", lostEvents).Msg("Lost events")
		} else {
			log.Warn().Str("line", line).Msg("Unknown line format, skipping")
		}
	}

	return nil
}

func jsonParseThenAppend(r io.Reader, appendRow appendRowFn) error {
	var startTime time.Time

	reuse := make(chan *simdjson.ParsedJson, 10)
	res := make(chan simdjson.Stream, 10)
	simdjson.ParseNDStream(r, res, reuse)

	for got := range res {
		err := got.Error
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		err = got.Value.ForEach(func(iter simdjson.Iter) error {
			var typeEl, dataEl *simdjson.Element
			typeEl, err = iter.FindElement(typeEl, "type")
			if err != nil {
				return errors.Wrap(err, "failed to find 'type' element")
			}
			var typeStr string
			typeStr, err = typeEl.Iter.String()
			if err != nil {
				return errors.Wrap(err, "failed to get 'type' as string")
			}
			dataEl, err = iter.FindElement(dataEl, "data")
			if err != nil {
				return errors.Wrap(err, "failed to find 'data' element")
			}

			switch typeStr {
			case "attached_probes":
				var probesEl *simdjson.Element
				probesEl, err = dataEl.Iter.FindElement(probesEl, "probes")
				if err != nil {
					return errors.Wrap(err, "failed to find 'probes' element")
				}
				var probes int64
				probes, err = probesEl.Iter.Int()
				if err != nil {
					return errors.Wrap(err, "failed to get 'probes' as int")
				}
				if probes <= 0 {
					return errors.New("probes not attached")
				}

			case "time":
				var timeStr string
				if !startTime.IsZero() {
					log.Warn().Msg("Received multiple 'time' messages, ignoring")
					return nil
				}
				timeStr, err = dataEl.Iter.String()
				if err != nil {
					return errors.Wrap(err, "failed to get 'time' data as string")
				}
				timeStr = strings.TrimSpace(timeStr)
				startTime, err = time.Parse(time.TimeOnly, timeStr)
				if err != nil {
					return errors.Wrap(err, "failed to parse time")
				}
				log.Info().Str("start_time", startTime.Format(time.TimeOnly)).Msg("Record start from")

			case "lost_events":
				var eventCountEl *simdjson.Element
				eventCountEl, err = dataEl.Iter.FindElement(eventCountEl, "events")
				if err != nil {
					return errors.Wrap(err, "failed to find 'events' element")
				}
				var lostEvents int64
				lostEvents, err = eventCountEl.Iter.Int()
				if err != nil {
					return errors.Wrap(err, "failed to get 'events' as int")
				}
				log.Info().Int64("lost_events", lostEvents).Msg("Lost events")

			case "printf":
				var buf []byte
				buf, err = dataEl.Iter.StringBytes()
				if err != nil {
					return errors.Wrap(err, "failed to get 'printf' data as string")
				}
				var e vfsEvent
				err = logfmt.Unmarshal(buf, &e)
				if err != nil {
					return errors.Wrap(err, "failed to unmarshal logfmt data")
				}
				err = appendRow(&e)
				if err != nil {
					return errors.Wrap(err, "failed to append row")
				}

			default:
				log.Warn().Str("type", typeStr).Msg("Unknown message type, skipping")
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

var vfsRawCmd = &cli.Command{
	Name: "raw",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "input",
			Aliases: []string{"i"},
			Value:   "-",
		},
		&cli.StringFlag{
			Name:     "dsn",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "table",
			Required: true,
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

		_, err = db.Exec(fmt.Sprintf(dropTableSql, tableName))
		if err != nil {
			return err
		}

		_, err = db.Exec(fmt.Sprintf(createTableSql, tableName))
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

		return jsonParseThenAppend(r, func(e *vfsEvent) error {
			return appender.AppendRow(e.Timestamp, e.Probe, e.Tid, e.ReturnValue,
				e.Path, e.Inode, e.Offset, e.Length)
		})
	},
}
