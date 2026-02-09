package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/minio/simdjson-go"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v3"
)

var vfsCmd = &cli.Command{
	Name: "vfs",
	Commands: []*cli.Command{
		vfsCountCmd,
		vfsRawCmd,
	},
}

var vfsCountCmd = &cli.Command{
	Name: "count",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "input",
			Aliases: []string{"i"},
			Value:   "-",
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

		// Validate format
		switch format {
		case "table", "json", "csv":
			// valid
		default:
			return fmt.Errorf("invalid format: %s (must be table, json, or csv)", format)
		}

		var startTime time.Time
		var totalEvent Event
		var intervalCount int

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
					return fmt.Errorf("failed to find 'type' element: %w", err)
				}
				typeStr, err := typeEl.Iter.String()
				if err != nil {
					return fmt.Errorf("failed to get 'type' as string: %w", err)
				}
				dataEl, err = iter.FindElement(dataEl, "data")
				if err != nil {
					return fmt.Errorf("failed to find 'data' element: %w", err)
				}

				switch typeStr {
				case "attached_probes":
					var probesEl *simdjson.Element
					probesEl, err = dataEl.Iter.FindElement(probesEl, "probes")
					if err != nil {
						return fmt.Errorf("failed to find 'probes' element: %w", err)
					}
					probes, err := probesEl.Iter.Int()
					if err != nil {
						return fmt.Errorf("failed to get 'probes' as int: %w", err)
					}
					if probes <= 0 {
						return errors.New("probes not attached")
					}

				case "time":
					if !startTime.IsZero() {
						log.Warn().Msg("Received multiple 'time' messages, ignoring")
						return nil
					}
					timeStr, err := dataEl.Iter.String()
					if err != nil {
						return fmt.Errorf("failed to get 'time' data as string: %w", err)
					}
					timeStr = strings.TrimSpace(timeStr)
					startTime, err = time.Parse(time.TimeOnly, timeStr)
					if err != nil {
						return fmt.Errorf("failed to parse time: %w", err)
					}
					log.Info().Str("start_time", startTime.Format(time.TimeOnly)).Msg("Record start from")

				case "map":
					var event Event
					if err := event.Fill(dataEl); err != nil {
						return fmt.Errorf("failed to fill event from map data: %w", err)
					}
					intervalCount++
					totalEvent.Add(&event)

					if live {
						printEvent(&event, format, intervalCount)
					}

				default:
					log.Warn().Str("type", typeStr).Msg("Unknown message type, skipping")
				}

				return nil
			})
			if err != nil {
				return err
			}

			reuse <- got.Value
		}

		// Print summary
		if !live {
			printEvent(&totalEvent, format, intervalCount)
		} else if intervalCount > 1 {
			// In live mode, print total at the end if there were multiple intervals
			fmt.Println("\n--- Total ---")
			printEvent(&totalEvent, format, intervalCount)
		}

		return nil
	},
}

func printEvent(e *Event, format string, intervalCount int) {
	switch format {
	case "json":
		output := struct {
			Event
			Intervals int   `json:"intervals"`
			Total     int64 `json:"total"`
		}{
			Event:     *e,
			Intervals: intervalCount,
			Total:     e.Total(),
		}
		data, _ := json.Marshal(output)
		fmt.Println(string(data))

	case "csv":
		w := csv.NewWriter(os.Stdout)
		_ = w.Write([]string{"Operation", "Count"})
		_ = w.Write([]string{"create", fmt.Sprintf("%d", e.Create)})
		_ = w.Write([]string{"open", fmt.Sprintf("%d", e.Open)})
		_ = w.Write([]string{"read", fmt.Sprintf("%d", e.Read)})
		_ = w.Write([]string{"readlink", fmt.Sprintf("%d", e.ReadLink)})
		_ = w.Write([]string{"readv", fmt.Sprintf("%d", e.ReadV)})
		_ = w.Write([]string{"write", fmt.Sprintf("%d", e.Write)})
		_ = w.Write([]string{"writev", fmt.Sprintf("%d", e.WriteV)})
		_ = w.Write([]string{"fsync", fmt.Sprintf("%d", e.FSync)})
		_ = w.Write([]string{"total", fmt.Sprintf("%d", e.Total())})
		w.Flush()

	default: // table
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "Operation\tCount")
		_, _ = fmt.Fprintln(tw, "---------\t-----")
		_, _ = fmt.Fprintf(tw, "create\t%d\n", e.Create)
		_, _ = fmt.Fprintf(tw, "open\t%d\n", e.Open)
		_, _ = fmt.Fprintf(tw, "read\t%d\n", e.Read)
		_, _ = fmt.Fprintf(tw, "readlink\t%d\n", e.ReadLink)
		_, _ = fmt.Fprintf(tw, "readv\t%d\n", e.ReadV)
		_, _ = fmt.Fprintf(tw, "write\t%d\n", e.Write)
		_, _ = fmt.Fprintf(tw, "writev\t%d\n", e.WriteV)
		_, _ = fmt.Fprintf(tw, "fsync\t%d\n", e.FSync)
		_, _ = fmt.Fprintln(tw, "---------\t-----")
		_, _ = fmt.Fprintf(tw, "Total\t%d\n", e.Total())
		_, _ = fmt.Fprintf(tw, "Intervals\t%d\n", intervalCount)
		_ = tw.Flush()
	}
}

type Event struct {
	Create   int64 `json:"create"`
	Open     int64 `json:"open"`
	Read     int64 `json:"read"`
	ReadLink int64 `json:"readlink"`
	ReadV    int64 `json:"readv"`
	Write    int64 `json:"write"`
	WriteV   int64 `json:"writev"`
	FSync    int64 `json:"fsync"`
}

// Add accumulates counts from another Event
func (e *Event) Add(other *Event) {
	e.Create += other.Create
	e.Open += other.Open
	e.Read += other.Read
	e.ReadLink += other.ReadLink
	e.ReadV += other.ReadV
	e.Write += other.Write
	e.WriteV += other.WriteV
	e.FSync += other.FSync
}

// Total returns the sum of all operation counts
func (e *Event) Total() int64 {
	return e.Create + e.Open + e.Read + e.ReadLink + e.ReadV + e.Write + e.WriteV + e.FSync
}

func (e *Event) Fill(el *simdjson.Element) error {
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
		case "vfs_create":
			e.Create = value
		case "vfs_open":
			e.Open = value
		case "vfs_read":
			e.Read = value
		case "vfs_readlink":
			e.ReadLink = value
		case "vfs_readv":
			e.ReadV = value
		case "vfs_write":
			e.Write = value
		case "vfs_writev":
			e.WriteV = value
		case "vfs_fsync":
			e.FSync = value
		default:
			log.Debug().Str("field", m.Name).Int64("value", value).Msg("Unknown field in map data")
		}
	}
	return nil
}
