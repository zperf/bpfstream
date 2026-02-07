package main

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/minio/simdjson-go"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// MessageHandler is called for each parsed message from the bpftrace output.
// The handler receives the message type and data element, returning an error if processing fails.
type MessageHandler func(msgType string, data *simdjson.Element) error

// NDJSONParser provides a common framework for parsing bpftrace NDJSON output.
type NDJSONParser struct {
	StartTime time.Time
}

// ParseStream reads NDJSON from the reader and calls the handler for each message.
// It handles common message types (attached_probes, time, lost_events) internally
// and delegates unknown types to the handler.
func (p *NDJSONParser) ParseStream(r io.Reader, handler MessageHandler) error {
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
			var err error

			typeEl, err = iter.FindElement(typeEl, "type")
			if err != nil {
				return errors.Wrap(err, "failed to find 'type' element")
			}
			typeStr, err := typeEl.Iter.String()
			if err != nil {
				return errors.Wrap(err, "failed to get 'type' as string")
			}
			dataEl, err = iter.FindElement(dataEl, "data")
			if err != nil {
				return errors.Wrap(err, "failed to find 'data' element")
			}

			// Handle common message types
			switch typeStr {
			case "attached_probes":
				return p.handleAttachedProbes(dataEl)
			case "time":
				return p.handleTime(dataEl)
			case "lost_events":
				return p.handleLostEvents(dataEl)
			default:
				// Delegate to custom handler
				return handler(typeStr, dataEl)
			}
		})
		if err != nil {
			return err
		}

		reuse <- got.Value
	}

	return nil
}

func (p *NDJSONParser) handleAttachedProbes(dataEl *simdjson.Element) error {
	var probesEl *simdjson.Element
	var err error
	probesEl, err = dataEl.Iter.FindElement(probesEl, "probes")
	if err != nil {
		return errors.Wrap(err, "failed to find 'probes' element")
	}
	probes, err := probesEl.Iter.Int()
	if err != nil {
		return errors.Wrap(err, "failed to get 'probes' as int")
	}
	if probes <= 0 {
		return errors.New("probes not attached")
	}
	log.Debug().Int64("probes", probes).Msg("Probes attached")
	return nil
}

func (p *NDJSONParser) handleTime(dataEl *simdjson.Element) error {
	if !p.StartTime.IsZero() {
		log.Warn().Msg("Received multiple 'time' messages, ignoring")
		return nil
	}
	timeStr, err := dataEl.Iter.String()
	if err != nil {
		return errors.Wrap(err, "failed to get 'time' data as string")
	}
	timeStr = strings.TrimSpace(timeStr)
	p.StartTime, err = time.Parse(time.TimeOnly, timeStr)
	if err != nil {
		return errors.Wrap(err, "failed to parse time")
	}
	log.Info().Str("start_time", p.StartTime.Format(time.TimeOnly)).Msg("Record start from")
	return nil
}

func (p *NDJSONParser) handleLostEvents(dataEl *simdjson.Element) error {
	var eventCountEl *simdjson.Element
	var err error
	eventCountEl, err = dataEl.Iter.FindElement(eventCountEl, "events")
	if err != nil {
		return errors.Wrap(err, "failed to find 'events' element")
	}
	lostEvents, err := eventCountEl.Iter.Int()
	if err != nil {
		return errors.Wrap(err, "failed to get 'events' as int")
	}
	log.Info().Int64("lost_events", lostEvents).Msg("Lost events")
	return nil
}

// SimpleLineParser provides a fast line-based parser for bpftrace output.
// It uses simple string matching for better performance when the format is known.
type SimpleLineParser struct {
	StartTime time.Time
}

// LineHandler is called for each printf line with the data content.
type LineHandler func(data string) error

// ParseLines reads lines from the reader and calls the handler for printf lines.
func (p *SimpleLineParser) ParseLines(r io.Reader, handler LineHandler) error {
	const attachedProbeKeyword = `{"type": "attached_probes", "data": {"probes": `
	const startTimeKeyword = `{"type": "time", "data": "`
	const printfKeyword = `{"type": "printf", "data": "`
	const lostEventsKeyword = `{"type": "lost_events", "data": {"events": `

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, attachedProbeKeyword) {
			pos := len(attachedProbeKeyword)
			data := line[pos : len(line)-len("}}")]
			probes, err := strconv.ParseUint(data, 10, 64)
			if err != nil {
				return err
			}
			if probes <= 0 {
				return errors.New("probes not attached")
			}
			log.Debug().Uint64("probes", probes).Msg("Probes attached")
		} else if strings.HasPrefix(line, startTimeKeyword) {
			pos := len(startTimeKeyword)
			data := line[pos : len(line)-len(`\n"}`)]
			startTime, err := time.Parse(time.TimeOnly, data)
			if err != nil {
				return err
			}
			p.StartTime = startTime
			log.Info().Str("start_time", startTime.Format(time.TimeOnly)).Msg("Record start from")
		} else if strings.HasPrefix(line, printfKeyword) {
			pos := len(printfKeyword)
			data := line[pos : len(line)-len(`"}`)]
			if err := handler(data); err != nil {
				return err
			}
		} else if strings.HasPrefix(line, lostEventsKeyword) {
			pos := len(lostEventsKeyword)
			data := line[pos : len(line)-len(`}}`)]
			lostEvents, err := strconv.ParseUint(data, 10, 64)
			if err != nil {
				return err
			}
			log.Info().Uint64("lost_events", lostEvents).Msg("Lost events")
		} else {
			log.Warn().Str("line", line).Msg("Unknown line format, skipping")
		}
	}

	return scanner.Err()
}
