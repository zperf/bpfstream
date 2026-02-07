package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"sort"
	"strings"
	"text/template"

	"github.com/goccy/go-json"
	"github.com/gookit/color"
	"github.com/pterm/pterm"
	"github.com/rs/zerolog"
)

type ptermField struct {
	Key string
	Val string
}

type ptermEvent struct {
	Timestamp string
	Level     string
	Message   string
	Fields    []ptermField
}

type ptermWriter struct {
	out         io.Writer
	levelStyles map[zerolog.Level]*pterm.Style
	tmpl        *template.Template
	keyStyles   map[string]*pterm.Style
	keyOrderFn  func(string, string) bool
}

func newPtermWriter() *ptermWriter {
	pw := &ptermWriter{
		out: os.Stdout,
		levelStyles: map[zerolog.Level]*pterm.Style{
			zerolog.TraceLevel: pterm.NewStyle(pterm.Bold, pterm.FgCyan),
			zerolog.DebugLevel: pterm.NewStyle(pterm.Bold, pterm.FgBlue),
			zerolog.InfoLevel:  pterm.NewStyle(pterm.Bold, pterm.FgGreen),
			zerolog.WarnLevel:  pterm.NewStyle(pterm.Bold, pterm.FgYellow),
			zerolog.ErrorLevel: pterm.NewStyle(pterm.Bold, pterm.FgRed),
			zerolog.FatalLevel: pterm.NewStyle(pterm.Bold, pterm.FgRed),
			zerolog.PanicLevel: pterm.NewStyle(pterm.Bold, pterm.FgRed),
			zerolog.NoLevel:    pterm.NewStyle(pterm.Bold, pterm.FgWhite),
		},
		keyStyles: map[string]*pterm.Style{
			zerolog.MessageFieldName:    pterm.NewStyle(pterm.Bold, pterm.FgWhite),
			zerolog.TimestampFieldName:  pterm.NewStyle(pterm.Bold, pterm.FgGray),
			zerolog.CallerFieldName:     pterm.NewStyle(pterm.Bold, pterm.FgGray),
			zerolog.ErrorFieldName:      pterm.NewStyle(pterm.Bold, pterm.FgRed),
			zerolog.ErrorStackFieldName: pterm.NewStyle(pterm.Bold, pterm.FgRed),
		},
		keyOrderFn: func(k1, k2 string) bool {
			score := func(s string) string {
				s = color.ClearCode(s)
				if s == zerolog.TimestampFieldName {
					return string([]byte{0, 0})
				}
				if s == zerolog.CallerFieldName {
					return string([]byte{0, 1})
				}
				if s == zerolog.ErrorFieldName {
					return string([]byte{math.MaxUint8, 0})
				}
				if s == zerolog.ErrorStackFieldName {
					return string([]byte{math.MaxUint8, 1})
				}
				return s
			}
			return score(k1) < score(k2)
		},
	}

	tmplStr := `{{ .Timestamp }} [{{ .Level }}] {{ .Message }}
{{- range $i, $field := .Fields }}
{{ space (totalLength 1 $.Timestamp $.Level) }}{{if (last $i $.Fields )}}└{{else}}├{{ end }} {{ .Key }}: {{ .Val }}
{{- end }}
`
	t, err := template.New("event").
		Funcs(template.FuncMap{
			"space": func(n int) string {
				return strings.Repeat(" ", n)
			},
			"totalLength": func(n int, s ...string) int {
				return len(color.ClearCode(strings.Join(s, ""))) - n
			},
			"last": func(x int, a interface{}) bool {
				return x == reflect.ValueOf(a).Len()-1
			},
		}).
		Parse(tmplStr)
	if err != nil {
		panic(fmt.Errorf("cannot parse template: %s", err))
	}
	pw.tmpl = t

	return pw
}

func (pw *ptermWriter) Write(p []byte) (n int, err error) {
	return pw.out.Write(p)
}

func (pw *ptermWriter) WriteLevel(lvl zerolog.Level, p []byte) (n int, err error) {
	var evt map[string]interface{}
	d := json.NewDecoder(bytes.NewReader(p))
	d.UseNumber()
	if err = d.Decode(&evt); err != nil {
		return n, fmt.Errorf("cannot decode event: %s", err)
	}

	var event ptermEvent
	if ts, ok := evt[zerolog.TimestampFieldName]; ok {
		event.Timestamp = pw.keyStyles[zerolog.TimestampFieldName].Sprint(ts)
	}
	event.Level = pw.levelStyles[lvl].Sprint(lvl)
	if msg, ok := evt[zerolog.MessageFieldName]; ok {
		event.Message = pw.keyStyles[zerolog.MessageFieldName].Sprint(msg)
	}

	event.Fields = make([]ptermField, 0, len(evt))
	for k, v := range evt {
		if k == zerolog.TimestampFieldName ||
			k == zerolog.LevelFieldName ||
			k == zerolog.MessageFieldName {
			continue
		}
		var key string
		if style, ok := pw.keyStyles[k]; ok {
			key = style.Sprint(k)
		} else {
			key = pw.levelStyles[lvl].Sprint(k)
		}
		val := pterm.Sprint(v)
		event.Fields = append(event.Fields, ptermField{Key: key, Val: val})
	}

	sort.Slice(event.Fields, func(i, j int) bool {
		return pw.keyOrderFn(event.Fields[i].Key, event.Fields[j].Key)
	})

	var buf bytes.Buffer
	if err = pw.tmpl.Execute(&buf, event); err != nil {
		return n, fmt.Errorf("cannot execute template: %s", err)
	}
	return pw.out.Write(buf.Bytes())
}
