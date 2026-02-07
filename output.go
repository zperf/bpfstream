package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
)

// OutputFormat represents supported output formats.
type OutputFormat string

const (
	FormatTable OutputFormat = "table"
	FormatJSON  OutputFormat = "json"
	FormatCSV   OutputFormat = "csv"
)

// ValidateFormat checks if the format string is valid.
func ValidateFormat(format string) error {
	switch format {
	case "table", "json", "csv":
		return nil
	default:
		return fmt.Errorf("invalid format: %s (must be table, json, or csv)", format)
	}
}

// CountData represents a key-value pair for count output.
type CountData struct {
	Key   string
	Value int64
}

// CountOutput provides methods to output count-style data in various formats.
type CountOutput struct {
	writer io.Writer
}

// NewCountOutput creates a new CountOutput writing to stdout.
func NewCountOutput() *CountOutput {
	return &CountOutput{writer: os.Stdout}
}

// NewCountOutputWriter creates a new CountOutput with a custom writer.
func NewCountOutputWriter(w io.Writer) *CountOutput {
	return &CountOutput{writer: w}
}

// PrintTable prints count data in table format.
func (o *CountOutput) PrintTable(header string, data []CountData, total int64, intervals int) {
	tw := tabwriter.NewWriter(o.writer, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "%s\tCount\n", header)
	_, _ = fmt.Fprintln(tw, strings.Repeat("-", len(header))+"\t-----")
	for _, d := range data {
		_, _ = fmt.Fprintf(tw, "%s\t%d\n", d.Key, d.Value)
	}
	_, _ = fmt.Fprintln(tw, strings.Repeat("-", len(header))+"\t-----")
	_, _ = fmt.Fprintf(tw, "Total\t%d\n", total)
	_, _ = fmt.Fprintf(tw, "Intervals\t%d\n", intervals)
	_ = tw.Flush()
}

// PrintJSON prints count data in JSON format.
func (o *CountOutput) PrintJSON(data any) {
	encoded, _ := json.Marshal(data)
	_, _ = fmt.Fprintln(o.writer, string(encoded))
}

// PrintCSV prints count data in CSV format.
func (o *CountOutput) PrintCSV(header string, data []CountData) {
	w := csv.NewWriter(o.writer)
	_ = w.Write([]string{header, "Count"})
	for _, d := range data {
		_ = w.Write([]string{d.Key, fmt.Sprintf("%d", d.Value)})
	}
	w.Flush()
}
