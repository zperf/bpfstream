package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/minio/simdjson-go"
)

// TestEventAdd tests the Event.Add() method
func TestEventAdd(t *testing.T) {
	tests := []struct {
		name     string
		initial  Event
		toAdd    Event
		expected Event
	}{
		{
			name:     "add zero event",
			initial:  Event{Create: 10, Open: 20, Read: 30},
			toAdd:    Event{},
			expected: Event{Create: 10, Open: 20, Read: 30},
		},
		{
			name:     "add to zero event",
			initial:  Event{},
			toAdd:    Event{Create: 5, Open: 10, Read: 15},
			expected: Event{Create: 5, Open: 10, Read: 15},
		},
		{
			name: "add two non-zero events",
			initial: Event{
				Create: 10, Open: 20, Read: 30, ReadLink: 5,
				ReadV: 3, Write: 15, WriteV: 7, FSync: 2,
			},
			toAdd: Event{
				Create: 5, Open: 10, Read: 25, ReadLink: 2,
				ReadV: 1, Write: 8, WriteV: 4, FSync: 1,
			},
			expected: Event{
				Create: 15, Open: 30, Read: 55, ReadLink: 7,
				ReadV: 4, Write: 23, WriteV: 11, FSync: 3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Add(&tt.toAdd)
			if tt.initial != tt.expected {
				t.Errorf("Event.Add() = %+v, want %+v", tt.initial, tt.expected)
			}
		})
	}
}

// TestEventTotal tests the Event.Total() method
func TestEventTotal(t *testing.T) {
	tests := []struct {
		name     string
		event    Event
		expected int64
	}{
		{
			name:     "zero event",
			event:    Event{},
			expected: 0,
		},
		{
			name: "all fields set",
			event: Event{
				Create: 10, Open: 20, Read: 30, ReadLink: 5,
				ReadV: 3, Write: 15, WriteV: 7, FSync: 2,
			},
			expected: 92,
		},
		{
			name:     "only some fields set",
			event:    Event{Create: 100, Read: 200},
			expected: 300,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.event.Total()
			if got != tt.expected {
				t.Errorf("Event.Total() = %d, want %d", got, tt.expected)
			}
		})
	}
}

// TestEventFill tests the Event.Fill() method with valid JSON data
func TestEventFill(t *testing.T) {
	// Create a wrapper structure similar to what vfs count receives
	wrapperJSON := `{"data": {"@": {"vfs_create": 10, "vfs_open": 20, "vfs_read": 30, "vfs_readlink": 5, "vfs_readv": 3, "vfs_write": 15, "vfs_writev": 7, "vfs_fsync": 2}}}`
	pj, err := simdjson.Parse([]byte(wrapperJSON), nil)
	if err != nil {
		t.Fatalf("failed to parse wrapper JSON: %v", err)
	}

	iter := pj.Iter()
	iter.AdvanceInto()

	var dataEl *simdjson.Element
	dataEl, err = iter.FindElement(dataEl, "data")
	if err != nil {
		t.Fatalf("failed to find 'data' element: %v", err)
	}

	var event Event
	err = event.Fill(dataEl)
	if err != nil {
		t.Fatalf("Event.Fill() error = %v", err)
	}

	expected := Event{
		Create: 10, Open: 20, Read: 30, ReadLink: 5,
		ReadV: 3, Write: 15, WriteV: 7, FSync: 2,
	}

	if event != expected {
		t.Errorf("Event.Fill() = %+v, want %+v", event, expected)
	}
}

// TestEventFillError tests the Event.Fill() method with invalid data
func TestEventFillError(t *testing.T) {
	// Missing '@' element
	testJSON := `{"data": {"other_key": {"vfs_create": 10}}}`
	pj, err := simdjson.Parse([]byte(testJSON), nil)
	if err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	iter := pj.Iter()
	iter.AdvanceInto()

	var dataEl *simdjson.Element
	dataEl, err = iter.FindElement(dataEl, "data")
	if err != nil {
		t.Fatalf("failed to find 'data' element: %v", err)
	}

	var event Event
	err = event.Fill(dataEl)
	if err == nil {
		t.Error("Event.Fill() expected error for missing '@' element, got nil")
	}
}

// TestEventFillUnknownField tests that unknown fields are handled gracefully
func TestEventFillUnknownField(t *testing.T) {
	// Include unknown field 'vfs_unknown'
	testJSON := `{"data": {"@": {"vfs_create": 10, "vfs_unknown": 999}}}`
	pj, err := simdjson.Parse([]byte(testJSON), nil)
	if err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	iter := pj.Iter()
	iter.AdvanceInto()

	var dataEl *simdjson.Element
	dataEl, err = iter.FindElement(dataEl, "data")
	if err != nil {
		t.Fatalf("failed to find 'data' element: %v", err)
	}

	var event Event
	err = event.Fill(dataEl)
	if err != nil {
		t.Errorf("Event.Fill() should not error on unknown fields, got: %v", err)
	}

	if event.Create != 10 {
		t.Errorf("Event.Create = %d, want 10", event.Create)
	}
}

// TestPrintEventTable tests the printEvent function with table format
func TestPrintEventTable(t *testing.T) {
	event := Event{
		Create: 10, Open: 20, Read: 30, ReadLink: 5,
		ReadV: 3, Write: 15, WriteV: 7, FSync: 2,
	}

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printEvent(&event, "table", 5)

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := buf.String()

	// Verify key elements are present
	expectedStrings := []string{
		"Operation", "Count",
		"create", "10",
		"open", "20",
		"read", "30",
		"readlink", "5",
		"readv", "3",
		"write", "15",
		"writev", "7",
		"fsync", "2",
		"Total", "92",
		"Intervals", "5",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(output, expected) {
			t.Errorf("printEvent(table) missing expected string: %q\nOutput:\n%s", expected, output)
		}
	}
}

// TestPrintEventJSON tests the printEvent function with JSON format
func TestPrintEventJSON(t *testing.T) {
	event := Event{
		Create: 10, Open: 20, Read: 30, ReadLink: 5,
		ReadV: 3, Write: 15, WriteV: 7, FSync: 2,
	}

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printEvent(&event, "json", 5)

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := strings.TrimSpace(buf.String())

	// Parse the JSON output
	var result struct {
		Create    int64 `json:"create"`
		Open      int64 `json:"open"`
		Read      int64 `json:"read"`
		ReadLink  int64 `json:"readlink"`
		ReadV     int64 `json:"readv"`
		Write     int64 `json:"write"`
		WriteV    int64 `json:"writev"`
		FSync     int64 `json:"fsync"`
		Intervals int   `json:"intervals"`
		Total     int64 `json:"total"`
	}

	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("failed to parse JSON output: %v\nOutput: %s", err, output)
	}

	if result.Create != 10 {
		t.Errorf("JSON create = %d, want 10", result.Create)
	}
	if result.Open != 20 {
		t.Errorf("JSON open = %d, want 20", result.Open)
	}
	if result.Total != 92 {
		t.Errorf("JSON total = %d, want 92", result.Total)
	}
	if result.Intervals != 5 {
		t.Errorf("JSON intervals = %d, want 5", result.Intervals)
	}
}

// TestPrintEventCSV tests the printEvent function with CSV format
func TestPrintEventCSV(t *testing.T) {
	event := Event{
		Create: 10, Open: 20, Read: 30, ReadLink: 5,
		ReadV: 3, Write: 15, WriteV: 7, FSync: 2,
	}

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printEvent(&event, "csv", 5)

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// Verify CSV header and content
	if len(lines) < 2 {
		t.Fatalf("CSV output has too few lines: %d", len(lines))
	}

	// Check header
	if !strings.Contains(lines[0], "Operation") || !strings.Contains(lines[0], "Count") {
		t.Errorf("CSV header missing expected columns: %s", lines[0])
	}

	// Verify specific rows exist
	expectedRows := map[string]string{
		"create": "10",
		"open":   "20",
		"read":   "30",
		"total":  "92",
	}

	for op, count := range expectedRows {
		found := false
		for _, line := range lines[1:] {
			if strings.Contains(line, op) && strings.Contains(line, count) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("CSV output missing row for %s=%s\nOutput:\n%s", op, count, output)
		}
	}
}
