package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
)

const testDataFile = "testdata/vfs-raw.ndjson"

// linux-amd64, cpu: AMD Ryzen 7 9700X 8-Core Processor

func TestSimpleParse(t *testing.T) {
	buffer, err := os.ReadFile(testDataFile)
	if err != nil {
		t.Fatal(err)
	}
	r := bytes.NewReader(buffer)
	err = simpleParseThenAppend(r, func(*vfsEvent) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
}

// ============================================
// Unit Tests for Parser Error Handling
// ============================================

// Sample test data for vfs raw tests
// Note: path field is parsed by stripping first and last character (quotes)
// The real data uses single quotes like path='filename'
const vfsRawTestDataValid = `{"type": "attached_probes", "data": {"probes": 8}}
{"type": "time", "data": "12:34:56\n"}
{"type": "printf", "data": "ts=1234567890 fn=vfs_read tid=1234 rc=100 path='test.txt' inode=12345 offset=0 len=100"}
{"type": "lost_events", "data": {"events": 5}}
`

// TestSimpleParseValidData tests simpleParseThenAppend with valid input
func TestSimpleParseValidData(t *testing.T) {
	r := strings.NewReader(vfsRawTestDataValid)
	var events []*vfsEvent

	err := simpleParseThenAppend(r, func(e *vfsEvent) error {
		events = append(events, e)
		return nil
	})

	if err != nil {
		t.Fatalf("simpleParseThenAppend() error = %v", err)
	}

	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}

	if events[0].Probe != "vfs_read" {
		t.Errorf("expected probe 'vfs_read', got '%s'", events[0].Probe)
	}

	if events[0].Tid != 1234 {
		t.Errorf("expected tid 1234, got %d", events[0].Tid)
	}
}

// TestSimpleParseUnknownFormat tests simpleParseThenAppend with unknown line format
func TestSimpleParseUnknownFormat(t *testing.T) {
	// Include an unknown format line - should log warning but not error
	testData := `{"type": "attached_probes", "data": {"probes": 8}}
{"type": "unknown_format", "data": "something"}
{"type": "printf", "data": "ts=1234567890 fn=vfs_read tid=1234 rc=100 path='test.txt' inode=12345 offset=0 len=100"}
`
	r := strings.NewReader(testData)
	var events []*vfsEvent

	err := simpleParseThenAppend(r, func(e *vfsEvent) error {
		events = append(events, e)
		return nil
	})

	if err != nil {
		t.Fatalf("simpleParseThenAppend() should not error on unknown format, got: %v", err)
	}

	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}
}

// TestSimpleParseProbesNotAttached tests error when probes <= 0
func TestSimpleParseProbesNotAttached(t *testing.T) {
	testData := `{"type": "attached_probes", "data": {"probes": 0}}`
	r := strings.NewReader(testData)

	err := simpleParseThenAppend(r, func(e *vfsEvent) error {
		return nil
	})

	if err == nil {
		t.Error("expected error when probes not attached, got nil")
	}

	if !strings.Contains(err.Error(), "probes not attached") {
		t.Errorf("expected 'probes not attached' error, got: %v", err)
	}
}

// TestSimpleParseInvalidTime tests error with invalid time format
func TestSimpleParseInvalidTime(t *testing.T) {
	testData := `{"type": "attached_probes", "data": {"probes": 8}}
{"type": "time", "data": "invalid-time\n"}`
	r := strings.NewReader(testData)

	err := simpleParseThenAppend(r, func(e *vfsEvent) error {
		return nil
	})

	if err == nil {
		t.Error("expected error for invalid time format, got nil")
	}
}

func isErrorUnsupportedPlatform(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "Unsupported platform"
}

// TestJsonParseValidData tests jsonParseThenAppend with valid input
func TestJsonParseValidData(t *testing.T) {
	r := strings.NewReader(vfsRawTestDataValid)
	var events []*vfsEvent

	err := jsonParseThenAppend(r, func(e *vfsEvent) error {
		events = append(events, e)
		return nil
	})
	if isErrorUnsupportedPlatform(err) {
		t.Skip()
		return
	}
	if err != nil {
		t.Fatalf("jsonParseThenAppend() error = %v", err)
	}

	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}

	if events[0].Probe != "vfs_read" {
		t.Errorf("expected probe 'vfs_read', got '%s'", events[0].Probe)
	}
}

// TestJsonParseUnknownType tests jsonParseThenAppend with unknown message type
func TestJsonParseUnknownType(t *testing.T) {
	// Unknown type should be logged but not cause error
	testData := `{"type": "attached_probes", "data": {"probes": 8}}
{"type": "unknown_type", "data": {"foo": "bar"}}
{"type": "printf", "data": "ts=1234567890 fn=vfs_write tid=5678 rc=50 path='out.txt' inode=54321 offset=10 len=50"}
`
	r := strings.NewReader(testData)
	var events []*vfsEvent

	err := jsonParseThenAppend(r, func(e *vfsEvent) error {
		events = append(events, e)
		return nil
	})
	if isErrorUnsupportedPlatform(err) {
		t.Skip()
		return
	}
	if err != nil {
		t.Fatalf("jsonParseThenAppend() should not error on unknown type, got: %v", err)
	}

	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}
}

// TestJsonParseMalformed tests jsonParseThenAppend with malformed JSON
func TestJsonParseMalformed(t *testing.T) {
	testData := `{"type": "attached_probes", "data": {"probes": 8}}
{invalid json here}
`
	r := strings.NewReader(testData)

	err := jsonParseThenAppend(r, func(e *vfsEvent) error {
		return nil
	})

	// malformed JSON should cause an error
	if err == nil {
		t.Error("expected error for malformed JSON, got nil")
	}
}

// TestJsonParseMissingType tests jsonParseThenAppend with missing type field
func TestJsonParseMissingType(t *testing.T) {
	testData := `{"data": {"probes": 8}}`
	r := strings.NewReader(testData)

	err := jsonParseThenAppend(r, func(e *vfsEvent) error {
		return nil
	})

	if err == nil {
		t.Error("expected error for missing type field, got nil")
	}
}

// TestJsonParseProbesNotAttached tests error when probes <= 0
func TestJsonParseProbesNotAttached(t *testing.T) {
	testData := `{"type": "attached_probes", "data": {"probes": 0}}`
	r := strings.NewReader(testData)

	err := jsonParseThenAppend(r, func(e *vfsEvent) error {
		return nil
	})
	if isErrorUnsupportedPlatform(err) {
		t.Skip()
		return
	}
	if err == nil {
		t.Error("expected error when probes not attached, got nil")
	}

	if !strings.Contains(err.Error(), "probes not attached") {
		t.Errorf("expected 'probes not attached' error, got: %v", err)
	}
}

// TestVfsEventHandleLogfmt tests the vfsEvent.HandleLogfmt method
func TestVfsEventHandleLogfmt(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		val       string
		wantErr   bool
		checkFunc func(e *vfsEvent) bool
	}{
		{
			name:      "parse timestamp",
			key:       "ts",
			val:       "1234567890",
			wantErr:   false,
			checkFunc: func(e *vfsEvent) bool { return e.Timestamp == 1234567890 },
		},
		{
			name:      "parse function name",
			key:       "fn",
			val:       "vfs_read",
			wantErr:   false,
			checkFunc: func(e *vfsEvent) bool { return e.Probe == "vfs_read" },
		},
		{
			name:      "parse tid",
			key:       "tid",
			val:       "9876",
			wantErr:   false,
			checkFunc: func(e *vfsEvent) bool { return e.Tid == 9876 },
		},
		{
			name:      "parse return code",
			key:       "rc",
			val:       "-1",
			wantErr:   false,
			checkFunc: func(e *vfsEvent) bool { return e.ReturnValue == -1 },
		},
		{
			name:      "parse path",
			key:       "path",
			val:       `'test.txt'`,
			wantErr:   false,
			checkFunc: func(e *vfsEvent) bool { return e.Path == "test.txt" },
		},
		{
			name:      "parse inode",
			key:       "inode",
			val:       "12345",
			wantErr:   false,
			checkFunc: func(e *vfsEvent) bool { return e.Inode == 12345 },
		},
		{
			name:      "parse offset",
			key:       "offset",
			val:       "1024",
			wantErr:   false,
			checkFunc: func(e *vfsEvent) bool { return e.Offset == 1024 },
		},
		{
			name:      "parse length",
			key:       "len",
			val:       "4096",
			wantErr:   false,
			checkFunc: func(e *vfsEvent) bool { return e.Length == 4096 },
		},
		{
			name:    "unknown field",
			key:     "unknown",
			val:     "value",
			wantErr: true,
		},
		{
			name:    "invalid timestamp",
			key:     "ts",
			val:     "not-a-number",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var e vfsEvent
			err := e.HandleLogfmt([]byte(tt.key), []byte(tt.val))

			if (err != nil) != tt.wantErr {
				t.Errorf("HandleLogfmt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.checkFunc != nil && !tt.checkFunc(&e) {
				t.Errorf("HandleLogfmt() did not set expected value for key %s", tt.key)
			}
		})
	}
}

// TestAppendRowCallback tests that appendRow callback errors are propagated
func TestAppendRowCallback(t *testing.T) {
	testData := `{"type": "attached_probes", "data": {"probes": 8}}
{"type": "printf", "data": "ts=1234567890 fn=vfs_read tid=1234 rc=100 path='test.txt' inode=12345 offset=0 len=100"}
`
	expectedErr := fmt.Errorf("callback error")

	// Test simpleParseThenAppend
	r := strings.NewReader(testData)
	err := simpleParseThenAppend(r, func(e *vfsEvent) error {
		return expectedErr
	})

	if err == nil {
		t.Error("simpleParseThenAppend() expected callback error to propagate, got nil")
	}

	// Test jsonParseThenAppend
	r = strings.NewReader(testData)
	err = jsonParseThenAppend(r, func(e *vfsEvent) error {
		return expectedErr
	})

	if err == nil {
		t.Error("jsonParseThenAppend() expected callback error to propagate, got nil")
	}
}
