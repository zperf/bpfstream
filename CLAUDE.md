# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

bpfstream is a fast bpftrace event stream processor that parses NDJSON output from bpftrace and either aggregates counts or writes raw events to DuckDB.

## Build and Test Commands

```bash
# Build
go build ./...

# Run all tests
go test ./...

# Run a single test
go test -run TestEventFill ./...

# Run benchmarks (requires testdata/vfs-raw.ndjson)
go test -bench=. ./...

# Lint
golangci-lint run
```

## Architecture

### Command Structure

The CLI uses urfave/cli/v3 with subcommands:
- `main.go` - Root command definition, registers all subcommands
- `cmd_<type>.go` - Each tracing type (vfs, net, proc, mem, syscall) has count and raw subcommands

### Tracing Types

| Command | Count Operations | Raw Event Fields |
|---------|-----------------|------------------|
| vfs | create, open, read, readlink, readv, write, writev, fsync | timestamp, probe, tid, rc, path, inode, offset, length |
| net | tcp_connect/accept/close, udp_send/recv, sock_create/close | timestamp, probe, tid, comm, src/dst addr/port, bytes, protocol |
| proc | exec, fork, exit, clone | timestamp, probe, pid, ppid, tid, comm, cmdline, exit_code |
| mem | mmap, munmap, brk, page_fault | timestamp, probe, pid, tid, comm, address, size, type |
| syscall | dynamic syscall names | timestamp, pid, tid, comm, syscall_nr, name, arg0-5, ret |

### Parsing Framework

- `parser.go` - Generic NDJSON parsing:
  - `NDJSONParser` - simdjson-based streaming parser for structured JSON
  - `SimpleLineParser` - Fast line-based parser using string matching
  - Both handle common bpftrace message types (attached_probes, time, lost_events)

### Output Framework

- `output.go` - Generic output formatting with `CountOutput` supporting table/json/csv formats

### Data Flow

1. bpftrace outputs NDJSON with `{"type": "...", "data": ...}` structure
2. Parser extracts message type and delegates to handler
3. **count** commands: Aggregate into typed event structs, print summary
4. **raw** commands: Parse logfmt data, append rows to DuckDB via go-duckdb appender

### Key Dependencies

- `simdjson-go` - High-performance JSON parsing for NDJSON streams
- `go-duckdb/v2` - DuckDB driver with bulk appender support
- `logfmt` - Parse bpftrace printf output in logfmt format
- `zerolog` - Structured logging

### Test Data

- `testdata/vfs-raw.ndjson` - Sample bpftrace output for benchmarks and tests (321k rows)
- `testdata/vfs-count.ndjson` - Sample count data
