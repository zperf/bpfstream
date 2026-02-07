# bpfstream

Fast bpftrace event stream processor, write records to DuckDB

## Installation

```bash
go install github.com/fanyang89/bpfstream@latest
```

## Usage

### vfs raw

Import raw VFS events from bpftrace into DuckDB:

```bash
# From stdin
sudo bpftrace scripts/vfs-raw.bt --format json | bpfstream vfs raw --dsn output.ddb --table vfs_events

# From file
bpfstream vfs raw -i recording.ndjson --dsn output.ddb --table vfs_events
```

### vfs count

Aggregate VFS operation counts from bpftrace:

```bash
# Table format (default)
sudo bpftrace scripts/vfs-count.bt --format json | bpfstream vfs count

# JSON format
bpfstream vfs count -i recording.ndjson --format json

# CSV format
bpfstream vfs count -i recording.ndjson --format csv

# Live mode - print each interval as it arrives
sudo bpftrace scripts/vfs-count.bt --format json | bpfstream vfs count --live
```

Output formats:
- `table` (default): Aligned columns with totals
- `json`: JSON object with all fields
- `csv`: CSV with Operation,Count columns

## Benchmark

```
linux-amd64, cpu: AMD Ryzen 7 9700X 8-Core Processor

BenchmarkImportFromBpf
elapsed_ns=1104481628/4 rows=321039
-> ns_per_row=860, 1240694 ops
```
