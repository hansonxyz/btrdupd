#  btrdupd - BTRFS Deduplication Daemon

  (COMING SOON)

  A lightweight, continuous file deduplication daemon for BTRFS filesystems that runs with minimal system impact.

 ##  Overview

  btrdupd is a daemon that continuously and passively deduplicates duplicate data on BTRFS filesystems.

  btrdupd takes a file-based, opinionated approach to deduplication. The strategy involves indexing the filesystem into a SQLite database, then monitoring for changes over time using the BTRFS copy-on-write (COW) transaction log. Passively over time, files with identical sizes in the filesystem index are hashed, and when duplicate files are identified,
  they are submitted to markfasheh's duperemove tool for verification and safe deduplication.

  btrdupd achieves low resource usage through careful engineering of each component. By leveraging SQLite, running with elevated ionice and cpunice settings, operating on a single processing thread, and implementing file-based rather than block-based deduplication, btrdupd can run continuously with minimal RAM, CPU, and disk space requirements. It
  reliably and asynchronously deduplicates all duplicate files, even on busy filesystems with numerous devices, sockets, snapshots, volatile databases, and mounted volumes.

## Usage


## How It Works


## Key Features

- **File Based Deduplication**: Continuously finds and deduplicates identical files
- **Multi-Volume Support**: Monitor multiple BTRFS volumes with a single daemon
- **Low System Impact**: Runs with lowest CPU/IO priority (nice 19, idle IO class)
- **Fast Snapshot Deduplication**: Automatically finds and deduplicates identical files in snapshots
- **Safe Operation**: Uses `duperemove` for verified, safe deduplication
- **Operational Modes**: One-time processing or continuous daemon mode

## How It Works

### For Administrators

btrdupd operates in two modes:

**One-Time Mode** (default):
1. Scans for new/changed files since last run using BTRFS generation tracking
2. Processes all pending deduplication tasks efficiently
3. Exits when complete - perfect for cron jobs

**Daemon Mode** (`--daemon`):
1. Runs continuously in the background
2. Monitors filesystem changes using BTRFS generation numbers
3. Provides socket interface for real-time status queries
4. Handles multiple volumes fairly in round-robin fashion

### Smart Deduplication Process

1. **Size Matching**: Only files with identical sizes are considered
2. **Stability Check**: New files wait 5 minutes to ensure they're stable
3. **Smart Hashing**: Only hash files that have potential duplicates
4. **Extent Checking**: Always verify if files are already deduplicated
5. **Reference Selection**: Choose optimal reference file for deduplication
6. **Batch Processing**: Handle large groups efficiently within system limits

### What Happens to Your Files

- **New Files**: Detected within minutes and queued for processing
- **Modified Files**: Re-analyzed after they stabilize (configurable delay)
- **Deleted Files**: Automatically cleaned from the database when encountered
- **Renamed Files**: Treated as new files and reprocessed if needed

### When Full Scans Occur

Full filesystem scans happen only when necessary:
- **First run** with empty database (immediate)
- **After extended downtime** when changes can't be tracked (scheduled for 2 AM)
- **Manual request** via `--force-rescan` flag (immediate)
- **Rate limited** to maximum once per 24 hours

## Installation

```bash
# Install dependencies
apt-get install duperemove btrfs-progs python3 python3-pip

# Install btrdupd
git clone https://github.com/yourusername/btrdupd
cd btrdupd
pip3 install -r requirements.txt
```

## Usage

### Basic Commands

```bash
# One-time run on a single volume
btrdupd /mnt/btrfs-volume

# One-time run on multiple volumes
btrdupd /mnt/vol1 /mnt/vol2 /mnt/vol3

# Run as daemon monitoring multiple volumes
btrdupd --daemon /mnt/vol1 /mnt/vol2

# Dry run - see what would be deduplicated without making changes
btrdupd --dry-run /mnt/btrfs-volume

# Query running daemon status
btrdupd --daemon-status /mnt/btrfs-volume

# View detailed statistics
btrdupd --status /mnt/btrfs-volume
```

### Systemd Service

```bash
# Install as systemd service for multiple volumes
cp btrdupd.service /etc/systemd/system/
# Edit the service file to include your volumes
systemctl enable btrdupd
systemctl start btrdupd
```

Example service file for multiple volumes:
```ini
[Unit]
Description=BTRFS Deduplication Daemon
After=multi-user.target

[Service]
Type=simple
ExecStart=/usr/local/bin/btrdupd --daemon /mnt/vol1 /mnt/vol2 /mnt/vol3
Restart=on-failure
Nice=19
IOSchedulingClass=idle

[Install]
WantedBy=multi-user.target
```

## Configuration

Create `.btrdupd.conf` in the root of your BTRFS volume to customize behavior:

```ini
[DEFAULT]
# Minimum file size to consider (bytes)
# Files smaller than this are never processed
min_file_size = 8192

# How long to wait before processing new files (minutes)
# Ensures files are stable before hashing
initial_wait_minutes = 5

# Maximum wait time for frequently changing files (hours)
# Files that change often are backed off exponentially
max_backoff_hours = 72

# Include files in snapshots when deduplicating
dedup_snapshots = true

# Maximum files to submit to duperemove at once (small groups)
max_dedup_batch = 5

# Threshold for using smart deduplication strategy
large_group_threshold = 10

# Batch size for large duplicate groups
dedup_batch_size = 50

# Maximum command line length (conservative limit)
max_arg_length = 1048576

# How often to optimize the database (days)
vacuum_interval_days = 30

# How often to reset and re-verify deduplication status (days)
dedup_reset_interval_days = 30

[change_detection]
# Generation gap that triggers a full scan
max_generation_gap = 10000

# Hour to run scheduled full scans (0-23, 24-hour format)
full_scan_hour = 2

# Minimum hours between full scans
min_hours_between_full_scans = 24
```

## Understanding the Process

### File Selection Strategy

btrdupd uses an efficient strategy to minimize resource usage:

1. **Size Matching**: Only files with identical sizes are considered for deduplication
2. **Stability Check**: New files wait 5 minutes before processing to ensure they're not being actively written
3. **Smart Hashing**: Only hashes files that have potential duplicates (same size)
4. **Immediate Action**: When duplicates are found, they're deduplicated right away

### Smart Deduplication for Large Groups

When btrdupd encounters large duplicate groups (e.g., 400+ identical files), it uses an intelligent strategy:

1. **Extent Analysis**: Checks which files are already deduplicated by examining shared extents
2. **Reference File**: Selects the best reference file:
   - Prefers already-deduplicated files to preserve existing extent sharing
   - Falls back to oldest file (most stable)
   - Automatically handles reference file deletion
3. **Order Preservation**: Reference file is always first in duperemove arguments to preserve its extents
4. **Dynamic Batching**: Batches adjusted based on both file count (50) and command line length (1MB)
5. **Cached References**: Remembers reference files to efficiently handle the "401st file" scenario
6. **Minimal Operations**: New duplicates are only deduped against the reference file, not all 400+ files

This approach ensures:
- No fragmentation of the reference file
- Efficient handling of large duplicate sets
- Safe operation within system limits
- Predictable deduplication behavior

### Snapshot Deduplication

btrdupd intelligently handles BTRFS snapshots:

1. **Automatic Detection**: Finds all snapshots of monitored volumes
2. **Same-Path Matching**: Identifies files in snapshots at the same relative path
3. **Size Verification**: Confirms files have identical sizes before deduplication
4. **Read-Only Handling**: Temporarily makes snapshots writable for deduplication
5. **Extent Sharing**: Enables space savings across snapshot boundaries

Example: If `/data/file.iso` exists in both the main volume and a snapshot at `/snapshots/daily/data/file.iso`, btrdupd will automatically include both in deduplication operations.

### Multi-Volume Processing

When monitoring multiple volumes:
- Each volume maintains its own database
- Volumes are processed in round-robin fashion
- One operation is performed per volume before moving to the next
- Ensures fair processing across all monitored volumes

### Process Priority

btrdupd automatically sets:
- **CPU Nice**: 19 (lowest priority)
- **IO Nice**: Idle class (only runs when system is idle)
- Ensures minimal impact on system performance

## Database and State

btrdupd maintains separate databases for each volume:

- **Location**: `.btrdupd.db` in volume root or custom path
- **Multi-Volume**: When using `--db-path`, creates `volumename.btrdupd.db` for each volume
- **Portable**: Database stays with the volume
- **Resumable**: Can stop and restart without losing progress
- **Version Checked**: Prevents incompatible version conflicts
- **Auto-Maintained**: Database is automatically optimized monthly

### Database Limits

- **File Size**: Up to 9.2 exabytes (64-bit integer)
- **Number of Files**: Up to 18 quintillion files
- **Path Length**: No practical limit (hierarchical storage)
- **Future-Proof**: 92,000x larger than current largest files

### Periodic Maintenance

btrdupd performs automatic maintenance to ensure optimal operation:

1. **Database Vacuum** (every 30 days):
   - Reclaims space from deleted entries
   - Optimizes query performance
   - Runs automatically during idle time

2. **Deduplication Reset** (every 30 days):
   - Re-verifies all deduplication status
   - Catches extent changes or fragmentation
   - Ensures optimal space savings over time
   - Preserves file hashes, only resets dedup status

## Daemon Communication

When running in daemon mode, btrdupd provides a socket interface:

```bash
# Query daemon status for a specific volume
btrdupd --daemon-status /mnt/vol1

# Socket location: /var/run/btrdupd/<volumename>.sock
# Returns: PID, uptime, queue length, processing rate, performance metrics
```

## Performance Considerations

- **CPU Usage**: Minimal due to nice level 19 (lowest priority)
- **I/O Impact**: Uses idle I/O class to avoid affecting other operations
- **Memory Usage**: Low memory footprint with efficient database design
- **Multi-Volume**: Processes one volume at a time to minimize resource usage
- **Single-Threaded**: All operations use a single CPU core

## Monitoring and Logs

```bash
# View current status and statistics
btrdupd --status /mnt/btrfs-volume

# Enable detailed logging

### Database Design
Uses SQLite with hierarchical path storage and optimized indexes for:
- Fast size-based file grouping
- Efficient FIFO candidate selection
- Change tracking with exponential backoff
- Multi-volume state management

### Memory Efficiency
- Streaming processing with configurable batch sizes
- Hierarchical path compression reduces storage overhead
- LRU caching for frequently accessed metadata
- Transaction batching minimizes I/O overhead

## Real-World Usage

**Home Server**: Run continuously to deduplicate downloads, backups, and media files across multiple BTRFS volumes.

**Development Workstation**: One-time runs to clean up build artifacts, dependency caches, and git repositories.

**Backup Storage**: Scheduled runs to deduplicate incremental backups and snapshot chains.

**Container Host**: Automatic deduplication of container images and overlay filesystems.

## Requirements

- Linux with BTRFS filesystem
- Python 3.8+
- `duperemove` and `btrfs-progs` packages
- Root permissions (for deduplication operations)

## Performance Characteristics

- **CPU Usage**: Minimal (nice 19, only during processing)
- **Memory Usage**: ~50-100MB for typical volumes
- **I/O Impact**: Idle priority, burst during scan/deduplication
- **Storage**: ~1MB database per 100K files tracked

## Database & State

Each BTRFS volume maintains its own SQLite database at `.btrdupd/btrdupd.db` containing:
- File metadata and change tracking
- Deduplication history and statistics
- Processing queue and backoff state
- Configuration and operational metadata

Databases are portable with the volume and survive system reinstalls.

## Monitoring

```bash
# Check daemon status
btrdupd --daemon-status /mnt/volume

# View current statistics
btrdupd --status /mnt/volume

# Monitor logs
sudo btrdupd --service logs

# Real-time status (daemon mode)
# Socket available at /var/run/btrdupd/<volume>.sock
```

## Safety Features

- **Extent verification**: Confirms deduplication actually occurred before updating metadata
- **Hash collision protection**: Physical extent comparison prevents data corruption
- **Graceful degradation**: Missing files and errors are handled without disruption
- **Atomic operations**: Database transactions ensure consistency
- **Single instance**: File locking prevents multiple daemons per volume

## License

MIT License - see LICENSE file for details.

## Contributing

btrdupd is designed to be the definitive solution for BTRFS deduplication. Contributions that improve safety, efficiency, or usability are welcome.
