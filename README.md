# btrdupd - BTRFS Deduplication Daemon

> **ALPHA SOFTWARE** - This software hasn't been tested outside of a test environment yet. In theory it shouldn't be dangerous since all deduplication is done through duperemove, which is well-tested and safe in operation. However, this doesn't prevent btrdupd from entering infinite loop scenarios, database size bloat, failing to deduplicate some files, or other unexpected glitches. **Use at your own risk. You have been warned.**

A daemon that continuously finds and deduplicates **identical files** on BTRFS, including copies in snapshots. Unlike block-level tools, btrdupd focuses on whole-file deduplication - if two files are byte-for-byte identical, they will be deduplicated.

## How It Differs from Other Tools

**vs duperemove**: duperemove finds duplicate blocks within and across files (partial matching). btrdupd finds duplicate *files* (exact matching). duperemove is also one-shot - you run it, it deduplicates, done. btrdupd runs continuously, tracking changes via BTRFS generation numbers, and automatically handles snapshots which duperemove doesn't address.

**vs bees**: bees does best-effort block-level deduplication - it doesn't promise complete deduplication. bees struggles with certain workloads: large static files (a 40GB video consumes hash table space but rarely has duplicates) and many small duplicate files (like source code) which may be deprioritized. bees also uses significantly more memory. btrdupd takes a different approach: it hashes every file and guarantees that all identical files will eventually be deduplicated.

## How It Works

1. **Tracks changes** using BTRFS generation numbers (`btrfs subvolume find-new`) - no inotify overhead
2. **Groups files by size** - only same-size files can be duplicates
3. **Hashes with xxhash64** - fast, and collisions don't matter because...
4. **Deduplicates via duperemove** - which verifies byte-for-byte equality before any operation
5. **Updates snapshots** - uses reflink to propagate deduplication to read-only snapshots

The key insight: BTRFS only reclaims space when *all* copies share extents. If you have 10 snapshots with copies of a file, deduplicating only the live copy saves nothing. btrdupd handles this automatically.

## Safety

- **duperemove verifies everything** - our fast hashing is just for candidate selection
- **Extent verification** - confirms deduplication actually occurred before updating state
- **Volatile file detection** - files that change frequently are automatically skipped
- **Low priority** - runs at nice 19 / idle IO to avoid impacting your system

## Quick Start

```bash
# One-time deduplication
sudo ./btrdupd.py /mnt/btrfs-volume

# Dry run to see what would happen
sudo ./btrdupd.py --dry-run /mnt/btrfs-volume

# Run as daemon
sudo ./btrdupd.py --daemon /mnt/btrfs-volume

# Check status
sudo ./btrdupd.py --status /mnt/btrfs-volume
```

## Installation

```bash
# 1. Install system dependencies (Ubuntu/Debian)
sudo apt install duperemove btrfs-progs python3-pip git

# 2. Clone the repository
git clone https://github.com/hansonxyz/btrdupd.git
cd btrdupd

# 3. Install Python dependencies
pip install .

# 4. Run (requires root)
sudo ./btrdupd.py /mnt/btrfs-volume
```

**Requirements:** Linux with BTRFS, Python 3.8+, root permissions

## Configuration

Configuration is optional. btrdupd creates `.btrdupd/config.toml` with sensible defaults on first run.

```toml
# Minimum file size (bytes) - smaller files are ignored
min_file_size = 32768

# Paths to exclude
ignore_paths = [
    "*/docker/overlay2/*",
    "*/.cache/*"
]

# Include snapshots in deduplication
dedup_snapshots = true

# Process priority (19 = lowest)
cpu_nice = 19
io_nice_class = 3

# Time window for deduplication (24-hour format)
dedup_hours_start = 0
dedup_hours_end = 23
```

## Multi-Volume Support

```bash
# Monitor multiple volumes (processed round-robin)
sudo ./btrdupd.py /mnt/vol1 /mnt/vol2 /mnt/vol3

# Each volume maintains its own database at .btrdupd/btrdupd.db
```

## Systemd Service

```bash
# Install as systemd service
sudo ./btrdupd.py /mnt/btrfs-volume --service install
sudo ./btrdupd.py --service start

# View logs
sudo ./btrdupd.py --service logs
```

## License

MIT - © HansonXyz 2025
