# btrdupd Algorithm Overview v1.0

This document describes the complete algorithm used by btrdupd to discover, track, and deduplicate files on BTRFS filesystems.

## Core Philosophy

btrdupd aims for **practical 99% accuracy** rather than perfect tracking. We use fast hashing (xxhash64) and file metadata for duplicate detection, trusting duperemove to verify actual byte equality before deduplication. Hash collisions are acceptable because they only cause unnecessary duperemove calls, never data corruption.

---

## 1. Startup and Initialization

### 1.1 Dependency Verification
On startup, btrdupd verifies all required tools are installed:
- `btrfs` (btrfs-progs) - filesystem operations
- `duperemove` - actual deduplication (unless dry-run mode)
- `ionice` - I/O priority control

If any dependency is missing, the program exits immediately with a clear error. There are no silent fallbacks.

### 1.2 Lock Acquisition
Each volume gets an exclusive file lock (`<db_path>.lock`) to prevent multiple instances from operating on the same volume simultaneously. If the lock cannot be acquired:
1. Check if the PID in the lock file is still running
2. If stale (process dead), remove lock and retry
3. If active, exit with error

### 1.3 Database Initialization
The SQLite database is stored at `<volume>/.btrdupd/btrdupd.db`. On first run:
1. Create database directory
2. Initialize schema (files, paths, metadata tables)
3. Set database version for future migrations

If an existing database has a newer version than the software supports, exit with error (prevents data corruption from version mismatch).

### 1.4 Process Priority
Set maximum niceness to minimize system impact:
- CPU nice level: 19 (lowest priority)
- I/O scheduling class: idle (only runs when disk is idle)

---

## 2. File Discovery

btrdupd uses two methods to discover files, depending on the situation.

### 2.1 Full Filesystem Scan
**Triggered when:**
- First run (empty database)
- Monthly periodic rescan (default: every 30 days)
- Large generation gap detected (>10,000 generations)
- Generation wraparound detected (current < last)
- Generation tracking errors

**Process:**
1. Record current BTRFS generation number at scan start
2. Walk entire directory tree recursively
3. For each file encountered:
   - Skip if smaller than minimum size (default: 4KB)
   - Skip if matches ignore patterns (e.g., `*.log`, `/tmp/*`)
   - Skip if on different device (mountpoint boundary)
   - Skip if special file type (socket, device, FIFO)
   - Check file attributes via `lsattr`:
     - Skip NOCOW (+C) files
     - Skip immutable (+i) files
     - Skip append-only (+a) files
   - Add/update file record in database with size, mtime, inode
4. Remove database records for files that no longer exist
5. Store generation number as `last_generation`
6. Vacuum database to reclaim space

**Batching:** Files are processed in batches of 100 for database efficiency.

### 2.2 Incremental Change Detection
**Used when:** Database exists and generation gap is reasonable

**Process:**
1. Get current BTRFS generation
2. Run `btrfs subvolume find-new <volume> <last_generation>`
3. Parse output to find files modified since last scan
4. For each changed file:
   - Apply same filters as full scan
   - Update database record
   - Clear hash if size changed (hash now invalid)
   - Increment change count
   - Mark as `possibly_duplicated = 1` (needs processing)
5. Update `last_generation`

**Key insight:** We only care about size and mtime changes. Generation-only changes (from dedup operations, metadata updates) are intentionally ignored to avoid reprocessing already-handled files.

---

## 3. Duplicate Detection

### 3.1 Size-First Filtering
Two files can only be duplicates if they have the same size. The database indexes files by size, making it fast to find "size twins."

**Query pattern:**
```sql
SELECT * FROM files 
WHERE size = ? AND id != ? AND hash IS NOT NULL
```

### 3.2 Hash Calculation
Files are only hashed when they have size twins (potential duplicates).

**Process:**
1. Verify file still exists and hasn't changed since database record
2. Open file and compute xxhash64 in 1MB chunks
3. After hashing, verify file didn't change during read (check mtime/size again)
4. If file changed during hash, discard result and increment change count
5. Store hash in database

**Hash storage:** xxhash64 produces a 64-bit hash stored as 16-character hex string.

### 3.3 FIFO Processing Order
Deduplication candidates are processed oldest-first by mtime:

```sql
SELECT * FROM files 
WHERE possibly_duplicated = 1 AND size >= ?
ORDER BY mtime ASC 
LIMIT 1
```

This ensures all files eventually get processed regardless of when they were discovered.

---

## 4. Deduplication Execution

### 4.1 Candidate Selection
When a file with `possibly_duplicated = 1` is selected:
1. Find all other files with same hash
2. Filter out files with special attributes (NOCOW, etc.)
3. Filter out files currently in backoff period
4. Need at least 2 files to deduplicate

### 4.2 Reference File Selection
The first file passed to duperemove is preserved as the "reference." Selection priority:
1. Already-deduplicated file (preserves existing extent sharing)
2. Oldest file by discovery time

### 4.3 Pre-Dedup Extent Capture
Before running duperemove, capture extent information for all files using `filefrag -v`. This records which physical disk blocks each file occupies.

### 4.4 Duperemove Execution
```bash
duperemove -d -r -q --dedupe-options=partial -b 4096 <reference> <file2> <file3> ...
```

- `-d` - deduplicate mode
- `-r` - recursive (though we pass specific files)
- `-q` - quiet output
- `--dedupe-options=partial` - better extent reuse, respects file order
- `-b 4096` - 4KB block size (BTRFS default)

**Batching:** Large groups (>10 files) are processed in batches of 50 to stay within command line limits (1MB max).

### 4.5 Post-Dedup Verification
After duperemove completes:
1. Re-read extent information for all files
2. Compare pre/post extents to confirm dedup occurred
3. Files with changed extents were successfully deduplicated
4. Record results in database

**Why verify?** Hash collisions could cause duperemove to be called on non-identical files. Duperemove will refuse to dedupe them, but we need to know this happened to avoid infinite retry loops.

---

## 5. Snapshot Deduplication

### 5.1 The Snapshot Problem
BTRFS only reclaims disk space when ALL copies of data are deduplicated. If you deduplicate files A and B on the main volume but snapshots still contain separate copies, you save zero space.

### 5.2 Snapshot Discovery
After successful main volume deduplication:
1. Query `btrfs subvolume list` to find all subvolumes
2. Identify root volume (ID 5 or no parent_uuid)
3. Find read-only snapshots that are children of the root volume

### 5.3 Extent-Based Verification
For each snapshot, check if files at the same path have the same extent as the pre-dedup main volume file:

```
Main volume file (pre-dedup):  extent 3000-4000
Snapshot file:                 extent 3000-4000  ← Match! Safe to update
```

If extents don't match, the snapshot file contains different data and must NOT be updated.

### 5.4 Reflink Copy Process
For verified snapshot files:
1. Record original file attributes (owner, group, permissions)
2. If snapshot is read-only, temporarily make it writable
3. Copy deduplicated file to snapshot: `cp --reflink=auto --preserve=timestamps`
4. Restore original ownership and permissions if they differed
5. Restore snapshot to read-only if it was originally read-only

### 5.5 Batched Snapshot Processing
To minimize overhead from toggling snapshot read-only status:
1. Queue snapshot updates in database table `enqueued_snapshot_copies`
2. When queue reaches threshold (default: 100 files), process batch
3. For each snapshot: make writable once, process all queued files, restore read-only once

This reduces snapshot state transitions from O(files) to O(snapshots).

---

## 6. Volatile File Handling

### 6.1 Change Detection
Files that change frequently (databases, VM images, logs) waste resources if constantly rehashed. btrdupd tracks:
- `change_count` - how many times file has changed
- `stable_since` - when file last became stable

### 6.2 Backoff Strategy
When a file changes:
1. Increment change count
2. Reset stable_since to now
3. Clear hash (now invalid)

Before rehashing, check if file has been stable long enough:
- Changed within 1 day of last hash → wait 2 days
- Changed within 2 days of last hash → wait 3 days
- Otherwise → wait 1 day minimum

### 6.3 Skip-Until Mechanism
Files that fail to hash (e.g., currently open, permission denied) get a `skip_until` timestamp. They won't be retried until that time passes.

---

## 7. Daemon Mode Operation

### 7.1 Processing Loop
In daemon mode, btrdupd runs continuously:

```
while running:
    for each volume:
        1. Check for BTRFS changes (incremental scan)
        2. Check if full scan needed (monthly, etc.)
        3. Process deduplication candidates (time-limited)
    
    if no work done across all volumes:
        sleep for scan_interval (default: 5 minutes)
```

### 7.2 Time Boundaries
- **Per-volume time limit:** 10 minutes max for deduplication phase
- **Time windows:** Configurable hours when dedup is allowed (e.g., overnight only)
- **Fair scheduling:** Each volume gets processing time before moving to next

### 7.3 Status Socket
Daemon exposes Unix socket at `/var/run/btrdupd/<volume>.sock` for status queries:
- Current task
- Queue length
- Processing rate
- Performance metrics (CPU, memory, disk I/O)

---

## 8. One-Time Mode Operation

When run without `--daemon`:
1. Process all volumes completely (no time limits)
2. Perform full scan if needed
3. Process all deduplication candidates until none remain
4. Show final statistics and exit

Useful for initial setup or periodic manual runs via cron.

---

## 9. Database Schema Summary

### Core Tables
- **files** - file metadata, hash, size, mtime, change tracking
- **paths** - hierarchical path storage (tree structure for efficiency)
- **metadata** - key-value store for settings (last_generation, db_version, etc.)
- **dedup_groups** - tracks groups of files with same hash
- **dedup_history** - log of deduplication operations
- **enqueued_snapshot_copies** - queue for batched snapshot processing
- **subvolumes** - cached subvolume information

### Key Indexes
- `(size, hash)` - fast duplicate lookup
- `(possibly_duplicated, mtime)` - FIFO candidate selection
- `(hash)` - find files by hash

### Path Tree Efficiency
Instead of storing `/home/user/documents/file.txt` as a string, we store:
```
paths: {1: (NULL, '/'), 2: (1, 'home'), 3: (2, 'user'), 4: (3, 'documents'), 5: (4, 'file.txt')}
files: {path_id: 5, ...}
```
This saves ~70% storage for typical path distributions.

---

## 10. Configuration

Configuration is read from `<volume>/.btrdupd/config.toml`:

```toml
min_file_size = 4096
ignore_paths = ["*.log", "*/cache/*", "*/docker/overlay2/*"]
dedup_snapshots = true
max_dedup_batch = 5
snapshot_batch_size = 100
scan_interval_minutes = 5
dedup_hours_start = 0
dedup_hours_end = 23
dedup_time_limit_minutes = 10
full_scan_interval_days = 30
cpu_nice = 19
io_nice_class = 3
```

Command-line arguments override config file values.

---

## 11. Error Handling Philosophy

### Crash-Safe
- Database uses WAL mode for atomic transactions
- Incomplete operations are detected and resumed on restart
- Lock files prevent corruption from multiple instances

### Graceful Degradation (Runtime Only)
- Missing files are removed from database
- Permission errors cause file to be skipped (not crash)
- Network/disk errors trigger retry with backoff

### No Silent Fallbacks (Dependencies)
- Missing required tools → immediate exit with clear error
- Never substitute a different algorithm or tool silently
- If configured for X and X isn't available, fail loudly

---

## 12. Summary: The Complete Flow

```
STARTUP
  ├─ Verify dependencies (btrfs, duperemove, ionice)
  ├─ Acquire volume lock
  ├─ Initialize/verify database
  └─ Set process priority (nice, ionice)

DISCOVERY LOOP
  ├─ IF first run OR monthly OR generation gap
  │     └─ Full filesystem scan
  │           ├─ Walk all directories
  │           ├─ Filter by size, type, attributes
  │           └─ Record files in database
  └─ ELSE
        └─ Incremental scan via btrfs find-new
              └─ Update changed files only

DEDUPLICATION LOOP (FIFO order)
  ├─ Select oldest unprocessed file
  ├─ Find size twins with same hash
  ├─ IF >= 2 candidates
  │     ├─ Capture pre-dedup extents
  │     ├─ Run duperemove
  │     ├─ Verify extent changes
  │     └─ Queue snapshot updates
  └─ Mark file as processed

SNAPSHOT BATCH PROCESSING (when queue full)
  ├─ Get all root volume snapshots
  ├─ For each snapshot
  │     ├─ Make writable if needed
  │     ├─ For each queued file
  │     │     ├─ Verify extent match
  │     │     └─ Reflink copy if safe
  │     └─ Restore read-only if needed
  └─ Clear processed queue

DAEMON SLEEP
  └─ If no work done, sleep 5 minutes
```

This cycle continues indefinitely in daemon mode, or until complete in one-time mode.
