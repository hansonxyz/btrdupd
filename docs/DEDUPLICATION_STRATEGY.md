# BTRFS File Filtering and Snapshot Deduplication Strategy
## Comprehensive Technical Specification v2.0

## 1. Strategic Overview

### Primary Deduplication Methods
1. **Main Volume & Read/Write Subvolumes**: Use `duperemove` for FIDEDUPERANGE-based deduplication
2. **Read-Only Snapshots**: Use reflink copying (`cp --reflink=auto`) after main volume deduplication
3. **Reference File Management**: Maintain reference files for large deduplication groups

### Key Design Principles
- **Real-time accuracy over caching**: Prioritize current data over performance optimization
- **Simplicity over premature optimization**: Accept ~10% overhead to avoid complexity
- **Comprehensive space reclamation**: Must deduplicate ALL instances (snapshots included) for space savings

## 2. File Filtering Strategy

### 2.1 File Type Filtering
**Included:**
- Regular files only (S_IFREG)
- Compressed files (`+c` chattr attribute)

**Excluded:**
- Block devices, character devices, FIFOs, sockets, swap files
- Symbolic links (potential infinite loops)
- Directories (not applicable to deduplication)

### 2.2 File Attribute Filtering (chattr)
**Performance Analysis:**
- **`lsattr` cost**: ~0.1-0.5ms per file
- **Full scan impact**: 100K files = 10-50 seconds additional time
- **Overhead percentage**: ~5-15% of total scan time
- **Decision**: Real-time calls preferred over caching complexity

**Excluded Attributes:**
- `+C` (NOCOW) - Copy-on-Write explicitly disabled
- `+i` (Immutable) - Cannot be modified  
- `+a` (Append-only) - Typically log files
- `+s` (Secure deletion) - Security-sensitive files
- `+u` (Undeletable) - System-critical files

### 2.3 Volume Boundary Enforcement
**Real-time Device Checking:**
- Use `stat().st_dev` to detect filesystem boundaries
- **Performance**: Marginal cost (~0.1ms per file)
- **Rationale**: Directory traversal already performs `stat()` calls

**Excluded Locations:**
- System pseudo-filesystems: `/proc`, `/sys`, `/dev`, `/run`, `/tmp`
- Different mountpoints (detected via device ID comparison)
- Files outside target BTRFS volume path

**Implementation:**
```python
def _is_different_mountpoint(self, file_path):
    volume_stat = self.volume_path.stat()
    file_stat = file_path.parent.stat()
    return volume_stat.st_dev != file_stat.st_dev
```

## 3. Subvolume and Snapshot Management

### 3.1 Subvolume Classification
- **Main Volume**: Root subvolume (ID 5 or no parent_uuid)
- **Read/Write Subvolumes**: Regular subvolumes for active data
- **Read-Only Snapshots**: Point-in-time copies for backup/versioning

### 3.2 Dynamic Snapshot Discovery
**Real-time Requirements:**
- Snapshots created every 15 minutes (common scenario)
- Missing new snapshots = wasted deduplication effort
- **Space reclamation reality**: Must deduplicate ALL instances for space savings

**Implementation:**
- Query `btrfs subvolume list` for each deduplication operation (~50-200ms)
- Cache snapshot list only during single dedup operation
- No persistent snapshot caching (filesystem changes constantly)

### 3.3 Root Volume Snapshot Identification
```python
def _get_root_volume_snapshots(self, all_subvolumes):
    # Find root volume (ID 5 or no parent_uuid)
    root_volume = next(s for s in all_subvolumes 
                      if s['id'] == 5 or not s.get('parent_uuid'))
    
    # Find snapshots of root volume only
    return [s for s in all_subvolumes 
            if s['read_only'] and s.get('parent_uuid') == root_volume['uuid']]
```

## 4. Deduplication Engine Architecture

### 4.1 Main Volume Deduplication (duperemove)
**Process:**
1. Check file attributes and defer if problematic
2. Find size twins among non-deferred files
3. Hash files with potential duplicates
4. Select reference file (prefer already-deduplicated)
5. Execute `duperemove` with reference file first
6. Record results in database

**Reference File Selection Priority:**
1. Cached reference (if valid and exists)
2. Already-deduplicated file (preserves extents)
3. Oldest file by discovery time

### 4.2 Snapshot Deduplication (reflink-based)
**Triggered After:** Successful main volume deduplication with extent changes

**Critical Safety Feature - Extent Verification:**
Due to potential hash collisions, we verify actual deduplication success by checking extent changes:
1. **Before duperemove**: Capture extent info for all files
2. **After duperemove**: Check if extents changed (indicating successful dedup)
3. **Snapshot processing**: Only update snapshots where the file has the SAME extent as the pre-dedup original

**Process Flow:**
1. **Extent Capture**: Record pre-dedup extents using `filefrag -v`
2. **Deduplication**: Run duperemove on main volume files
3. **Success Verification**: Compare pre/post extents to confirm dedup occurred
4. **Discovery**: Get current list of root volume snapshots
5. **Path Matching**: Find files at identical relative paths in snapshots
6. **Extent Verification**: Verify snapshot file has same extent as pre-dedup main file
7. **Reflink Update**: Use `cp --reflink=auto --preserve=timestamps,ownership,mode`
8. **State Management**: Temporarily make snapshots writable if needed
9. **Tracking**: Record reflink operations separately from main dedup

**Example Scenario:**
```
Pre-dedup state:
  Main: /data/a.iso (extent: 1000-2000)
        /data/b.iso (extent: 3000-4000)
  Snap: /snap1/data/b.iso (extent: 3000-4000)  <- Same as main pre-dedup

After duperemove:
  Main: /data/a.iso (extent: 1000-2000)  <- Reference, unchanged
        /data/b.iso (extent: 1000-2000)  <- Now shares extent with a.iso
  
Snapshot update:
  - Check /snap1/data/b.iso extent (3000-4000)
  - Matches pre-dedup b.iso extent ✓
  - Safe to reflink copy from deduplicated b.iso
  - Result: /snap1/data/b.iso (extent: 1000-2000)
```

**Safety Example - Preventing Corruption:**
```
If snapshot had different extent (e.g., 5000-6000), we would NOT update it
because it represents different data, despite having the same path.
```

### 4.3 Large Group Handling
**Batching Strategy:**
- **Threshold**: >10 files triggers smart strategy
- **Batch Size**: 50 files per `duperemove` call
- **Command Line Limit**: 1MB conservative limit
- **Reference Preservation**: Always place reference file first

## 5. Database Schema v2

### 5.1 Enhanced Tables
**files table additions:**
```sql
deferred INTEGER DEFAULT 0,        -- Permanently skip file
deferred_reason TEXT,              -- Why deferred
deferred_at REAL,                  -- When deferred
subvolume_id INTEGER               -- Which subvolume contains file
```

**dedup_history additions:**
```sql
operation_type TEXT DEFAULT 'main_volume',  -- 'main_volume' or 'snapshot_reflink'
target_subvolume_id INTEGER                 -- Which snapshot was updated
```

**New dedup_references table:**
```sql
CREATE TABLE dedup_references (
    hash TEXT PRIMARY KEY,
    file_id INTEGER NOT NULL,
    selected_at REAL NOT NULL,
    FOREIGN KEY (file_id) REFERENCES files(id)
);
```

### 5.2 Performance Optimization Indexes
**Critical indexes for query performance:**
- `idx_files_deferred_size` on `(deferred, size)` - Size twin queries
- `idx_queue_status_scheduled` on `(status, scheduled_for, priority DESC, created_at ASC)` - Task selection
- `idx_dedup_group_success` on `(group_id, success)` - Dedup status queries
- `idx_subvolumes_type` on `(subvolume_type)` - Snapshot filtering

## 6. Sequential Processing Strategy (v3.0)

### 6.1 Processing Architecture
**Key Change**: Moved from task-queue to sequential phase-based processing

**Volume Processing Loop:**
1. For each volume in sequence:
   - Phase 1: BTRFS change detection (generation tracking)
   - Phase 2: Full scan check (monthly or as needed)
   - Phase 3: Deduplication processing (FIFO, time-bounded)
2. Sleep only if no work done across ALL volumes

**Benefits:**
- Fair resource allocation across volumes
- Predictable behavior and debugging
- No volume can monopolize resources
- Simpler architecture without task queues

### 6.2 FIFO Deduplication Ordering
**Selection Query:**
```sql
SELECT * FROM files 
WHERE possibly_duplicated = 1 
  AND size >= ?
ORDER BY mtime ASC 
LIMIT 1
```

**Rationale**: Oldest files first ensures all files eventually processed

### 6.3 Time Boundaries
- **Per-volume dedup time**: 10 minutes maximum
- **Time windows**: Configurable hours (e.g., 22:00-06:00)
- **Adaptive sleep**: 5 minutes only when no work done

## 7. Performance Analysis and Trade-offs

### 7.1 Syscall Overhead Analysis
| Operation | Cost per File | 100K Files Impact | Decision |
|-----------|---------------|-------------------|----------|
| `lsattr` | 0.1-0.5ms | 10-50 seconds | Accept overhead for simplicity |
| `stat()` device check | ~0.1ms | ~10 seconds | Acceptable (already doing stat) |
| `btrfs subvolume list` | 50-200ms per dedup | N/A | Accept for real-time accuracy |
| Batch DB operations | ~0.01ms | ~1 second | Significant improvement |

### 7.2 Caching vs Real-time Trade-offs
**Decisions Made:**
1. **No chattr caching**: Simplicity > 10% performance gain
2. **No persistent snapshot caching**: Real-time accuracy > stale data risk
3. **No volume boundary caching**: Filesystem changes require real-time checks

## 8. Implementation Quality Standards

### 8.1 Error Handling
- **Graceful degradation**: Continue operation despite failures
- **Atomic operations**: Reflink copying is atomic
- **State restoration**: Always restore snapshot read-only status
- **Resource cleanup**: Properly close files and connections

### 8.2 Logging and Monitoring
**Comprehensive tracking:**
```python
# Example logging detail levels
logger.info(f"Skipping file: {path} (NOCOW attribute)")
logger.debug(f"Found {count} root volume snapshots")
logger.info(f"Updated {count} snapshot files via reflink")
```

### 8.3 Configuration Management
**Real-time configuration:**
- CLI arguments override config file
- Per-volume configuration support
- Dynamic pattern matching (exclude/include)

## 9. Testing and Validation

### 9.1 Comprehensive Test Scenarios
1. **File Filtering**: Verify all attribute types are properly filtered
2. **Volume Boundaries**: Test mountpoint detection and system directory exclusion
3. **Snapshot Discovery**: Validate real-time snapshot enumeration
4. **Reflink Operations**: Verify attribute preservation and state management
5. **Large Groups**: Test batching limits and reference file handling

### 9.2 Performance Validation
- **Full scan timing**: Measure attribute checking overhead
- **Snapshot discovery**: Verify acceptable real-time performance
- **Database queries**: Validate index effectiveness with query plans

## 10. Operational Considerations

### 10.1 Space Reclamation Requirements
**Critical insight**: Space is only reclaimed when ALL file instances are deduplicated, including every snapshot copy. Missing even one snapshot copy means zero space savings.

### 10.2 Maintenance Operations
- **Database vacuum**: Every 30 days
- **Dedup status reset**: Every 30 days (catches fragmentation)
- **Reference file cleanup**: Automatic when files are deleted

This comprehensive strategy prioritizes correctness, real-time accuracy, and complete space reclamation while maintaining code simplicity and operational reliability.