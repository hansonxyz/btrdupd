"""
Database operations for btrdupd
Handles all SQLite operations including efficient path storage
"""

import sqlite3
import logging
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
import hashlib
import json

from constants import CURRENT_DB_VERSION

logger = logging.getLogger('btrdupd.database')

SCHEMA = """
-- Path tree structure for efficient storage
CREATE TABLE IF NOT EXISTS paths (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER,
    name TEXT NOT NULL,
    FOREIGN KEY (parent_id) REFERENCES paths(id),
    UNIQUE(parent_id, name)
);
CREATE INDEX IF NOT EXISTS idx_paths_parent_name ON paths(parent_id, name);

-- Files table with enhanced tracking
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    path_id INTEGER NOT NULL,
    size INTEGER,
    mtime REAL,
    hash TEXT,
    hash_checked_at REAL,              -- When hash was last calculated
    inode INTEGER,
    discovered_at REAL NOT NULL,
    size_checked_at REAL,
    stable_since REAL,
    change_count INTEGER DEFAULT 0,
    possibly_duplicated INTEGER DEFAULT 1,  -- 1 = needs dedup check, 0 = processed
    dry_run_dedup INTEGER DEFAULT 0,        -- 1 = processed in dry run, 0 = not processed
    deferred INTEGER DEFAULT 0,     -- 1 = permanently skip this file
    deferred_reason TEXT,           -- Why it was deferred
    deferred_at REAL,              -- When it was marked as deferred
    subvolume_id INTEGER,          -- Which subvolume contains this file
    skip_until REAL DEFAULT 0,     -- Skip this file until this timestamp (for open files)
    FOREIGN KEY (path_id) REFERENCES paths(id),
    FOREIGN KEY (subvolume_id) REFERENCES subvolumes(id)
);
CREATE INDEX IF NOT EXISTS idx_files_path ON files(path_id);
CREATE INDEX IF NOT EXISTS idx_files_size ON files(size);
CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);
CREATE INDEX IF NOT EXISTS idx_files_hash_checked_at ON files(hash_checked_at);
CREATE INDEX IF NOT EXISTS idx_files_stable_since ON files(stable_since);
CREATE INDEX IF NOT EXISTS idx_files_hash_backoff ON files(hash_checked_at, stable_since, mtime);
CREATE INDEX IF NOT EXISTS idx_files_deferred ON files(deferred);
CREATE INDEX IF NOT EXISTS idx_files_deferred_size ON files(deferred, size);
CREATE INDEX IF NOT EXISTS idx_files_subvolume ON files(subvolume_id);
CREATE INDEX IF NOT EXISTS idx_files_hash_not_null ON files(hash) WHERE hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_files_possibly_dup_size ON files(possibly_duplicated, size);
CREATE INDEX IF NOT EXISTS idx_files_mtime_possibly_dup ON files(mtime, possibly_duplicated) WHERE possibly_duplicated = 1;
CREATE INDEX IF NOT EXISTS idx_files_dry_run_dedup ON files(dry_run_dedup) WHERE dry_run_dedup = 0;
CREATE INDEX IF NOT EXISTS idx_files_possibly_dup_dry_run ON files(possibly_duplicated, dry_run_dedup) WHERE possibly_duplicated = 1 AND dry_run_dedup = 0;
CREATE INDEX IF NOT EXISTS idx_files_skip_until ON files(skip_until) WHERE skip_until > 0;

-- Task queue for async processing  
CREATE TABLE IF NOT EXISTS task_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL,
    task_type TEXT NOT NULL,  -- 'discovered', 'check_size', 'hash', 'dedup'
    priority INTEGER DEFAULT 0,
    created_at REAL NOT NULL,
    scheduled_for REAL NOT NULL,
    status TEXT DEFAULT 'pending',  -- 'pending', 'processing', 'complete', 'failed'
    attempts INTEGER DEFAULT 0,
    FOREIGN KEY (file_id) REFERENCES files(id),
    CHECK (task_type IN ('discovered', 'check_size', 'hash', 'dedup'))
);
CREATE INDEX IF NOT EXISTS idx_queue_status_scheduled ON task_queue(status, scheduled_for, priority DESC, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_queue_file_type_status ON task_queue(file_id, task_type, status);
CREATE INDEX IF NOT EXISTS idx_queue_file_id ON task_queue(file_id);

-- Duplicate groups
CREATE TABLE IF NOT EXISTS dedup_groups (
    id INTEGER PRIMARY KEY,
    hash TEXT NOT NULL UNIQUE,
    size INTEGER NOT NULL,
    file_count INTEGER NOT NULL,
    snapshot_files_updated INTEGER DEFAULT 0  -- Count of snapshot files updated via reflink
);
CREATE INDEX IF NOT EXISTS idx_dedup_groups_hash ON dedup_groups(hash);
CREATE INDEX IF NOT EXISTS idx_dedup_groups_waste ON dedup_groups(file_count DESC, size DESC);

-- Deduplication history with operation tracking
CREATE TABLE IF NOT EXISTS dedup_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    attempted_at REAL NOT NULL,
    success INTEGER NOT NULL,
    dry_run INTEGER DEFAULT 0,
    operation_type TEXT DEFAULT 'main_volume',  -- 'main_volume' or 'snapshot_reflink'
    target_subvolume_id INTEGER,               -- Which snapshot was updated (for reflink ops)
    FOREIGN KEY (group_id) REFERENCES dedup_groups(id),
    FOREIGN KEY (file_id) REFERENCES files(id),
    FOREIGN KEY (target_subvolume_id) REFERENCES subvolumes(id)
);
CREATE INDEX IF NOT EXISTS idx_dedup_group_success ON dedup_history(group_id, success);
CREATE INDEX IF NOT EXISTS idx_dedup_file ON dedup_history(file_id);
CREATE INDEX IF NOT EXISTS idx_dedup_operation_type ON dedup_history(operation_type);

-- Reference file management for efficient deduplication
CREATE TABLE IF NOT EXISTS dedup_references (
    hash TEXT PRIMARY KEY,
    file_id INTEGER NOT NULL,
    selected_at REAL NOT NULL,
    last_verified_at REAL,
    FOREIGN KEY (file_id) REFERENCES files(id)
);
CREATE INDEX IF NOT EXISTS idx_dedup_references_file ON dedup_references(file_id);

-- Enhanced subvolume/snapshot tracking
CREATE TABLE IF NOT EXISTS subvolumes (
    id INTEGER PRIMARY KEY,
    uuid TEXT UNIQUE NOT NULL,
    parent_uuid TEXT,
    path TEXT NOT NULL,
    generation INTEGER,
    read_only INTEGER DEFAULT 0,
    subvolume_type TEXT NOT NULL,           -- 'main', 'subvolume', 'snapshot'
    source_subvolume_uuid TEXT,             -- For snapshots, UUID of source subvolume
    discovered_at REAL NOT NULL,            -- When we first discovered this subvolume
    last_scanned_at REAL                    -- When we last scanned this subvolume
);
CREATE INDEX IF NOT EXISTS idx_subvolumes_uuid ON subvolumes(uuid);
CREATE INDEX IF NOT EXISTS idx_subvolumes_type ON subvolumes(subvolume_type);
CREATE INDEX IF NOT EXISTS idx_subvolumes_source ON subvolumes(source_subvolume_uuid);
CREATE INDEX IF NOT EXISTS idx_subvolumes_path ON subvolumes(path);

-- Metadata and configuration
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Statistics tracking
CREATE TABLE IF NOT EXISTS stats (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);

-- Activity log for debugging and recovery
CREATE TABLE IF NOT EXISTS activity_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    action TEXT NOT NULL,
    details TEXT
);
CREATE INDEX IF NOT EXISTS idx_activity_log_timestamp ON activity_log(timestamp DESC);

-- Queue for batched snapshot deduplication
CREATE TABLE IF NOT EXISTS enqueued_snapshot_copies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path TEXT NOT NULL,              -- Full path of file on main volume
    before_dedup_extents TEXT NOT NULL,   -- JSON blob of extent info before dedup
    after_dedup_extents TEXT NOT NULL,    -- JSON blob of extent info after dedup
    file_size INTEGER NOT NULL,           -- File size for tracking bytes saved
    enqueued_at REAL NOT NULL,            -- Timestamp when enqueued
    file_hash TEXT                        -- Optional: hash for verification
);
CREATE INDEX IF NOT EXISTS idx_enqueued_snapshot_copies_enqueued ON enqueued_snapshot_copies(enqueued_at);
"""

class BtrdupdDatabase:
    """SQLite database manager for btrdupd.

    Handles all database operations including:
    - Path tree storage for efficient path deduplication
    - File metadata and hash tracking
    - Deduplication group management
    - Subvolume/snapshot tracking
    - Statistics and activity logging

    The database uses a path tree structure where each path component is stored
    once and referenced by parent_id, reducing storage for files in the same
    directories.
    """

    # Threshold for permanently deferring volatile files (moved from line 816)
    PERMANENT_DEFER_THRESHOLD = 30

    # ==========================================================================
    # INITIALIZATION AND CONNECTION
    # ==========================================================================

    def __init__(self, db_path, debug_sql=False):
        """Initialize database manager.

        Args:
            db_path: Path to SQLite database file
            debug_sql: If True, log all SQL queries at DEBUG level
        """
        self.db_path = str(db_path)  # Ensure it's a string
        self.debug_sql = debug_sql
        self.conn = None
        self._path_cache = {}  # Cache for path lookups

    def connect(self):
        """Connect to database and initialize schema"""
        logger.info(f"Opening database at {self.db_path}")
        
        try:
            # Check if db_path exists as a directory (error condition)
            if os.path.isdir(self.db_path):
                logger.error(f"Database path exists as a directory: {self.db_path}")
                raise Exception(f"Database path is a directory, not a file: {self.db_path}")
            
            # Ensure parent directory exists
            db_dir = os.path.dirname(self.db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)
                logger.debug(f"Database directory: {db_dir}")
                logger.debug(f"Directory exists: {os.path.exists(db_dir)}")
                logger.debug(f"Directory writable: {os.access(db_dir, os.W_OK)}")
            
            # Try to create the database file first if it doesn't exist
            if not os.path.exists(self.db_path):
                logger.debug(f"Database file doesn't exist, creating: {self.db_path}")
                try:
                    # Touch the file
                    open(self.db_path, 'a').close()
                    logger.debug(f"Created empty database file")
                except Exception as e:
                    logger.error(f"Failed to create database file: {e}")
            
            logger.debug(f"Attempting to connect to: {self.db_path}")
            
            # Try different connection parameters
            try:
                self.conn = sqlite3.connect(
                    self.db_path,
                    isolation_level='DEFERRED'  # Use transactions
                )
                self.conn.row_factory = sqlite3.Row
            except sqlite3.OperationalError as e:
                logger.error(f"SQLite error details: {e}")
                logger.error(f"Database path type: {type(self.db_path)}")
                logger.error(f"Database path repr: {repr(self.db_path)}")
                # Try again with explicit string conversion
                self.conn = sqlite3.connect(
                    str(self.db_path),
                    isolation_level='DEFERRED'
                )
                self.conn.row_factory = sqlite3.Row
            
            # Skip WAL mode for now - might be causing issues
            # self.execute("PRAGMA journal_mode=WAL")
            self.execute("PRAGMA synchronous=NORMAL")
            self.execute("PRAGMA cache_size=-64000")  # 64MB cache
            self.execute("PRAGMA temp_store=MEMORY")
            
            # Initialize schema
            self._init_schema()
            
            # Check integrity
            if not self._check_integrity():
                raise Exception("Database integrity check failed")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def execute(self, query, params=None):
        """Execute a query with optional debug logging"""
        if self.debug_sql:
            logger.debug(f"SQL: {query}")
            if params:
                logger.debug(f"Params: {params}")
        
        if params:
            return self.conn.execute(query, params)
        else:
            return self.conn.execute(query)
    
    def begin_transaction(self):
        """Begin a new transaction"""
        self.execute("BEGIN")
        if self.debug_sql:
            logger.debug("Started transaction")
    
    def commit_transaction(self):
        """Commit the current transaction"""
        self.conn.commit()
        if self.debug_sql:
            logger.debug("Committed transaction")
    
    def rollback_transaction(self):
        """Rollback the current transaction"""
        self.conn.rollback()
        if self.debug_sql:
            logger.debug("Rolled back transaction")
    
    def ensure_clean_transaction_state(self):
        """Ensure we're not in a transaction by committing any pending work"""
        if self.conn.in_transaction:
            logger.debug("Committing pending transaction before BTRFS processing")
            self.conn.commit()
    
    def _init_schema(self):
        """Initialize database schema"""
        logger.debug("Initializing database schema")
        
        # First check if we have existing tables
        existing_tables = [row[0] for row in self.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )]
        
        # Check schema version first if metadata table exists
        current_version = 0
        if 'metadata' in existing_tables:
            try:
                row = self.execute(
                    "SELECT value FROM metadata WHERE key = 'db_version'"
                ).fetchone()
                current_version = int(row['value']) if row else 0
            except:
                current_version = 0
        
        # Handle schema version mismatches
        if current_version > CURRENT_DB_VERSION:
            raise Exception(f"Database schema version {current_version} is newer than code version {CURRENT_DB_VERSION}. Please update btrdupd.")
        elif current_version < CURRENT_DB_VERSION and current_version > 0:
            # Need to migrate - for now we just recreate for v1 to v2
            logger.warning(f"Database schema v{current_version} detected, upgrading to v{CURRENT_DB_VERSION}")
            logger.warning("This will delete all existing data and recreate the database")
            
            # Close connection and delete the database
            self.conn.close()
            os.remove(self.db_path)
            logger.info(f"Deleted old database at {self.db_path}")
            
            # Reconnect to create fresh database
            self.conn = sqlite3.connect(
                self.db_path,
                isolation_level='DEFERRED'
            )
            self.conn.row_factory = sqlite3.Row
            self.execute("PRAGMA synchronous=NORMAL")
            self.execute("PRAGMA cache_size=-64000")
            self.execute("PRAGMA temp_store=MEMORY")
        
        # Create schema
        logger.debug("Creating database schema")
        self.conn.executescript(SCHEMA)
        
        # Set schema version
        self.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            ('db_version', str(CURRENT_DB_VERSION))
        )
        
        # Commit schema creation
        self.commit_transaction()
            
        # Initialize other metadata if needed
        self._init_metadata()
    
    
    def _init_metadata(self):
        """Initialize metadata with defaults"""
        defaults = {
            'last_vacuum': '0',
            'last_full_scan': '0',
            'last_generation': '0'
        }
        
        for key, value in defaults.items():
            self.execute(
                "INSERT OR IGNORE INTO metadata (key, value) VALUES (?, ?)",
                (key, value)
            )
    
    def _check_integrity(self):
        """Check database integrity"""
        logger.debug("Checking database integrity")
        
        result = self.execute("PRAGMA integrity_check").fetchone()
        if result[0] != 'ok':
            logger.error(f"Database integrity check failed: {result[0]}")
            return False
        
        # Check for required tables
        tables = [row[0] for row in self.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )]
        required_tables = ['paths', 'files', 'dedup_groups', 'subvolumes', 'metadata']
        
        for table in required_tables:
            if table not in tables:
                logger.error(f"Missing required table: {table}")
                return False
        
        return True
    
    # ==========================================================================
    # PATH TREE OPERATIONS
    # ==========================================================================

    def add_path(self, file_path):
        """Add a path to the tree structure and return its ID.

        The path tree stores each path component (directory name, filename) once,
        with a parent_id reference. This deduplicates storage for files in the
        same directories.

        Args:
            file_path: Full file path to add

        Returns:
            The path_id for this path (creates path components as needed)
        """
        parts = Path(file_path).parts
        parent_id = None
        
        for part in parts:
            # Check cache first
            cache_key = (parent_id, part)
            if cache_key in self._path_cache:
                parent_id = self._path_cache[cache_key]
                continue
            
            # Check database
            row = self.execute(
                "SELECT id FROM paths WHERE parent_id IS ? AND name = ?",
                (parent_id, part)
            ).fetchone()
            
            if row:
                parent_id = row['id']
            else:
                # Insert new path component
                cursor = self.execute(
                    "INSERT INTO paths (parent_id, name) VALUES (?, ?)",
                    (parent_id, part)
                )
                parent_id = cursor.lastrowid
            
            # Update cache
            self._path_cache[cache_key] = parent_id
        
        return parent_id
    
    def get_path_id(self, file_path):
        """Get the path ID for an existing path (does not create if missing)"""
        parts = Path(file_path).parts
        parent_id = None
        
        for part in parts:
            # Check cache first
            cache_key = (parent_id, part)
            if cache_key in self._path_cache:
                parent_id = self._path_cache[cache_key]
                continue
            
            # Check database
            row = self.execute(
                "SELECT id FROM paths WHERE parent_id IS ? AND name = ?",
                (parent_id, part)
            ).fetchone()
            
            if row:
                parent_id = row['id']
                # Update cache
                self._path_cache[cache_key] = parent_id
            else:
                # Path doesn't exist
                return None
        
        return parent_id
    
    def get_path(self, path_id):
        """Reconstruct full path from path ID"""
        parts = []
        current_id = path_id
        
        while current_id is not None:
            row = self.execute(
                "SELECT parent_id, name FROM paths WHERE id = ?",
                (current_id,)
            ).fetchone()
            
            if not row:
                break
            
            parts.append(row['name'])
            current_id = row['parent_id']
        
        parts.reverse()
        return os.path.join(*parts) if parts else None
    
    # ==========================================================================
    # FILE CRUD OPERATIONS
    # ==========================================================================

    def add_file(self, file_path):
        """Add a newly discovered file to the database.

        Args:
            file_path: Full path to the file

        Returns:
            The file_id (existing or newly created)
        """
        path_id = self.add_path(file_path)
        
        # Check if file already exists
        existing = self.execute(
            "SELECT id FROM files WHERE path_id = ?",
            (path_id,)
        ).fetchone()
        
        if existing:
            return existing['id']
        
        # Insert new file record
        cursor = self.execute("""
            INSERT INTO files (path_id, discovered_at)
            VALUES (?, ?)
        """, (path_id, time.time()))
        
        return cursor.lastrowid
    
    def process_directory_batch(self, directory_path, filenames, should_skip_func):
        """Efficiently process multiple files in the same directory"""
        if not filenames:
            return {'new': 0, 'updated': 0, 'deferred': 0, 'skipped': 0}
        
        # Get directory path_id once for all files
        dir_path_id = self.add_path(str(directory_path))
        
        # Filter files first (before any database operations)
        valid_files = []
        stats = {'new': 0, 'updated': 0, 'deferred': 0, 'skipped': 0}
        
        for filename in filenames:
            filepath = directory_path / filename
            should_skip, skip_reason = should_skip_func(filepath)
            
            if should_skip:
                if skip_reason in ['NOCOW attribute set (+C)', 'Immutable attribute set (+i)', 
                                 'Append-only attribute set (+a)', 'Secure deletion attribute set (+s)', 
                                 'Undeletable attribute set (+u)']:
                    valid_files.append((filename, 'deferred', skip_reason))
                else:
                    stats['skipped'] += 1
                    # Log ignored files at debug level
                    if skip_reason and skip_reason.startswith('Ignored by pattern:'):
                        logger.debug(f"Skipping file {filepath}: {skip_reason}")
            else:
                valid_files.append((filename, 'normal', None))
        
        if not valid_files:
            return stats
        
        # Batch lookup: find existing files in this directory
        file_paths = [str(directory_path / filename) for filename, _, _ in valid_files]
        path_ids = [self.add_path(fp) for fp in file_paths]  # This could be optimized further
        
        existing_files = {}
        if path_ids:
            placeholders = ','.join('?' * len(path_ids))
            rows = self.execute(f"""
                SELECT id, path_id, size, mtime, hash FROM files 
                WHERE path_id IN ({placeholders})
            """, path_ids).fetchall()
            
            existing_files = {row['path_id']: row for row in rows}
        
        # Separate into new files vs updates
        new_file_data = []
        update_data = []
        defer_data = []
        task_data = []
        
        for i, (filename, file_type, skip_reason) in enumerate(valid_files):
            filepath = directory_path / filename
            path_id = path_ids[i]
            
            try:
                file_stat = filepath.stat()
            except OSError:
                continue  # File disappeared
            
            if path_id in existing_files:
                # Existing file - check if changed
                existing = existing_files[path_id]
                size_changed = existing['size'] != file_stat.st_size
                time_changed = existing['mtime'] != file_stat.st_mtime
                
                if size_changed or time_changed:
                    update_data.append({
                        'id': existing['id'],
                        'size': file_stat.st_size,
                        'mtime': file_stat.st_mtime,
                        'hash': None if size_changed else existing['hash']  # Clear hash if size changed
                    })
                    stats['updated'] += 1
                    
                    # Debug logging for file updates
                    logger.debug(f"Updated existing file record #{existing['id']} for {filepath} (size: {existing['size']} → {file_stat.st_size}, mtime changed: {time_changed})")
                    
                    # Re-queue for processing if changed
                    task_data.append((existing['id'], 'check_size'))
            else:
                # New file
                new_file_data.append((path_id, file_stat.st_size, file_stat.st_mtime, time.time()))
                stats['new'] += 1
                
                # Debug logging for new files
                # logger.debug(f"Creating new file record for {filepath} (size: {file_stat.st_size})")
                
                if file_type == 'deferred':
                    defer_data.append((skip_reason,))  # Will need file_id after insert
                    stats['deferred'] += 1
        
        # Batch operations
        new_file_ids = []
        if new_file_data:
            # Batch insert new files
            new_file_ids = self._batch_insert_files(new_file_data)
            
            # Debug log the new file IDs
            logger.debug(f"Batch inserted {len(new_file_ids)} new files with IDs: {new_file_ids[0]} to {new_file_ids[-1] if new_file_ids else 'none'}")
            
            # Queue tasks for new files (except deferred ones)
            normal_new_files = new_file_ids[:-len(defer_data)] if defer_data else new_file_ids
            if normal_new_files:
                task_data.extend([(file_id, 'check_size') for file_id in normal_new_files])
        
        # Batch update existing files
        if update_data:
            self._batch_update_files(update_data)
        
        # Handle deferred files
        if defer_data and new_file_ids:
            deferred_file_ids = new_file_ids[-len(defer_data):]
            for i, (reason,) in enumerate(defer_data):
                self.defer_file(deferred_file_ids[i], reason)
        
        # Batch queue tasks
        if task_data:
            self._batch_queue_tasks(task_data, delay_minutes=5)
        
        return stats
    
    def _batch_insert_files(self, file_data):
        """Batch insert multiple files, returns list of file IDs"""
        if not file_data:
            return []
        
        placeholders = ','.join(['(?,?,?,?)'] * len(file_data))
        flat_data = [item for row in file_data for item in row]
        
        cursor = self.execute(f"""
            INSERT INTO files (path_id, size, mtime, discovered_at) 
            VALUES {placeholders}
        """, flat_data)
        
        # Return the range of IDs that were inserted
        # lastrowid returns the ID of the LAST inserted row, so we calculate first_id from it
        last_id = cursor.lastrowid
        first_id = last_id - len(file_data) + 1
        return list(range(first_id, last_id + 1))
    
    def _batch_update_files(self, update_data):
        """Batch update multiple files"""
        if not update_data:
            return
        
        # Build update query with CASE statements for efficiency
        ids = [str(item['id']) for item in update_data]
        
        size_cases = ' '.join([f"WHEN id = {item['id']} THEN {item['size']}" for item in update_data])
        mtime_cases = ' '.join([f"WHEN id = {item['id']} THEN {item['mtime']}" for item in update_data])
        hash_cases = ' '.join([f"WHEN id = {item['id']} THEN {'NULL' if item['hash'] is None else repr(item['hash'])}" for item in update_data])
        
        self.execute(f"""
            UPDATE files SET
                size = CASE {size_cases} END,
                mtime = CASE {mtime_cases} END,
                hash = CASE {hash_cases} END
            WHERE id IN ({','.join(ids)})
        """)
    
    def _batch_queue_tasks(self, task_data, delay_minutes=0):
        """Batch queue multiple tasks"""
        if not task_data:
            return
        
        scheduled_for = time.time() + (delay_minutes * 60)
        
        # Remove duplicates and existing tasks
        unique_tasks = {}
        for file_id, task_type in task_data:
            unique_tasks[(file_id, task_type)] = (file_id, task_type, 0, time.time(), scheduled_for)
        
        if unique_tasks:
            placeholders = ','.join(['(?,?,?,?,?)'] * len(unique_tasks))
            flat_data = [item for row in unique_tasks.values() for item in row]
            
            self.execute(f"""
                INSERT OR IGNORE INTO task_queue (file_id, task_type, priority, created_at, scheduled_for)
                VALUES {placeholders}
            """, flat_data)
    
    # ==========================================================================
    # TASK QUEUE OPERATIONS
    # ==========================================================================

    def queue_task(self, file_id, task_type, priority=0, delay_minutes=0):
        """Queue a task for processing.

        Args:
            file_id: ID of the file to process
            task_type: Type of task ('discovered', 'check_size', 'hash', 'dedup')
            priority: Higher priority tasks are processed first (default 0)
            delay_minutes: Minutes to wait before task becomes eligible
        """
        scheduled_for = time.time() + (delay_minutes * 60)
        
        # Check if task already exists
        existing = self.execute(
            "SELECT id FROM task_queue WHERE file_id = ? AND task_type = ? AND status = 'pending'",
            (file_id, task_type)
        ).fetchone()
        
        if not existing:
            self.execute("""
                INSERT INTO task_queue 
                (file_id, task_type, priority, created_at, scheduled_for)
                VALUES (?, ?, ?, ?, ?)
            """, (file_id, task_type, priority, time.time(), scheduled_for))
    
    def get_next_task(self):
        """Get the next task to process (oldest first within priority)"""
        now = time.time()
        
        # Get oldest task within highest priority
        row = self.execute("""
            SELECT id, file_id, task_type
            FROM task_queue
            WHERE status = 'pending' AND scheduled_for <= ?
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
        """, (now,)).fetchone()
        
        if row:
            # Mark as processing
            self.execute(
                "UPDATE task_queue SET status = 'processing' WHERE id = ?",
                (row['id'],)
            )
            
        return row
    
    # ==========================================================================
    # FILE SIZE AND CHANGE TRACKING
    # ==========================================================================

    def update_file_size(self, file_id, size, mtime):
        """Update file size and mtime, handling change detection.

        Tracks how often files change to implement backoff for volatile files.
        Files that change frequently are eventually deferred permanently.

        Args:
            file_id: ID of the file to update
            size: New file size in bytes
            mtime: New modification timestamp

        Returns:
            List of file IDs that are size twins (same size, different file)
        """
        # Get current file info
        current = self.execute("""
            SELECT size, mtime, hash, change_count, stable_since
            FROM files WHERE id = ?
        """, (file_id,)).fetchone()
        
        now = time.time()
        
        if current:
            # Check if file has changed
            if current['mtime'] != mtime or current['size'] != size:
                # File has changed
                new_change_count = min((current['change_count'] or 0) + 1, 100)
                new_stable_since = now
                
                # Clear hash if size changed (hash is invalid)
                new_hash = None if current['size'] != size else current['hash']
                
                self.execute("""
                    UPDATE files
                    SET size = ?, mtime = ?, size_checked_at = ?,
                        change_count = ?, stable_since = ?,
                        hash = ?, possibly_duplicated = 1
                    WHERE id = ?
                """, (size, mtime, now, new_change_count, new_stable_since,
                      new_hash, file_id))

                logger.debug(f"File #{file_id} marked for reprocessing - size/mtime changed (change count: {new_change_count})")

                # Check if file should be permanently deferred due to volatility
                if new_change_count >= self.PERMANENT_DEFER_THRESHOLD:
                    self.check_and_defer_volatile_file(file_id)
            else:
                # File unchanged, just update check time
                self.execute("""
                    UPDATE files 
                    SET size_checked_at = ?
                    WHERE id = ?
                """, (now, file_id))
        else:
            # New file or first update
            self.execute("""
                UPDATE files 
                SET size = ?, mtime = ?, size_checked_at = ?, stable_since = ?
                WHERE id = ?
            """, (size, mtime, now, now, file_id))
        
        # Check for size twins
        twins = self.execute("""
            SELECT id FROM files 
            WHERE size = ? AND id != ?
        """, (size, file_id)).fetchall()
        
        return [row['id'] for row in twins]
    
    # ==========================================================================
    # HASH OPERATIONS AND BACKOFF
    # ==========================================================================

    def update_file_hash(self, file_id, file_hash):
        """Update file hash and record when it was checked.

        Also triggers duplicate detection and dedup group creation if
        other files share the same hash.

        Args:
            file_id: ID of the file
            file_hash: Calculated xxhash64 hash string
        """
        self.execute("""
            UPDATE files 
            SET hash = ?, hash_checked_at = ?
            WHERE id = ?
        """, (file_hash, time.time(), file_id))
        
        # Check for duplicates
        duplicates = self.execute("""
            SELECT id, path_id FROM files 
            WHERE hash = ? AND id != ?
        """, (file_hash, file_id)).fetchall()
        
        if duplicates:
            # Create or update dedup group
            self._update_dedup_group(file_hash, file_id, duplicates)
        
        # Auto-commit hash updates
        self.conn.commit()
    
    def _calculate_backoff(self, change_count, max_backoff_hours=168):
        """Calculate exponential backoff based on change count

        Extended backoff range per GAP_ANALYSIS:
        - change_count < 5: No backoff
        - change_count 5-9: 1 hour
        - change_count 10-14: 4 hours
        - change_count 15-19: 24 hours (1 day)
        - change_count 20-24: 48 hours (2 days)
        - change_count >= 25: exponential up to max_backoff_hours (7 days)

        Files with change_count >= PERMANENT_DEFER_THRESHOLD should be
        permanently deferred instead of using backoff.

        Args:
            change_count: Number of times file has changed
            max_backoff_hours: Maximum backoff in hours (default 168 = 7 days)
        """
        if change_count < 5:
            return 0  # No backoff
        elif change_count < 10:
            return 1  # 1 hour
        elif change_count < 15:
            return 4  # 4 hours
        elif change_count < 20:
            return 24  # 1 day
        elif change_count < 25:
            return 48  # 2 days
        else:
            # Exponential backoff: 72, 96, 120, 144, 168 hours
            return min(max_backoff_hours, 72 + 24 * (change_count - 25))

    def check_and_defer_volatile_file(self, file_id):
        """Check if a file should be permanently deferred due to high change rate

        Files that have changed >= PERMANENT_DEFER_THRESHOLD times are considered
        too volatile for deduplication (e.g., VM images, databases, logs).

        Returns:
            True if file was deferred, False otherwise
        """
        file_info = self.execute("""
            SELECT change_count, deferred FROM files WHERE id = ?
        """, (file_id,)).fetchone()

        if not file_info:
            return False

        # Already deferred
        if file_info['deferred']:
            return True

        change_count = file_info['change_count'] or 0
        if change_count >= self.PERMANENT_DEFER_THRESHOLD:
            self.defer_file(file_id, f"Volatile file (changed {change_count}+ times)")
            logger.info(f"Permanently deferred file {file_id} - changed {change_count} times")
            return True

        return False

    def calculate_hash_backoff_days(self, file_id):
        """Calculate how many days to wait before re-hashing based on change frequency
        
        Uses exponential backoff based on change_count to avoid repeatedly
        hashing files that change frequently
        """
        file_info = self.execute("""
            SELECT change_count, hash_checked_at, stable_since
            FROM files
            WHERE id = ?
        """, (file_id,)).fetchone()
        
        if not file_info or not file_info['hash_checked_at']:
            return 0  # No previous hash, can hash immediately
        
        # Use exponential backoff based on how often the file changes
        change_count = file_info['change_count'] or 0
        backoff_hours = self._calculate_backoff(change_count)
        
        # Convert hours to days (minimum 1 day for any backoff)
        if backoff_hours == 0:
            return 0
        elif backoff_hours < 24:
            return 1
        else:
            return backoff_hours / 24
    
    def should_hash_file(self, file_id, current_mtime):
        """Check if we should hash a file based on backoff rules
        
        Returns: (should_hash, reason)
        """
        file_info = self.execute("""
            SELECT mtime, hash, hash_checked_at, stable_since
            FROM files
            WHERE id = ?
        """, (file_id,)).fetchone()
        
        if not file_info:
            return False, "File not found"
        
        now = time.time()
        
        # File changed since we have info
        if file_info['mtime'] != current_mtime:
            return False, "File has changed, needs update first"
        
        # No hash yet
        if not file_info['hash']:
            return True, "No hash calculated yet"
        
        # Calculate hash-specific backoff
        backoff_days = self.calculate_hash_backoff_days(file_id)
        if backoff_days > 0:
            min_wait_seconds = backoff_days * 86400
            if file_info['hash_checked_at']:
                time_since_hash = now - file_info['hash_checked_at'] 
                if time_since_hash < min_wait_seconds:
                    wait_until = file_info['hash_checked_at'] + min_wait_seconds
                    return False, f"Hash backoff: wait {backoff_days} days (until {time.ctime(wait_until)})"
        
        # Check if file has been stable for the required period
        if file_info['stable_since']:
            stable_duration = now - file_info['stable_since']
            required_stable_days = backoff_days if backoff_days > 0 else 0
            required_stable_seconds = required_stable_days * 86400
            
            if stable_duration < required_stable_seconds:
                stable_until = file_info['stable_since'] + required_stable_seconds
                return False, f"File must be stable for {required_stable_days} days (until {time.ctime(stable_until)})"
        
        return True, "OK to hash"
    
    def handle_hash_failure(self, file_id, reason="Hash failed"):
        """Handle when hashing fails (e.g., file changed during hash)"""
        # Increment change count and reset stable_since
        current = self.execute("""
            SELECT change_count FROM files WHERE id = ?
        """, (file_id,)).fetchone()
        
        if current:
            new_change_count = min((current['change_count'] or 0) + 1, 100)
            now = time.time()
            
            self.execute("""
                UPDATE files
                SET change_count = ?, stable_since = ?
                WHERE id = ?
            """, (new_change_count, now, file_id))
            
            logger.debug(f"Hash failed for file {file_id}: {reason}, "
                       f"change count: {new_change_count}")
    
    # ==========================================================================
    # DEDUPLICATION GROUP OPERATIONS
    # ==========================================================================

    def _update_dedup_group(self, file_hash, file_id, duplicates):
        """Update or create deduplication group.

        Called when files with matching hashes are detected. Creates a dedup
        group entry to track the duplicate set.

        Args:
            file_hash: The shared hash value
            file_id: ID of the file that triggered detection
            duplicates: List of other files with the same hash
        """
        # Get file info
        file_info = self.execute(
            "SELECT size FROM files WHERE id = ?",
            (file_id,)
        ).fetchone()
        
        if not file_info:
            return
            
        size = file_info['size']
        file_count = len(duplicates) + 1  # Include the current file
        
        # Create or update group
        existing = self.execute(
            "SELECT id FROM dedup_groups WHERE hash = ?",
            (file_hash,)
        ).fetchone()
        
        if existing:
            self.execute("""
                UPDATE dedup_groups 
                SET file_count = ?
                WHERE id = ?
            """, (file_count, existing['id']))
            group_id = existing['id']
        else:
            cursor = self.execute("""
                INSERT INTO dedup_groups (hash, size, file_count)
                VALUES (?, ?, ?)
            """, (file_hash, size, file_count))
            group_id = cursor.lastrowid
        
        logger.info(f"Found {file_count} duplicates for hash {file_hash[:16]}... (size: {size})")
        
        # Queue deduplication task
        self.queue_task(file_id, 'dedup', priority=20)
    
    def get_file_path(self, file_id):
        """Get full path for a file ID"""
        row = self.execute(
            "SELECT path_id FROM files WHERE id = ?",
            (file_id,)
        ).fetchone()
        
        if row:
            return self.get_path(row['path_id'])
        return None
    
    def get_file_by_path(self, file_path):
        """Get file info by path"""
        path_id = self.add_path(file_path)
        row = self.execute(
            "SELECT * FROM files WHERE path_id = ?",
            (path_id,)
        ).fetchone()
        return row
    
    def remove_file(self, file_id):
        """Remove file from database (lazy deletion)"""
        # Get path for logging
        path = self.get_file_path(file_id)
        
        # Remove from task queue
        self.execute(
            "DELETE FROM task_queue WHERE file_id = ?",
            (file_id,)
        )
        
        # Remove from files
        self.execute(
            "DELETE FROM files WHERE id = ?",
            (file_id,)
        )
        
        if path:
            logger.debug(f"Removed deleted file from database: {path}")
    
    def get_files_in_directory(self, directory_path):
        """Get all files in database for a specific directory"""
        # Get the path_id for this directory
        dir_path_id = self.get_path_id(str(directory_path))
        if not dir_path_id:
            return []
        
        rows = self.execute("""
            SELECT f.id, f.path_id, p.name as filename
            FROM files f
            JOIN paths p ON f.path_id = p.id
            WHERE p.parent_id = ?
        """, (dir_path_id,)).fetchall()
        
        return [{'id': row['id'], 'path_id': row['path_id'], 'filename': row['filename']} for row in rows]
    
    def cleanup_directory_files(self, directory_path):
        """Remove database entries for files that no longer exist in directory"""
        db_files = self.get_files_in_directory(directory_path)
        removed_count = 0
        
        for db_file in db_files:
            file_path = directory_path / db_file['filename']
            if not file_path.exists():
                logger.debug(f"Removing deleted file from database: {file_path}")
                self.remove_file(db_file['id'])
                removed_count += 1
        
        return removed_count
    
    def mark_task_complete(self, task_id):
        """Mark task as complete"""
        self.execute(
            "UPDATE task_queue SET status = 'complete' WHERE id = ?",
            (task_id,)
        )
    
    def mark_task_failed(self, task_id):
        """Mark task as failed"""
        self.execute("""
            UPDATE task_queue 
            SET status = 'failed', attempts = attempts + 1
            WHERE id = ?
        """, (task_id,))
    
    # ==========================================================================
    # METADATA OPERATIONS
    # ==========================================================================

    def get_metadata(self, key, default=None):
        """Get metadata value.

        Args:
            key: Metadata key to retrieve
            default: Value to return if key doesn't exist

        Returns:
            The metadata value as string, or default
        """
        row = self.execute(
            "SELECT value FROM metadata WHERE key = ?",
            (key,)
        ).fetchone()
        
        return row['value'] if row else default
    
    def set_metadata(self, key, value):
        """Set metadata value"""
        self.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            (key, str(value))
        )
        # Auto-commit metadata changes
        self.conn.commit()
    
    def get_files_by_hash(self, file_hash):
        """Get all files with a given hash"""
        rows = self.execute("""
            SELECT id, path_id, size, mtime, inode
            FROM files
            WHERE hash = ?
        """, (file_hash,)).fetchall()
        
        results = []
        for row in rows:
            path = self.get_path(row['path_id'])
            if path:
                results.append({
                    'id': row['id'],
                    'path': path,
                    'size': row['size'],
                    'mtime': row['mtime'],
                    'inode': row['inode']
                })
        
        return results
    
    def get_dedup_batch(self, file_hash, max_batch=5):
        """Get a batch of files to deduplicate"""
        files = self.get_files_by_hash(file_hash)
        return files[:max_batch]
    
    # ==========================================================================
    # SUBVOLUME AND SNAPSHOT OPERATIONS
    # ==========================================================================

    def add_subvolume(self, uuid, path, parent_uuid=None, generation=None, read_only=False):
        """Add or update subvolume information.

        Args:
            uuid: Unique identifier of the subvolume
            path: Mount path of the subvolume
            parent_uuid: UUID of parent subvolume (for snapshots)
            generation: BTRFS generation number
            read_only: Whether subvolume is read-only
        """
        self.execute("""
            INSERT OR REPLACE INTO subvolumes 
            (uuid, parent_uuid, path, generation, read_only)
            VALUES (?, ?, ?, ?, ?)
        """, (uuid, parent_uuid, path, generation, int(read_only)))
    
    def get_subvolumes(self):
        """Get all known subvolumes"""
        return self.execute("""
            SELECT id, uuid, parent_uuid, path, generation, read_only
            FROM subvolumes
            ORDER BY path
        """).fetchall()
    
    # ==========================================================================
    # ACTIVITY LOGGING
    # ==========================================================================

    def log_activity(self, action, details=None):
        """Log an activity for debugging/audit.

        Args:
            action: Short description of the action (e.g., 'scan_complete')
            details: Optional dict with additional details (stored as JSON)
        """
        self.execute(
            "INSERT INTO activity_log (timestamp, action, details) VALUES (?, ?, ?)",
            (time.time(), action, json.dumps(details) if details else None)
        )
    
    # ==========================================================================
    # STATISTICS AND REPORTING
    # ==========================================================================

    def get_statistics(self):
        """Get database statistics for status display.

        Returns:
            Dict with keys: total_files, hashed_files, pending_files,
            dedup_candidates, queued_tasks, duplicate_groups, total_duplicates,
            space_saveable, space_saved, path_nodes, last_full_scan, last_generation
        """
        stats = {}
        
        # File counts
        stats['total_files'] = self.execute(
            "SELECT COUNT(*) FROM files"
        ).fetchone()[0]
        
        stats['hashed_files'] = self.execute(
            "SELECT COUNT(*) FROM files WHERE hash IS NOT NULL"
        ).fetchone()[0]
        
        stats['pending_files'] = self.execute(
            "SELECT COUNT(*) FROM files WHERE size IS NULL"
        ).fetchone()[0]
        
        # Calculate eligible deduplication candidates
        # Files with possibly_duplicated=1 that have at least one other file with same size
        stats['dedup_candidates'] = self.execute("""
            SELECT COUNT(*)
            FROM files f1
            WHERE f1.possibly_duplicated = 1
              AND EXISTS (
                  SELECT 1 FROM files f2
                  WHERE f2.size = f1.size
                    AND f2.id != f1.id
              )
        """).fetchone()[0]
        
        # Legacy task queue stats (for compatibility)
        stats['queued_tasks'] = stats['dedup_candidates']
        
        # Duplicate stats
        stats['duplicate_groups'] = self.execute(
            "SELECT COUNT(*) FROM dedup_groups"
        ).fetchone()[0]
        
        stats['total_duplicates'] = self.execute(
            "SELECT SUM(file_count) FROM dedup_groups"
        ).fetchone()[0] or 0
        
        # Calculate space that could be saved
        stats['space_saveable'] = self.execute("""
            SELECT SUM((dg.file_count - 1) * dg.size)
            FROM dedup_groups dg
            WHERE NOT EXISTS (
                SELECT 1 FROM dedup_history dh 
                WHERE dh.group_id = dg.id AND dh.success = 1
            )
        """).fetchone()[0] or 0
        
        # Calculate space already saved
        stats['space_saved'] = self.execute("""
            SELECT SUM((dg.file_count - 1) * dg.size)
            FROM dedup_groups dg
            WHERE EXISTS (
                SELECT 1 FROM dedup_history dh 
                WHERE dh.group_id = dg.id AND dh.success = 1
            )
        """).fetchone()[0] or 0
        
        # Path tree stats
        stats['path_nodes'] = self.execute(
            "SELECT COUNT(*) FROM paths"
        ).fetchone()[0]
        
        # Get last scan info
        stats['last_full_scan'] = self.get_metadata('last_full_scan', '0')
        stats['last_generation'] = self.get_metadata('last_generation', '0')
        
        return stats
    
    def reset_dedup_status(self):
        """Reset deduplication status for all files (for periodic full scans)"""
        logger.info("Resetting deduplication status for all files")
        
        # Clear dedup history but keep file hashes
        self.execute("DELETE FROM dedup_history")
        
        # Clear cached reference files (both old and new systems)
        self.execute("DELETE FROM metadata WHERE key LIKE 'dedup_reference_%'")
        self.execute("DELETE FROM dedup_references")
        
        # Re-queue all files with hashes for dedup checking
        self.execute("""
            INSERT INTO task_queue (file_id, task_type, priority, created_at, scheduled_for)
            SELECT id, 'dedup', 20, ?, ?
            FROM files 
            WHERE hash IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM task_queue tq 
                WHERE tq.file_id = files.id 
                AND tq.task_type = 'dedup' 
                AND tq.status = 'pending'
            )
        """, (time.time(), time.time()))
        
        affected = self.conn.total_changes
        logger.info(f"Queued {affected} files for deduplication re-check")
        
        # Reset statistics
        self.execute("DELETE FROM stats")
        
        # Log the reset
        self.log_activity("dedup_status_reset", {"queued_files": affected})
        
        return affected
    
    def reset_dry_run_dedup_flags(self):
        """Reset all dry_run_dedup flags to 0 at the start of a dry run session"""
        logger.info("Resetting dry_run_dedup flags for new dry run session")
        self.execute("UPDATE files SET dry_run_dedup = 0")
        affected = self.conn.total_changes
        logger.info(f"Reset {affected} dry_run_dedup flags")
        return affected
    
    # ==========================================================================
    # DEDUPLICATION CANDIDATE SELECTION
    # ==========================================================================

    def get_next_dedup_candidate_fifo(self, min_file_size=32768, dry_run=False):
        """Get next file for deduplication using FIFO ordering (oldest mtime first).

        Only returns files that have at least one other file with the same size.
        This strategy processes files in the order they were discovered, which
        is fair but not optimal for space savings.

        Args:
            min_file_size: Minimum file size to consider
            dry_run: If True, also check dry_run_dedup flag

        Returns:
            Dict with file info including 'path', or None if no candidates
        """
        if dry_run:
            # In dry run mode, also check dry_run_dedup flag
            row = self.execute("""
                SELECT f.id, f.path_id, f.size, f.mtime, f.hash
                FROM files f
                WHERE f.possibly_duplicated = 1
                  AND f.dry_run_dedup = 0
                  AND f.size >= ?
                  AND f.deferred = 0
                  AND f.skip_until <= ?
                  AND EXISTS (
                      SELECT 1 FROM files f2 
                      WHERE f2.size = f.size 
                      AND f2.id != f.id 
                      AND f2.deferred = 0
                  )
                ORDER BY f.mtime ASC
                LIMIT 1
            """, (min_file_size, time.time())).fetchone()
        else:
            row = self.execute("""
                SELECT f.id, f.path_id, f.size, f.mtime, f.hash
                FROM files f
                WHERE f.possibly_duplicated = 1
                  AND f.size >= ?
                  AND f.deferred = 0
                  AND f.skip_until <= ?
                  AND EXISTS (
                      SELECT 1 FROM files f2 
                      WHERE f2.size = f.size 
                      AND f2.id != f.id 
                      AND f2.deferred = 0
                  )
                ORDER BY f.mtime ASC
                LIMIT 1
            """, (min_file_size, time.time())).fetchone()
        
        if row:
            result = dict(row)
            # Add the full path
            result['path'] = self.get_path(row['path_id'])
            return result
        return None
    
    def get_next_dedup_candidate_large_first(self, min_file_size=32768, dry_run=False):
        """Get next file for deduplication prioritizing larger files

        This strategy optimizes for throughput by processing large files first.
        Large duplicate groups yield more space savings per operation.
        """
        if dry_run:
            row = self.execute("""
                SELECT f.id, f.path_id, f.size, f.mtime, f.hash
                FROM files f
                WHERE f.possibly_duplicated = 1
                  AND f.dry_run_dedup = 0
                  AND f.size >= ?
                  AND f.deferred = 0
                  AND f.skip_until <= ?
                  AND EXISTS (
                      SELECT 1 FROM files f2
                      WHERE f2.size = f.size
                      AND f2.id != f.id
                      AND f2.deferred = 0
                  )
                ORDER BY f.size DESC, f.mtime ASC
                LIMIT 1
            """, (min_file_size, time.time())).fetchone()
        else:
            row = self.execute("""
                SELECT f.id, f.path_id, f.size, f.mtime, f.hash
                FROM files f
                WHERE f.possibly_duplicated = 1
                  AND f.size >= ?
                  AND f.deferred = 0
                  AND f.skip_until <= ?
                  AND EXISTS (
                      SELECT 1 FROM files f2
                      WHERE f2.size = f.size
                      AND f2.id != f.id
                      AND f2.deferred = 0
                  )
                ORDER BY f.size DESC, f.mtime ASC
                LIMIT 1
            """, (min_file_size, time.time())).fetchone()

        if row:
            result = dict(row)
            result['path'] = self.get_path(row['path_id'])
            return result
        return None

    def get_next_dedup_candidate(self, min_file_size=32768, dry_run=False, strategy='fifo'):
        """Get next file for deduplication using specified strategy

        Args:
            min_file_size: Minimum file size to consider
            dry_run: Whether in dry run mode
            strategy: 'fifo' (default, oldest first) or 'large_first' (throughput optimized)
        """
        if strategy == 'large_first':
            return self.get_next_dedup_candidate_large_first(min_file_size, dry_run)
        else:
            return self.get_next_dedup_candidate_fifo(min_file_size, dry_run)

    def find_duplicate_for_dedup(self, size, file_hash, exclude_id):
        """Find a duplicate file for deduplication"""
        row = self.execute("""
            SELECT f.id, f.path_id, f.size, f.mtime, f.hash
            FROM files f
            WHERE f.size = ?
              AND f.hash = ?
              AND f.possibly_duplicated = 0
              AND f.id != ?
            ORDER BY f.id ASC
            LIMIT 1
        """, (size, file_hash, exclude_id)).fetchone()
        
        if row:
            result = dict(row)
            # Add the full path
            result['path'] = self.get_path(row['path_id'])
            return result
        return None
    
    def mark_file_dedup_processed(self, file_id, dry_run=False):
        """Mark file as processed for deduplication"""
        if dry_run:
            # In dry run, only mark dry_run_dedup flag
            self.execute("""
                UPDATE files
                SET dry_run_dedup = 1
                WHERE id = ?
            """, (file_id,))
        else:
            self.execute("""
                UPDATE files
                SET possibly_duplicated = 0
                WHERE id = ?
            """, (file_id,))
        # Auto-commit individual file updates
        self.conn.commit()
    
    def mark_file_changed(self, file_id):
        """Mark file as changed (needs reprocessing)"""
        self.execute("""
            UPDATE files
            SET hash = NULL,
                possibly_duplicated = 1
            WHERE id = ?
        """, (file_id,))
        # Auto-commit individual file updates
        self.conn.commit()
    
    def close(self):
        """Close database connection"""
        if self.conn:
            logger.debug("Closing database connection")
            self.conn.close()
            self.conn = None
    
    def get_potential_space_savings(self):
        """Calculate potential space savings from deduplication"""
        row = self.execute("""
            SELECT SUM((file_count - 1) * size) as potential_saved
            FROM (
                SELECT COUNT(*) as file_count, size
                FROM files
                WHERE possibly_duplicated = 1
                  AND hash IS NOT NULL
                GROUP BY hash, size
                HAVING COUNT(*) > 1
            )
        """).fetchone()
        
        return row['potential_saved'] if row and row['potential_saved'] else 0
    
    def get_dry_run_ready_count(self):
        """Get count of files ready for deduplication in dry run"""
        row = self.execute("""
            SELECT COUNT(DISTINCT f1.id) as ready_count
            FROM files f1
            WHERE f1.possibly_duplicated = 1
              AND f1.dry_run_dedup = 0
              AND f1.hash IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM files f2
                  WHERE f2.hash = f1.hash
                    AND f2.size = f1.size
                    AND f2.id != f1.id
              )
        """).fetchone()
        
        return row['ready_count'] if row else 0
    
    def get_actual_space_saved(self):
        """Calculate actual space saved from completed deduplications"""
        row = self.execute("""
            SELECT SUM((file_count - 1) * size) as space_saved
            FROM (
                SELECT COUNT(*) as file_count, size
                FROM files
                WHERE possibly_duplicated = 0
                  AND hash IS NOT NULL
                GROUP BY hash, size
                HAVING COUNT(*) > 1
            )
        """).fetchone()
        
        return row['space_saved'] if row and row['space_saved'] else 0
    
    def vacuum(self):
        """Vacuum database to reclaim space"""
        logger.info("Vacuuming database...")
        self.execute("VACUUM")
        logger.info("Vacuum complete")
    
    # ==========================================================================
    # FILE DEFERRAL AND SKIP OPERATIONS
    # ==========================================================================

    def defer_file(self, file_id, reason):
        """Mark a file as permanently deferred from deduplication.

        Deferred files are excluded from all future deduplication processing.
        Used for files with special attributes (NOCOW, immutable) or volatile
        files that change too frequently.

        Args:
            file_id: ID of the file to defer
            reason: Human-readable reason for deferral
        """
        self.execute("""
            UPDATE files 
            SET deferred = 1, deferred_reason = ?, deferred_at = ?
            WHERE id = ?
        """, (reason, time.time(), file_id))
        logger.debug(f"Deferred file {file_id}: {reason}")
    
    def set_skip_until(self, file_id, skip_seconds=3600):
        """Set skip_until timestamp for a file (e.g., when it's open)
        
        Args:
            file_id: ID of the file to skip
            skip_seconds: How many seconds to skip (default 1 hour)
        """
        skip_until = time.time() + skip_seconds
        self.execute("""
            UPDATE files 
            SET skip_until = ?
            WHERE id = ?
        """, (skip_until, file_id))
        logger.debug(f"Set file {file_id} to skip until {time.ctime(skip_until)}")
    
    def process_btrfs_change(self, file_info, should_skip_func):
        """Process a single file change from BTRFS
        
        Args:
            file_info: Dict with 'path', 'inode', 'generation'
            should_skip_func: Function to determine if file should be skipped
            
        Returns:
            'new', 'updated', 'unchanged', or 'skipped'
        """
        filepath = Path(file_info['path'])
        
        # Check if we should skip this file
        should_skip, skip_reason = should_skip_func(filepath)
        if should_skip and skip_reason not in ['NOCOW attribute set (+C)', 'Immutable attribute set (+i)', 
                                               'Append-only attribute set (+a)', 'Secure deletion attribute set (+s)', 
                                               'Undeletable attribute set (+u)']:
            return 'skipped'
        
        try:
            file_stat = filepath.stat()
        except OSError:
            logger.debug(f"File disappeared: {filepath}")
            return 'skipped'
        
        # Get or create path_id
        path_id = self.add_path(str(filepath))
        
        # Check if file exists in database
        existing = self.execute("""
            SELECT id, size, mtime, possibly_duplicated
            FROM files 
            WHERE path_id = ?
        """, (path_id,)).fetchone()
        
        if existing:
            # File exists - check if it actually changed
            # Note: We only track files with mtime/size changes, not just BTRFS generation changes
            # This is intentional - we're aiming for 99% accuracy, not perfect representation
            
            # Check if file has actually changed (size or mtime)
            if existing['size'] != file_stat.st_size or existing['mtime'] != file_stat.st_mtime:
                # File content likely changed - clear hash and mark for reprocessing
                self.execute("""
                    UPDATE files 
                    SET size = ?, mtime = ?, possibly_duplicated = 1, 
                        hash = NULL, change_count = MIN(change_count + 1, 100),
                        stable_since = ?
                    WHERE id = ?
                """, (file_stat.st_size, file_stat.st_mtime, time.time(), existing['id']))
                
                logger.debug(f"Updated file #{existing['id']} ({filepath}) - size/mtime changed, marked for reprocessing")
                return 'updated'
            else:
                # File unchanged (same size/mtime) - just a generation change
                # Don't set possibly_duplicated=1, don't clear hash
                # This could be from deduplication, metadata changes, etc.
                logger.debug(f"File #{existing['id']} ({filepath}) generation changed but size/mtime unchanged - ignoring")
                return 'unchanged'
        else:
            # New file - insert it
            cursor = self.execute("""
                INSERT INTO files (path_id, size, mtime, discovered_at, possibly_duplicated)
                VALUES (?, ?, ?, ?, 1)
            """, (path_id, file_stat.st_size, file_stat.st_mtime, time.time()))
            
            file_id = cursor.lastrowid
            # logger.debug(f"Created new file record #{file_id} for {filepath}")
            
            # Handle deferred files
            if should_skip and skip_reason in ['NOCOW attribute set (+C)', 'Immutable attribute set (+i)', 
                                              'Append-only attribute set (+a)', 'Secure deletion attribute set (+s)', 
                                              'Undeletable attribute set (+u)']:
                self.defer_file(file_id, skip_reason)
                logger.debug(f"File #{file_id} deferred: {skip_reason}")
            
            return 'new'
    
    # ==========================================================================
    # FILE QUERY OPERATIONS
    # ==========================================================================

    def get_pending_files_by_size(self, size, exclude_id=None):
        """Get all pending files (possibly_duplicated=1) with a specific size.

        Used to find "size twins" - files that might be duplicates based on
        matching file size. These files are candidates for hashing.

        Args:
            size: File size to match
            exclude_id: Optional file ID to exclude from results

        Returns:
            List of file info dicts with keys: id, path_id, size, mtime, hash,
            possibly_duplicated, path
        """
        query = """
            SELECT f.id, f.path_id, f.size, f.mtime, f.hash, f.possibly_duplicated
            FROM files f
            WHERE f.size = ? 
            AND f.possibly_duplicated = 1
            AND f.deferred = 0
        """
        params = [size]
        
        if exclude_id:
            query += " AND f.id != ?"
            params.append(exclude_id)
            
        query += " ORDER BY f.mtime ASC"  # Maintain FIFO order within size group
        
        rows = self.execute(query, params).fetchall()
        
        # Add the full path to each result
        results = []
        for row in rows:
            result = dict(row)
            result['path'] = self.get_path(row['path_id'])
            results.append(result)
        
        return results
    
    def get_files_by_hash_not_deferred(self, file_hash):
        """Get all non-deferred files with a given hash"""
        rows = self.execute("""
            SELECT id, path_id, size, mtime, inode
            FROM files
            WHERE hash = ? AND deferred = 0
        """, (file_hash,)).fetchall()
        
        results = []
        for row in rows:
            path = self.get_path(row['path_id'])
            if path:
                results.append({
                    'id': row['id'],
                    'path': path,
                    'size': row['size'],
                    'mtime': row['mtime'],
                    'inode': row['inode']
                })
        
        return results
    
    def get_size_twins_not_deferred(self, size, exclude_file_id):
        """Get size twins that are not deferred"""
        twins = self.execute("""
            SELECT id FROM files 
            WHERE size = ? AND id != ? AND deferred = 0
        """, (size, exclude_file_id)).fetchall()
        
        return [row['id'] for row in twins]
    
    # ==========================================================================
    # REFERENCE FILE OPERATIONS
    # ==========================================================================

    def set_reference_file(self, file_hash, file_id):
        """Set or update the reference file for a hash.

        For large duplicate groups, we select one file as the "reference"
        and deduplicate all others against it, rather than running duperemove
        on all files at once.

        Args:
            file_hash: The hash value
            file_id: ID of the file to use as reference
        """
        self.execute("""
            INSERT OR REPLACE INTO dedup_references 
            (hash, file_id, selected_at)
            VALUES (?, ?, ?)
        """, (file_hash, file_id, time.time()))
    
    def get_reference_file(self, file_hash):
        """Get the reference file for a hash"""
        row = self.execute("""
            SELECT dr.file_id, dr.selected_at
            FROM dedup_references dr
            JOIN files f ON dr.file_id = f.id
            WHERE dr.hash = ? AND f.deferred = 0
        """, (file_hash,)).fetchone()
        
        if row:
            file_path = self.get_file_path(row['file_id'])
            if file_path and os.path.exists(file_path):
                return {
                    'file_id': row['file_id'],
                    'path': file_path,
                    'selected_at': row['selected_at']
                }
        
        return None
    
    def clear_reference_file(self, file_hash):
        """Clear the reference file for a hash"""
        self.execute("""
            DELETE FROM dedup_references WHERE hash = ?
        """, (file_hash,))
    
    # ==========================================================================
    # ENHANCED SUBVOLUME OPERATIONS
    # ==========================================================================

    def add_enhanced_subvolume(self, uuid, path, parent_uuid=None, generation=None,
                             read_only=False, subvolume_type='main', source_uuid=None):
        """Add or update subvolume with enhanced information.

        Enhanced version that tracks subvolume type and source for snapshots.

        Args:
            uuid: Unique identifier of the subvolume
            path: Mount path of the subvolume
            parent_uuid: UUID of parent subvolume
            generation: BTRFS generation number
            read_only: Whether subvolume is read-only
            subvolume_type: 'main', 'subvolume', or 'snapshot'
            source_uuid: For snapshots, UUID of the source subvolume
        """
        self.execute("""
            INSERT OR REPLACE INTO subvolumes 
            (uuid, parent_uuid, path, generation, read_only, subvolume_type, 
             source_subvolume_uuid, discovered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (uuid, parent_uuid, path, generation, int(read_only), 
              subvolume_type, source_uuid, time.time()))
    
    def get_root_volume_snapshots(self):
        """Get all snapshots of the root volume"""
        return self.execute("""
            SELECT id, uuid, path, generation, read_only
            FROM subvolumes
            WHERE subvolume_type = 'snapshot' 
            AND source_subvolume_uuid IN (
                SELECT uuid FROM subvolumes WHERE subvolume_type = 'main'
            )
            ORDER BY path
        """).fetchall()
    
    def get_subvolume_by_path(self, path):
        """Get subvolume information by path"""
        return self.execute("""
            SELECT id, uuid, subvolume_type, read_only
            FROM subvolumes
            WHERE path = ?
        """, (str(path),)).fetchone()
    
    # ==========================================================================
    # DEDUPLICATION HISTORY RECORDING
    # ==========================================================================

    def record_snapshot_dedup_result(self, file_hash, main_file_id, snapshot_subvolume_id, success):
        """Record a snapshot deduplication attempt.

        Args:
            file_hash: Hash of the deduplicated file
            main_file_id: ID of the main volume file
            snapshot_subvolume_id: ID of the snapshot subvolume
            success: Whether the dedup operation succeeded
        """
        # Get or create dedup group
        group = self.execute("""
            SELECT id FROM dedup_groups WHERE hash = ?
        """, (file_hash,)).fetchone()
        
        if not group:
            logger.warning(f"No dedup group found for hash {file_hash}")
            return
        
        group_id = group['id']
        
        # Record the snapshot dedup attempt
        self.execute("""
            INSERT INTO dedup_history 
            (group_id, file_id, attempted_at, success, operation_type, target_subvolume_id)
            VALUES (?, ?, ?, ?, 'snapshot_reflink', ?)
        """, (group_id, main_file_id, time.time(), int(success), snapshot_subvolume_id))
        
        # Update snapshot files count if successful
        if success:
            self.execute("""
                UPDATE dedup_groups 
                SET snapshot_files_updated = snapshot_files_updated + 1
                WHERE id = ?
            """, (group_id,))
    
    def record_main_dedup_result(self, file_hash, file_ids, success, dry_run=False):
        """Record a main volume deduplication attempt"""
        # Get or create dedup group
        group = self.execute("""
            SELECT id FROM dedup_groups WHERE hash = ?
        """, (file_hash,)).fetchone()
        
        if not group:
            logger.warning(f"No dedup group found for hash {file_hash}")
            return
        
        group_id = group['id']
        
        # Record the attempt for each file
        for file_id in file_ids:
            self.execute("""
                INSERT INTO dedup_history 
                (group_id, file_id, attempted_at, success, dry_run, operation_type)
                VALUES (?, ?, ?, ?, ?, 'main_volume')
            """, (group_id, file_id, time.time(), int(success), int(dry_run)))
    
    def get_enhanced_statistics(self):
        """Get enhanced statistics including new schema features"""
        stats = self.get_statistics()  # Get base stats
        
        # Add new statistics
        stats['deferred_files'] = self.execute("""
            SELECT COUNT(*) FROM files WHERE deferred = 1
        """).fetchone()[0]
        
        stats['reference_files'] = self.execute("""
            SELECT COUNT(*) FROM dedup_references
        """).fetchone()[0]
        
        stats['snapshot_dedups'] = self.execute("""
            SELECT COUNT(*) FROM dedup_history WHERE operation_type = 'snapshot_reflink'
        """).fetchone()[0]
        
        stats['successful_snapshot_dedups'] = self.execute("""
            SELECT COUNT(*) FROM dedup_history 
            WHERE operation_type = 'snapshot_reflink' AND success = 1
        """).fetchone()[0]
        
        stats['total_snapshot_files_updated'] = self.execute("""
            SELECT COALESCE(SUM(snapshot_files_updated), 0) FROM dedup_groups
        """).fetchone()[0]
        
        # Subvolume statistics
        subvolume_stats = self.execute("""
            SELECT subvolume_type, COUNT(*) as count
            FROM subvolumes
            GROUP BY subvolume_type
        """).fetchall()
        
        for row in subvolume_stats:
            stats[f'{row["subvolume_type"]}_subvolumes'] = row['count']
        
        return stats
    
    # ==========================================================================
    # SNAPSHOT COPY QUEUE OPERATIONS
    # ==========================================================================

    def enqueue_snapshot_copy(self, file_path, before_extents, after_extents, file_size, file_hash=None):
        """Add a file to the snapshot copy queue
        
        Args:
            file_path: Full path of the file on main volume
            before_extents: JSON string of extent info before dedup
            after_extents: JSON string of extent info after dedup
            file_size: Size of the file in bytes
            file_hash: Optional file hash for verification
        
        Returns:
            ID of the inserted record
        """
        cursor = self.execute("""
            INSERT INTO enqueued_snapshot_copies 
            (file_path, before_dedup_extents, after_dedup_extents, file_size, enqueued_at, file_hash)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (file_path, before_extents, after_extents, file_size, time.time(), file_hash))
        
        return cursor.lastrowid
    
    def get_snapshot_copy_queue_size(self):
        """Get count of queued snapshot copies"""
        row = self.execute("""
            SELECT COUNT(*) as count FROM enqueued_snapshot_copies
        """).fetchone()
        
        return row['count'] if row else 0
    
    def get_all_snapshot_copies(self):
        """Get all queued snapshot copies for batch processing
        
        Returns:
            List of dict objects with all queue data
        """
        rows = self.execute("""
            SELECT id, file_path, before_dedup_extents, after_dedup_extents, 
                   file_size, enqueued_at, file_hash
            FROM enqueued_snapshot_copies
            ORDER BY enqueued_at ASC
        """).fetchall()
        
        return [dict(row) for row in rows]
    
    def delete_snapshot_copies(self, ids):
        """Delete processed snapshot copies by ID list

        Args:
            ids: List of record IDs to delete
        """
        if not ids:
            return

        # Use placeholders for safe SQL
        placeholders = ','.join('?' * len(ids))
        self.execute(f"""
            DELETE FROM enqueued_snapshot_copies
            WHERE id IN ({placeholders})
        """, ids)

        # Auto-commit deletions
        self.conn.commit()

    # ==========================================================================
    # CUMULATIVE STATISTICS
    # ==========================================================================

    def increment_stat(self, key, amount=1):
        """Increment a cumulative statistic

        Args:
            key: Stat key (e.g., 'files_deduped', 'bytes_potential_savings')
            amount: Amount to increment by (default 1)
        """
        self.execute("""
            INSERT INTO stats (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = value + excluded.value
        """, (key, amount))

    def get_stat(self, key, default=0):
        """Get a cumulative statistic value

        Args:
            key: Stat key
            default: Default value if key doesn't exist

        Returns:
            The stat value as integer
        """
        row = self.execute("""
            SELECT value FROM stats WHERE key = ?
        """, (key,)).fetchone()
        return row['value'] if row else default

    def set_stat(self, key, value):
        """Set a cumulative statistic to a specific value

        Args:
            key: Stat key
            value: Value to set
        """
        self.execute("""
            INSERT INTO stats (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, value))

    def record_dedup_stats(self, files_count, bytes_size, snapshot_files_count=0):
        """Record deduplication statistics

        Called after successful deduplication to update cumulative stats.

        Args:
            files_count: Number of files deduplicated
            bytes_size: Potential bytes saved (file_size for each dedup operation)
            snapshot_files_count: Number of snapshot files included
        """
        self.increment_stat('total_files_deduped', files_count)
        self.increment_stat('total_bytes_potential_savings', bytes_size)
        self.increment_stat('total_dedup_operations', 1)
        if snapshot_files_count > 0:
            self.increment_stat('total_snapshot_files_deduped', snapshot_files_count)

    def get_cumulative_stats(self):
        """Get all cumulative statistics

        Returns:
            Dict with all stat keys and values
        """
        rows = self.execute("SELECT key, value FROM stats").fetchall()
        return {row['key']: row['value'] for row in rows}

    def get_dedup_stats_summary(self):
        """Get a summary of deduplication statistics for display

        Returns:
            Dict with summary statistics:
            - files_deduped: Total files that have been deduplicated
            - bytes_potential_savings: Cumulative potential savings
            - dedup_operations: Number of dedup operations performed
            - snapshot_files_deduped: Number of snapshot files included in dedup
        """
        return {
            'files_deduped': self.get_stat('total_files_deduped', 0),
            'bytes_potential_savings': self.get_stat('total_bytes_potential_savings', 0),
            'dedup_operations': self.get_stat('total_dedup_operations', 0),
            'snapshot_files_deduped': self.get_stat('total_snapshot_files_deduped', 0),
        }