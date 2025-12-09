"""
Global constants for btrdupd
"""

# Current database schema version
# Version history:
# - v1: Initial schema
# - v2: Added dry_run_dedup column and migration support
# - v3: Added hash_checked_at column for better hash backoff tracking
# - v4: Removed obsolete columns (last_seen_gen, backoff_until) and added hash backoff indexes
# - v5: Added skip_until column for handling open files
# - v6: Added enqueued_snapshot_copies table for batched snapshot processing
CURRENT_DB_VERSION = 6

# Application version
APP_VERSION = "0.1.0"