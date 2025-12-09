"""
File monitoring and processing for btrdupd
Implements the main processing loop

Philosophy: btrdupd aims for practical 99% accuracy, not perfect representation.
We use fast hashing (xxhash64) and file size/mtime for duplicate detection,
trusting duperemove to verify actual file equality before deduplication.
Hash collisions are acceptable - duperemove verifies byte equality before dedup.
This approach prioritizes performance and reduces unnecessary work.
"""

import os
import time
import logging
import subprocess
import fnmatch
import stat
from pathlib import Path
from datetime import datetime, timedelta
import configparser
import xxhash
try:
    import tomli as toml  # Python < 3.11 (for reading)
except ImportError:
    import tomllib as toml  # Python >= 3.11 (for reading)
import tomlkit  # For writing TOML with comments

from database import BtrdupdDatabase
from btrfs import BtrfsFilesystem
import metrics

logger = logging.getLogger('btrdupd.monitor')

class VolumeLogger:
    """Logger wrapper that prepends volume path to messages"""
    def __init__(self, base_logger, volume_path):
        self.base_logger = base_logger
        self.volume_path = str(volume_path)
        
    def _format_msg(self, msg):
        return f"[{self.volume_path}] {msg}"
        
    def debug(self, msg, *args, **kwargs):
        self.base_logger.debug(self._format_msg(msg), *args, **kwargs)
        
    def info(self, msg, *args, **kwargs):
        self.base_logger.info(self._format_msg(msg), *args, **kwargs)
        
    def warning(self, msg, *args, **kwargs):
        self.base_logger.warning(self._format_msg(msg), *args, **kwargs)
        
    def error(self, msg, *args, **kwargs):
        self.base_logger.error(self._format_msg(msg), *args, **kwargs)
        
    def critical(self, msg, *args, **kwargs):
        self.base_logger.critical(self._format_msg(msg), *args, **kwargs)

DEFAULT_CONFIG = {
    'min_file_size': '32768',  # 32KB minimum
    'scan_interval_minutes': '5',  # Sleep between volume cycles when no work
    'dedup_hours_start': '0',  # Start hour for dedup window (0-23)
    'dedup_hours_end': '23',  # End hour for dedup window (0-23)
    'dedup_time_limit_minutes': '10',  # Max time per volume for dedup
    'full_scan_interval_days': '30',  # Force full scan if older
    'dedup_snapshots': 'true',
    'max_dedup_batch': '5',
    'large_group_threshold': '10',  # When to use smart dedup strategy
    'dedup_batch_size': '50',  # Max files per duperemove call
    'max_arg_length': '1048576',  # Max command line length (1MB, half of typical limit)
    'cpu_nice': '19',
    'io_nice_class': '3',
    'max_generation_gap': '10000',
    'file_size_format': 'commas',  # 'number', 'commas', 'human'
    'size_unit_type': 'decimal',  # 'decimal' (KB/MB/GB), 'binary' (KiB/MiB/GiB)
    'debug_stats_interval': '60',  # seconds between debug stats in debug mode
    'ignore_paths': '*/docker/overlay2/*,*/docker/image/*,*/docker/containers/*,*/docker/buildkit/*,*/docker/tmp/*,*/.cache/*',  # Default paths to ignore
    'snapshot_batch_size': '100',  # Process snapshots when this many files are queued (legacy)
    'max_snapshots_per_dedup': '100',  # Max snapshots to include per duperemove call
    'dedup_strategy': 'fifo'  # 'fifo' (oldest first) or 'large_first' (throughput optimized)
}

class FileMonitor:
    """Main file monitoring and deduplication orchestrator.

    Handles the complete workflow of:
    - Scanning filesystem for files
    - Tracking file changes via BTRFS generation numbers
    - Computing xxhash64 hashes for duplicate detection
    - Orchestrating deduplication via duperemove
    - Managing snapshot deduplication via reflink copying

    The monitor operates in two modes:
    - One-time mode: Full scan, hash, and dedup, then exit
    - Daemon mode: Continuous monitoring with periodic scans
    """

    # ==========================================================================
    # INITIALIZATION AND CONFIGURATION
    # ==========================================================================

    def __init__(self, volume_path, db_path, config=None):
        self.volume_path = Path(volume_path).resolve()
        self.db = BtrdupdDatabase(db_path)
        self.btrfs = BtrfsFilesystem(volume_path)
        self.config = config or self.load_config()
        self.running = True
        self.debug = False
        self.dry_run = False
        
        # Set up volume-specific logger
        global logger
        logger = VolumeLogger(logging.getLogger('btrdupd.monitor'), self.volume_path)
        
        # Parse exclude/include patterns
        self.exclude_patterns = []
        self.include_patterns = []
        
        # Load ignore patterns from config
        self.ignore_patterns = []
        self._load_ignore_patterns()
        
        # Periodic debug stats tracking
        self.last_debug_stats = 0
        self.directories_processed = 0
        self.dedup_calls_made = 0
        self.session_bytes_deduped = 0  # Track bytes saved this session
        self.last_periodic_stats = 0  # Universal tracker for all periodic stats
        self.operation_start_time = time.time()  # Track when current operation started
        self.files_hashed_session = 0  # Track files hashed this session
        self.files_deduped_session = 0  # Track files deduplicated this session
        self.last_stats_content = None  # Track last written stats to avoid unnecessary writes
        
    def load_config(self):
        """Load configuration from .btrdupd/config.toml or .btrdupd.conf"""
        btrdupd_dir = self.volume_path / '.btrdupd'
        toml_config_path = btrdupd_dir / 'config.toml'
        ini_config_path = self.volume_path / '.btrdupd.conf'
        
        # Try TOML first
        if toml_config_path.exists():
            try:
                logger.info(f"Loading TOML config from {toml_config_path}")
                with open(toml_config_path, 'rb') as f:
                    config_data = toml.load(f)
                
                # Convert to string values for compatibility
                config = {}
                for key, value in DEFAULT_CONFIG.items():
                    toml_value = config_data.get(key, value)
                    # Keep lists as lists (for ignore_paths)
                    if isinstance(toml_value, list):
                        config[key] = toml_value
                    else:
                        config[key] = str(toml_value)
                
                # Validate minimum file size
                min_size = int(config.get('min_file_size', '32768'))
                if min_size < 2048:
                    logger.warning(f"Minimum file size {min_size} is below BTRFS inline threshold (2048 bytes)")
                    logger.warning("Setting minimum file size to 2048 bytes")
                    config['min_file_size'] = '2048'
                
                return config
            except Exception as e:
                logger.error(f"Error loading TOML config: {e}")
                logger.info("Falling back to defaults")
        
        # Try INI format for backwards compatibility
        elif ini_config_path.exists():
            logger.info(f"Loading INI config from {ini_config_path}")
            config_parser = configparser.ConfigParser(defaults=DEFAULT_CONFIG)
            config_parser.read(ini_config_path)
            
            # Validate minimum file size
            min_size = int(config_parser['DEFAULT'].get('min_file_size', '32768'))
            if min_size < 2048:
                logger.warning(f"Minimum file size {min_size} is below BTRFS inline threshold (2048 bytes)")
                logger.warning("Setting minimum file size to 2048 bytes")
                config_parser['DEFAULT']['min_file_size'] = '2048'
                
            return dict(config_parser['DEFAULT'])
        
        # Create default TOML config if none exists
        else:
            logger.info("No config found, creating default TOML config")
            self.create_default_toml_config(toml_config_path)
            return DEFAULT_CONFIG.copy()
    
    def create_default_toml_config(self, config_path):
        """Create a default TOML configuration file with comments"""
        if not tomlkit:
            logger.warning("tomlkit not available, cannot create TOML config")
            return
        
        try:
            # Create directory if needed
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create TOML document with comments
            doc = tomlkit.document()
            
            doc.add(tomlkit.comment("btrdupd configuration file"))
            doc.add(tomlkit.comment(""))
            doc.add(tomlkit.comment("This file configures the behavior of btrdupd for this volume"))
            doc.add(tomlkit.nl())
            
            # File processing settings
            doc.add(tomlkit.comment("=== File Processing Settings ==="))
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Minimum file size to consider for deduplication (in bytes)"))
            doc.add(tomlkit.comment("Files smaller than this will be ignored"))
            doc.add(tomlkit.comment("BTRFS inline threshold is 2048 bytes, so this should be at least that"))
            doc["min_file_size"] = 32768
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Paths to ignore during scanning"))
            doc.add(tomlkit.comment("Wildcards (*) allowed only at start or end of patterns"))
            doc.add(tomlkit.comment("Docker disposable data is ignored but volumes are still scanned"))
            ignore_paths = tomlkit.array()
            ignore_paths.multiline(True)
            ignore_paths.append("*/docker/overlay2/*")    # Container layers
            ignore_paths.append("*/docker/image/*")       # Image storage
            ignore_paths.append("*/docker/containers/*")  # Container metadata/logs
            ignore_paths.append("*/docker/buildkit/*")    # Build cache
            ignore_paths.append("*/docker/tmp/*")         # Temporary files
            ignore_paths.append("*/.cache/*")             # User cache directories
            doc["ignore_paths"] = ignore_paths
            doc.add(tomlkit.nl())
            
            # Deduplication settings
            doc.add(tomlkit.comment("=== Deduplication Settings ==="))
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Enable deduplication of files in snapshots"))
            doc.add(tomlkit.comment("If true, will use reflink to update matching files in read-only snapshots"))
            doc["dedup_snapshots"] = True
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Maximum files to process in a single duperemove batch"))
            doc["max_dedup_batch"] = 5
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Threshold for using smart deduplication strategy for large groups"))
            doc.add(tomlkit.comment("Groups with more files than this will use reference-based deduplication"))
            doc["large_group_threshold"] = 10
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Maximum files per duperemove call for large groups"))
            doc["dedup_batch_size"] = 50
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Maximum command line length for duperemove (in bytes)"))
            doc.add(tomlkit.comment("Conservative limit to avoid 'argument list too long' errors"))
            doc["max_arg_length"] = 1048576
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Number of deduplicated files to queue before batch processing snapshots"))
            doc.add(tomlkit.comment("Lower values mean more frequent but smaller batches"))
            doc.add(tomlkit.comment("Higher values mean less frequent but larger batches (more efficient)"))
            doc["snapshot_batch_size"] = 100
            doc.add(tomlkit.nl())
            
            # Timing settings
            doc.add(tomlkit.comment("=== Timing and Scheduling ==="))
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Interval between filesystem scans when idle (in minutes)"))
            doc["scan_interval_minutes"] = 5
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Time window for deduplication (24-hour format)"))
            doc.add(tomlkit.comment("Deduplication will only run during these hours"))
            doc["dedup_hours_start"] = 0
            doc["dedup_hours_end"] = 23
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Maximum time to spend on deduplication per cycle (in minutes)"))
            doc.add(tomlkit.comment("Prevents deduplication from running too long in daemon mode"))
            doc["dedup_time_limit_minutes"] = 10
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Force full filesystem scan if older than this (in days)"))
            doc["full_scan_interval_days"] = 30
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Maximum BTRFS generation gap before forcing full scan"))
            doc.add(tomlkit.comment("If generation number jumps by more than this, do full scan"))
            doc["max_generation_gap"] = 10000
            doc.add(tomlkit.nl())
            
            # Performance settings
            doc.add(tomlkit.comment("=== Performance Settings ==="))
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Process nice level (0-19, higher = lower priority)"))
            doc["cpu_nice"] = 19
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("I/O scheduling class (1=real-time, 2=best-effort, 3=idle)"))
            doc["io_nice_class"] = 3
            doc.add(tomlkit.nl())
            
            # Display settings
            doc.add(tomlkit.comment("=== Display Settings ==="))
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("File size display format"))
            doc.add(tomlkit.comment("Options: 'number' (12345678), 'commas' (12,345,678), 'human' (12.3 MB)"))
            doc["file_size_format"] = "commas"
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Size unit type for human-readable format"))
            doc.add(tomlkit.comment("Options: 'decimal' (KB/MB/GB), 'binary' (KiB/MiB/GiB)"))
            doc["size_unit_type"] = "decimal"
            doc.add(tomlkit.nl())
            
            doc.add(tomlkit.comment("Interval for debug statistics output (in seconds)"))
            doc["debug_stats_interval"] = 60
            
            # Write the config file
            with open(config_path, 'w') as f:
                f.write(tomlkit.dumps(doc))
            
            logger.info(f"Created default TOML config at {config_path}")
            
        except Exception as e:
            logger.error(f"Error creating TOML config: {e}")
    
    # ==========================================================================
    # PATH FILTERING AND SKIP LOGIC
    # ==========================================================================

    def _load_ignore_patterns(self):
        """Load and parse ignore patterns from config."""
        ignore_paths = self.config.get('ignore_paths', '')
        if not ignore_paths:
            return

        # Handle both string (from INI) and list (from TOML) formats
        if isinstance(ignore_paths, str):
            ignore_paths = ignore_paths.strip()
            if not ignore_paths:
                return
            # INI format: comma or newline separated
            patterns = [p.strip() for p in ignore_paths.replace(',', '\n').split('\n') if p.strip()]
        elif isinstance(ignore_paths, list):
            # TOML format: already a list
            patterns = [str(p).strip() for p in ignore_paths if p]
        else:
            patterns = []
        
        for pattern in patterns:
            if not pattern:
                continue
                
            # Validate wildcard placement (only at beginning or end)
            wildcard_count = pattern.count('*')
            if wildcard_count > 0:
                # Check if wildcards are only at beginning/end
                stripped = pattern.strip('*')
                if '*' in stripped:
                    logger.warning(f"Invalid pattern '{pattern}': wildcards only allowed at beginning or end")
                    continue
            
            self.ignore_patterns.append(pattern)
            
        if self.ignore_patterns:
            logger.info(f"Loaded {len(self.ignore_patterns)} ignore patterns")
            if self.debug:
                for pattern in self.ignore_patterns:
                    logger.debug(f"  Ignore pattern: {pattern}")
    
    def _should_ignore_path(self, path):
        """Check if a path should be ignored based on ignore patterns
        
        Returns: (should_ignore, pattern_matched)
        """
        if not self.ignore_patterns:
            return False, None
        
        # Convert to Path object if needed
        path_obj = Path(path) if not isinstance(path, Path) else path
        
        # Get both absolute and volume-relative paths
        abs_path = path_obj.resolve()
        abs_str = str(abs_path)
        
        # Try to get volume-relative path
        try:
            rel_path = abs_path.relative_to(self.volume_path)
            rel_str = '/' + str(rel_path)  # Add leading slash for consistency
        except ValueError:
            # Path is not under volume root
            rel_str = None
        
        for pattern in self.ignore_patterns:
            # Check if this is a wildcard pattern
            if '*' in pattern:
                # Handle wildcards at beginning/end
                if pattern.startswith('*') and pattern.endswith('*'):
                    # Pattern like */docker/overlay2/*
                    middle = pattern[1:-1]
                    # Check if path contains the middle part
                    if middle in abs_str:
                        return True, pattern
                    if rel_str and middle in rel_str:
                        return True, pattern
                    # Also check if path starts with middle (for directory at root)
                    if abs_str.startswith(self.volume_path.as_posix() + middle):
                        return True, pattern
                elif pattern.startswith('*'):
                    # Pattern like *.log
                    suffix = pattern[1:]
                    if abs_str.endswith(suffix) or (rel_str and rel_str.endswith(suffix)):
                        return True, pattern
                elif pattern.endswith('*'):
                    # Pattern like /home/*
                    prefix = pattern[:-1]
                    if abs_str.startswith(prefix) or (rel_str and rel_str.startswith(prefix)):
                        return True, pattern
            else:
                # Exact match - check both absolute and relative paths
                if abs_str == pattern or abs_str.startswith(pattern + '/'):
                    return True, pattern
                if rel_str and (rel_str == pattern or rel_str.startswith(pattern + '/')):
                    return True, pattern
        
        return False, None
    
    def set_filters(self, exclude_patterns=None, include_patterns=None):
        """Set file filtering patterns"""
        self.exclude_patterns = exclude_patterns or []
        self.include_patterns = include_patterns or []
    
    def should_skip_file(self, file_path):
        """Check if a file should be skipped based on all filtering criteria"""
        file_path = Path(file_path)
        
        # Check ignore patterns first
        should_ignore, pattern = self._should_ignore_path(file_path)
        if should_ignore:
            return True, f"Ignored by pattern: {pattern}"
        
        # Check if file exists
        if not file_path.exists():
            return True, "File does not exist"
        
        # Check file type - only regular files
        if not file_path.is_file():
            return True, "Not a regular file"
        
        # Check if symlink
        if file_path.is_symlink():
            return True, "Symbolic link"
        
        # Check minimum file size
        try:
            file_size = file_path.stat().st_size
            min_size = int(self.config.get('min_file_size', '32768'))
            if file_size < min_size:
                return True, f"File too small ({file_size} < {min_size})"
        except OSError:
            return True, "Cannot stat file"
        
        # Check volume boundaries
        skip_reason = self._check_volume_boundaries(file_path)
        if skip_reason:
            return True, skip_reason
        
        # Check file attributes (chattr)
        skip_reason = self._check_file_attributes(file_path)
        if skip_reason:
            return True, skip_reason
        
        # Check exclude/include patterns
        skip_reason = self._check_patterns(file_path)
        if skip_reason:
            return True, skip_reason
        
        return False, None
    
    def should_skip_directory(self, dir_path):
        """Check if a directory should be skipped during traversal"""
        dir_path = Path(dir_path)
        
        # Check ignore patterns first
        should_ignore, pattern = self._should_ignore_path(dir_path)
        if should_ignore:
            return True, f"Ignored by pattern: {pattern}"
        
        # Check if directory exists
        if not dir_path.exists():
            return True, "Directory does not exist"
        
        # Check volume boundaries (mountpoints, system dirs, etc)
        skip_reason = self._check_volume_boundaries(dir_path)
        if skip_reason:
            return True, skip_reason
        
        # We don't check patterns or attributes for directories during traversal
        return False, None
    
    def _check_volume_boundaries(self, file_path):
        """Check if file is within volume boundaries"""
        try:
            # Resolve to absolute path
            abs_path = file_path.resolve()
            
            # Check if it's within our target volume
            try:
                abs_path.relative_to(self.volume_path)
            except ValueError:
                return "Outside target volume"
            
            # Check if it's on a different mountpoint
            if self._is_different_mountpoint(abs_path):
                return "Different filesystem/mountpoint"
            
            # Also check if this path IS a mountpoint itself (but NOT the target volume root)
            if abs_path != self.volume_path and self._is_mountpoint(abs_path):
                return "Is a mountpoint"
            
            # Check if it's in system directories
            path_parts = abs_path.parts
            if len(path_parts) >= 2:
                system_dirs = {'/proc', '/sys', '/dev', '/run', '/tmp'}
                root_dir = '/' + path_parts[1] if path_parts[0] == '/' else path_parts[0]
                if root_dir in system_dirs:
                    return f"System directory: {root_dir}"
            
            return None
        except Exception as e:
            logger.debug(f"Error checking volume boundaries for {file_path}: {e}")
            return "Error checking boundaries"
    
    def _is_different_mountpoint(self, file_path):
        """Check if file/directory is on a different mountpoint than our target volume"""
        try:
            # Get device ID of our target volume
            volume_stat = self.volume_path.stat()
            
            # Get device ID of the file/directory itself
            # For directories, we check the directory; for files, we check the file
            file_stat = file_path.stat()
            
            # Different device = different mountpoint
            return volume_stat.st_dev != file_stat.st_dev
        except:
            return True  # Assume different if we can't check
    
    def _is_mountpoint(self, path):
        """Check if the path itself is a mountpoint"""
        try:
            # A directory is a mountpoint if its device differs from its parent's device
            if path.is_dir() and path != path.parent:
                path_stat = path.stat()
                parent_stat = path.parent.stat()
                return path_stat.st_dev != parent_stat.st_dev
            return False
        except:
            return False
    
    def _check_file_attributes(self, file_path):
        """Check file attributes using lsattr and skip problematic ones"""
        try:
            result = subprocess.run(
                ['lsattr', str(file_path)],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode != 0:
                # lsattr failed, probably not a regular file or permission issue
                return None
            
            # Parse lsattr output: "-------------e-- /path/to/file"
            output = result.stdout.strip()
            if not output:
                return None
            
            # Extract attribute flags (first part before the filename)
            parts = output.split(None, 1)
            if len(parts) < 2:
                return None
            
            attrs = parts[0]
            
            # Check for problematic attributes
            if 'C' in attrs:
                return "NOCOW attribute set (+C)"
            if 'i' in attrs:
                return "Immutable attribute set (+i)"
            if 'a' in attrs:
                return "Append-only attribute set (+a)"
            if 's' in attrs:
                return "Secure deletion attribute set (+s)"
            if 'u' in attrs:
                return "Undeletable attribute set (+u)"
            
            # Note: 'c' (compressed) is allowed for deduplication
            return None
            
        except Exception as e:
            logger.debug(f"Error checking file attributes for {file_path}: {e}")
            return None  # If we can't check, don't skip
    
    def _check_patterns(self, file_path):
        """Check file against exclude/include patterns"""
        if not self.exclude_patterns and not self.include_patterns:
            return None
        
        # Get relative path for pattern matching
        try:
            rel_path = file_path.relative_to(self.volume_path)
            path_str = str(rel_path)
        except ValueError:
            path_str = str(file_path)
        
        # Check exclude patterns
        for pattern in self.exclude_patterns:
            if fnmatch.fnmatch(path_str, pattern) or fnmatch.fnmatch(file_path.name, pattern):
                return f"Matches exclude pattern: {pattern}"
        
        # Check include patterns (if any exist, file must match at least one)
        if self.include_patterns:
            for pattern in self.include_patterns:
                if fnmatch.fnmatch(path_str, pattern) or fnmatch.fnmatch(file_path.name, pattern):
                    return None  # Matches include pattern, don't skip
            return "Does not match any include pattern"
        
        return None
    
    # ==========================================================================
    # FORMATTING AND DISPLAY UTILITIES
    # ==========================================================================

    def format_file_size(self, bytes_value):
        """Format file size according to configuration."""
        format_type = self.config.get('file_size_format', 'commas')
        
        if format_type == 'number':
            return str(bytes_value)
        elif format_type == 'commas':
            return f"{bytes_value:,}"
        elif format_type == 'human':
            return self._format_bytes_human(bytes_value, for_summary=False)
        else:
            return f"{bytes_value:,}"  # Default to commas
    
    def format_summary_size(self, bytes_value):
        """Format summary sizes (always human readable)"""
        return self._format_bytes_human(bytes_value, for_summary=True)
    
    def _format_bytes_human(self, bytes_value, for_summary=False):
        """Format bytes in human-readable form"""
        unit_type = self.config.get('size_unit_type', 'decimal')
        
        if unit_type == 'binary':
            # Binary units (1024-based): KiB, MiB, GiB, TiB, PiB
            units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
            divisor = 1024.0
        else:
            # Decimal units (1000-based): KB, MB, GB, TB, PB
            units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
            divisor = 1000.0
        
        value = float(bytes_value)
        for unit in units:
            if value < divisor or unit == units[-1]:
                if unit == 'B':
                    return f"{int(value)} {unit}"
                else:
                    return f"{value:.1f} {unit}"
            value /= divisor
        
        return f"{value:.1f} {units[-1]}"
    
    def format_elapsed_time(self, seconds):
        """Format elapsed time in hh:mm:ss format"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    
    def check_and_show_periodic_stats(self, operation_name="Processing"):
        """Check if it's time to show periodic stats and display them if needed"""
        if not self.debug:
            return
            
        current_time = time.time()
        if current_time - self.last_periodic_stats >= 60:
            elapsed = current_time - self.operation_start_time
            elapsed_str = self.format_elapsed_time(elapsed)
            header = f"=== PERIODIC STATUS - {operation_name} - Elapsed: {elapsed_str} ==="
            separator = "=" * (len(header) + 30)  # Add 30 extra = characters
            logger.info(separator)
            logger.info(header)
            logger.info(separator)
            try:
                stats = self.db.get_statistics()
                total_files = stats.get('total_files', 0)
                hashed_files = stats.get('hashed_files', 0)
                dedup_candidates = stats.get('dedup_candidates', 0)
                session_saved_str = self.format_summary_size(self.session_bytes_deduped) if self.session_bytes_deduped > 0 else "0 B"
                
                logger.info(f"=== Files: {total_files:,}, Hashed: {hashed_files:,}, "
                           f"Eligible: {dedup_candidates:,}, Session saved: {session_saved_str}, "
                           f"Dedup calls: {self.dedup_calls_made:,}, Dirs: {self.directories_processed:,}")
            except Exception as e:
                logger.debug(f"Error showing periodic stats: {e}")
            logger.info(separator)
            self.last_periodic_stats = current_time
            
            # Write stats to file atomically
            self.write_stats_file()
    
    def write_stats_file(self):
        """Write current statistics to btrdupd.stats.txt atomically"""
        try:
            btrdupd_dir = self.volume_path / '.btrdupd'
            btrdupd_dir.mkdir(parents=True, exist_ok=True)
            
            stats_file = btrdupd_dir / 'btrdupd.stats.txt'
            temp_file = btrdupd_dir / 'btrdupd.stats.tmp'
            
            # Get current statistics
            stats = self.db.get_statistics()
            total_files = stats.get('total_files', 0)
            hashed_files = stats.get('hashed_files', 0)
            dedup_candidates = stats.get('dedup_candidates', 0)
            
            # Format current time
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Format session bytes saved
            session_saved_str = self.format_summary_size(self.session_bytes_deduped) if self.session_bytes_deduped > 0 else "0 B"
            
            # Build stats content (without timestamp for comparison)
            stats_data = {
                'total_files': total_files,
                'hashed_files': hashed_files,
                'dedup_candidates': dedup_candidates,
                'session_files_hashed': self.files_hashed_session,
                'session_files_deduped': self.files_deduped_session,
                'session_bytes_saved': self.session_bytes_deduped,
                'dedup_calls': self.dedup_calls_made,
                'directories': self.directories_processed
            }
            
            # Check if stats have changed
            if self.last_stats_content == stats_data:
                # No changes, skip write
                return
                
            # Stats have changed, update and write
            self.last_stats_content = stats_data.copy()
            
            # Build display content
            content = []
            content.append(f"btrdupd Statistics for {self.volume_path}")
            content.append(f"Last updated: {current_time}")
            content.append("=" * 60)
            content.append(f"Total files: {total_files:,}")
            content.append(f"Files with hashes: {hashed_files:,}")
            content.append(f"Files eligible for deduplication: {dedup_candidates:,}")
            content.append(f"Session files hashed: {self.files_hashed_session:,}")
            content.append(f"Session files deduplicated: {self.files_deduped_session:,}")
            content.append(f"Session space saved: {session_saved_str}")
            content.append(f"Deduplication calls made: {self.dedup_calls_made:,}")
            content.append(f"Directories processed: {self.directories_processed:,}")
            
            # Add recent operations if we track them
            content.append("\n" + "=" * 60)
            content.append("Recent Operations:")
            content.append("(Feature to be implemented)")
            
            # Write to temp file
            with open(temp_file, 'w') as f:
                f.write('\n'.join(content))
                f.write('\n')
            
            # Atomically rename
            temp_file.replace(stats_file)
            
        except Exception as e:
            logger.debug(f"Error writing stats file: {e}")
    
    def periodic_events(self):
        """Handle periodic events during long operations"""
        current_time = time.time()
        
        # Don't show individual volume stats in periodic events
        # The daemon will show all volumes together
        # This prevents duplicate/confusing output
    
    def show_debug_stats(self):
        """Show periodic debug statistics"""
        try:
            stats = self.db.get_statistics()
            
            total_files = stats.get('total_files', 0)
            hashed_files = stats.get('hashed_files', 0)
            dedup_candidates = stats.get('dedup_candidates', 0)
            
            logger.info(f"DEBUG STATS: Files: {total_files:,}, Dirs: {self.directories_processed:,}, "
                       f"Hashed: {hashed_files:,}, Dedup calls: {self.dedup_calls_made:,}, "
                       f"Eligible for dedup: {dedup_candidates:,}")
        except Exception as e:
            logger.debug(f"Error showing debug stats: {e}")
    
    # ==========================================================================
    # PROCESS MANAGEMENT
    # ==========================================================================

    def set_process_priority(self):
        """Set CPU and IO nice values for low-priority background processing."""
        try:
            # Set CPU nice
            nice_level = int(self.config['cpu_nice'])
            os.nice(nice_level)
            logger.debug(f"Set CPU nice to {nice_level}")
            
            # Set IO nice
            io_class = self.config['io_nice_class']
            subprocess.run([
                'ionice', '-c', io_class, '-p', str(os.getpid())
            ], check=True)
            logger.debug(f"Set IO nice class to {io_class}")
        except Exception as e:
            logger.warning(f"Could not set process priority: {e}")
    
    # ==========================================================================
    # MAIN PROCESSING LOOP
    # ==========================================================================

    def process_one_iteration(self, daemon_mode=True):
        """Process one iteration of the monitor loop.

        Each iteration consists of three phases:
        1. Check for BTRFS generation changes
        2. Perform full scan if needed
        3. Process deduplication candidates

        Args:
            daemon_mode: If True, respects time windows for deduplication

        Returns:
            True if any work was done, False if idle
        """
        work_done = False
        
        try:
            # Phase 1: Update changed files from BTRFS
            if self.debug:
                logger.debug(f"[PHASE] Starting BTRFS change detection for {self.volume_path}")
            
            changes_found = self.check_for_changes()
            if changes_found:
                work_done = True
                
            if self.debug:
                logger.debug(f"[PHASE] Completed BTRFS change detection (changes: {changes_found})")
            
            # Phase 2: Check if full scan needed
            if self.debug:
                logger.debug(f"[PHASE] Checking if full scan needed for {self.volume_path}")
                
            if self.check_and_perform_full_scan(daemon_mode):
                work_done = True
                
            # Phase 3: Deduplication processing 
            # Check time window only in daemon mode
            should_dedup = True
            if daemon_mode:
                current_hour = datetime.now().hour
                start_hour = int(self.config.get('dedup_hours_start', '0'))
                end_hour = int(self.config.get('dedup_hours_end', '23'))
                should_dedup = start_hour <= current_hour <= end_hour
                
                if not should_dedup and self.debug:
                    logger.debug(f"[PHASE] Skipping deduplication - outside time window ({current_hour} not in {start_hour}-{end_hour})")
            
            if should_dedup:
                if self.debug:
                    logger.debug(f"[PHASE] Starting deduplication processing for {self.volume_path}")
                    
                if self.process_deduplication_phase(daemon_mode):
                    work_done = True
                    
                if self.debug:
                    logger.debug(f"[PHASE] Completed deduplication processing")
            
            return work_done

        except Exception as e:
            logger.error(f"Error in iteration: {e}")
            return False

    def process_until_idle(self, max_iterations=10000, show_progress=True,
                           progress_interval=10):
        """
        Process all pending work until the monitor reaches an idle state.

        This method is designed for testing scenarios where we need btrdupd to
        fully process all pending operations before checking state. It runs
        the full scan→hash→dedup pipeline repeatedly until no more work remains.

        Unlike daemon mode, this method:
        - Has no time limits on deduplication
        - Ignores dedup time windows (processes immediately)
        - Does not sleep between iterations
        - Continues until truly idle (no pending work in any phase)

        Args:
            max_iterations: Safety limit to prevent infinite loops (default 10000)
            show_progress: If True, log progress at regular intervals
            progress_interval: How often to show progress (every N iterations)

        Returns:
            dict: Processing statistics including:
                - iterations: Number of processing cycles completed
                - idle: True if stopped due to idle, False if hit max_iterations
                - files_scanned: Files discovered during this run
                - files_hashed: Files hashed during this run
                - files_deduped: Files deduplicated during this run
                - bytes_saved: Estimated bytes saved from deduplication
                - final_stats: Full database statistics at completion

        Example:
            >>> monitor = FileMonitor(volume_path, db_path, config)
            >>> monitor.db.connect()
            >>> result = monitor.process_until_idle()
            >>> print(f"Processed {result['iterations']} iterations")
            >>> assert result['idle'], "Should have reached idle state"
        """
        work_done = True
        iteration = 0

        # Track session statistics
        initial_stats = self.db.get_statistics()
        initial_hashed = initial_stats.get('hashed_files', 0)

        while work_done and iteration < max_iterations:
            iteration += 1

            if show_progress and iteration % progress_interval == 0:
                current_stats = self.db.get_statistics()
                logger.info(
                    f"[PROCESS_UNTIL_IDLE] Iteration {iteration}: "
                    f"files={current_stats.get('total_files', 0)}, "
                    f"hashed={current_stats.get('hashed_files', 0)}, "
                    f"dedup_candidates={current_stats.get('dedup_candidates', 0)}"
                )

            work_done = self.process_one_iteration(daemon_mode=False)

            # Run periodic events to update stats
            if work_done:
                self.periodic_events()

        # Gather final statistics
        final_stats = self.db.get_statistics()

        result = {
            'iterations': iteration,
            'idle': not work_done,  # True if we stopped because no work, not max_iterations
            'files_scanned': final_stats.get('total_files', 0),
            'files_hashed': final_stats.get('hashed_files', 0) - initial_hashed,
            'files_deduped': getattr(self, 'files_deduped_session', 0),
            'bytes_saved': getattr(self, 'session_bytes_deduped', 0),
            'final_stats': final_stats,
        }

        if show_progress:
            logger.info(
                f"[PROCESS_UNTIL_IDLE] Complete after {iteration} iterations. "
                f"Idle={result['idle']}, Files hashed={result['files_hashed']}"
            )

        return result

    def run(self, daemon_mode=True):
        """Main monitoring loop"""
        logger.info("Starting file monitor")
        self.set_process_priority()
        
        # Connect to database
        self.db.connect()
        
        # Check if we need initial scan
        if self.needs_initial_scan():
            if daemon_mode:
                logger.info("Performing mandatory initial full scan")
            else:
                logger.info("Performing initial full scan (one-time mode)")
            self.perform_full_scan()
        
        # For one-time mode, just process until done
        if not daemon_mode:
            work_done = True
            iteration = 0
            
            while work_done:
                iteration += 1
                if self.debug:
                    logger.debug(f"[ONE-TIME] Processing iteration {iteration}")
                
                work_done = self.process_one_iteration(daemon_mode=False)
                
                if work_done:
                    self.periodic_events()
                else:
                    logger.info("Processing complete")
            
            return
        
        # Daemon mode - continuous monitoring
        while self.running:
            try:
                # Process one iteration for this volume
                work_done = self.process_one_iteration(daemon_mode=True)
                
                # Show periodic stats regardless of work done
                self.periodic_events()
                
                # Only sleep if no work was done
                if not work_done:
                    sleep_minutes = float(self.config.get('scan_interval_minutes', '5'))
                    if self.debug:
                        logger.debug(f"[PHASE] No work done, sleeping for {sleep_minutes} minutes")
                    time.sleep(sleep_minutes * 60)
                elif self.debug:
                    logger.debug(f"[PHASE] Work was done, continuing immediately")
                    
            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                self.running = False
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(60)  # Wait before retrying
    
    # ==========================================================================
    # FILESYSTEM SCANNING
    # ==========================================================================

    def needs_initial_scan(self):
        """Check if initial scan is needed (database is empty)."""
        total_files = self.db.get_statistics()['total_files']
        return total_files == 0

    def perform_full_scan(self):
        """Perform full filesystem scan"""
        logger.info(f"[{self.volume_path}] Starting full filesystem scan")
        start_time = time.time()
        self.operation_start_time = start_time  # Reset operation timer
        
        # Capture BTRFS generation at scan start
        try:
            scan_start_generation = self.btrfs.get_current_generation()
            self.db.set_metadata('scan_start_generation', str(scan_start_generation))
            logger.info(f"Scan starting at BTRFS generation {scan_start_generation}")
        except Exception as e:
            logger.warning(f"Could not capture generation at scan start: {e}")
        
        file_count = 0
        skipped_count = 0
        skip_reasons = {}
        transaction_count = 0
        total_removed_files = 0
        
        try:
            # Start initial transaction
            self.db.begin_transaction()
            
            for root, dirs, files in os.walk(self.volume_path, followlinks=False):
                root_path = Path(root)
                
                # Check for periodic stats
                self.check_and_show_periodic_stats("Full Scan")
                
                # Track directories processed
                self.directories_processed += 1
                
                # Track files added/updated in this directory
                dir_new_files = 0
                dir_updated_files = 0
                dir_deferred_files = 0
                
                # Debug: Log large directories with timing
                dir_start_time = time.time()
                if self.debug and len(files) > 500:
                    logger.info(f"Scanning large directory: {root_path} (contains {len(files)} files, {len(dirs)} subdirs)")
                elif self.debug:
                    logger.debug(f"Scanning directory: {root_path} (contains {len(files)} files, {len(dirs)} subdirs)")
                
                # Skip directories that are on different mountpoints or system directories
                should_skip, skip_reason = self.should_skip_directory(root_path)
                if should_skip:
                    # Always log at info level if it's due to ignore patterns
                    if skip_reason and skip_reason.startswith('Ignored by pattern:'):
                        logger.info(f"Skipping directory: {root_path} ({skip_reason})")
                    elif self.debug or self.dry_run:
                        logger.info(f"Skipping directory: {root_path} ({skip_reason})")
                    dirs.clear()  # Don't recurse into this directory
                    continue
                
                # Filter subdirectories before traversing into them
                new_dirs = []
                for d in dirs:
                    dir_path = root_path / d

                    # First check if it's a mount point or system directory
                    should_skip, skip_reason = self.should_skip_directory(dir_path)
                    if should_skip:
                        # Always log at info level if it's due to ignore patterns
                        if skip_reason and skip_reason.startswith('Ignored by pattern:'):
                            logger.info(f"Skipping directory: {dir_path} ({skip_reason})")
                        elif self.debug or self.dry_run:
                            logger.info(f"Skipping directory: {dir_path} ({skip_reason})")
                        continue

                    # Skip nested subvolumes (inode 256 check) - we only index main volume
                    # This skips both regular subvolumes and snapshots
                    if self._is_subvolume_root(dir_path):
                        if self.debug or self.dry_run:
                            logger.debug(f"Skipping nested subvolume: {dir_path}")
                        continue

                    # Regular directory, include it
                    new_dirs.append(d)
                dirs[:] = new_dirs
                
                # Process files in batches for efficiency
                batch_size = 50
                for i in range(0, len(files), batch_size):
                    batch_files = files[i:i + batch_size]
                    
                    # Check for periodic stats in file processing loop
                    self.check_and_show_periodic_stats("Full Scan - File Processing")
                    
                    # Process batch efficiently
                    batch_stats = self.db.process_directory_batch(root_path, batch_files, self.should_skip_file)
                    
                    # Update counters
                    dir_new_files += batch_stats['new']
                    dir_updated_files += batch_stats['updated'] 
                    dir_deferred_files += batch_stats['deferred']
                    skipped_count += batch_stats['skipped']
                    
                    # Update skip reasons (we don't get detailed reasons from batch, but that's ok)
                    if batch_stats['skipped'] > 0:
                        skip_reasons['Various (batch processed)'] = skip_reasons.get('Various (batch processed)', 0) + batch_stats['skipped']
                    
                    file_count += len(batch_files)
                    
                    # Commit transaction every 1000 files
                    transaction_count += batch_stats['new'] + batch_stats['updated']
                    if transaction_count >= 1000:
                        self.db.commit_transaction()
                        self.db.begin_transaction()
                        transaction_count = 0
                        if self.debug:
                            logger.debug(f"Committed transaction at {file_count} files")
                    
                    if file_count % 1000 == 0:
                        logger.info(f"Scanned {file_count} files, skipped {skipped_count}...")
                    
                    # Debug: Show progress in large directories  
                    if self.debug and len(files) > 1000 and i % (batch_size * 10) == 0:
                        logger.debug(f"  Processed {i + len(batch_files)}/{len(files)} files in {root_path}")
                
                # Cleanup: Remove database entries for files that no longer exist in this directory
                cleanup_start = time.time()
                removed_count = self.db.cleanup_directory_files(root_path)
                total_removed_files += removed_count
                if removed_count > 0:
                    logger.info(f"Removed {removed_count} deleted files from database in {root_path}")
                    cleanup_time = time.time() - cleanup_start
                    if self.debug and cleanup_time > 1.0:
                        logger.debug(f"Directory cleanup took {cleanup_time:.1f}s")
                
                # Debug: Output directory processing summary with timing
                dir_elapsed = time.time() - dir_start_time
                if self.debug and (dir_new_files > 0 or dir_updated_files > 0 or dir_deferred_files > 0):
                    summary_parts = []
                    if dir_new_files > 0:
                        summary_parts.append(f"Added {dir_new_files} new files")
                    if dir_updated_files > 0:
                        summary_parts.append(f"Updated {dir_updated_files} files")
                    if dir_deferred_files > 0:
                        summary_parts.append(f"{dir_deferred_files} deferred")
                    
                    if len(files) > 500 or dir_elapsed > 5.0:
                        logger.info(f"Completed directory: {root_path} ({', '.join(summary_parts)}) in {dir_elapsed:.1f}s")
                    else:
                        logger.debug(f"Scanning directory: {root_path} (contains {len(files)} files, {len(dirs)} subdirs) ({', '.join(summary_parts)})")
            
            # Update metadata
            self.db.set_metadata('last_full_scan_completed', str(time.time()))
            
            # Use the generation captured at scan start for consistency
            scan_start_gen = self.db.get_metadata('scan_start_generation')
            if scan_start_gen:
                self.db.set_metadata('last_generation', scan_start_gen)
                logger.info(f"Set last_generation to scan start: {scan_start_gen}")
            else:
                # Fallback to current generation if we couldn't capture at start
                current_gen = self.btrfs.get_current_generation()
                self.db.set_metadata('last_generation', str(current_gen))
                logger.info(f"Set last_generation to current: {current_gen}")
            
            # Commit final transaction
            self.db.commit_transaction()
            
            elapsed = time.time() - start_time
            logger.info(f"[{self.volume_path}] Full scan complete: {file_count} files processed, {skipped_count} files skipped, {total_removed_files} deleted files removed in {elapsed:.1f} seconds")

            # Record metrics
            metrics.inc('files_scanned_total', file_count)
            metrics.set_gauge('last_scan_duration_seconds', elapsed)
            metrics.set_gauge('current_queue_depth', self.db.get_statistics()['dedup_candidates'])
            metrics.log_structured('scan_complete',
                                   scan_type='full',
                                   files_processed=file_count,
                                   files_skipped=skipped_count,
                                   files_removed=total_removed_files,
                                   duration_seconds=elapsed)

            # Log skip reasons summary
            if skip_reasons:
                logger.info("Files skipped by reason:")
                for reason, count in sorted(skip_reasons.items(), key=lambda x: x[1], reverse=True):
                    logger.info(f"  {reason}: {count} files")
                    
        except Exception as e:
            # Rollback transaction on error
            logger.error(f"Error during full scan: {e}")
            try:
                self.db.rollback_transaction()
            except:
                pass
            raise
    
    def check_for_changes(self):
        """Check for filesystem changes using BTRFS generations
        
        Note: We only process files where mtime/size actually changed, not just
        generation changes. This avoids re-processing files after deduplication
        or metadata-only changes, trading perfect accuracy for performance.
        """
        try:
            last_gen = int(self.db.get_metadata('last_generation', '0'))
            current_gen = self.btrfs.get_current_generation()
            
            if current_gen == last_gen:
                return False  # No changes
            
            # Check for generation wraparound
            if current_gen < last_gen:
                logger.warning(f"Generation wraparound detected ({last_gen} → {current_gen}), marking for full scan")
                self.db.set_metadata('needs_full_scan', '1')
                self.db.set_metadata('full_scan_reason', 'generation_wraparound')
                return False
            
            # Check if generation gap is too large
            max_gap = int(self.config.get('max_generation_gap', '10000'))
            if current_gen - last_gen > max_gap:
                logger.warning(f"Large generation gap ({current_gen - last_gen}), marking for full scan")
                self.db.set_metadata('needs_full_scan', '1')
                self.db.set_metadata('full_scan_reason', 'large_generation_gap')
                return False
            
            # Get changed files using callback for memory efficiency
            logger.debug(f"Checking for changes since generation {last_gen}")
            
            # Ensure we're not in a transaction before processing BTRFS changes
            self.db.ensure_clean_transaction_state()
            
            # Track stats for batch processing
            batch_stats = {'new': 0, 'updated': 0, 'unchanged': 0, 'skipped': 0}
            
            def process_batch(files):
                """Process a batch of files from BTRFS"""
                nonlocal batch_stats
                
                # Start transaction for this batch of files
                self.db.begin_transaction()
                try:
                    # Process all files in the batch within a single transaction
                    for file_info in files:
                        result = self.db.process_btrfs_change(file_info, self.should_skip_file)
                        batch_stats[result] += 1
                    
                    # Commit the entire batch
                    self.db.commit_transaction()
                    logger.debug(f"Committed batch of {len(files)} files to database")
                    
                except Exception as e:
                    # Rollback on error
                    logger.error(f"Error processing batch: {e}")
                    try:
                        self.db.rollback_transaction()
                    except:
                        pass
                    raise
                
                # Check for periodic stats during BTRFS change processing
                self.check_and_show_periodic_stats("BTRFS Change Processing")
            
            # Process files with streaming and batching
            # Exclude database files - they can't be deduplicated and would cause infinite loops
            db_exclude_patterns = ['*.db', '*.db-journal', '*.db-wal', '*.db-shm']
            total_processed, _ = self.btrfs.find_new_files(self.volume_path, last_gen,
                                                          process_callback=process_batch,
                                                          batch_size=1000,
                                                          exclude_patterns=db_exclude_patterns)
            
            # Log results if any files were processed
            if total_processed > 0:
                logger.info(f"Processed {total_processed} changed files (generation {last_gen} → {current_gen})")
                if batch_stats['new'] > 0 or batch_stats['updated'] > 0 or batch_stats['unchanged'] > 0:
                    logger.info(f"BTRFS changes: {batch_stats['new']} new files, {batch_stats['updated']} updated, {batch_stats['unchanged']} already marked")
                self.operation_start_time = time.time()  # Reset operation timer for BTRFS changes
            
            # Update last generation
            self.db.set_metadata('last_generation', str(current_gen))
            
            return total_processed > 0
                
        except Exception as e:
            logger.error(f"Error checking for changes: {e}")
            # Mark for full scan on error
            self.db.set_metadata('needs_full_scan', '1')
            self.db.set_metadata('full_scan_reason', 'generation_tracking_error')
            return False
    
    def check_and_perform_full_scan(self, daemon_mode=True):
        """Check if full scan is needed and perform it if so"""
        needs_scan = self.db.get_metadata('needs_full_scan', '0') == '1'
        last_scan = self.db.get_metadata('last_full_scan_completed')
        scan_interval_days = int(self.config.get('full_scan_interval_days', '30'))
        
        # Check if periodic rescan needed
        if last_scan:
            try:
                last_scan_time = float(last_scan)
                age_days = (time.time() - last_scan_time) / 86400
                if age_days >= scan_interval_days:
                    needs_scan = True
                    self.db.set_metadata('full_scan_reason', 'periodic_rescan')
                    logger.info(f"Monthly rescan due (last scan {age_days:.1f} days ago)")
            except:
                needs_scan = True
                self.db.set_metadata('full_scan_reason', 'invalid_last_scan_time')
        else:
            needs_scan = True  # No scan recorded
            self.db.set_metadata('full_scan_reason', 'initial_scan')
        
        if not needs_scan:
            return False
        
        # Check rate limiting only in daemon mode
        if daemon_mode:
            last_started = self.db.get_metadata('last_full_scan_started')
            if last_started:
                try:
                    age_hours = (time.time() - float(last_started)) / 3600
                    if age_hours < 24:
                        if self.debug:
                            logger.debug(f"Full scan rate limited ({age_hours:.1f} hours since last)")
                        return False
                except:
                    pass
        
        # Log reason for scan if available
        scan_reason = self.db.get_metadata('full_scan_reason', 'scheduled')
        logger.info(f"Full scan reason: {scan_reason}")
        
        # Capture BTRFS generation at scan start
        try:
            scan_start_generation = self.btrfs.get_current_generation()
            self.db.set_metadata('scan_start_generation', str(scan_start_generation))
            logger.info(f"Starting full filesystem scan at generation {scan_start_generation}")
        except Exception as e:
            logger.warning(f"Could not get generation at scan start: {e}")
        
        # Perform the full scan
        self.db.set_metadata('last_full_scan_started', str(time.time()))
        
        try:
            self.perform_full_scan()
            
            # Clear the needs_full_scan flag and reason
            self.db.set_metadata('needs_full_scan', '0')
            self.db.execute("DELETE FROM metadata WHERE key = 'full_scan_reason'")
            
            # Update last generation to scan start generation
            scan_start_gen = self.db.get_metadata('scan_start_generation')
            if scan_start_gen:
                self.db.set_metadata('last_generation', scan_start_gen)
                logger.info(f"Updated last_generation to scan start: {scan_start_gen}")
            
            # Perform vacuum after scan
            logger.info("Performing database vacuum after full scan")
            self.db.vacuum()
            
            return True
        except Exception as e:
            logger.error(f"Error during full scan: {e}")
            return False
    
    # ==========================================================================
    # DEDUPLICATION PROCESSING
    # ==========================================================================

    def process_deduplication_phase(self, daemon_mode=True):
        """Process deduplication using FIFO ordering with time limit.

        This is the main deduplication loop that:
        1. Gets next candidate file from database
        2. Hashes the file if not already hashed
        3. Finds and hashes all size twins
        4. Groups files by hash
        5. Runs duperemove on each group with 2+ files

        Args:
            daemon_mode: If True, enforces time limits and time windows
        """
        start_time = time.time()
        self.operation_start_time = start_time  # Reset operation timer for deduplication
        time_limit_minutes = float(self.config.get('dedup_time_limit_minutes', '10'))
        time_limit_seconds = time_limit_minutes * 60
        files_processed = 0
        
        # In non-daemon mode, process all files without time limit
        if not daemon_mode:
            time_limit_seconds = float('inf')
            if self.debug:
                logger.debug(f"Starting deduplication phase (no time limit - non-daemon mode)")
        else:
            if self.debug:
                logger.debug(f"Starting deduplication phase (max {time_limit_minutes} minutes)")
        
        while True:
            # Check for periodic stats during deduplication
            self.check_and_show_periodic_stats("Deduplication")
            
            # Check time limit only in daemon mode
            if daemon_mode:
                elapsed = time.time() - start_time
                if elapsed > time_limit_seconds:
                    if self.debug:
                        logger.debug(f"Deduplication time limit reached ({elapsed:.1f}s)")
                    break
                
                # Check if still within time window
                current_hour = datetime.now().hour
                start_hour = int(self.config.get('dedup_hours_start', '0'))
                end_hour = int(self.config.get('dedup_hours_end', '23'))
                if not (start_hour <= current_hour <= end_hour):
                    if self.debug:
                        logger.debug("Deduplication window closed")
                    break
            
            # Get next file to process (FIFO by mtime)
            min_size = int(self.config.get('min_file_size', '32768'))
            candidate = self.db.get_next_dedup_candidate_fifo(min_size, dry_run=self.dry_run)
            if not candidate:
                if self.debug:
                    logger.debug("No more deduplication candidates")
                break
            
            # Process the file
            file_path = candidate['path']
            file_id = candidate['id']
            
            # Verify file still exists and hasn't changed
            if not self.verify_file_current(candidate):
                continue
            
            # Check if file is open - skip for now if it is
            if self.is_file_open(file_path):
                # Check if file has changed while open
                try:
                    current_stat = os.stat(file_path)
                    if current_stat.st_mtime != candidate.get('mtime', 0):
                        # File changed while open - update database
                        self.db.update_file_size(file_id, current_stat.st_size, current_stat.st_mtime)
                        logger.debug(f"Open file {file_path} has changed - updated mtime and incremented change_count")
                except Exception as e:
                    logger.debug(f"Error checking open file {file_path}: {e}")
                
                # Set skip_until to prevent re-selection for 1 hour
                self.db.set_skip_until(file_id, skip_seconds=3600)
                logger.info(f"File is open, skipping for 1 hour: {file_path}")
                continue
            
            
            # Check if we already have a valid hash or should calculate one
            newly_hashed = False
            if candidate.get('hash'):
                # Have a hash, but check if we should re-hash based on backoff rules
                should_hash, reason = self.db.should_hash_file(file_id, candidate['mtime'])
                if should_hash:
                    logger.info(f"Re-hashing {self.format_file_size(candidate['size'])} byte file: {file_path} ({reason})")
                    file_hash = self.hash_file(file_path)
                    if not file_hash:
                        # File changed during hash or other error
                        self.db.handle_hash_failure(file_id, "File changed during hash")
                        continue
                    logger.info(f"  Hash: {file_hash}")
                    self.db.update_file_hash(file_id, file_hash)
                    self.files_hashed_session = getattr(self, 'files_hashed_session', 0) + 1
                    newly_hashed = True
                else:
                    # Use existing hash
                    file_hash = candidate['hash']
                    if self.debug:
                        logger.debug(f"Using existing hash for {file_path}: {file_hash[:16]}... ({reason})")
            else:
                # No hash yet, check if we should hash now
                should_hash, reason = self.db.should_hash_file(file_id, candidate['mtime'])
                if not should_hash:
                    if self.debug:
                        logger.debug(f"Skipping hash for {file_path}: {reason}")
                    # Don't mark as processed, will retry later
                    continue
                    
                # Need to calculate hash
                logger.info(f"Hashing {self.format_file_size(candidate['size'])} byte file: {file_path}...")
                file_hash = self.hash_file(file_path)
                if not file_hash:
                    # File changed during hash or other error
                    self.db.handle_hash_failure(file_id, "File changed during hash")
                    continue
                    
                logger.info(f"  Hash: {file_hash}")
                
                # Update hash in database
                self.db.update_file_hash(file_id, file_hash)
                
                # Track that we hashed a file
                self.files_hashed_session = getattr(self, 'files_hashed_session', 0) + 1
                newly_hashed = True
            
            # NEW OPTIMIZATION: If we just hashed a file, hash all pending files with the same size
            if newly_hashed:
                logger.info(f"Looking for other {self.format_file_size(candidate['size'])} files to hash...")
                size_twins = self.db.get_pending_files_by_size(candidate['size'], exclude_id=file_id)
                
                if size_twins:
                    logger.info(f"Found {len(size_twins)} pending files with same size")

                    # Track all files by hash for batch deduplication
                    # Use a dict to group files by their hash
                    files_by_hash = {file_hash: [(file_id, file_path, file_hash)]}

                    for twin in size_twins:
                        twin_id = twin['id']
                        twin_path = twin['path']

                        # Check if we should hash this twin
                        should_hash, reason = self.db.should_hash_file(twin_id, twin['mtime'])
                        if not should_hash:
                            if self.debug:
                                logger.debug(f"Skipping hash for size twin {twin_path}: {reason}")
                            continue

                        # Verify file still exists and hasn't changed
                        if not self.verify_file_current(twin):
                            continue

                        # Check if file is open
                        if self.is_file_open(twin_path):
                            if self.debug:
                                logger.debug(f"Size twin is open, skipping: {twin_path}")
                            continue

                        # Hash the size twin
                        logger.info(f"Hashing size twin: {twin_path}...")
                        twin_hash = self.hash_file(twin_path)
                        if not twin_hash:
                            self.db.handle_hash_failure(twin_id, "File changed during hash")
                            continue

                        logger.info(f"  Hash: {twin_hash}")
                        self.db.update_file_hash(twin_id, twin_hash)
                        self.files_hashed_session += 1

                        # Group files by their hash (not just matching original)
                        if twin_hash not in files_by_hash:
                            files_by_hash[twin_hash] = []
                        files_by_hash[twin_hash].append((twin_id, twin_path, twin_hash))

                    # Now deduplicate each hash group with 2+ files
                    from dedup import Deduplicator
                    dedup = Deduplicator(self.db, self.btrfs, self.config)
                    if hasattr(self, 'dry_run') and self.dry_run:
                        dedup.set_dry_run(True)

                    for hash_val, files_with_hash in files_by_hash.items():
                        if len(files_with_hash) > 1:
                            logger.info(f"Found {len(files_with_hash)} files with identical hash {hash_val[:16]}...")

                            # Use the first file as the reference
                            ref_id, ref_path, ref_hash = files_with_hash[0]

                            # Deduplicate all other files against the reference
                            for dup_id, dup_path, dup_hash in files_with_hash[1:]:
                                success, bytes_saved = dedup.deduplicate_pair_safely(dup_path, ref_path)
                                self.dedup_calls_made += 1

                                if success and bytes_saved > 0:
                                    self.session_bytes_deduped += bytes_saved
                                    self.files_deduped_session += 1

                                    # Update stats
                                    stats = self.db.get_statistics()
                                    logger.info(f"Space saved this session: {self.format_summary_size(self.session_bytes_deduped)}")
                                    logger.info(f"Status: {self.format_summary_size(stats.get('space_saveable', 0))} available for deduplication")

                                # Mark as processed
                                self.db.mark_file_dedup_processed(dup_id, dry_run=self.dry_run)

                        # Mark all files in this hash group as processed
                        # (single files with unique hashes also need to be marked)
                        for fid, fpath, fhash in files_with_hash:
                            self.db.mark_file_dedup_processed(fid, dry_run=self.dry_run)
            
            # Look for existing duplicates (files already processed)
            duplicate = self.db.find_duplicate_for_dedup(candidate['size'], file_hash, file_id)
            if duplicate:
                # Verify duplicate is still current
                if self.verify_file_current(duplicate):
                    # Attempt deduplication
                    from dedup import Deduplicator
                    dedup = Deduplicator(self.db, self.btrfs, self.config)
                    if hasattr(self, 'dry_run') and self.dry_run:
                        dedup.set_dry_run(True)
                    
                    # Safe deduplication with extent checking
                    success, bytes_saved = dedup.deduplicate_pair_safely(file_path, duplicate['path'])
                    self.dedup_calls_made += 1
                    
                    if success and bytes_saved > 0:
                        # Track session bytes saved
                        self.session_bytes_deduped += bytes_saved
                        self.files_deduped_session += 1
                        
                        # Update stats
                        stats = self.db.get_statistics()
                        saved_str = self.format_summary_size(stats.get('space_saved', 0))
                        logger.info(f"Deduplication complete. Total space saved: {saved_str}")
            
            # Mark as processed
            self.db.mark_file_dedup_processed(file_id, dry_run=self.dry_run)
            files_processed += 1
            
            # Check if we should process snapshot batch
            if files_processed % 10 == 0:  # Check every 10 files
                self.check_and_process_snapshot_batch(force=False)
        
        # Process any remaining queued snapshots at the end
        logger.debug("Processing any remaining queued snapshot copies...")
        snapshot_files, snapshot_bytes = self.check_and_process_snapshot_batch(force=True)
        
        # Calculate elapsed time for final message
        total_elapsed = time.time() - start_time
        
        if files_processed > 0 or snapshot_files > 0:
            logger.info(f"Processed {files_processed} files for deduplication in {total_elapsed:.1f}s")
            if snapshot_files > 0:
                logger.info(f"Additionally processed {snapshot_files} snapshot files")
        elif self.debug:
            logger.debug("No files were processed in deduplication phase")
        
        return files_processed > 0 or snapshot_files > 0
    
    # ==========================================================================
    # FILE VERIFICATION AND HASHING
    # ==========================================================================

    def verify_file_current(self, file_record):
        """Verify file exists and matches database metadata.

        Args:
            file_record: Dict with file info including 'path', 'id', 'size', 'mtime'

        Returns:
            True if file exists and metadata matches, False otherwise
        """
        file_path = file_record['path']
        
        if not os.path.exists(file_path):
            # File deleted
            self.db.remove_file(file_record['id'])
            return False
        
        try:
            stat = os.stat(file_path)
            if stat.st_size != file_record['size'] or stat.st_mtime != file_record.get('mtime', 0):
                # File changed, mark for reprocessing
                self.db.mark_file_changed(file_record['id'])
                return False
        except Exception as e:
            logger.debug(f"Error verifying file {file_path}: {e}")
            return False
        
        return True
    
    
    def is_file_open(self, filepath):
        """Check if file is open using lsof"""
        try:
            result = subprocess.run(
                ['lsof', str(filepath)],
                capture_output=True,
                check=False
            )
            return result.returncode == 0
        except:
            return False
    
    def hash_file(self, filepath):
        """Hash a file using configured algorithm, detecting changes during hash
        
        Note: We use fast hashing algorithms (Blake3/xxhash) for performance.
        While these have a small collision probability, duperemove will verify
        actual file content equality before any deduplication operation.
        """
        
        # Get file stats before hashing
        try:
            stat_before = os.stat(filepath)
            mtime_before = stat_before.st_mtime
            size_before = stat_before.st_size
        except (OSError, IOError):
            logger.warning(f"Cannot stat file before hashing: {filepath}")
            return None
        
        try:
            hasher = xxhash.xxh64()
            
            with open(filepath, 'rb') as f:
                while chunk := f.read(1024 * 1024):
                    hasher.update(chunk)
            
            # Check if file changed during hashing
            try:
                stat_after = os.stat(filepath)
                if (stat_after.st_mtime != mtime_before or 
                    stat_after.st_size != size_before):
                    logger.warning(f"File changed during hashing: {filepath}")
                    return None
            except (OSError, IOError):
                logger.warning(f"Cannot verify file after hashing: {filepath}")
                return None
            
            # Record successful hash
            metrics.inc('files_hashed_total')
            return hasher.hexdigest()

        except (OSError, IOError) as e:
            logger.warning(f"Error hashing file {filepath}: {e}")
            metrics.inc('hash_failures_total')
            return None
    
    # ==========================================================================
    # SUBVOLUME AND SNAPSHOT OPERATIONS
    # ==========================================================================

    def is_subvolume(self, path):
        """Check if path is a subvolume using fast inode 256 check.

        On BTRFS, inode 256 is always the root directory of a subvolume.
        This is much faster than calling 'btrfs subvolume show'.
        """
        try:
            path = Path(path) if not isinstance(path, Path) else path
            return path.is_dir() and path.stat().st_ino == 256
        except (OSError, IOError):
            return False

    def _is_subvolume_root(self, path):
        """Check if path is a nested subvolume root (not our main volume)

        Returns True if path is a subvolume AND is not the main volume we're indexing.
        Used during directory walk to skip nested subvolumes.
        """
        try:
            path = Path(path) if not isinstance(path, Path) else path
            # Must be a directory with inode 256 (subvolume root)
            # AND must not be our main volume path
            return (path.is_dir() and
                    path.stat().st_ino == 256 and
                    path.resolve() != self.volume_path.resolve())
        except (OSError, IOError):
            return False
    
    
    def check_and_process_snapshot_batch(self, force=False):
        """Check if snapshot batch processing is needed
        
        Args:
            force: If True, process regardless of queue size
            
        Returns:
            Tuple of (files_processed, bytes_saved)
        """
        queue_size = self.db.get_snapshot_copy_queue_size()
        batch_size_threshold = int(self.config.get('snapshot_batch_size', '100'))
        
        if queue_size == 0:
            return 0, 0
            
        if not force and queue_size < batch_size_threshold:
            if self.debug:
                logger.debug(f"Snapshot queue has {queue_size} entries, waiting for {batch_size_threshold}")
            return 0, 0
        
        logger.info(f"Processing snapshot batch (queue size: {queue_size}, "
                   f"threshold: {batch_size_threshold}, forced: {force})")
        
        # Create deduplicator instance and process the batch
        from dedup import Deduplicator
        dedup = Deduplicator(self.db, self.btrfs, self.config)
        if self.dry_run:
            dedup.set_dry_run(True)
            
        files_processed, bytes_saved = dedup.process_snapshot_batch()
        
        if bytes_saved > 0:
            self.session_bytes_deduped += bytes_saved
            logger.info(f"Snapshot batch processing saved {self.format_summary_size(bytes_saved)}")
            
        return files_processed, bytes_saved
    
    # ==========================================================================
    # STATUS DISPLAY
    # ==========================================================================

    def show_status(self):
        """Show current status when idle."""
        stats = self.db.get_statistics()
        
        # Calculate space saved in human-readable format
        space_saved = stats.get('space_saved', 0)
        space_saveable = stats.get('space_saveable', 0)
        
        if space_saved > 0:
            saved_str = self.format_summary_size(space_saved)
            logger.info(f"Status: {saved_str} deduplicated")
        
        if space_saveable > 0:
            saveable_str = self.format_summary_size(space_saveable)
            logger.info(f"Status: {saveable_str} available for deduplication")
        
        dedup_candidates = stats.get('dedup_candidates', 0)
        if dedup_candidates > 0:
            logger.info(f"Status: {dedup_candidates} files eligible for deduplication")
        else:
            logger.info("Status: Idle, waiting for changes")
    
