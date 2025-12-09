#!/usr/bin/env python3
"""
btrdupd - Btrfs deduplication daemon
Author: Brian Hanson (HansonXyz)
License: MIT
"""

import argparse
import logging
import os
import sys
import signal
import time
from pathlib import Path
import colorlog

from constants import APP_VERSION as __version__

def setup_logging(verbosity=0, quiet=False, log_file=None):
    """Setup logging with color support and verbosity levels"""
    if quiet:
        level = logging.ERROR
    elif verbosity == 0:
        level = logging.INFO
    elif verbosity == 1:
        level = logging.DEBUG
    else:
        level = logging.DEBUG  # Max verbosity shows everything
    
    # Console handler with color
    console_handler = colorlog.StreamHandler()
    console_handler.setFormatter(
        colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            }
        )
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        # Create log directory if needed
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Add rotating file handler
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        )
        root_logger.addHandler(file_handler)
        
        # Log to file that we're logging to file
        logger = logging.getLogger('btrdupd')
        logger.info(f"Logging to file: {log_file}")
    
    # Set third-party loggers to WARNING
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    return logging.getLogger('btrdupd')

def parse_size(size_str):
    """Parse human-readable size strings like '1M', '500K', '2G'"""
    units = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}
    size_str = size_str.upper().strip()
    
    if size_str[-1] in units:
        return int(float(size_str[:-1]) * units[size_str[-1]])
    else:
        return int(size_str)

def create_parser():
    """Create argument parser with all options including test flags"""
    parser = argparse.ArgumentParser(
        description='Btrfs file-level deduplication daemon',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  btrdupd /mnt/btrfs                         # Basic usage
  btrdupd /mnt/btrfs --min-size 1M           # Only deduplicate files >= 1MB
  btrdupd /mnt/btrfs --exclude "*.log"       # Exclude log files
  btrdupd /mnt/btrfs --test-generation       # Test generation tracking
        """
    )
    
    # Positional argument
    parser.add_argument('mount_points', nargs='+',
                       help='Btrfs filesystem mount point(s) to monitor')
    
    # Basic options
    parser.add_argument('-d', '--daemon', action='store_true',
                       help='Run as daemon in background')
    parser.add_argument('-v', '--verbose', action='count', default=0,
                       help='Increase verbosity (use multiple times for more)')
    parser.add_argument('-q', '--quiet', action='store_true',
                       help='Suppress non-error output')
    parser.add_argument('--version', action='version', 
                       version=f'%(prog)s {__version__}')
    parser.add_argument('--status', action='store_true',
                       help='Show status of running daemon')
    parser.add_argument('--daemon-status', action='store_true',
                       help='Query running daemon via socket for status')
    
    # Hidden debug flag (undocumented)
    parser.add_argument('--debug', action='store_true',
                       help=argparse.SUPPRESS)
    
    # File selection options
    parser.add_argument('--min-size', type=parse_size, default=32768,
                       help='Minimum file size to consider (default: 32K)')
    parser.add_argument('--max-size', type=parse_size, default=None,
                       help='Maximum file size to consider (default: unlimited)')
    parser.add_argument('--exclude', action='append', default=[],
                       help='Exclude files matching pattern (can be used multiple times)')
    parser.add_argument('--include-only', action='append', default=[],
                       help='Only process files matching pattern')
    
    # Performance options
    parser.add_argument('--nice-level', type=int, default=19,
                       help='Process nice level (default: 19)')
    parser.add_argument('--ionice-class', choices=['idle', 'best-effort', 'none'],
                       default='idle', help='I/O scheduling class (default: idle)')
    parser.add_argument('--scan-interval', type=int, default=120,
                       help='Interval between filesystem scans in seconds (default: 120)')
    parser.add_argument('--hash-threads', type=int, default=1,
                       help='Number of hashing threads (default: 1)')
    
    # Database options
    parser.add_argument('--db', type=str, action='append', default=[],
                       help='Database path for the preceding volume (can be used multiple times)')
    parser.add_argument('--dry-run', action='store_true',
                       help="Don't actually deduplicate, just report what would be done")
    
    # Snapshot handling
    parser.add_argument('--snapshot-mode', 
                       choices=['auto', 'skip', 'aggressive'], default='auto',
                       help='How to handle snapshots (default: auto)')
    parser.add_argument('--dedup-snapshots', action='store_true', default=None,
                       help='Enable reflink-based snapshot deduplication (default: from config)')
    parser.add_argument('--no-dedup-snapshots', action='store_true', default=False,
                       help='Disable snapshot deduplication')
    
    # Backoff options
    parser.add_argument('--backoff-threshold', type=int, default=5,
                       help='Number of changes before exponential backoff (default: 5)')
    parser.add_argument('--max-backoff', type=int, default=72,
                       help='Maximum backoff time in hours for volatile files (default: 72)')
    
    # Logging options
    parser.add_argument('--log-file', type=str, default=None,
                       help='Log to file in addition to console')
    
    # Display options
    parser.add_argument('--file-size-format', choices=['number', 'commas', 'human'],
                       default='commas', help='How to display file sizes (default: commas)')
    parser.add_argument('--size-unit-type', choices=['decimal', 'binary'],
                       default='decimal', help='Unit type: decimal (KB/MB/GB) or binary (KiB/MiB/GiB) (default: decimal)')
    
    # Service management
    parser.add_argument('--service', 
                       choices=['install', 'start', 'stop', 'restart', 'remove', 'status', 'logs'],
                       help='Systemd service management commands')
    
    # TEST FLAGS - For development and debugging
    test_group = parser.add_argument_group('test flags (for development)')
    
    test_group.add_argument('--test-generation', action='store_true',
                           help='Test generation tracking between filesystem changes')
    test_group.add_argument('--test-find-new', action='store_true',
                           help='Test finding new/modified files since generation')
    test_group.add_argument('--test-snapshot-enum', action='store_true',
                           help='Test snapshot enumeration and relationships')
    test_group.add_argument('--test-db-init', action='store_true',
                           help='Test database initialization and schema')
    test_group.add_argument('--test-hash-file', type=str,
                           help='Test hashing a specific file')
    test_group.add_argument('--test-path-tree', action='store_true',
                           help='Test path tree storage efficiency')
    test_group.add_argument('--test-backoff', action='store_true',
                           help='Test exponential backoff calculations')
    test_group.add_argument('--test-dedup-detection', action='store_true',
                           help='Test duplicate detection logic')
    test_group.add_argument('--test-snapshot-dedup', action='store_true',
                           help='Test snapshot deduplication strategy')
    test_group.add_argument('--test-inode-tracking', action='store_true',
                           help='Test inode tracking across snapshots')
    test_group.add_argument('--test-exclude-patterns', action='store_true',
                           help='Test file exclusion pattern matching')
    test_group.add_argument('--test-resume', action='store_true',
                           help='Test resume capability after interruption')
    test_group.add_argument('--test-monitor-loop', type=int,
                           help='Run monitor loop for N iterations in test mode')
    test_group.add_argument('--test-all', action='store_true',
                           help='Run all tests in sequence')
    test_group.add_argument('--test-mode', action='store_true',
                           help='Run in test mode with 5 second sleep instead of 5 minutes')
    
    # Debug options
    test_group.add_argument('--debug-sql', action='store_true',
                           help='Log all SQL queries')
    test_group.add_argument('--debug-btrfs-commands', action='store_true',
                           help='Log all btrfs command executions')
    test_group.add_argument('--force-rescan', action='store_true',
                           help='Force full filesystem rescan on startup')
    
    return parser

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger = logging.getLogger('btrdupd')
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    # TODO: Set shutdown flag for main loop
    sys.exit(0)

def check_dependencies(dry_run=False):
    """Check that required dependencies are installed"""
    logger = logging.getLogger('btrdupd')
    import shutil
    
    required = ['btrfs', 'sqlite3']
    if not dry_run:
        required.append('duperemove')
    
    missing = []
    for tool in required:
        if not shutil.which(tool):
            missing.append(tool)
    
    if missing:
        logger.error(f"Missing required tools: {', '.join(missing)}")
        logger.error("Please install missing dependencies and try again")
        return False
    
    logger.debug("All required dependencies found")
    return True

def verify_btrfs_mount(mount_point):
    """Verify that the mount point is a Btrfs filesystem"""
    logger = logging.getLogger('btrdupd')
    
    # Check if path exists and is a directory
    if not os.path.exists(mount_point):
        logger.error(f"Mount point does not exist: {mount_point}")
        return False
    
    if not os.path.isdir(mount_point):
        logger.error(f"Mount point is not a directory: {mount_point}")
        return False
    
    # Actually check if it's a Btrfs filesystem
    try:
        from btrfs import BtrfsFilesystem
        btrfs = BtrfsFilesystem(mount_point)
        logger.debug(f"Verified Btrfs filesystem at {mount_point}")
        return True
    except Exception as e:
        logger.error(f"Not a Btrfs filesystem at {mount_point}: {e}")
        return False

def run_tests(args, logger):
    """Run requested tests"""
    # Import test modules as needed
    from tests import test_runner
    
    results = []
    
    if args.test_all:
        logger.info("Running all tests...")
        results = test_runner.run_all_tests(args)
    else:
        if args.test_generation:
            logger.info("Testing generation tracking...")
            results.append(test_runner.test_generation_tracking(args))
        
        if args.test_find_new:
            logger.info("Testing find-new functionality...")
            results.append(test_runner.test_find_new(args))
        
        if args.test_snapshot_enum:
            logger.info("Testing snapshot enumeration...")
            results.append(test_runner.test_snapshot_enumeration(args))
        
        if args.test_db_init:
            logger.info("Testing database initialization...")
            results.append(test_runner.test_database_init(args))
        
        if args.test_hash_file:
            logger.info(f"Testing file hashing for: {args.test_hash_file}")
            results.append(test_runner.test_hash_file(args))
        
        if args.test_path_tree:
            logger.info("Testing path tree storage...")
            results.append(test_runner.test_path_tree(args))
        
        if args.test_backoff:
            logger.info("Testing exponential backoff...")
            results.append(test_runner.test_backoff(args))
        
        if args.test_dedup_detection:
            logger.info("Testing duplicate detection...")
            results.append(test_runner.test_dedup_detection(args))
        
        if args.test_snapshot_dedup:
            logger.info("Testing snapshot deduplication...")
            results.append(test_runner.test_snapshot_dedup(args))
        
        if args.test_inode_tracking:
            logger.info("Testing inode tracking...")
            results.append(test_runner.test_inode_tracking(args))
        
        if args.test_exclude_patterns:
            logger.info("Testing exclude patterns...")
            results.append(test_runner.test_exclude_patterns(args))
        
        if args.test_resume:
            logger.info("Testing resume capability...")
            results.append(test_runner.test_resume(args))
        
        if args.test_monitor_loop is not None:
            logger.info(f"Testing monitor loop for {args.test_monitor_loop} iterations...")
            results.append(test_runner.test_monitor_loop(args))
    
    # Summarize results
    failed = [r for r in results if not r[1]]
    if failed:
        logger.error(f"Tests failed: {len(failed)}/{len(results)}")
        for test_name, success, message in failed:
            logger.error(f"  FAILED: {test_name} - {message}")
        return False
    else:
        logger.info(f"All tests passed: {len(results)}/{len(results)}")
        return True

def parse_volume_db_paths(args):
    """Parse volume and database path associations from command line"""
    import sys
    
    # Parse raw command line to associate --db with volumes
    volume_db_map = {}
    argv = sys.argv[1:]
    
    current_volume = None
    i = 0
    while i < len(argv):
        arg = argv[i]
        
        # Check if it's a volume path (starts with / and not a flag)
        if arg.startswith('/') and not arg.startswith('--'):
            current_volume = arg
            volume_db_map[current_volume] = None  # Default to None
        # Check if it's a --db argument
        elif arg == '--db' and i + 1 < len(argv):
            if current_volume:
                volume_db_map[current_volume] = argv[i + 1]
            i += 1  # Skip the db path value
        elif arg.startswith('--db='):
            if current_volume:
                volume_db_map[current_volume] = arg.split('=', 1)[1]
        
        i += 1
    
    return volume_db_map

def main():
    """Main entry point"""
    parser = create_parser()
    args = parser.parse_args()
    
    # Parse volume-specific database paths
    volume_db_map = parse_volume_db_paths(args)
    
    # Setup logging
    logger = setup_logging(args.verbose, args.quiet, args.log_file)
    
    # Handle service management commands
    if args.service:
        from service_manager import ServiceManager
        manager = ServiceManager(args.mount_points, args)
        return manager.execute_command(args.service)
    
    # Handle daemon-status command
    if args.daemon_status:
        from status import DaemonStatusClient
        for mount_point in args.mount_points:
            client = DaemonStatusClient(mount_point)
            client.query_daemon_status()
        return 0
    
    # Handle status command
    if args.status:
        from status import StatusClient
        for mount_point in args.mount_points:
            client = StatusClient(mount_point)
            success = client.display_status(args)
            if not success:
                return 1
        return 0
    
    logger.info(f"btrdupd v{__version__} starting...")
    logger.debug(f"Command line args: {sys.argv[1:]}")
    
    # Verify all mount points
    for mount_point in args.mount_points:
        if not verify_btrfs_mount(mount_point):
            return 1
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Check if we're running tests (test_mode is for daemon, not running tests)
    test_flags = [arg for arg in vars(args) if arg.startswith('test_') and arg != 'test_mode' and getattr(args, arg)]
    if test_flags:
        logger.info("Running tests...")
        success = run_tests(args, logger)
        return 0 if success else 1
    
    # Check dependencies
    if not check_dependencies(args.dry_run):
        return 1
    
    # Check for root permissions if not in dry run mode
    if not args.dry_run:
        if os.geteuid() != 0:
            logger.error("btrdupd requires root privileges for deduplication operations")
            logger.error("Please run with sudo or as root, or use --dry-run for testing")
            return 1
    
    # Normal operation
    try:
        from btrdupd_daemon import BtrdupdDaemon
        
        daemon = BtrdupdDaemon(args, volume_db_map)
        
        if args.daemon:
            # Run as daemon
            logger.info("Running in daemon mode")
            daemon.run_daemon()
        else:
            # Run one-time and exit
            logger.info("Running in one-time mode")
            daemon.run_once()
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())