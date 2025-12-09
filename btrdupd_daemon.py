"""
Main daemon implementation for btrdupd
"""

import logging
import time
import fcntl
import os
import sys
import socket
import json
import threading
import subprocess
from pathlib import Path

from monitor import FileMonitor
from database import BtrdupdDatabase
from dedup import Deduplicator
from btrfs import BtrfsFilesystem
from status import StatusServer
from constants import CURRENT_DB_VERSION

logger = logging.getLogger('btrdupd.daemon')

SOCKET_DIR = "/var/run/btrdupd"

class BtrdupdDaemon:
    def __init__(self, args, volume_db_map=None):
        self.args = args
        self.running = True
        self.lock_fds = {}  # One lock per volume
        self.volume_paths = [Path(mp).resolve() for mp in args.mount_points]
        self.monitors = {}  # One monitor per volume
        self.status_servers = {}  # One status server per volume
        self.current_volume_index = 0
        self.volume_db_map = volume_db_map or {}
        
    def _get_db_path(self, volume_path):
        """Get database path for a volume"""
        # Check if there's a specific db path for this volume
        volume_str = str(volume_path)
        if volume_str in self.volume_db_map and self.volume_db_map[volume_str]:
            return Path(self.volume_db_map[volume_str])
        
        # Default: create database in .btrdupd directory in the volume root
        return volume_path / '.btrdupd' / 'btrdupd.db'
        
    def acquire_locks(self):
        """Acquire exclusive locks for all volumes"""
        for volume_path in self.volume_paths:
            db_path = self._get_db_path(volume_path)
            lock_file = f"{db_path}.lock"
            
            try:
                # Open lock file
                lock_fd = open(lock_file, 'w')
                
                # Try to acquire exclusive lock
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                
                # Write our PID
                lock_fd.write(str(os.getpid()))
                lock_fd.flush()
                
                self.lock_fds[volume_path] = lock_fd
                logger.info(f"Acquired lock for {volume_path}: {lock_file}")
                
            except IOError:
                # Check if lock is stale
                if os.path.exists(lock_file):
                    try:
                        with open(lock_file, 'r') as f:
                            pid = int(f.read().strip())
                        
                        # Check if process exists
                        os.kill(pid, 0)
                        
                        # Process exists, lock is valid
                        logger.error(f"Another btrdupd instance is running for {volume_path} (PID: {pid})")
                        self.release_locks()
                        return False
                        
                    except (ValueError, OSError, ProcessLookupError):
                        # Stale lock, remove it
                        logger.info(f"Removing stale lock file for {volume_path}")
                        try:
                            os.unlink(lock_file)
                            # Retry this volume
                            return self.acquire_locks()
                        except:
                            pass
                
                logger.error(f"Could not acquire lock for {volume_path}")
                self.release_locks()
                return False
        
        return True
    
    def release_locks(self):
        """Release all locks"""
        for volume_path, lock_fd in self.lock_fds.items():
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                lock_fd.close()
                db_path = self._get_db_path(volume_path)
                lock_file = f"{db_path}.lock"
                if os.path.exists(lock_file):
                    os.unlink(lock_file)
                logger.info(f"Released lock for {volume_path}")
            except:
                pass
        self.lock_fds.clear()
    
    def check_database_versions(self):
        """Check database version compatibility for all volumes"""
        for volume_path in self.volume_paths:
            db_path = self._get_db_path(volume_path)
            db = BtrdupdDatabase(str(db_path))
            db.connect()
            
            try:
                db_version = int(db.get_metadata('db_version', '0'))
                
                if db_version > CURRENT_DB_VERSION:
                    logger.error(f"Database version {db_version} is newer than supported version {CURRENT_DB_VERSION} for {volume_path}")
                    logger.error("Please upgrade btrdupd or delete the database to start fresh")
                    return False
                    
                if db_version == 0:
                    # New database
                    logger.info(f"Initializing new database for {volume_path}")
                    db.set_metadata('db_version', str(CURRENT_DB_VERSION))
                elif db_version < CURRENT_DB_VERSION:
                    # Need to migrate
                    logger.info(f"Database needs migration from v{db_version} to v{CURRENT_DB_VERSION} for {volume_path}")
                    # Migration is handled automatically by database.py
                    
            finally:
                db.close()
        
        return True
    
    def setup_nice_levels(self):
        """Set CPU and IO nice levels"""
        try:
            # Set CPU nice
            nice_level = self.args.nice_level if hasattr(self.args, 'nice_level') else 19
            os.nice(nice_level)
            logger.debug(f"Set CPU nice to {nice_level}")
            
            # Set IO nice
            io_class = self.args.ionice_class if hasattr(self.args, 'ionice_class') else 'idle'
            io_class_num = {'idle': '3', 'best-effort': '2', 'none': '0'}.get(io_class, '3')
            subprocess.run([
                'ionice', '-c', io_class_num, '-p', str(os.getpid())
            ], check=True)
            logger.debug(f"Set IO nice class to {io_class}")
        except Exception as e:
            logger.warning(f"Could not set process priority: {e}")
    
    def create_monitors(self):
        """Create monitor instances for all volumes"""
        for volume_path in self.volume_paths:
            db_path = self._get_db_path(volume_path)
            monitor = FileMonitor(
                str(volume_path),
                str(db_path),
                config=None  # Will load from .btrdupd.conf
            )
            
            # Override config with command line arguments
            if hasattr(self.args, 'file_size_format'):
                monitor.config['file_size_format'] = self.args.file_size_format
            if hasattr(self.args, 'size_unit_type'):
                monitor.config['size_unit_type'] = self.args.size_unit_type
            if hasattr(self.args, 'min_size'):
                monitor.config['min_file_size'] = str(self.args.min_size)
            
            # Set exclude/include patterns from command line
            monitor.set_filters(
                exclude_patterns=self.args.exclude,
                include_patterns=self.args.include_only
            )
            
            # Set debug and dry-run flags
            if hasattr(self.args, 'debug') and self.args.debug:
                monitor.debug = True
                monitor.btrfs.debug_commands = True  # Enable btrfs debug too
            if hasattr(self.args, 'debug_btrfs_commands') and self.args.debug_btrfs_commands:
                monitor.btrfs.debug_commands = True
            if hasattr(self.args, 'dry_run') and self.args.dry_run:
                monitor.dry_run = True
            
            self.monitors[volume_path] = monitor
            logger.info(f"Created monitor for {volume_path}")
    
    def show_single_volume_stats(self, volume_path, monitor):
        """Show stats for a single volume (used in one-time mode)"""
        logger.info("=== VOLUME STATUS ===")
        
        try:
            stats = monitor.db.get_statistics()
            
            total_files = stats.get('total_files', 0)
            hashed_files = stats.get('hashed_files', 0)
            dedup_candidates = stats.get('dedup_candidates', 0)
            
            # Format session bytes saved
            session_saved_str = monitor.format_summary_size(monitor.session_bytes_deduped) if monitor.session_bytes_deduped > 0 else "0 B"
            
            logger.info(f"[{volume_path}] Files: {total_files:,}, Hashed: {hashed_files:,}, "
                       f"Eligible: {dedup_candidates:,}, Session saved: {session_saved_str}, "
                       f"Dedup calls: {monitor.dedup_calls_made:,}, Dirs: {monitor.directories_processed:,}")
        except Exception as e:
            logger.debug(f"Error getting stats for {volume_path}: {e}")
        
        logger.info("=====================")
    
    def show_all_volume_stats(self):
        """Show debug stats for all volumes at once"""
        logger.info("=== ALL VOLUME STATUS ===")
        
        for volume_path in self.volume_paths:
            monitor = self.monitors.get(volume_path)
            if not monitor or not monitor.db:
                continue
                
            try:
                stats = monitor.db.get_statistics()
                
                total_files = stats.get('total_files', 0)
                hashed_files = stats.get('hashed_files', 0)
                dedup_candidates = stats.get('dedup_candidates', 0)
                
                # Format session bytes saved
                session_saved_str = monitor.format_summary_size(monitor.session_bytes_deduped) if monitor.session_bytes_deduped > 0 else "0 B"
                
                logger.info(f"[{volume_path}] Files: {total_files:,}, Hashed: {hashed_files:,}, "
                           f"Eligible: {dedup_candidates:,}, Session saved: {session_saved_str}, "
                           f"Dedup calls: {monitor.dedup_calls_made:,}, Dirs: {monitor.directories_processed:,}")
            except Exception as e:
                logger.debug(f"Error getting stats for {volume_path}: {e}")
        
        logger.info("========================")
    
    def setup_status_servers(self):
        """Setup status servers for daemon mode"""
        # Create socket directory
        socket_dir = Path(SOCKET_DIR)
        socket_dir.mkdir(parents=True, exist_ok=True)
        
        for volume_path in self.volume_paths:
            status_server = StatusServer(self, str(volume_path))
            status_server.start()
            self.status_servers[volume_path] = status_server
    
    def run_daemon(self):
        """Run in daemon mode with socket listener"""
        logger.info("Starting btrdupd daemon...")
        for i, volume_path in enumerate(self.volume_paths):
            logger.info(f"Volume {i+1}: {volume_path}")
        
        # Set process priority
        self.setup_nice_levels()
        
        # Acquire locks
        if not self.acquire_locks():
            logger.error("Could not acquire locks, exiting")
            return
        
        try:
            # Check database versions
            if not self.check_database_versions():
                return
            
            # Create monitors
            self.create_monitors()
            
            # Setup status servers
            self.setup_status_servers()
            
            # Connect all databases
            for monitor in self.monitors.values():
                monitor.db.connect()
            
            # Reset dry_run_dedup flags if in dry run mode
            if self.args.dry_run:
                logger.info("Dry run mode: Resetting dry_run_dedup flags")
                for monitor in self.monitors.values():
                    monitor.db.reset_dry_run_dedup_flags()
            
            # Track last time we showed all-volume stats
            last_all_volume_stats = 0
            
            
            # Main daemon loop
            while self.running:
                try:
                    work_done_this_cycle = False
                    
                    # Check if it's time to show all-volume debug stats
                    current_time = time.time()
                    if self.args.debug and current_time - last_all_volume_stats > 60:
                        self.show_all_volume_stats()
                        last_all_volume_stats = current_time
                    
                    # Process all volumes in sequence
                    for volume_path in self.volume_paths:
                        monitor = self.monitors[volume_path]
                        
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"[DAEMON] Processing volume: {volume_path}")
                        
                        # Process one iteration for this volume
                        processed = monitor.process_one_iteration()
                        
                        if processed:
                            work_done_this_cycle = True
                        
                        # Handle status requests
                        for status_server in self.status_servers.values():
                            status_server.handle_requests()
                    
                    # Only sleep if no work was done across all volumes
                    if not work_done_this_cycle:
                        # Check for test mode - use 5 seconds instead of 5 minutes
                        if getattr(self.args, 'test_mode', False):
                            sleep_seconds = 5
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(f"[DAEMON] Test mode: sleeping for {sleep_seconds} seconds")
                            time.sleep(sleep_seconds)
                        else:
                            # Get sleep interval from first monitor's config
                            sleep_minutes = 5  # default
                            if self.monitors:
                                first_monitor = next(iter(self.monitors.values()))
                                sleep_minutes = float(first_monitor.config.get('scan_interval_minutes', '5'))

                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(f"[DAEMON] No work done across all volumes, sleeping for {sleep_minutes} minutes")

                            time.sleep(sleep_minutes * 60)
                    elif logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"[DAEMON] Work was done, continuing immediately")
                        
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal")
                    self.running = False
                except Exception as e:
                    logger.error(f"Error in daemon loop: {e}")
                    time.sleep(60)
            
        except Exception as e:
            logger.exception(f"Fatal error: {e}")
        finally:
            # Show session summary if we processed anything
            if hasattr(self, 'monitors') and self.monitors:
                try:
                    # Calculate total session stats
                    total_files_hashed_session = 0
                    total_files_deduped_session = 0
                    total_bytes_saved = 0
                    
                    for monitor in self.monitors.values():
                        total_files_hashed_session += monitor.files_hashed_session
                        total_files_deduped_session += monitor.files_deduped_session
                        total_bytes_saved += monitor.session_bytes_deduped
                    
                    if total_files_hashed_session > 0 or total_files_deduped_session > 0:
                        logger.info("=== SESSION SUMMARY ===")
                        logger.info(f"Files hashed: {total_files_hashed_session:,}")
                        
                        if self.args.dry_run:
                            logger.info("DRY RUN - No files were deduplicated")
                        else:
                            logger.info(f"Files deduplicated: {total_files_deduped_session:,}")
                            if total_bytes_saved > 0:
                                saved_str = self.monitors[list(self.monitors.keys())[0]].format_summary_size(total_bytes_saved)
                                logger.info(f"Space saved: {saved_str}")
                        logger.info("=======================")
                except:
                    pass
            
            # Stop status servers
            for status_server in self.status_servers.values():
                status_server.stop()
            
            # Close databases
            for monitor in self.monitors.values():
                if monitor.db.conn:
                    monitor.db.close()
            
            self.release_locks()
    
    def run_once(self):
        """Run once and exit"""
        start_time = time.time()  # Track total runtime
        logger.info("Running btrdupd in one-time mode...")
        for i, volume_path in enumerate(self.volume_paths):
            logger.info(f"Volume {i+1}: {volume_path}")
        
        # Set process priority
        self.setup_nice_levels()
        
        # Acquire locks
        if not self.acquire_locks():
            logger.error("Could not acquire locks, exiting")
            return
        
        try:
            # Check database versions
            if not self.check_database_versions():
                return
            
            # Create monitors
            self.create_monitors()
            
            # Connect all databases
            for monitor in self.monitors.values():
                monitor.db.connect()
            
            # Reset dry_run_dedup flags if in dry run mode
            if self.args.dry_run:
                logger.info("Dry run mode: Resetting dry_run_dedup flags")
                for monitor in self.monitors.values():
                    monitor.db.reset_dry_run_dedup_flags()
            
            # Track last time we showed stats (for one-time mode)
            last_stats_time = 0
            
            # Process each volume fully
            for volume_path in self.volume_paths:
                logger.info(f"Processing volume: {volume_path}")
                monitor = self.monitors[volume_path]
                
                # Ensure initial scan if needed (without rate limiting)
                if monitor.needs_initial_scan():
                    logger.info("Performing initial full scan (one-time mode)")
                    monitor.perform_full_scan()
                
                # Process all phases without time restrictions
                iteration = 0
                work_done = True
                
                while work_done:
                    iteration += 1
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"[ONE-TIME] Iteration {iteration} for {volume_path}")
                    
                    # Show stats every minute in debug mode (one-time mode shows only current volume)
                    current_time = time.time()
                    if self.args.debug and current_time - last_stats_time > 60:
                        self.show_single_volume_stats(volume_path, monitor)
                        last_stats_time = current_time
                    
                    # Process with daemon_mode=False (no time limits)
                    work_done = monitor.process_one_iteration(daemon_mode=False)
                    
                    if work_done:
                        # Show progress
                        monitor.periodic_events()
                    else:
                        logger.info(f"Volume {volume_path} processing complete")
                
                # Final statistics for this volume
                monitor.show_status()
                logger.info(f"Completed processing {volume_path}")
            
            # Show final stats for all volumes
            logger.info("=== FINAL SUMMARY ===")
            self.show_all_volume_stats()
            
            # Calculate runtime
            total_runtime = time.time() - start_time
            hours = int(total_runtime // 3600)
            minutes = int((total_runtime % 3600) // 60)
            seconds = int(total_runtime % 60)
            runtime_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            
            # Calculate total session stats
            total_files = 0
            total_hashed = 0
            total_eligible = 0
            total_bytes_saved = 0
            total_dedup_calls = 0
            total_files_hashed_session = 0
            total_files_deduped_session = 0
            total_potential_savings = 0
            total_ready_for_dedup = 0
            
            for monitor in self.monitors.values():
                try:
                    stats = monitor.db.get_statistics()
                    total_files += stats.get('total_files', 0)
                    total_hashed += stats.get('hashed_files', 0)
                    total_eligible += stats.get('dedup_candidates', 0)
                    total_bytes_saved += monitor.session_bytes_deduped
                    total_dedup_calls += monitor.dedup_calls_made
                    total_files_hashed_session += monitor.files_hashed_session
                    total_files_deduped_session += monitor.files_deduped_session
                    
                    # Get potential savings for dry run
                    if self.args.dry_run:
                        total_potential_savings += monitor.db.get_potential_space_savings()
                        total_ready_for_dedup += monitor.db.get_dry_run_ready_count()
                except Exception as e:
                    logger.debug(f"Error gathering stats: {e}")
                    pass
            
            # Show enhanced summary
            logger.info("=== SESSION STATISTICS ===")
            logger.info(f"Total runtime: {runtime_str}")
            logger.info(f"Total files tracked: {total_files:,}")
            logger.info(f"Files hashed this session: {total_files_hashed_session:,}")
            logger.info(f"Total files with hashes: {total_hashed:,}")
            
            if self.args.dry_run:
                # Dry run mode statistics
                logger.info(f"Files ready for deduplication: {total_ready_for_dedup:,}")
                if total_potential_savings > 0:
                    potential_str = self.monitors[self.volume_paths[0]].format_summary_size(total_potential_savings)
                    logger.info(f"Potential space savings: {potential_str}")
                logger.info("DRY RUN COMPLETE - No actual deduplication performed")
            else:
                # Live mode statistics
                logger.info(f"Files deduplicated this session: {total_files_deduped_session:,}")
                if total_bytes_saved > 0:
                    saved_str = self.monitors[self.volume_paths[0]].format_summary_size(total_bytes_saved)
                    logger.info(f"Space saved this session: {saved_str}")
                
                # Get total space saved all time
                total_space_saved = 0
                for monitor in self.monitors.values():
                    try:
                        total_space_saved += monitor.db.get_actual_space_saved()
                    except:
                        pass
                
                if total_space_saved > 0:
                    total_saved_str = self.monitors[self.volume_paths[0]].format_summary_size(total_space_saved)
                    logger.info(f"Total space saved (all time): {total_saved_str}")
            
            logger.info(f"Files eligible for deduplication: {total_eligible:,}")
            logger.info("==========================")
            logger.info("All volumes processed successfully")
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.exception(f"Fatal error: {e}")
        finally:
            # Close databases
            for monitor in self.monitors.values():
                if monitor.db.conn:
                    monitor.db.close()
            
            self.release_locks()