"""
Status display and daemon communication for btrdupd
"""

import os
import json
import socket
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta
import humanize
from tabulate import tabulate

from database import BtrdupdDatabase
from btrfs import BtrfsFilesystem

logger = logging.getLogger('btrdupd.status')

SOCKET_DIR = "/var/run/btrdupd"

class DaemonStatusClient:
    """Client for querying daemon status via socket"""
    
    def __init__(self, mount_point):
        self.mount_point = Path(mount_point).resolve()
        self.socket_path = Path(SOCKET_DIR) / f"{self.mount_point.name}.sock"
    
    def query_daemon_status(self):
        """Query daemon status and display it"""
        if not self.socket_path.exists():
            print(f"No daemon running for {self.mount_point} (socket not found)")
            return
        
        try:
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.settimeout(5)
            client.connect(str(self.socket_path))
            
            # Send status request
            request = json.dumps({"command": "status"})
            client.send(request.encode())
            
            # Receive response
            response = b""
            while True:
                data = client.recv(4096)
                if not data:
                    break
                response += data
            
            client.close()
            
            status = json.loads(response.decode())
            
            # Display status
            print(f"\n=== Daemon Status for {self.mount_point} ===")
            print(f"PID: {status.get('pid', 'unknown')}")
            print(f"Uptime: {humanize.naturaldelta(status.get('uptime', 0))}")
            print(f"Current task: {status.get('current_task', 'idle')}")
            print(f"Queue length: {status.get('queue_length', 0)}")
            print(f"Processing rate: {status.get('files_per_hour', 0):.1f} files/hour")
            
            if 'performance' in status:
                perf = status['performance']
                print(f"\nPerformance:")
                print(f"  CPU: {perf.get('cpu_percent', 0):.1f}%")
                print(f"  Memory: {humanize.naturalsize(perf.get('memory_bytes', 0))}")
                print(f"  Disk read: {humanize.naturalsize(perf.get('disk_read_rate', 0))}/s")
                print(f"  Hash rate: {humanize.naturalsize(perf.get('hash_rate', 0))}/s")
            
        except socket.timeout:
            print(f"Timeout connecting to daemon for {self.mount_point}")
        except ConnectionRefusedError:
            print(f"Daemon socket exists but connection refused for {self.mount_point}")
        except Exception as e:
            print(f"Error querying daemon for {self.mount_point}: {e}")

class StatusClient:
    """Client for communicating with running daemon"""
    
    def __init__(self, mount_point):
        self.mount_point = Path(mount_point).resolve()
        self.socket_path = Path(SOCKET_DIR) / f"{self.mount_point.name}.sock"
    
    def get_daemon_status(self):
        """Try to get status from running daemon via socket"""
        if not self.socket_path.exists():
            return None
        
        try:
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.settimeout(5)
            client.connect(str(self.socket_path))
            
            # Send status request
            request = json.dumps({"command": "status"})
            client.send(request.encode())
            
            # Receive response
            response = b""
            while True:
                data = client.recv(4096)
                if not data:
                    break
                response += data
            
            client.close()
            return json.loads(response.decode())
            
        except Exception as e:
            logger.debug(f"Could not connect to daemon: {e}")
            return None
    
    def display_status(self, args):
        """Display comprehensive status information"""
        # Try to get daemon status first
        daemon_status = self.get_daemon_status()
        
        # Get database path
        db_path = args.db_path or (self.mount_point / ".btrdupd.db.sqlite")
        
        if not db_path.exists():
            print(f"No btrdupd database found at {db_path}")
            print("Has btrdupd been run on this filesystem?")
            return False
        
        # Connect to database
        try:
            db = BtrdupdDatabase(db_path, debug_sql=False)
            db.connect()
            
            # Get statistics
            stats = db.get_statistics()
            
            # Get filesystem info
            btrfs = BtrfsFilesystem(self.mount_point)
            fs_info = btrfs.get_filesystem_info()
            
            # Calculate additional metrics
            if stats['total_files'] > 0:
                hash_progress = (stats['hashed_files'] / stats['total_files']) * 100
            else:
                hash_progress = 0
            
            # Get recent activity
            recent_activities = db.execute("""
                SELECT timestamp, action, details 
                FROM activity_log 
                ORDER BY timestamp DESC 
                LIMIT 10
            """).fetchall()
            
            # Get duplicate groups by size
            top_duplicates = db.execute("""
                SELECT hash, size, file_count, (file_count - 1) * size as waste
                FROM dedup_groups 
                ORDER BY waste DESC 
                LIMIT 10
            """).fetchall()
            
            # Display header
            print("\n" + "="*80)
            print(f"btrdupd Status Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*80)
            
            # Daemon status
            if daemon_status:
                print("\n📡 DAEMON STATUS")
                print(f"  Status: Running (PID: {daemon_status.get('pid', 'unknown')})")
                print(f"  Uptime: {humanize.naturaldelta(daemon_status.get('uptime', 0))}")
                print(f"  Current task: {daemon_status.get('current_task', 'idle')}")
            else:
                print("\n📡 DAEMON STATUS")
                print("  Status: Not running")
            
            # Filesystem info
            print("\n💾 FILESYSTEM INFO")
            print(f"  Mount point: {fs_info.get('mount_point', 'unknown')}")
            print(f"  Generation: {fs_info.get('generation', 'unknown')}")
            if 'device_size' in fs_info:
                print(f"  Total size: {fs_info['device_size']}")
            if 'free_estimated' in fs_info:
                print(f"  Free space: {fs_info['free_estimated']}")
            
            # Check for configuration issues
            config_path = Path(args.mount_point) / '.btrdupd.conf'
            if config_path.exists():
                import configparser
                config = configparser.ConfigParser()
                config.read(config_path)
                min_size = int(config.get('DEFAULT', 'min_file_size', '8192'))
                if min_size < 2048:
                    print(f"\n⚠️  WARNING: Configured minimum file size ({min_size} bytes) is below BTRFS inline threshold (2048 bytes)")
                    print("   Using 2048 bytes as minimum")
            
            # File statistics
            print("\n📊 FILE STATISTICS")
            print(f"  Total files tracked: {stats['total_files']:,}")
            print(f"  Files hashed: {stats['hashed_files']:,} ({hash_progress:.1f}%)")
            print(f"  Files pending: {stats['pending_files']:,}")
            print(f"  Path nodes in DB: {stats['path_nodes']:,}")
            
            # Deduplication statistics
            print("\n♻️  DEDUPLICATION STATISTICS")
            print(f"  Duplicate groups found: {stats['duplicate_groups']:,}")
            print(f"  Total duplicate files: {stats['total_duplicates']:,}")
            print(f"  Space currently wasted: {humanize.naturalsize(stats['space_saveable'])}")
            print(f"  Space already saved: {humanize.naturalsize(stats['space_saved'])}")

            if stats['space_saveable'] > 0 and stats['space_saved'] > 0:
                total_dedup = stats['space_saveable'] + stats['space_saved']
                dedup_ratio = (stats['space_saved'] / total_dedup) * 100
                print(f"  Deduplication progress: {dedup_ratio:.1f}%")

            # Cumulative stats (all-time totals)
            cumulative = db.get_dedup_stats_summary()
            if cumulative['dedup_operations'] > 0:
                print(f"\n📈 CUMULATIVE STATISTICS (All-Time)")
                print(f"  Dedup operations: {cumulative['dedup_operations']:,}")
                print(f"  Files deduplicated: {cumulative['files_deduped']:,}")
                print(f"  Potential space savings: {humanize.naturalsize(cumulative['bytes_potential_savings'])}")
                if cumulative['snapshot_files_deduped'] > 0:
                    print(f"  Snapshot files included: {cumulative['snapshot_files_deduped']:,}")
            
            # Get actual reflink stats from filesystem
            try:
                reflink_stats = self._get_reflink_stats()
                if reflink_stats:
                    print(f"\n🔗 FILESYSTEM REFLINK STATISTICS")
                    print(f"  Total reflinked data: {humanize.naturalsize(reflink_stats['shared_bytes'])}")
                    print(f"  Reflink savings: {humanize.naturalsize(reflink_stats['saved_bytes'])}")
            except:
                pass
            
            # Queue status
            if daemon_status and 'queue_length' in daemon_status:
                print(f"\n📋 QUEUE STATUS")
                print(f"  Files queued for hashing: {daemon_status['queue_length']:,}")
                print(f"  Processing rate: {daemon_status.get('files_per_hour', 0):.1f} files/hour")
                
                if daemon_status.get('files_per_hour', 0) > 0 and stats['pending_files'] > 0:
                    eta_hours = stats['pending_files'] / daemon_status['files_per_hour']
                    print(f"  Estimated completion: {humanize.naturaldelta(eta_hours * 3600)}")
            
            # Top duplicate groups
            if top_duplicates:
                print("\n🏆 TOP DUPLICATE GROUPS (by wasted space)")
                table_data = []
                for i, dup in enumerate(top_duplicates, 1):
                    table_data.append([
                        i,
                        dup['hash'][:12] + "...",
                        humanize.naturalsize(dup['size']),
                        dup['file_count'],
                        humanize.naturalsize(dup['waste'])
                    ])
                
                headers = ['#', 'Hash', 'File Size', 'Count', 'Wasted Space']
                print(tabulate(table_data, headers=headers, tablefmt='simple'))
            
            # Recent activity
            if recent_activities:
                print("\n📜 RECENT ACTIVITY")
                for act in recent_activities[:5]:
                    timestamp = datetime.fromtimestamp(act['timestamp'])
                    time_ago = humanize.naturaltime(timestamp)
                    print(f"  {time_ago}: {act['action']}")
            
            # Performance metrics
            if daemon_status and 'performance' in daemon_status:
                perf = daemon_status['performance']
                print("\n⚡ PERFORMANCE METRICS")
                print(f"  CPU usage: {perf.get('cpu_percent', 0):.1f}%")
                print(f"  Memory usage: {humanize.naturalsize(perf.get('memory_bytes', 0))}")
                print(f"  Disk read rate: {humanize.naturalsize(perf.get('disk_read_rate', 0))}/s")
                print(f"  Hash rate: {humanize.naturalsize(perf.get('hash_rate', 0))}/s")
            
            print("\n" + "="*80)
            
            db.close()
            return True
            
        except Exception as e:
            logger.error(f"Failed to display status: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _get_reflink_stats(self):
        """Try to get reflink statistics from btrfs"""
        try:
            # This is a placeholder - actual implementation would parse
            # btrfs filesystem df output for shared extents
            return None
        except:
            return None

class StatusServer:
    """Server side for daemon status reporting"""
    
    def __init__(self, daemon, mount_point):
        self.daemon = daemon
        self.mount_point = Path(mount_point).resolve()
        self.socket_path = Path(SOCKET_DIR) / f"{self.mount_point.name}.sock"
        self.server = None
        self.start_time = time.time()
    
    def start(self):
        """Start status server"""
        try:
            # Create socket directory
            self.socket_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Remove old socket if exists
            if self.socket_path.exists():
                self.socket_path.unlink()
            
            # Create Unix socket
            self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.server.bind(str(self.socket_path))
            self.server.listen(1)
            self.server.settimeout(0.1)  # Non-blocking
            
            logger.info(f"Status server listening on {self.socket_path}")
            
        except Exception as e:
            logger.error(f"Failed to start status server: {e}")
    
    def handle_requests(self):
        """Handle status requests (call from main loop)"""
        if not self.server:
            return
        
        try:
            conn, _ = self.server.accept()
            data = conn.recv(1024)
            
            if data:
                request = json.loads(data.decode())
                
                if request.get('command') == 'status':
                    response = self._get_status()
                    conn.send(json.dumps(response).encode())
            
            conn.close()
            
        except socket.timeout:
            pass  # No requests
        except Exception as e:
            logger.error(f"Error handling status request: {e}")
    
    def _get_status(self):
        """Get current daemon status"""
        return {
            'pid': os.getpid(),
            'uptime': time.time() - self.start_time,
            'current_task': self.daemon.current_task if hasattr(self.daemon, 'current_task') else 'idle',
            'queue_length': self.daemon.queue_length if hasattr(self.daemon, 'queue_length') else 0,
            'files_per_hour': self.daemon.files_per_hour if hasattr(self.daemon, 'files_per_hour') else 0,
            'performance': {
                'cpu_percent': self._get_cpu_usage(),
                'memory_bytes': self._get_memory_usage(),
                'disk_read_rate': self.daemon.disk_read_rate if hasattr(self.daemon, 'disk_read_rate') else 0,
                'hash_rate': self.daemon.hash_rate if hasattr(self.daemon, 'hash_rate') else 0
            }
        }
    
    def _get_cpu_usage(self):
        """Get current CPU usage"""
        try:
            import psutil
            return psutil.Process().cpu_percent()
        except:
            return 0
    
    def _get_memory_usage(self):
        """Get current memory usage"""
        try:
            import psutil
            return psutil.Process().memory_info().rss
        except:
            return 0
    
    def stop(self):
        """Stop status server"""
        if self.server:
            self.server.close()
            if self.socket_path.exists():
                self.socket_path.unlink()