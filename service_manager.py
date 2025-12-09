"""
Systemd service management for btrdupd
Handles installation, control, and monitoring of btrdupd systemd services
"""

import os
import sys
import subprocess
import logging
from pathlib import Path
import shutil

logger = logging.getLogger('btrdupd.service')

class ServiceManager:
    """Manages systemd service for btrdupd"""
    
    # Common systemd paths by distribution
    SYSTEMD_PATHS = [
        '/etc/systemd/system',           # Most common location
        '/usr/lib/systemd/system',       # Fallback location
        '/lib/systemd/system',           # Debian/Ubuntu alternative
    ]
    
    SERVICE_NAME = 'btrdupd.service'
    
    def __init__(self, mount_points, args):
        self.mount_points = mount_points
        self.args = args
        self.service_path = self._find_systemd_path()
        
    def _find_systemd_path(self):
        """Find the appropriate systemd service directory"""
        for path in self.SYSTEMD_PATHS:
            if os.path.exists(path) and os.path.isdir(path):
                return path
        return None
        
    def _check_root(self):
        """Check if running as root"""
        if os.geteuid() != 0:
            logger.error("Service management requires root privileges")
            logger.error("Please run with sudo or as root")
            return False
        return True
        
    def _check_systemd(self):
        """Check if systemd is available"""
        try:
            result = subprocess.run(['systemctl', '--version'], 
                                  capture_output=True, check=False)
            if result.returncode != 0:
                logger.error("systemd not found or not available")
                return False
            return True
        except FileNotFoundError:
            logger.error("systemctl command not found - is systemd installed?")
            return False
            
    def execute_command(self, command):
        """Execute the requested service command"""
        # Check prerequisites
        if not self._check_systemd():
            return 1
            
        if command != 'status' and not self._check_root():
            return 1
            
        if not self.service_path and command == 'install':
            logger.error("Could not find systemd service directory")
            logger.error(f"Searched in: {', '.join(self.SYSTEMD_PATHS)}")
            return 1
            
        # Execute the command
        method = getattr(self, f'cmd_{command}', None)
        if method:
            return method()
        else:
            logger.error(f"Unknown service command: {command}")
            return 1
            
    def cmd_install(self):
        """Install systemd service"""
        logger.info("Installing btrdupd systemd service...")
        
        # Find btrdupd executable
        btrdupd_path = shutil.which('btrdupd')
        if not btrdupd_path:
            # Try current script location
            btrdupd_path = os.path.abspath(sys.argv[0])
            if not os.path.exists(btrdupd_path):
                logger.error("Could not find btrdupd executable")
                return 1
                
        logger.info(f"Using btrdupd at: {btrdupd_path}")
        
        # Create service content
        service_content = f"""[Unit]
Description=BTRFS Deduplication Daemon
After=multi-user.target
# Monitored volumes: {', '.join(self.mount_points)}

[Service]
Type=simple
ExecStart={btrdupd_path} {' '.join(self.mount_points)} --daemon
Restart=on-failure
RestartSec=30
StandardOutput=journal
StandardError=journal
SyslogIdentifier=btrdupd

# Security settings
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths={' '.join(self.mount_points)}
ReadWritePaths=/var/run/btrdupd

[Install]
WantedBy=multi-user.target
"""
        
        # Write service file
        service_file = os.path.join(self.service_path, self.SERVICE_NAME)
        try:
            with open(service_file, 'w') as f:
                f.write(service_content)
            os.chmod(service_file, 0o644)
            logger.info(f"Created service file: {service_file}")
        except Exception as e:
            logger.error(f"Failed to create service file: {e}")
            return 1
            
        # Reload systemd
        try:
            subprocess.run(['systemctl', 'daemon-reload'], check=True)
            logger.info("Reloaded systemd configuration")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to reload systemd: {e}")
            return 1
            
        # Enable service
        try:
            subprocess.run(['systemctl', 'enable', self.SERVICE_NAME], check=True)
            logger.info("Enabled btrdupd service")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to enable service: {e}")
            return 1
            
        # Start service
        try:
            subprocess.run(['systemctl', 'start', self.SERVICE_NAME], check=True)
            logger.info("Started btrdupd service")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start service: {e}")
            logger.info("You can start it manually with: systemctl start btrdupd")
            return 1
            
        logger.info("btrdupd service installed and started successfully")
        return 0
        
    def cmd_start(self):
        """Start the service"""
        logger.info("Starting btrdupd service...")
        try:
            subprocess.run(['systemctl', 'start', self.SERVICE_NAME], check=True)
            logger.info("Service started successfully")
            return 0
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start service: {e}")
            return 1
            
    def cmd_stop(self):
        """Stop the service"""
        logger.info("Stopping btrdupd service...")
        try:
            subprocess.run(['systemctl', 'stop', self.SERVICE_NAME], check=True)
            logger.info("Service stopped successfully")
            return 0
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to stop service: {e}")
            return 1
            
    def cmd_restart(self):
        """Restart the service"""
        logger.info("Restarting btrdupd service...")
        try:
            subprocess.run(['systemctl', 'restart', self.SERVICE_NAME], check=True)
            logger.info("Service restarted successfully")
            return 0
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to restart service: {e}")
            return 1
            
    def cmd_remove(self):
        """Remove the service"""
        logger.info("Removing btrdupd service...")
        
        # Stop service if running
        try:
            subprocess.run(['systemctl', 'stop', self.SERVICE_NAME], 
                         check=False, capture_output=True)
        except:
            pass
            
        # Disable service
        try:
            subprocess.run(['systemctl', 'disable', self.SERVICE_NAME], 
                         check=False, capture_output=True)
            logger.info("Disabled service")
        except:
            pass
            
        # Remove service file
        service_file = None
        for path in self.SYSTEMD_PATHS:
            test_path = os.path.join(path, self.SERVICE_NAME)
            if os.path.exists(test_path):
                service_file = test_path
                break
                
        if service_file:
            try:
                os.remove(service_file)
                logger.info(f"Removed service file: {service_file}")
            except Exception as e:
                logger.error(f"Failed to remove service file: {e}")
                return 1
        else:
            logger.warning("Service file not found")
            
        # Reload systemd
        try:
            subprocess.run(['systemctl', 'daemon-reload'], check=True)
            logger.info("Reloaded systemd configuration")
        except:
            pass
            
        logger.info("btrdupd service removed successfully")
        return 0
        
    def cmd_status(self):
        """Show service status"""
        # First check if service exists
        result = subprocess.run(['systemctl', 'list-unit-files', self.SERVICE_NAME],
                              capture_output=True, text=True)
        if self.SERVICE_NAME not in result.stdout:
            logger.error("btrdupd service is not installed")
            logger.info("Install it with: btrdupd <volumes> --service install")
            return 1
            
        # Get service details to find monitored volumes
        result = subprocess.run(['systemctl', 'cat', self.SERVICE_NAME],
                              capture_output=True, text=True)
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if line.startswith('# Monitored volumes:'):
                    logger.info(line)
                    break
                    
        # Show service status
        logger.info("\n=== Service Status ===")
        subprocess.run(['systemctl', 'status', self.SERVICE_NAME, '--no-pager'])
        
        # Try to show stats for each volume
        logger.info("\n=== Volume Statistics ===")
        result = subprocess.run(['systemctl', 'show', self.SERVICE_NAME, '-p', 'ExecStart'],
                              capture_output=True, text=True)
        if result.returncode == 0 and 'ExecStart' in result.stdout:
            # Parse volumes from ExecStart
            exec_line = result.stdout.strip()
            parts = exec_line.split()
            volumes = []
            
            # Extract volume paths (they should come before --daemon)
            collecting = False
            for part in parts:
                if part.endswith('btrdupd') or part.endswith('btrdupd.py'):
                    collecting = True
                    continue
                if collecting:
                    if part.startswith('--') or part.startswith('-'):
                        break
                    if part.startswith('/'):
                        volumes.append(part)
                        
            # Show stats for each volume
            for volume in volumes:
                stats_file = Path(volume) / '.btrdupd' / 'btrdupd.stats.txt'
                if stats_file.exists():
                    logger.info(f"\nVolume: {volume}")
                    try:
                        with open(stats_file, 'r') as f:
                            logger.info(f.read())
                    except Exception as e:
                        logger.error(f"Error reading stats: {e}")
                else:
                    logger.info(f"\nVolume: {volume} - No stats file found")
                    
        return 0
        
    def cmd_logs(self):
        """Show service logs"""
        logger.info("Showing btrdupd service logs (Ctrl+C to exit)...")
        try:
            # Use journalctl to follow logs
            subprocess.run(['journalctl', '-u', self.SERVICE_NAME, '-f'])
        except KeyboardInterrupt:
            logger.info("\nStopped following logs")
        except Exception as e:
            logger.error(f"Error showing logs: {e}")
            return 1
        return 0