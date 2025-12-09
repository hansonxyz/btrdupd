"""
Btrfs filesystem operations for btrdupd
Handles all Btrfs-specific functionality
"""

import subprocess
import logging
import os
import re
import fnmatch
from pathlib import Path
import json
from datetime import datetime

logger = logging.getLogger('btrdupd.btrfs')

class BtrfsError(Exception):
    """Btrfs operation error"""
    pass

class BtrfsFilesystem:
    def __init__(self, mount_point, debug_commands=False):
        self.mount_point = Path(mount_point).resolve()
        self.debug_commands = debug_commands
        self._verify_btrfs()
        
    def _run_command(self, cmd, check=True):
        """Run a command and return output"""
        if self.debug_commands:
            logger.debug(f"Running command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=check
            )
            
            if self.debug_commands:
                # Don't log raw stdout for find-new command - it's too verbose
                if result.stdout and 'find-new' not in ' '.join(cmd):
                    logger.debug(f"stdout: {result.stdout}")
                if result.stderr:
                    logger.debug(f"stderr: {result.stderr}")
            
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {' '.join(cmd)}")
            logger.error(f"stderr: {e.stderr}")
            raise BtrfsError(f"Command failed: {e.stderr}")
    
    def _verify_btrfs(self):
        """Verify this is a Btrfs filesystem"""
        try:
            output = self._run_command(['findmnt', '-n', '-o', 'FSTYPE', str(self.mount_point)])
            fstype = output.strip()
            
            if fstype != 'btrfs':
                raise BtrfsError(f"Not a Btrfs filesystem: {self.mount_point} (found: {fstype})")
            
            logger.info(f"Verified Btrfs filesystem at {self.mount_point}")
        except Exception as e:
            raise BtrfsError(f"Failed to verify filesystem: {e}")
    
    def get_current_generation(self):
        """Get current generation number of the filesystem"""
        try:
            # Get filesystem info
            output = self._run_command(['btrfs', 'filesystem', 'show', str(self.mount_point)])
            
            # Find device path
            device_match = re.search(r'devid\s+\d+.*path\s+(\S+)', output)
            if not device_match:
                raise BtrfsError("Could not find device path")
            
            device = device_match.group(1)
            
            # Get generation from superblock
            output = self._run_command(['btrfs', 'inspect-internal', 'dump-super', device])
            
            gen_match = re.search(r'generation\s+(\d+)', output)
            if not gen_match:
                raise BtrfsError("Could not find generation number")
            
            generation = int(gen_match.group(1))
            logger.debug(f"Current generation: {generation}")
            return generation
            
        except Exception as e:
            logger.error(f"Failed to get generation: {e}")
            raise
    
    def find_new_files(self, subvolume_path, last_generation, process_callback=None, batch_size=1000, exclude_patterns=None):
        """Find files modified since a given generation

        Args:
            subvolume_path: Path to the subvolume
            last_generation: Generation number to find changes since
            process_callback: Optional callback function to process batches of files
            batch_size: Number of unique files to collect before processing (default 1000)
            exclude_patterns: List of filename patterns to exclude (e.g., ['*.db', '*.db-journal'])

        Returns:
            If no callback provided: (files, new_transid)
            If callback provided: (total_processed, new_transid)
        """
        logger.info(f"Finding files modified since generation {last_generation}")
        
        try:
            cmd = ['btrfs', 'subvolume', 'find-new', str(subvolume_path), str(last_generation)]
            
            # If we have a callback, we'll process in batches
            if process_callback:
                # Use subprocess.Popen for streaming
                import subprocess
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                
                unique_files = {}  # Use dict to track unique files by path
                total_processed = 0
                new_transid = None
                
                # Process output line by line
                lines_processed = 0
                matches_found = 0
                sample_lines_logged = 0
                for line in process.stdout:
                    lines_processed += 1
                    line = line.rstrip()  # Remove trailing whitespace
                    
                    # Log first few lines for debugging
                    if sample_lines_logged < 5 and line and not line.startswith('transid'):
                        logger.debug(f"BTRFS output sample line {sample_lines_logged + 1}: {line}")
                        sample_lines_logged += 1
                    
                    # Parse output format
                    match = re.match(r'^inode\s+(\d+)\s+file\s+offset\s+\d+\s+len\s+\d+\s+disk\s+start\s+\d+\s+offset\s+\d+\s+gen\s+(\d+)\s+flags\s+\S+\s+(.+)$', line)
                    if match:
                        matches_found += 1
                        inode = int(match.group(1))
                        gen = int(match.group(2))
                        path = match.group(3)
                        
                        # Skip if it's not a regular file
                        full_path = Path(subvolume_path) / path

                        # Skip files matching exclude patterns (e.g., database files)
                        if exclude_patterns:
                            filename = full_path.name
                            if any(fnmatch.fnmatch(filename, pattern) for pattern in exclude_patterns):
                                continue

                        if full_path.exists() and full_path.is_file():
                            # Only add if we haven't seen this file yet
                            # Always use the latest generation number for the file
                            if str(full_path) not in unique_files:
                                unique_files[str(full_path)] = {
                                    'path': str(full_path),
                                    'inode': inode,
                                    'generation': gen
                                }
                            else:
                                # Update to latest generation if this one is newer
                                if gen > unique_files[str(full_path)]['generation']:
                                    unique_files[str(full_path)]['generation'] = gen
                                
                                # Process batch when we reach batch_size
                                if len(unique_files) >= batch_size:
                                    logger.debug(f"Processing batch of {len(unique_files)} unique files")
                                    process_callback(list(unique_files.values()))
                                    total_processed += len(unique_files)
                                    unique_files.clear()
                    
                    # Check for transid marker
                    transid_match = re.search(r'transid marker was (\d+)', line)
                    if transid_match:
                        new_transid = int(transid_match.group(1))
                        logger.debug(f"New transid marker: {new_transid}")
                
                # Wait for process to complete
                process.wait()
                
                # Process any remaining files
                if unique_files:
                    logger.debug(f"Processing final batch of {len(unique_files)} unique files")
                    process_callback(list(unique_files.values()))
                    total_processed += len(unique_files)
                
                logger.debug(f"BTRFS parsing complete: {lines_processed} lines processed, {matches_found} matches found, {total_processed} unique files processed")
                logger.info(f"Processed {total_processed} unique modified files since generation {last_generation}")
                return total_processed, new_transid
            
            else:
                # Original behavior - return all files at once
                output = self._run_command(cmd)
                
                unique_files = {}
                for line in output.splitlines():
                    match = re.match(r'^inode\s+(\d+)\s+file\s+offset\s+\d+\s+len\s+\d+\s+disk\s+start\s+\d+\s+offset\s+\d+\s+gen\s+(\d+)\s+flags\s+\S+\s+(.+)$', line)
                    if match:
                        inode = int(match.group(1))
                        gen = int(match.group(2))
                        path = match.group(3)
                        
                        full_path = Path(subvolume_path) / path

                        # Skip files matching exclude patterns (e.g., database files)
                        if exclude_patterns:
                            filename = full_path.name
                            if any(fnmatch.fnmatch(filename, pattern) for pattern in exclude_patterns):
                                continue

                        if full_path.exists() and full_path.is_file():
                            unique_files[str(full_path)] = {
                                'path': str(full_path),
                                'inode': inode,
                                'generation': gen
                            }

                # Get transid
                transid_match = re.search(r'transid marker was (\d+)', output)
                new_transid = int(transid_match.group(1)) if transid_match else None
                
                files = list(unique_files.values())
                logger.info(f"Found {len(files)} unique modified files since generation {last_generation}")
                return files, new_transid
            
        except Exception as e:
            logger.error(f"Failed to find new files: {e}")
            return (0, None) if process_callback else ([], None)
    
    def list_subvolumes(self):
        """List all subvolumes and their relationships"""
        logger.info("Listing subvolumes")
        
        try:
            output = self._run_command(['btrfs', 'subvolume', 'list', '-puq', str(self.mount_point)])
            
            subvolumes = []
            for line in output.splitlines():
                # Parse: ID 257 gen 12 parent 5 top level 5 parent_uuid - uuid 1234... path home
                match = re.match(
                    r'ID\s+(\d+)\s+gen\s+(\d+)\s+parent\s+(\d+).*?parent_uuid\s+(\S+)\s+uuid\s+(\S+)\s+path\s+(.+)',
                    line
                )
                if match:
                    subvol_id = int(match.group(1))
                    generation = int(match.group(2))
                    parent_id = int(match.group(3))
                    parent_uuid = match.group(4) if match.group(4) != '-' else None
                    uuid = match.group(5)
                    path = match.group(6)
                    
                    # Get absolute path
                    abs_path = self.mount_point / path
                    
                    # Check if read-only
                    try:
                        ro_output = self._run_command(
                            ['btrfs', 'property', 'get', str(abs_path), 'ro'],
                            check=False
                        )
                        read_only = 'ro=true' in ro_output
                    except:
                        read_only = False
                    
                    subvolumes.append({
                        'id': subvol_id,
                        'uuid': uuid,
                        'parent_uuid': parent_uuid,
                        'parent_id': parent_id,
                        'path': str(abs_path),
                        'rel_path': path,
                        'generation': generation,
                        'read_only': read_only
                    })
                    
                    if self.debug_commands:
                        logger.debug(f"Found subvolume: {path} (uuid: {uuid[:8]}..., ro: {read_only})")
            
            logger.info(f"Found {len(subvolumes)} subvolumes")
            return subvolumes
            
        except Exception as e:
            logger.error(f"Failed to list subvolumes: {e}")
            return []
    
    def get_subvolume_info(self, path):
        """Get detailed information about a specific subvolume.

        Note: Returns None silently if path is not a subvolume - this is expected
        behavior when checking if a regular file is in a snapshot.
        """
        try:
            # Use check=False to avoid error logging for non-subvolume paths
            result = subprocess.run(
                ['btrfs', 'subvolume', 'show', str(path)],
                capture_output=True,
                text=True,
                check=False
            )

            if result.returncode != 0:
                # Not a subvolume - this is expected for regular files
                return None

            info = {}
            for line in result.stdout.splitlines():
                if ':' in line:
                    key, value = line.split(':', 1)
                    info[key.strip().lower()] = value.strip()

            return info
        except Exception:
            # Unexpected error
            return None
    
    def set_subvolume_readonly(self, path, readonly=True):
        """Set subvolume read-only property"""
        try:
            value = 'true' if readonly else 'false'
            self._run_command(['btrfs', 'property', 'set', str(path), 'ro', value])
            logger.info(f"Set {path} read-only: {readonly}")
            return True
        except Exception as e:
            logger.error(f"Failed to set read-only property: {e}")
            return False
    
    def get_file_extent_info(self, file_path):
        """Get extent information for a file (useful for dedup verification)"""
        try:
            output = self._run_command(['filefrag', '-v', str(file_path)])
            
            extents = []
            for line in output.splitlines():
                # Parse extent information
                match = re.match(r'\s*(\d+):\s+(\d+)\.\.\s*(\d+):\s+(\d+)\.\.\s*(\d+):\s+(\d+):', line)
                if match:
                    extents.append({
                        'logical_start': int(match.group(2)),
                        'logical_end': int(match.group(3)),
                        'physical_start': int(match.group(4)),
                        'physical_end': int(match.group(5)),
                        'length': int(match.group(6))
                    })
            
            return extents
        except Exception as e:
            logger.error(f"Failed to get extent info: {e}")
            return []
    
    def check_files_same_extent(self, file1, file2):
        """Check if two files share the same extent (already deduped)"""
        try:
            extents1 = self.get_file_extent_info(file1)
            extents2 = self.get_file_extent_info(file2)
            
            if not extents1 or not extents2:
                return False
            
            # Check if physical extents match
            for e1 in extents1:
                for e2 in extents2:
                    if (e1['physical_start'] == e2['physical_start'] and
                        e1['length'] == e2['length']):
                        return True
            
            return False
        except Exception as e:
            logger.error(f"Failed to check extent sharing: {e}")
            return False
    
    def find_files_by_inode(self, subvolume_path, inode):
        """Find all files with a specific inode in a subvolume"""
        try:
            # Use find command to locate files by inode
            output = self._run_command([
                'find', str(subvolume_path),
                '-inum', str(inode),
                '-type', 'f',
                '-print0'
            ])
            
            files = [f for f in output.split('\0') if f]
            return files
        except Exception as e:
            logger.error(f"Failed to find files by inode: {e}")
            return []
    
    def get_filesystem_info(self):
        """Get general filesystem information"""
        try:
            # Get filesystem usage
            df_output = self._run_command(['btrfs', 'filesystem', 'df', str(self.mount_point)])
            
            # Get device stats
            usage_output = self._run_command(['btrfs', 'filesystem', 'usage', str(self.mount_point)])
            
            # Parse usage information
            info = {
                'mount_point': str(self.mount_point),
                'generation': self.get_current_generation()
            }
            
            # Extract size information
            size_match = re.search(r'Device size:\s+(\S+)', usage_output)
            if size_match:
                info['device_size'] = size_match.group(1)
            
            used_match = re.search(r'Device allocated:\s+(\S+)', usage_output)
            if used_match:
                info['device_allocated'] = used_match.group(1)
            
            free_match = re.search(r'Free \(estimated\):\s+(\S+)', usage_output)
            if free_match:
                info['free_estimated'] = free_match.group(1)
            
            return info
        except Exception as e:
            logger.error(f"Failed to get filesystem info: {e}")
            return {}
    
    def verify_btrfs_progs(self):
        """Verify required btrfs-progs tools are available"""
        required_tools = ['btrfs', 'filefrag', 'findmnt']
        missing = []
        
        for tool in required_tools:
            try:
                self._run_command(['which', tool], check=True)
            except:
                missing.append(tool)
        
        if missing:
            raise BtrfsError(f"Missing required tools: {', '.join(missing)}")
        
        logger.debug("All required btrfs tools are available")
        return True

# =============================================================================
# TEST AND DEBUGGING FUNCTIONS
# =============================================================================
# These functions are for development and testing only. They are not used by
# the main btrdupd code and can be run standalone for debugging BTRFS operations.

def test_btrfs_operations(mount_point, debug=True):
    """Test various Btrfs operations"""
    logger.info("Testing Btrfs operations...")
    
    try:
        btrfs = BtrfsFilesystem(mount_point, debug_commands=debug)
        
        # Test 1: Get current generation
        logger.info("\n=== Test 1: Get current generation ===")
        gen = btrfs.get_current_generation()
        logger.info(f"Current generation: {gen}")
        
        # Test 2: List subvolumes
        logger.info("\n=== Test 2: List subvolumes ===")
        subvols = btrfs.list_subvolumes()
        for subvol in subvols:
            logger.info(f"Subvolume: {subvol['rel_path']} (UUID: {subvol['uuid'][:8]}..., RO: {subvol['read_only']})")
        
        # Test 3: Find new files (if we have a previous generation)
        logger.info("\n=== Test 3: Find new files ===")
        if gen > 100:  # Assume we have some history
            files, new_gen = btrfs.find_new_files(mount_point, gen - 100)
            logger.info(f"Found {len(files)} files modified in last 100 generations")
            for f in files[:5]:  # Show first 5
                logger.info(f"  Modified: {f['path']}")
        
        # Test 4: Get filesystem info
        logger.info("\n=== Test 4: Filesystem info ===")
        info = btrfs.get_filesystem_info()
        for key, value in info.items():
            logger.info(f"{key}: {value}")
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False