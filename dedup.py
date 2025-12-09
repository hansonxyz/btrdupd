"""
Deduplication logic for btrdupd
Integrates with duperemove for safe deduplication

Safety Note: We use fast hashing and mtime/size checks for candidate identification,
but rely on duperemove's byte-by-byte verification for actual deduplication.
This "99% accurate" approach avoids unnecessary work while maintaining data safety.
"""

import subprocess
import logging
import os
import time
import json
from pathlib import Path
import metrics

logger = logging.getLogger('btrdupd.dedup')

class Deduplicator:
    """Handles deduplication of files using duperemove.

    Manages the deduplication process including:
    - Reference file selection for large duplicate groups
    - Extent comparison to avoid redundant operations
    - Snapshot deduplication via reflink copying
    - Integration with duperemove for actual deduplication

    Safety Note: While we use fast hashing for candidate identification,
    duperemove performs byte-by-byte verification before any deduplication,
    ensuring data integrity.
    """

    # ==========================================================================
    # INITIALIZATION AND CONFIGURATION
    # ==========================================================================

    def __init__(self, db, btrfs, config):
        """Initialize deduplicator.

        Args:
            db: BtrdupdDatabase instance
            btrfs: BtrfsFilesystem instance
            config: Configuration dict
        """
        self.db = db
        self.btrfs = btrfs
        self.config = config
        self.dry_run = False
        self.debug = False

    def set_dry_run(self, dry_run):
        """Enable or disable dry run mode."""
        self.dry_run = dry_run

    # ==========================================================================
    # MAIN DEDUPLICATION ENTRY POINTS
    # ==========================================================================

    def process_dedup_task(self, file_id):
        """Process a deduplication task"""
        # Get file info
        file_path = self.db.get_file_path(file_id)
        if not file_path:
            logger.error(f"File {file_id} not found in database")
            return False
            
        if not os.path.exists(file_path):
            logger.info(f"File no longer exists: {file_path}")
            self.db.remove_file(file_id)
            return False
            
        # Get file hash
        file_info = self.db.execute(
            "SELECT hash, size FROM files WHERE id = ?",
            (file_id,)
        ).fetchone()
        
        if not file_info or not file_info['hash']:
            logger.error(f"No hash found for file {file_id}")
            return False
            
        # Get all non-deferred files with this hash
        all_duplicates = self.db.get_files_by_hash_not_deferred(file_info['hash'])
        
        if len(all_duplicates) < 2:
            logger.info(f"Not enough files to deduplicate for hash {file_info['hash'][:16]}...")
            return False
            
        # IMPORTANT: Check if already deduplicated by comparing extents
        if self.dry_run:
            logger.info(f"DRY RUN: Checking deduplication status for {file_path}")
            logger.info(f"  Hash: {file_info['hash'][:16]}...")
            logger.info(f"  Size: {file_info['size']} bytes")
            logger.info(f"  Found {len(all_duplicates)} total files with this hash")
        
        existing_dedup = self.find_existing_dedup_group(all_duplicates)
        if existing_dedup and file_path in existing_dedup:
            logger.info(f"File already deduplicated: {file_path}")
            if self.dry_run:
                logger.info(f"  Part of dedup group with {len(existing_dedup)} files")
            # Record in database that it's deduplicated
            self.record_dedup_result(file_info['hash'], 
                                   [{'id': file_id, 'path': file_path}], True)
            return True
            
        # Try to find or select a reference file
        reference_file = self.select_reference_file(all_duplicates, file_info['hash'])
        
        if reference_file and reference_file != file_path:
            if self.dry_run:
                logger.info(f"DRY RUN: Selected reference file: {reference_file}")
            
            # Check if already deduplicated with reference
            if self.dry_run:
                logger.info(f"DRY RUN: Would check if {file_path} shares extents with reference")
                # Simulate extent check
                already_deduped = hash(file_path) % 5 == 0  # Arbitrary for testing
                if already_deduped:
                    logger.info(f"DRY RUN: Simulating that file is already deduplicated")
                    return True
            elif self.btrfs.check_files_same_extent(reference_file, file_path):
                logger.info(f"Already deduplicated with reference: {file_path}")
                
                # Even though main volume files share extents, check if snapshots need updating
                # Get the shared extent info from reference file
                ref_extents = self.btrfs.get_file_extent_info(reference_file)
                if ref_extents:
                    ref_extent_sig = tuple((e['physical_start'], e['length']) for e in ref_extents)
                    # Check snapshots with a dummy "extent change" to the already-shared extent
                    extent_changes = {file_path: (None, ref_extent_sig)}
                    snapshot_updates = self.deduplicate_snapshots_via_reflink(
                        [{'id': file_id, 'path': file_path}], 
                        file_info['hash'], 
                        extent_changes
                    )
                    if snapshot_updates > 0:
                        logger.info(f"Updated {snapshot_updates} snapshot files to match already-deduped extent")
                
                self.record_dedup_result(file_info['hash'], 
                                       [{'id': file_id, 'path': file_path}], True)
                return True
                
            # Deduplicate against reference
            logger.info(f"Deduplicating against reference for hash {file_info['hash'][:16]}...")
            if self.dry_run:
                logger.info(f"  Reference: {reference_file}")
                logger.info(f"  Target: {file_path}")
            file_paths = [reference_file, file_path]
            success, extent_changes = self.deduplicate_files(file_paths)
            if success and extent_changes:
                # Try to deduplicate snapshots if actual dedup occurred
                self.deduplicate_snapshots_via_reflink([{'id': file_id, 'path': file_path}], 
                                                      file_info['hash'], extent_changes)
            if success:
                self.record_dedup_result(file_info['hash'], 
                                       [{'id': file_id, 'path': file_path}], success)
            return success
            
        # Smart deduplication strategy for large groups
        large_threshold = int(self.config.get('large_group_threshold', '10'))
        if len(all_duplicates) > large_threshold:
            return self.process_large_dedup_group(file_id, file_info['hash'], all_duplicates)
        else:
            # Small group - use original batch strategy (main volume only)
            batch = all_duplicates[:int(self.config['max_dedup_batch'])]
            file_paths = [f['path'] for f in batch]
            
            # Deduplicate main volume files only
            success, extent_changes = self.deduplicate_files(file_paths)
            
            # If successful and actual dedup occurred, try to deduplicate snapshots via reflink
            if success and extent_changes:
                snapshot_updates = self.deduplicate_snapshots_via_reflink(batch, file_info['hash'], extent_changes)
                if snapshot_updates > 0:
                    logger.info(f"Additionally updated {snapshot_updates} files in snapshots")
            
            # Record result
            self.record_dedup_result(file_info['hash'], batch, success)
            
            return success
        
    # ==========================================================================
    # DEDUP GROUP AND REFERENCE FILE MANAGEMENT
    # ==========================================================================

    def find_existing_dedup_group(self, all_duplicates):
        """Find files that are already deduplicated by checking extents.

        Compares physical extents of files to detect if they're already
        sharing storage. This avoids redundant duperemove calls.

        Args:
            all_duplicates: List of file info dicts with 'path' keys

        Returns:
            List of paths that share extents, or None if no group found
        """
        if self.dry_run:
            logger.info("DRY RUN: Checking for existing dedup groups among files")
        
        dedup_groups = []
        checked_files = set()
        
        for file1 in all_duplicates:
            if file1['path'] in checked_files or not os.path.exists(file1['path']):
                continue
                
            group = [file1['path']]
            checked_files.add(file1['path'])
            
            for file2 in all_duplicates:
                if file2['path'] in checked_files or not os.path.exists(file2['path']):
                    continue
                    
                if self.dry_run:
                    # In dry run, we can't actually check extents
                    logger.debug(f"DRY RUN: Would check extents between {file1['path']} and {file2['path']}")
                    # Simulate finding some dedup groups for testing
                    if hash(file1['path']) % 3 == 0:  # Arbitrary condition for testing
                        group.append(file2['path'])
                        checked_files.add(file2['path'])
                elif self.btrfs.check_files_same_extent(file1['path'], file2['path']):
                    group.append(file2['path'])
                    checked_files.add(file2['path'])
            
            if len(group) > 1:
                dedup_groups.append(group)
                logger.info(f"Found existing dedup group with {len(group)} files sharing extents")
        
        # Return the largest group
        if dedup_groups:
            return max(dedup_groups, key=len)
        return None
        
    def select_reference_file(self, all_duplicates, file_hash):
        """Select the best reference file for deduplication
        
        Strategy:
        1. Use existing cached reference if valid
        2. Select from already deduplicated files
        3. Use oldest file (first discovered)
        4. Handle all files being deleted
        """
        # Try cached reference from new reference table
        cached_ref = self.db.get_reference_file(file_hash)
        if cached_ref:
            if self.dry_run:
                logger.info(f"DRY RUN: Found cached reference file: {cached_ref['path']}")
            else:
                logger.debug(f"Using cached reference: {cached_ref['path']}")
            return cached_ref['path']
        elif self.dry_run:
            logger.info(f"DRY RUN: No cached reference found for hash {file_hash[:16]}...")
            
        # Sort by discovery time (oldest first)
        sorted_files = sorted(all_duplicates, key=lambda x: x.get('id', 0))
        
        # First, try to find an already-deduplicated file as reference
        for file_info in sorted_files:
            if not os.path.exists(file_info['path']):
                continue
                
            # Check if this file is part of a dedup group
            for other in sorted_files:
                if other['path'] != file_info['path'] and os.path.exists(other['path']):
                    if self.btrfs.check_files_same_extent(file_info['path'], other['path']):
                        logger.info(f"Selected existing deduped file as reference: {file_info['path']}")
                        # Cache this reference in new table
                        self.db.set_reference_file(file_hash, file_info['id'])
                        return file_info['path']
        
        # No deduped files found, use oldest existing file
        for file_info in sorted_files:
            if os.path.exists(file_info['path']):
                logger.info(f"Selected oldest file as reference: {file_info['path']}")
                # Cache this reference in new table
                self.db.set_reference_file(file_hash, file_info['id'])
                return file_info['path']
        
        logger.error("No valid files found for deduplication")
        return None
        
    # ==========================================================================
    # SNAPSHOT COLLECTION AND MANAGEMENT
    # ==========================================================================

    def collect_snapshot_paths(self, main_file_paths, max_snapshots=None):
        """Collect snapshot copies of main volume files for direct duperemove inclusion.

        This is the simplified snapshot strategy: instead of complex extent
        verification and reflink copying, we simply find all snapshot copies
        and pass them directly to duperemove. Duperemove will verify content
        equality and deduplicate what it can in a single atomic operation.

        Args:
            main_file_paths: List of main volume file paths
            max_snapshots: Optional limit on number of snapshots to include

        Returns:
            List of snapshot file paths that exist and have matching sizes
        """
        if self.config.get('dedup_snapshots', 'true').lower() != 'true':
            logger.debug("Snapshot deduplication disabled in config")
            return []

        if not main_file_paths:
            return []

        # Get max snapshots limit from config
        if max_snapshots is None:
            max_snapshots = int(self.config.get('max_snapshots_per_dedup', '100'))

        # Get all subvolumes and filter to snapshots
        all_subvolumes = self.btrfs.list_subvolumes()
        root_snapshots = self._get_root_volume_snapshots(all_subvolumes)

        # If no root snapshots found, try alternative approach
        if not root_snapshots and main_file_paths:
            root_snapshots = self._find_snapshots_containing_files(
                all_subvolumes,
                [{'path': p} for p in main_file_paths[:1]]  # Use first file as test
            )

        if not root_snapshots:
            logger.debug("No relevant snapshots found")
            return []

        # Sort snapshots by creation time (newest first) and limit
        # This prioritizes recent snapshots which are more likely to benefit from dedup
        try:
            root_snapshots = sorted(
                root_snapshots,
                key=lambda s: s.get('otime', 0),
                reverse=True
            )[:max_snapshots]
        except Exception:
            root_snapshots = root_snapshots[:max_snapshots]

        logger.debug(f"Checking {len(root_snapshots)} snapshots for file copies")

        snapshot_paths = []

        for main_path in main_file_paths:
            main_file = Path(main_path)

            if not main_file.exists():
                continue

            try:
                main_size = main_file.stat().st_size
                rel_path = main_file.relative_to(self.btrfs.mount_point)
            except (ValueError, OSError):
                continue

            # Check each snapshot for this file
            for snapshot in root_snapshots:
                snapshot_file = Path(snapshot['path']) / rel_path

                # Check if snapshot file exists with same size
                try:
                    if snapshot_file.exists() and snapshot_file.stat().st_size == main_size:
                        snapshot_paths.append(str(snapshot_file))
                except OSError:
                    continue

        if snapshot_paths:
            logger.info(f"Found {len(snapshot_paths)} snapshot copies to include in dedup")

        return snapshot_paths

    def deduplicate_snapshots_via_reflink(self, main_volume_files, file_hash, extent_changes, enqueue_only=True):
        """DEPRECATED: Legacy reflink-based snapshot deduplication

        This method is kept for backwards compatibility but the new simplified approach
        uses collect_snapshot_paths() + direct duperemove inclusion instead.

        The new approach per GAP_ANALYSIS is simpler and more reliable:
        - Pass all paths (main + snapshots) directly to duperemove
        - Let duperemove verify content and deduplicate atomically
        - No need for extent verification or reflink copying
        """
        if self.config.get('dedup_snapshots', 'true').lower() != 'true':
            logger.debug("Snapshot deduplication disabled in config")
            return 0

        # New simplified approach: collect paths and let caller include in duperemove
        # For backwards compatibility, return 0 (no files processed via old method)
        logger.debug("Using simplified snapshot strategy - snapshots included in main duperemove call")
        return 0
    
    def _get_root_volume_snapshots(self, all_subvolumes):
        """Filter subvolumes to only include snapshots of the root volume"""
        root_snapshots = []
        
        # Debug: Show what we're working with
        logger.debug(f"Analyzing {len(all_subvolumes)} subvolumes to find root snapshots")
        
        # Find the root volume - this could be:
        # 1. The subvolume mounted at the mount point
        # 2. Subvolume with ID 5 (traditional root)
        # 3. A subvolume with no parent_uuid
        root_volume = None
        mount_point_str = str(self.btrfs.mount_point)
        
        # First, try to find the subvolume that matches our mount point
        for subvol in all_subvolumes:
            if subvol.get('path') == mount_point_str:
                root_volume = subvol
                logger.debug(f"Identified root volume by mount point: ID={subvol['id']}, UUID={subvol['uuid']}, Path={subvol.get('path', 'N/A')}")
                break
        
        # If not found, fall back to ID 5 or no parent_uuid
        if not root_volume:
            for subvol in all_subvolumes:
                if subvol['id'] == 5 or not subvol.get('parent_uuid'):
                    root_volume = subvol
                    logger.debug(f"Identified root volume by ID/parent: ID={subvol['id']}, UUID={subvol['uuid']}, Path={subvol.get('path', 'N/A')}")
                    break
        
        if not root_volume:
            logger.warning("Could not identify root volume")
            # Debug: Show some subvolumes to understand the structure
            logger.debug("First 5 subvolumes:")
            for i, subvol in enumerate(all_subvolumes[:5]):
                logger.debug(f"  Subvolume {i}: ID={subvol.get('id')}, UUID={subvol.get('uuid', 'N/A')[:8]}..., "
                           f"Parent UUID={subvol.get('parent_uuid', 'N/A')[:8] if subvol.get('parent_uuid') else 'None'}..., "
                           f"Path={subvol.get('path', 'N/A')}, RO={subvol.get('read_only', False)}")
            return []
        
        # Find snapshots of the root volume
        root_uuid = root_volume['uuid']
        logger.debug(f"Looking for snapshots with parent_uuid={root_uuid}")
        
        # Debug: Count different types of subvolumes
        read_only_count = 0
        matching_parent_count = 0
        
        for subvol in all_subvolumes:
            if subvol['read_only']:
                read_only_count += 1
                if subvol.get('parent_uuid') == root_uuid:
                    matching_parent_count += 1
                    
            if (subvol['read_only'] and 
                subvol.get('parent_uuid') == root_uuid and
                subvol['id'] != root_volume['id']):
                root_snapshots.append(subvol)
                logger.debug(f"Found root snapshot: {subvol.get('path', 'N/A')}")
        
        logger.debug(f"Summary: {read_only_count} read-only subvolumes, "
                    f"{matching_parent_count} with matching parent UUID, "
                    f"{len(root_snapshots)} qualified as root snapshots")
        
        # If no snapshots found, let's see what parent_uuids we have
        if len(root_snapshots) == 0 and read_only_count > 0:
            logger.debug("No root snapshots found, checking parent UUIDs of read-only subvolumes:")
            parent_uuids = {}
            for subvol in all_subvolumes:
                if subvol['read_only'] and subvol.get('parent_uuid'):
                    parent_uuid = subvol['parent_uuid']
                    if parent_uuid not in parent_uuids:
                        parent_uuids[parent_uuid] = []
                    parent_uuids[parent_uuid].append(subvol['path'])
            
            # Show up to 5 different parent UUIDs
            for i, (uuid, paths) in enumerate(list(parent_uuids.items())[:5]):
                logger.debug(f"  Parent UUID {uuid[:8]}... has {len(paths)} snapshots")
                for path in paths[:2]:  # Show first 2 paths for each parent
                    logger.debug(f"    - {path}")
        
        logger.debug(f"Found {len(root_snapshots)} root volume snapshots")
        return root_snapshots
    
    def _find_snapshots_containing_files(self, all_subvolumes, main_volume_files):
        """Find snapshots that contain the files we're deduplicating

        This is an alternative approach when we can't identify the root volume snapshots
        by parent UUID. We check if the snapshot contains the same relative paths.

        Includes both read-only and read-write snapshots.
        """
        relevant_snapshots = []

        # Get a sample file to test with
        if not main_volume_files:
            return []

        test_file = main_volume_files[0]
        test_path = Path(test_file['path'])

        # Try to get relative path from mount point
        try:
            rel_path = test_path.relative_to(self.btrfs.mount_point)
        except ValueError:
            logger.debug(f"Test file {test_path} is not under mount point {self.btrfs.mount_point}")
            return []

        logger.debug(f"Looking for snapshots containing relative path: {rel_path}")

        # Check each subvolume (both read-only and read-write)
        checked_count = 0
        for subvol in all_subvolumes:
            checked_count += 1
            snapshot_path = Path(subvol['path'])
            test_snapshot_file = snapshot_path / rel_path

            # Check if this snapshot contains the file
            if test_snapshot_file.exists():
                relevant_snapshots.append(subvol)
                ro_status = "read-only" if subvol['read_only'] else "read-write"
                logger.debug(f"Found snapshot with matching file: {subvol['path']} ({ro_status})")

        logger.debug(f"Checked {checked_count} subvolumes, found {len(relevant_snapshots)} containing our files")
        return relevant_snapshots
    
    def _verify_snapshot_has_pre_dedup_extent(self, snapshot_file, pre_extent):
        """Verify that snapshot file has the same extent as the pre-dedup main file"""
        try:
            # For dry run, we can't actually check extents
            if self.dry_run:
                logger.debug(f"DRY RUN: Would verify snapshot extent for {snapshot_file}")
                return True  # Assume it matches for testing
            
            # Get current extent of snapshot file
            snapshot_extents = self.btrfs.get_file_extent_info(str(snapshot_file))
            if not snapshot_extents:
                logger.debug(f"Could not get extent info for snapshot file: {snapshot_file}")
                return False
            
            # Create extent signature for comparison
            snapshot_extent_sig = tuple((e['physical_start'], e['length']) for e in snapshot_extents)
            
            # Compare with pre-dedup extent
            if snapshot_extent_sig == pre_extent:
                logger.debug(f"Snapshot file has matching pre-dedup extent: {snapshot_file}")
                return True
            else:
                logger.debug(f"Snapshot file has different extent, skipping: {snapshot_file}")
                return False
            
        except Exception as e:
            logger.debug(f"Error verifying snapshot extent: {e}")
            return False
    
    def _update_snapshot_via_reflink(self, main_file, snapshot_file, snapshot_info):
        """Update snapshot file using reflink copying while preserving destination attributes"""
        if self.dry_run:
            logger.info(f"DRY RUN: Would update snapshot file via reflink:")
            logger.info(f"  Source: {main_file}")
            logger.info(f"  Target: {snapshot_file}")
            logger.info(f"  Snapshot: {snapshot_info['path']}")
            if snapshot_info['read_only']:
                logger.info(f"  DRY RUN: Would temporarily make snapshot writable")
                logger.info(f"  DRY RUN: Would perform reflink copy")
                logger.info(f"  DRY RUN: Would restore snapshot to read-only")
            else:
                logger.info(f"  DRY RUN: Would perform reflink copy (snapshot already writable)")
            logger.info(f"  DRY RUN: Would preserve file attributes (owner, group, permissions)")
            return True
        
        # Track snapshot state for cleanup in exception handler
        was_readonly = snapshot_info['read_only']
        snapshot_path = Path(snapshot_info['path'])
        made_writable = False

        try:
            # Get original destination file attributes
            dest_stat = snapshot_file.stat()
            dest_uid = dest_stat.st_uid
            dest_gid = dest_stat.st_gid
            dest_mode = dest_stat.st_mode

            # Get source file attributes for comparison
            src_stat = main_file.stat()
            src_uid = src_stat.st_uid
            src_gid = src_stat.st_gid
            src_mode = src_stat.st_mode

            # Check if attributes match
            attrs_match = (dest_uid == src_uid and
                          dest_gid == src_gid and
                          dest_mode == src_mode)
            
            if was_readonly:
                if not self.btrfs.set_subvolume_readonly(snapshot_path, False):
                    logger.error(f"Failed to make snapshot writable: {snapshot_path}")
                    return False
                made_writable = True
                logger.debug(f"Made snapshot writable: {snapshot_path}")
            
            # Perform reflink copy
            if attrs_match:
                # Attributes match, can use preserve flags
                logger.debug("Source and destination have matching attributes, using preserve flags")
                result = subprocess.run([
                    'cp', '--reflink=auto', '--preserve=timestamps,ownership,mode',
                    str(main_file), str(snapshot_file)
                ], capture_output=True, text=True)
            else:
                # Attributes don't match, copy without preserve and restore after
                logger.debug("Source and destination have different attributes, will restore after copy")
                result = subprocess.run([
                    'cp', '--reflink=auto', '--preserve=timestamps',
                    str(main_file), str(snapshot_file)
                ], capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"Reflink copy failed: {result.stderr}")
                success = False
            else:
                logger.debug(f"Successfully updated snapshot file: {snapshot_file}")
                success = True
                
                # Restore original destination attributes if they were different
                if success and not attrs_match:
                    try:
                        # Restore ownership
                        os.chown(str(snapshot_file), dest_uid, dest_gid)
                        # Restore permissions
                        os.chmod(str(snapshot_file), dest_mode)
                        logger.debug(f"Restored original ownership ({dest_uid}:{dest_gid}) and mode ({oct(dest_mode)}) to {snapshot_file}")
                    except Exception as e:
                        logger.warning(f"Failed to restore original attributes to {snapshot_file}: {e}")
                        # Don't fail the operation if we can't restore attributes
            
            # Restore snapshot to read-only if it was originally read-only
            if was_readonly:
                if self.btrfs.set_subvolume_readonly(snapshot_path, True):
                    logger.debug(f"Restored snapshot to read-only: {snapshot_path}")
                else:
                    logger.warning(f"Failed to restore snapshot to read-only: {snapshot_path}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error updating snapshot file: {e}")
            
            # Try to restore read-only status on error (only if we made it writable)
            if made_writable:
                try:
                    self.btrfs.set_subvolume_readonly(snapshot_path, True)
                except Exception:
                    pass
            
            return False
    
    def enqueue_snapshot_copies(self, main_volume_files, extent_changes, file_hash=None):
        """Queue snapshot updates for batch processing
        
        Args:
            main_volume_files: List of file info dicts that were deduplicated
            extent_changes: Dict mapping file paths to (pre_extent, post_extent) tuples
            file_hash: Optional file hash for verification
            
        Returns:
            Number of files enqueued
        """
        enqueued_count = 0
        
        for file_info in main_volume_files:
            file_path = file_info.get('path', file_info) if isinstance(file_info, dict) else str(file_info)
            
            # Skip if no extent change recorded for this file
            if file_path not in extent_changes:
                continue
                
            pre_extent, post_extent = extent_changes[file_path]
            
            # Get file size
            try:
                file_size = os.path.getsize(file_path)
            except OSError:
                logger.debug(f"Could not get size for {file_path}, skipping enqueue")
                continue
            
            # Serialize extent data as JSON
            before_json = json.dumps([{"physical_start": ps, "length": l} for ps, l in pre_extent])
            after_json = json.dumps([{"physical_start": ps, "length": l} for ps, l in post_extent])
            
            # Enqueue to database
            try:
                self.db.enqueue_snapshot_copy(
                    file_path=file_path,
                    before_extents=before_json,
                    after_extents=after_json,
                    file_size=file_size,
                    file_hash=file_hash
                )
                enqueued_count += 1
                logger.debug(f"Enqueued snapshot copy for {file_path}")
            except Exception as e:
                logger.error(f"Failed to enqueue snapshot copy for {file_path}: {e}")
        
        if enqueued_count > 0:
            logger.info(f"Enqueued {enqueued_count} files for batch snapshot processing")
            
        return enqueued_count
    
    # ==========================================================================
    # BATCH SNAPSHOT PROCESSING
    # ==========================================================================

    def process_snapshot_batch(self):
        """Process all queued snapshot updates in batches.

        Processes files that were queued for snapshot deduplication via reflink.
        For read-write snapshots, uses direct reflink. For read-only snapshots,
        temporarily makes them writable, updates the file, then restores read-only.

        Returns:
            Tuple of (files_processed, bytes_saved)
        """
        logger.info("Processing queued snapshot updates...")
        
        # Load all queued records into memory
        queued_copies = self.db.get_all_snapshot_copies()
        if not queued_copies:
            logger.debug("No queued snapshot copies to process")
            return 0, 0
        
        logger.info(f"Processing batch of {len(queued_copies)} queued snapshot copies")
        
        # Get all snapshots
        all_subvolumes = self.btrfs.list_subvolumes()
        root_snapshots = self._get_root_volume_snapshots(all_subvolumes)
        
        # If no root snapshots found, try alternative approach
        if not root_snapshots and queued_copies:
            logger.debug("No root volume snapshots found, checking all read-only snapshots")
            # Use first queued file to find relevant snapshots
            test_file_path = queued_copies[0]['file_path']
            root_snapshots = self._find_snapshots_containing_files(
                all_subvolumes, 
                [{'path': test_file_path}]
            )
        
        if not root_snapshots:
            logger.info("No relevant snapshots found for batch processing")
            # Still delete the queued records since we tried
            record_ids = [record['id'] for record in queued_copies]
            self.db.delete_snapshot_copies(record_ids)
            return 0, 0
        
        # Track results
        total_files_processed = 0
        total_bytes_saved = 0
        processed_record_ids = []
        
        # Process each snapshot
        for snapshot in root_snapshots:
            snapshot_path = Path(snapshot['path'])
            was_readonly = snapshot['read_only']
            snapshot_files_updated = 0
            
            logger.info(f"Processing snapshot: {snapshot_path}")
            
            # Make snapshot writable if needed
            if was_readonly:
                if not self.btrfs.set_subvolume_readonly(snapshot_path, False):
                    logger.error(f"Failed to make snapshot writable: {snapshot_path}")
                    continue
                logger.debug(f"Made snapshot writable: {snapshot_path}")
            
            try:
                # Process all queued files for this snapshot
                for record in queued_copies:
                    file_path = record['file_path']
                    main_file = Path(file_path)
                    
                    # Skip if main file no longer exists
                    if not main_file.exists():
                        logger.debug(f"Main file no longer exists: {main_file}")
                        continue
                    
                    # Deserialize extent data
                    try:
                        before_extent_data = json.loads(record['before_dedup_extents'])
                        after_extent_data = json.loads(record['after_dedup_extents'])
                        
                        # Convert back to tuple format
                        before_extent = tuple((e['physical_start'], e['length']) for e in before_extent_data)
                        after_extent = tuple((e['physical_start'], e['length']) for e in after_extent_data)
                    except Exception as e:
                        logger.error(f"Failed to deserialize extent data for {file_path}: {e}")
                        continue
                    
                    # Verify main file still has the after_dedup extents
                    current_extents = self.btrfs.get_file_extent_info(str(main_file))
                    if not current_extents:
                        logger.debug(f"Could not get current extents for {main_file}")
                        continue
                        
                    current_extent_sig = tuple((e['physical_start'], e['length']) for e in current_extents)
                    if current_extent_sig != after_extent:
                        logger.debug(f"Main file extents have changed since enqueue: {main_file}")
                        continue
                    
                    # Get relative path from volume root
                    try:
                        rel_path = main_file.relative_to(self.btrfs.mount_point)
                    except ValueError:
                        continue
                    
                    # Check if snapshot has this file
                    snapshot_file = snapshot_path / rel_path
                    if not snapshot_file.exists():
                        continue
                    
                    # Verify snapshot file has the before_dedup extent
                    if self._verify_snapshot_has_pre_dedup_extent(snapshot_file, before_extent):
                        # Update via reflink
                        if self._update_snapshot_via_reflink(main_file, snapshot_file, snapshot):
                            snapshot_files_updated += 1
                            total_bytes_saved += record['file_size']
                            logger.debug(f"Updated snapshot file: {snapshot_file}")
                
                # Track that we processed all records for this snapshot
                if snapshot_files_updated > 0:
                    logger.info(f"Updated {snapshot_files_updated} files in snapshot {snapshot_path}")
                    total_files_processed += snapshot_files_updated
                
            finally:
                # Always restore read-only status if it was originally read-only
                if was_readonly:
                    if self.btrfs.set_subvolume_readonly(snapshot_path, True):
                        logger.debug(f"Restored snapshot to read-only: {snapshot_path}")
                    else:
                        logger.warning(f"Failed to restore snapshot to read-only: {snapshot_path}")
        
        # Delete all processed records
        record_ids = [record['id'] for record in queued_copies]
        if record_ids:
            self.db.delete_snapshot_copies(record_ids)
            logger.debug(f"Deleted {len(record_ids)} processed snapshot copy records")
        
        if total_files_processed > 0:
            logger.info(f"Batch snapshot processing complete: {total_files_processed} files updated, "
                       f"{self._format_bytes(total_bytes_saved)} saved")
        else:
            logger.info("Batch snapshot processing complete: no files needed updating")
            
        return total_files_processed, total_bytes_saved
    
    # ==========================================================================
    # UTILITY METHODS
    # ==========================================================================

    def _format_bytes(self, bytes_value):
        """Format bytes in human-readable form."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"

    # ==========================================================================
    # LARGE GROUP DEDUPLICATION
    # ==========================================================================

    def process_large_dedup_group(self, file_id, file_hash, all_duplicates):
        """Smart handling of large duplicate groups (400+ files)"""
        logger.info(f"Processing large duplicate group: {len(all_duplicates)} files with hash {file_hash[:16]}...")
        
        # First, find which files are already deduplicated by checking extents
        deduped_groups = []
        standalone_files = []
        
        for file_info in all_duplicates:
            if not os.path.exists(file_info['path']):
                continue
                
            # Check if this file shares extents with any known group
            matched_group = None
            for group in deduped_groups:
                if self.btrfs.check_files_same_extent(file_info['path'], group[0]['path']):
                    matched_group = group
                    break
            
            if matched_group:
                matched_group.append(file_info)
            else:
                # Check if this starts a new group
                is_standalone = True
                for other in standalone_files[:]:
                    if self.btrfs.check_files_same_extent(file_info['path'], other['path']):
                        # Start a new group
                        new_group = [other, file_info]
                        deduped_groups.append(new_group)
                        standalone_files.remove(other)
                        is_standalone = False
                        break
                
                if is_standalone:
                    standalone_files.append(file_info)
        
        logger.info(f"Found {len(deduped_groups)} existing dedup groups and {len(standalone_files)} standalone files")
        
        # Use our improved reference selection
        reference_file = self.select_reference_file(all_duplicates, file_hash)
        if not reference_file:
            logger.error("No valid reference file found")
            return False
        
        # Now deduplicate all standalone files against the reference
        # Process in batches to avoid command line limits
        batch_size = int(self.config.get('dedup_batch_size', '50'))
        success_count = 0
        
        # Conservative ARG_MAX limit (half of typical 2MB limit)
        max_arg_length = int(self.config.get('max_arg_length', '1048576'))  # 1MB default
        
        current_batch = [reference_file]
        current_length = len(reference_file) + 100  # Buffer for command overhead
        
        for file_info in standalone_files:
            if not os.path.exists(file_info['path']):
                continue
                
            path_length = len(file_info['path']) + 1  # +1 for space separator
            
            # Check if adding this file would exceed limits
            if (len(current_batch) >= batch_size or 
                current_length + path_length > max_arg_length):
                
                # Process current batch if it has at least 2 files
                if len(current_batch) >= 2:
                    logger.info(f"Deduplicating batch of {len(current_batch)} files against reference")
                    success, changes = self.deduplicate_files(current_batch)
                    if success:
                        success_count += len(current_batch) - 1
                
                # Start new batch with reference file
                current_batch = [reference_file]
                current_length = len(reference_file) + 100
            
            current_batch.append(file_info['path'])
            current_length += path_length
        
        # Process final batch
        if len(current_batch) >= 2:
            logger.info(f"Deduplicating final batch of {len(current_batch)} files against reference")
            success, changes = self.deduplicate_files(current_batch)
            if success:
                success_count += len(current_batch) - 1
        
        # For large groups, snapshot deduplication is queued for batch processing
        if success_count > 0:
            logger.debug("Snapshot deduplication queued for batch processing (large group)")
        
        # Record results
        self.record_dedup_result(file_hash, all_duplicates, success_count > 0)
        
        logger.info(f"Large group deduplication complete: {success_count} files deduplicated")
        return success_count > 0
        
    # ==========================================================================
    # CORE DEDUPLICATION OPERATIONS
    # ==========================================================================

    def deduplicate_pair_safely(self, source_path, target_path):
        """Safely deduplicate a pair of files with extent verification.

        Captures extent info before and after deduplication to verify the
        operation succeeded. Also propagates deduplication to snapshots.

        Args:
            source_path: Path to source file
            target_path: Path to target file

        Returns:
            Tuple of (success, bytes_saved) where bytes_saved is file size if dedup occurred
        """
        try:
            # Check for special BTRFS attributes
            if not self._check_dedup_allowed(source_path) or not self._check_dedup_allowed(target_path):
                logger.debug(f"Skipping deduplication due to special file attributes")
                return False, 0
            
            # Get file size for tracking bytes saved
            file_size = os.path.getsize(source_path)
            
            # Capture pre-deduplication extents
            source_pre_extents = self.btrfs.get_file_extent_info(source_path)
            target_pre_extents = self.btrfs.get_file_extent_info(target_path)
            if not source_pre_extents or not target_pre_extents:
                logger.warning(f"Failed to get extent info for files")
                return False, 0
            
            # Create extent signatures for comparison
            source_extent_sig = tuple((e['physical_start'], e['length']) for e in source_pre_extents)
            target_extent_sig = tuple((e['physical_start'], e['length']) for e in target_pre_extents)
            
            # If files already share extents, no dedup needed but check snapshots
            if source_extent_sig == target_extent_sig:
                logger.debug("Files already share the same extents")
                
                # Check if source file is on main volume and propagate shared extent to snapshots
                if not self._is_snapshot_path(source_path):
                    # Use the shared extent signature for snapshot updates
                    extent_changes = {source_path: (source_extent_sig, source_extent_sig)}
                    snapshot_updates = self.deduplicate_snapshots_via_reflink(
                        [{'path': source_path}], 
                        None,  # hash not needed for single file
                        extent_changes
                    )
                    if snapshot_updates > 0:
                        logger.info(f"Updated {snapshot_updates} snapshot files to match already-shared extent")
                
                return True, 0
            
            # Run deduplication
            success, extent_changes = self.deduplicate_files([source_path, target_path])
            
            if not success:
                return False, 0
            
            # Check if deduplication actually occurred by verifying extent changes
            bytes_saved = 0
            snapshot_count = 0
            if extent_changes:
                # If either file's extents changed, deduplication occurred
                if source_path in extent_changes or target_path in extent_changes:
                    bytes_saved = file_size

                    # Count how many snapshot files were included
                    for path in extent_changes.keys():
                        if self._is_snapshot_path(path):
                            snapshot_count += 1

            # Record cumulative stats and metrics if dedup occurred
            if bytes_saved > 0 and not self.dry_run:
                self.db.record_dedup_stats(
                    files_count=1,
                    bytes_size=bytes_saved,
                    snapshot_files_count=snapshot_count
                )
                # Record observability metrics
                metrics.inc('dedup_operations_total')
                metrics.inc('dedup_files_total', 2)  # source + target
                metrics.inc('potential_bytes_saved_total', bytes_saved)
                if snapshot_count > 0:
                    metrics.inc('snapshot_files_included_total', snapshot_count)

            return True, bytes_saved
            
        except Exception as e:
            logger.error(f"Error in safe deduplication: {e}")
            return False, 0
    
    def deduplicate_files(self, file_paths, include_snapshots=True):
        """Run duperemove on a set of files, optionally including snapshot copies

        IMPORTANT: duperemove behavior regarding which file becomes the "source":
        - By default, duperemove tries to minimize fragmentation
        - It typically preserves the file with the most existing extents
        - We can influence this by putting our reference file first
        - Using --dedupe-options=partial ensures better extent reuse

        Args:
            file_paths: List of main volume file paths to deduplicate
            include_snapshots: If True, collect and include snapshot copies in the
                              duperemove call (simplified strategy per GAP_ANALYSIS)

        Returns: (success, extent_changes) where extent_changes is a dict mapping
                 file paths to their (old_extent, new_extent) tuples
        """
        if len(file_paths) < 2:
            return False, {}

        # Collect snapshot paths if enabled (simplified strategy)
        all_paths = list(file_paths)
        if include_snapshots:
            snapshot_paths = self.collect_snapshot_paths(file_paths)
            if snapshot_paths:
                all_paths.extend(snapshot_paths)
                logger.info(f"Including {len(snapshot_paths)} snapshot copies in dedup call")

        logger.info(f"Deduplicating {len(all_paths)} files ({len(file_paths)} main + {len(all_paths) - len(file_paths)} snapshots)")
        
        if self.dry_run:
            logger.info("DRY RUN: Would deduplicate:")
            for path in all_paths[:10]:  # Show first 10 in dry run
                logger.info(f"  - {path}")
            if len(all_paths) > 10:
                logger.info(f"  ... and {len(all_paths) - 10} more files")
            # Simulate extent changes for main volume files except the first (reference)
            extent_changes = {}
            if len(file_paths) >= 2:
                for i in range(1, len(file_paths)):
                    extent_changes[file_paths[i]] = (
                        ((12345 + i*1000, 4096), (23456 + i*1000, 8192)),
                        ((12345, 4096), (12345, 8192))
                    )
            return True, extent_changes

        # CRITICAL: Order matters! First file is preferred as source
        # Main volume files come first, then snapshots
        if len(all_paths) > 2:
            logger.debug(f"Reference file (will be preserved): {all_paths[0]}")

        # Capture extent information before deduplication (main volume files only)
        pre_extents = {}
        for path in file_paths:  # Only track main volume files
            extents = self.btrfs.get_file_extent_info(path)
            if extents:
                extent_sig = tuple((e['physical_start'], e['length']) for e in extents)
                pre_extents[path] = extent_sig
                if self.debug:
                    logger.debug(f"Pre-dedup extents for {path}: {extent_sig}")

        # Build duperemove command
        cmd = [
            'duperemove',
            '-d',  # Deduplicate
            '-r',  # Recursive (though we're giving specific files)
            '-q',  # Quiet
            '--dedupe-options=partial',  # Better extent reuse, respects file order
            '-b', '4096'  # Use 4K block size (BTRFS default)
        ]

        # Build safe command respecting length limits
        base_cmd_length = sum(len(arg) + 1 for arg in cmd)
        max_length = int(self.config.get('max_arg_length', '1048576'))

        # Always include at least reference + 1 file from main volume
        safe_file_paths = [all_paths[0]]  # Reference file
        current_length = base_cmd_length + len(all_paths[0]) + 1

        for path in all_paths[1:]:
            path_length = len(path) + 1
            if current_length + path_length > max_length:
                logger.warning(f"Truncating batch at {len(safe_file_paths)} files due to command line limit")
                break
            safe_file_paths.append(path)
            current_length += path_length

        # Must have at least 2 files
        if len(safe_file_paths) < 2:
            logger.error("Cannot fit even 2 files within command line limit")
            return False, {}

        cmd.extend(safe_file_paths)

        if len(safe_file_paths) < len(all_paths):
            logger.info(f"Processing {len(safe_file_paths)} of {len(all_paths)} files due to length limit")

        try:
            # Run duperemove (includes main volume + snapshot files)
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )

            if result.returncode == 0:
                logger.info("Deduplication command completed")

                # Capture extent information after deduplication (main volume files only)
                extent_changes = {}
                dedup_occurred = False

                for path in file_paths:  # Only check main volume files
                    if path not in safe_file_paths:
                        continue
                    post_extents = self.btrfs.get_file_extent_info(path)
                    if post_extents:
                        post_sig = tuple((e['physical_start'], e['length']) for e in post_extents)
                        pre_sig = pre_extents.get(path, ())

                        if pre_sig and pre_sig != post_sig:
                            extent_changes[path] = (pre_sig, post_sig)
                            dedup_occurred = True
                            if self.debug:
                                logger.debug(f"Extent changed for {path}: {pre_sig} -> {post_sig}")
                        elif self.debug:
                            logger.debug(f"No extent change for {path}")
                
                if dedup_occurred:
                    logger.info(f"Deduplication successful - {len(extent_changes)} files had extent changes")
                else:
                    logger.info("No actual deduplication occurred (files may have already been deduped)")
                
                return True, extent_changes
            else:
                logger.error(f"Deduplication failed: {result.stderr}")
                return False, {}
                
        except Exception as e:
            logger.error(f"Error during deduplication: {e}")
            return False, {}
            
    # ==========================================================================
    # SNAPSHOT READ-ONLY MANAGEMENT
    # ==========================================================================

    def make_snapshot_writable(self, file_path):
        """Make a snapshot temporarily writable if needed.

        For read-only snapshots, we need to temporarily remove the read-only
        flag to perform reflink updates, then restore it afterward.
        """
        try:
            # Check if file is in a read-only subvolume
            subvol_info = self.btrfs.get_subvolume_info(file_path)
            if subvol_info and subvol_info.get('Flags', '').find('readonly') != -1:
                # Make writable
                subvol_path = Path(file_path).parent
                if self.btrfs.set_subvolume_readonly(subvol_path, False):
                    logger.debug(f"Made snapshot writable: {subvol_path}")
                    return True
        except:
            pass
        return False
        
    def restore_snapshot_readonly(self, file_path):
        """Restore read-only status to snapshot"""
        try:
            subvol_path = Path(file_path).parent
            self.btrfs.set_subvolume_readonly(subvol_path, True)
            logger.debug(f"Restored read-only: {subvol_path}")
        except:
            pass
            
    # ==========================================================================
    # RESULT RECORDING AND FILE ATTRIBUTE CHECKS
    # ==========================================================================

    def record_dedup_result(self, file_hash, batch, success):
        """Record deduplication attempt in database."""
        file_ids = [file_info['id'] for file_info in batch]
        self.db.record_main_dedup_result(file_hash, file_ids, success, self.dry_run)
        logger.info(f"Recorded dedup result: {'success' if success else 'failed'}")
    
    def _check_dedup_allowed(self, file_path):
        """Check if file has special attributes that prevent deduplication"""
        try:
            result = subprocess.run(
                ['lsattr', str(file_path)],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode != 0:
                return True  # If we can't check, allow dedup
            
            # Parse attributes
            output = result.stdout.strip()
            if not output:
                return True
            
            attrs = output.split()[0] if output else ''
            
            # Only allow files with no special attributes or just compressed/cow
            forbidden_attrs = ['C', 'i', 'a', 's', 'u']  # nocow, immutable, append-only, etc
            for attr in forbidden_attrs:
                if attr in attrs:
                    return False
            
            return True
        except:
            return True  # If we can't check, allow dedup
    
    def _is_snapshot_path(self, file_path):
        """Check if file is within a snapshot"""
        try:
            path_parts = Path(file_path).parts
            # Common snapshot directory patterns
            snapshot_indicators = ['.snapshot', 'snapshot', '@', '.snapshots']
            
            for part in path_parts:
                # Check if this path component contains snapshot indicators
                part_lower = part.lower()
                for indicator in snapshot_indicators:
                    if indicator in part_lower:
                        return True
            
            # Also check if the file is in a read-only subvolume (likely a snapshot)
            # This is a more accurate check for BTRFS
            try:
                subvol_info = self.btrfs.get_subvolume_info(file_path)
                if subvol_info and 'read-only' in str(subvol_info.get('flags', '')).lower():
                    return True
            except:
                pass
            
            return False
        except:
            return False
        
    # ==========================================================================
    # STATISTICS
    # ==========================================================================

    def get_dedup_stats(self):
        """Get deduplication statistics from the database."""
        stats = {}
        
        # Total dedup attempts
        stats['total_attempts'] = self.db.execute(
            "SELECT COUNT(*) FROM dedup_history"
        ).fetchone()[0]
        
        # Successful dedups
        stats['successful'] = self.db.execute(
            "SELECT COUNT(*) FROM dedup_history WHERE success = 1"
        ).fetchone()[0]
        
        # Failed dedups
        stats['failed'] = stats['total_attempts'] - stats['successful']
        
        # Space saved
        stats['space_saved'] = self.db.execute("""
            SELECT SUM((dg.file_count - 1) * dg.size)
            FROM dedup_groups dg
            WHERE EXISTS (
                SELECT 1 FROM dedup_history dh 
                WHERE dh.group_id = dg.id AND dh.success = 1
            )
        """).fetchone()[0] or 0
        
        return stats