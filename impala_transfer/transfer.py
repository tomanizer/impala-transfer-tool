"""
File transfer operations module.
Handles transferring files to target clusters via HDFS, SCP, or distcp.
"""

import os
import logging
import subprocess
from typing import List, Optional


class FileTransferManager:
    """Handles file transfer operations to target cluster."""
    
    def __init__(self, target_hdfs_path: str = None, use_distcp: bool = True, 
                 source_hdfs_path: str = None, target_cluster: str = None,
                 scp_target_host: str = None, scp_target_path: str = None):
        """
        Initialize file transfer manager.
        
        Args:
            target_hdfs_path: HDFS path on target cluster (if using HDFS transfer)
            use_distcp: Whether to use distcp for cross-cluster transfers (default: True)
            source_hdfs_path: Source HDFS path (required for distcp)
            target_cluster: Target cluster name/address (required for distcp)
            scp_target_host: Target host for SCP transfer (if using SCP)
            scp_target_path: Target directory path for SCP transfer (if using SCP)
        """
        self.target_hdfs_path = target_hdfs_path
        self.use_distcp = use_distcp
        self.source_hdfs_path = source_hdfs_path
        self.target_cluster = target_cluster
        self.scp_target_host = scp_target_host
        self.scp_target_path = scp_target_path
    
    def transfer_files(self, filepaths: List[str], target_table: str) -> bool:
        """
        Transfer files to target cluster.
        
        Args:
            filepaths: List of file paths to transfer
            target_table: Target table name (for logging purposes)
            
        Returns:
            bool: True if transfer successful, False otherwise
        """
        try:
            # Try distcp first (if configured)
            if self.use_distcp and self.source_hdfs_path and self.target_cluster:
                logging.info("Attempting transfer via distcp...")
                if self._transfer_via_distcp(filepaths):
                    return True
                else:
                    logging.warning("Distcp transfer failed, trying fallback methods...")
            
            # Try hdfs put (local to HDFS)
            if self.target_hdfs_path:
                logging.info("Attempting transfer via hdfs put...")
                if self._transfer_via_hdfs_put(filepaths):
                    return True
                else:
                    logging.warning("HDFS put transfer failed, trying hdfs cp...")
                    
                    # Try hdfs cp (within HDFS)
                    if self._transfer_via_hdfs_cp(filepaths):
                        return True
                    else:
                        logging.warning("HDFS cp transfer failed, trying SCP...")
            
            # Fallback to SCP
            logging.info("Attempting transfer via SCP...")
            return self._transfer_via_scp(filepaths)
            
        except Exception as e:
            logging.error(f"Transfer to cluster 2 failed: {e}")
            return False
    
    def _transfer_via_hdfs_put(self, filepaths: List[str]) -> bool:
        """
        Transfer files to HDFS using hdfs dfs -put (local to HDFS).
        
        Args:
            filepaths: List of file paths to transfer
            
        Returns:
            bool: True if transfer successful
        """
        logging.info("Transferring files to HDFS via hdfs put...")
        
        # Ensure HDFS path exists
        if not self._ensure_hdfs_path_exists():
            return False
        
        # Transfer files
        for filepath in filepaths:
            if not self._copy_file_via_hdfs_put(filepath):
                return False
        
        logging.info("Files transferred to HDFS via hdfs put successfully")
        return True
    
    def _ensure_hdfs_path_exists(self) -> bool:
        """
        Ensure HDFS path exists, create if it doesn't.
        
        Returns:
            bool: True if path exists or was created successfully
        """
        logging.info(f"Checking if HDFS path exists: {self.target_hdfs_path}")
        check_cmd = f"hdfs dfs -test -d {self.target_hdfs_path}"
        result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.info(f"HDFS path does not exist, creating: {self.target_hdfs_path}")
            mkdir_cmd = f"hdfs dfs -mkdir -p {self.target_hdfs_path}"
            mkdir_result = subprocess.run(mkdir_cmd, shell=True, capture_output=True, text=True)
            
            if mkdir_result.returncode != 0:
                logging.error(f"Failed to create HDFS path: {mkdir_result.stderr}")
                return False
            else:
                logging.info(f"HDFS path created successfully: {self.target_hdfs_path}")
        else:
            logging.info(f"HDFS path already exists: {self.target_hdfs_path}")
        
        return True
    
    def _copy_file_via_hdfs_put(self, filepath: str) -> bool:
        """
        Copy a single file to HDFS using hdfs dfs -put.
        
        Args:
            filepath: Path to the file to copy
            
        Returns:
            bool: True if copy successful
        """
        filename = os.path.basename(filepath)
        hdfs_path = f"{self.target_hdfs_path}/{filename}"
        
        cmd = f"hdfs dfs -put {filepath} {hdfs_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.error(f"HDFS put failed: {result.stderr}")
            return False
        
        logging.info(f"Successfully copied {filename} via hdfs put")
        return True
    
    def _transfer_via_hdfs_cp(self, filepaths: List[str]) -> bool:
        """
        Transfer files within HDFS using hdfs dfs -cp.
        
        Args:
            filepaths: List of file paths to transfer
            
        Returns:
            bool: True if transfer successful
        """
        logging.info("Transferring files within HDFS via hdfs cp...")
        
        # Ensure target HDFS path exists
        if not self._ensure_hdfs_path_exists():
            return False
        
        # Transfer files
        for filepath in filepaths:
            if not self._copy_file_via_hdfs_cp(filepath):
                return False
        
        logging.info("Files transferred within HDFS via hdfs cp successfully")
        return True
    
    def _copy_file_via_hdfs_cp(self, filepath: str) -> bool:
        """
        Copy a single file within HDFS using hdfs dfs -cp.
        
        Args:
            filepath: Path to the file to copy (assumed to be already in HDFS)
            
        Returns:
            bool: True if copy successful
        """
        filename = os.path.basename(filepath)
        source_hdfs_path = filepath  # Assume filepath is already in HDFS
        target_hdfs_path = f"{self.target_hdfs_path}/{filename}"
        
        cmd = f"hdfs dfs -cp {source_hdfs_path} {target_hdfs_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.error(f"HDFS cp failed: {result.stderr}")
            return False
        
        logging.info(f"Successfully copied {filename} via hdfs cp")
        return True
    
    def _transfer_via_scp(self, filepaths: List[str]) -> bool:
        """
        Transfer files via SCP.
        
        Args:
            filepaths: List of file paths to transfer
            
        Returns:
            bool: True if transfer successful
        """
        logging.info("Transferring files via SCP...")
        
        # Use configured target details
        target_host = self.scp_target_host
        target_path = self.scp_target_path
        
        if not target_host or not target_path:
            logging.error("SCP target host and path must be configured.")
            return False
        
        # Ensure target directory exists
        if not self._ensure_remote_directory_exists(target_host, target_path):
            return False
        
        # Transfer files
        for filepath in filepaths:
            if not self._copy_file_via_scp(filepath, target_host, target_path):
                return False
        
        return True
    
    def _ensure_remote_directory_exists(self, target_host: str, target_path: str) -> bool:
        """
        Ensure remote directory exists.
        
        Args:
            target_host: Target host name
            target_path: Target directory path
            
        Returns:
            bool: True if directory exists or was created successfully
        """
        logging.info(f"Checking if target directory exists on {target_host}: {target_path}")
        check_cmd = f"ssh {target_host} 'test -d {target_path}'"
        result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.info(f"Target directory does not exist, creating: {target_path}")
            mkdir_cmd = f"ssh {target_host} 'mkdir -p {target_path}'"
            mkdir_result = subprocess.run(mkdir_cmd, shell=True, capture_output=True, text=True)
            
            if mkdir_result.returncode != 0:
                logging.error(f"Failed to create target directory: {mkdir_result.stderr}")
                return False
            else:
                logging.info(f"Target directory created successfully: {target_path}")
        else:
            logging.info(f"Target directory already exists: {target_path}")
        
        return True
    
    def _copy_file_via_scp(self, filepath: str, target_host: str, target_path: str) -> bool:
        """
        Copy a single file via SCP.
        
        Args:
            filepath: Path to the file to copy
            target_host: Target host name
            target_path: Target directory path
            
        Returns:
            bool: True if copy successful
        """
        cmd = f"scp {filepath} {target_host}:{target_path}/"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.error(f"SCP transfer failed: {result.stderr}")
            return False
        
        return True
    
    def _transfer_via_distcp(self, filepaths: List[str]) -> bool:
        """
        Transfer files using distcp for cross-cluster copying.
        
        Args:
            filepaths: List of file paths to transfer
            
        Returns:
            bool: True if transfer successful
        """
        logging.info("Transferring files using distcp...")
        
        # Ensure target HDFS path exists on target cluster
        if not self._ensure_target_hdfs_path_exists():
            return False
        
        # Transfer files using distcp
        for filepath in filepaths:
            if not self._copy_file_via_distcp(filepath):
                return False
        
        logging.info("Files transferred via distcp successfully")
        return True
    
    def _ensure_target_hdfs_path_exists(self) -> bool:
        """
        Ensure target HDFS path exists on target cluster.
        
        Returns:
            bool: True if path exists or was created successfully
        """
        logging.info(f"Checking if target HDFS path exists: {self.target_hdfs_path}")
        
        # Use distcp to check if path exists (more reliable for cross-cluster)
        check_cmd = f"hadoop distcp -dryrun {self.source_hdfs_path} {self.target_cluster}{self.target_hdfs_path}"
        result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.info(f"Target HDFS path does not exist, creating: {self.target_hdfs_path}")
            mkdir_cmd = f"hdfs dfs -mkdir -p {self.target_hdfs_path}"
            mkdir_result = subprocess.run(mkdir_cmd, shell=True, capture_output=True, text=True)
            
            if mkdir_result.returncode != 0:
                logging.error(f"Failed to create target HDFS path: {mkdir_result.stderr}")
                return False
            else:
                logging.info(f"Target HDFS path created successfully: {self.target_hdfs_path}")
        else:
            logging.info(f"Target HDFS path already exists: {self.target_hdfs_path}")
        
        return True
    
    def _copy_file_via_distcp(self, filepath: str) -> bool:
        """
        Copy a single file using distcp.
        
        Args:
            filepath: Path to the file to copy
            
        Returns:
            bool: True if copy successful
        """
        filename = os.path.basename(filepath)
        source_hdfs_path = f"{self.source_hdfs_path}/{filename}"
        target_hdfs_path = f"{self.target_cluster}{self.target_hdfs_path}/{filename}"
        
        # Use distcp with optimization flags
        cmd = f"hadoop distcp -update -delete -strategy dynamic {source_hdfs_path} {target_hdfs_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.error(f"Distcp copy failed: {result.stderr}")
            return False
        
        logging.info(f"Successfully copied {filename} via distcp")
        return True
    
    def get_transfer_info(self) -> dict:
        """
        Get information about the transfer configuration.
        
        Returns:
            dict: Transfer configuration information
        """
        # Determine primary transfer method
        if self.use_distcp and self.source_hdfs_path and self.target_cluster:
            primary_method = 'distcp'
        elif self.target_hdfs_path:
            primary_method = 'hdfs_put'
        else:
            primary_method = 'scp'
            
        return {
            'primary_transfer_method': primary_method,
            'available_methods': ['distcp', 'hdfs_put', 'hdfs_cp', 'scp'],
            'target_hdfs_path': self.target_hdfs_path,
            'use_distcp': self.use_distcp,
            'source_hdfs_path': self.source_hdfs_path,
            'target_cluster': self.target_cluster,
            'scp_target_host': self.scp_target_host,
            'scp_target_path': self.scp_target_path
        }
    
    def validate_transfer_config(self) -> bool:
        """
        Validate the transfer configuration.
        
        Returns:
            bool: True if configuration is valid
        """
        if self.use_distcp:
            # Validate distcp configuration
            if not self.source_hdfs_path:
                logging.error("Source HDFS path is required for distcp transfers")
                return False
            if not self.target_cluster:
                logging.error("Target cluster is required for distcp transfers")
                return False
            if not self.target_hdfs_path:
                logging.error("Target HDFS path is required for distcp transfers")
                return False
            
            # Validate HDFS path formats
            if not self.source_hdfs_path.startswith('/'):
                logging.error("Source HDFS path must start with '/'")
                return False
            if not self.target_hdfs_path.startswith('/'):
                logging.error("Target HDFS path must start with '/'")
                return False
                
            logging.info("Distcp configuration validated successfully")
            
        elif self.target_hdfs_path:
            # Validate HDFS path format for hdfs_put and hdfs_cp
            if not self.target_hdfs_path.startswith('/'):
                logging.error("HDFS path must start with '/'")
                return False
            logging.info("HDFS transfer configuration validated successfully")
        else:
            # For SCP, validate host and path configuration
            if not self.scp_target_host or not self.scp_target_path:
                logging.error("SCP target host and path must be configured.")
                return False
            logging.info("SCP transfer configuration validated successfully")
        
        return True 