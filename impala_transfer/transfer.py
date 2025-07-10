"""
File transfer operations module.
Handles transferring files to target clusters via HDFS or SCP.
"""

import os
import logging
import subprocess
from typing import List


class FileTransferManager:
    """Handles file transfer operations to target cluster."""
    
    def __init__(self, target_hdfs_path: str = None):
        """
        Initialize file transfer manager.
        
        Args:
            target_hdfs_path: HDFS path on target cluster (if using HDFS transfer)
        """
        self.target_hdfs_path = target_hdfs_path
    
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
            if self.target_hdfs_path:
                return self._transfer_to_hdfs(filepaths)
            else:
                return self._transfer_via_scp(filepaths)
        except Exception as e:
            logging.error(f"Transfer to cluster 2 failed: {e}")
            return False
    
    def _transfer_to_hdfs(self, filepaths: List[str]) -> bool:
        """
        Transfer files to HDFS.
        
        Args:
            filepaths: List of file paths to transfer
            
        Returns:
            bool: True if transfer successful
        """
        logging.info("Transferring files to HDFS...")
        
        # Ensure HDFS path exists
        if not self._ensure_hdfs_path_exists():
            return False
        
        # Transfer files
        for filepath in filepaths:
            if not self._copy_file_to_hdfs(filepath):
                return False
        
        logging.info("Files transferred to HDFS successfully")
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
    
    def _copy_file_to_hdfs(self, filepath: str) -> bool:
        """
        Copy a single file to HDFS.
        
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
            logging.error(f"HDFS copy failed: {result.stderr}")
            return False
        
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
        
        # Configure target details (should be configurable)
        target_host = "cluster2.example.com"
        target_path = "/path/to/landing/zone"
        
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
    
    def get_transfer_info(self) -> dict:
        """
        Get information about the transfer configuration.
        
        Returns:
            dict: Transfer configuration information
        """
        return {
            'transfer_method': 'hdfs' if self.target_hdfs_path else 'scp',
            'target_hdfs_path': self.target_hdfs_path,
            'scp_target_host': 'cluster2.example.com',  # Should be configurable
            'scp_target_path': '/path/to/landing/zone'  # Should be configurable
        }
    
    def validate_transfer_config(self) -> bool:
        """
        Validate the transfer configuration.
        
        Returns:
            bool: True if configuration is valid
        """
        if self.target_hdfs_path:
            # Validate HDFS path format
            if not self.target_hdfs_path.startswith('/'):
                logging.error("HDFS path must start with '/'")
                return False
        else:
            # For SCP, we would validate host and path configuration
            # This is currently hardcoded but should be configurable
            logging.warning("SCP transfer configuration is hardcoded")
        
        return True 