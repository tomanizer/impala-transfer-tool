"""
File transfer operations module.
Handles transferring files to target clusters via HDFS, SCP, distcp, or fsspec.
"""

import os
import logging
import subprocess
from typing import List, Optional, Dict, Any, Union
from pathlib import Path

try:
    import fsspec
    from fsspec import AbstractFileSystem
    FSSPEC_AVAILABLE = True
except ImportError:
    FSSPEC_AVAILABLE = False
    AbstractFileSystem = None


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


class FSSpecFileTransferManager:
    """Handles file transfer operations using fsspec for filesystem abstraction."""
    
    def __init__(self, source_fs_config: Dict[str, Any] = None, 
                 target_fs_config: Dict[str, Any] = None,
                 source_fs: AbstractFileSystem = None,
                 target_fs: AbstractFileSystem = None,
                 transfer_options: Dict[str, Any] = None):
        """
        Initialize fsspec-based file transfer manager.
        
        Args:
            source_fs_config: Configuration for source filesystem (alternative to source_fs)
            target_fs_config: Configuration for target filesystem (alternative to target_fs)
            source_fs: Pre-configured source filesystem instance
            target_fs: Pre-configured target filesystem instance
            transfer_options: Additional transfer options
        """
        if not FSSPEC_AVAILABLE:
            raise ImportError("fsspec is required for FSSpecFileTransferManager")
        
        self.source_fs_config = source_fs_config or {}
        self.target_fs_config = target_fs_config or {}
        self.transfer_options = transfer_options or {}
        
        # Initialize filesystems - prefer passed instances over config
        self.source_fs = source_fs
        self.target_fs = target_fs
        
        # Create filesystems from config if instances not provided
        if self.source_fs is None and source_fs_config:
            self.source_fs = self._create_filesystem(source_fs_config)
        if self.target_fs is None and target_fs_config:
            self.target_fs = self._create_filesystem(target_fs_config)
    
    def _create_filesystem(self, config: Dict[str, Any]) -> AbstractFileSystem:
        """
        Create a filesystem instance from configuration.
        
        Args:
            config: Filesystem configuration dictionary
            
        Returns:
            AbstractFileSystem: Configured filesystem instance
        """
        # If config is already a filesystem instance, return it
        if hasattr(config, 'protocol') and hasattr(config, 'open'):
            logging.info(f"Using provided filesystem instance with protocol: {getattr(config, 'protocol', 'unknown')}")
            return config
        
        protocol = config.get('protocol', 'file')
        fs_kwargs = {k: v for k, v in config.items() if k != 'protocol'}
        
        logging.info(f"Creating filesystem with protocol: {protocol}")
        return fsspec.filesystem(protocol, **fs_kwargs)
    
    def transfer_files(self, filepaths: List[str], target_table: str, target_path: str = None) -> bool:
        """
        Transfer files using fsspec.
        
        Args:
            filepaths: List of file paths to transfer
            target_table: Target table name (for logging purposes)
            target_path: Target directory path (overrides config if provided)
            
        Returns:
            bool: True if transfer successful, False otherwise
        """
        try:
            if not self.target_fs:
                logging.error("Target filesystem not configured")
                return False
            
            # Use provided target_path or fall back to config
            if target_path is None:
                target_path = self.target_fs_config.get('target_path', '/')
            
            if not target_path.endswith('/'):
                target_path += '/'
            
            # Ensure target directory exists
            if not self._ensure_target_path_exists(target_path):
                return False
            
            # Transfer files
            for filepath in filepaths:
                if not self._transfer_single_file(filepath, target_path):
                    return False
            
            logging.info(f"Successfully transferred {len(filepaths)} files to {target_path}")
            return True
            
        except Exception as e:
            logging.error(f"FSSpec transfer failed: {e}")
            return False
    
    def _ensure_target_path_exists(self, target_path: str) -> bool:
        """
        Ensure target path exists on target filesystem.
        
        Args:
            target_path: Target directory path
            
        Returns:
            bool: True if path exists or was created successfully
        """
        try:
            if not self.target_fs.exists(target_path):
                logging.info(f"Creating target directory: {target_path}")
                self.target_fs.makedirs(target_path, exist_ok=True)
                logging.info(f"Target directory created: {target_path}")
            else:
                logging.info(f"Target directory already exists: {target_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to create target directory {target_path}: {e}")
            return False
    
    def _transfer_single_file(self, source_filepath: str, target_path: str) -> bool:
        """
        Transfer a single file using fsspec.
        
        Args:
            source_filepath: Path to source file
            target_path: Target directory path
            
        Returns:
            bool: True if transfer successful
        """
        try:
            filename = Path(source_filepath).name
            target_filepath = f"{target_path}{filename}"
            
            # Determine source filesystem
            if self.source_fs:
                # Source is also a filesystem
                source_fs = self.source_fs
                # For filesystem-to-filesystem transfers, we need to handle paths correctly
                # If source_filepath is a local path but source_fs is not local, we need to adjust
                if hasattr(source_fs, 'protocol') and source_fs.protocol != ('file', 'local'):
                    # For non-local source filesystems, assume the path is already in the correct format
                    source_path = source_filepath
                else:
                    # For local filesystem, use the path as-is
                    source_path = source_filepath
            else:
                # Source is local filesystem
                source_fs = fsspec.filesystem('file')
                source_path = source_filepath
            
            # Transfer file
            logging.info(f"Transferring {filename} to {target_filepath}")
            
            # Simple read/write transfer
            with source_fs.open(source_path, 'rb') as src_file:
                with self.target_fs.open(target_filepath, 'wb') as dst_file:
                    dst_file.write(src_file.read())
            
            logging.info(f"Successfully transferred {filename}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to transfer {source_filepath}: {e}")
            return False
    
    def list_files(self, path: str = None) -> List[str]:
        """
        List files in the target filesystem.
        
        Args:
            path: Path to list (defaults to root or configured target_path)
            
        Returns:
            List[str]: List of file paths
        """
        if not self.target_fs:
            return []
        
        try:
            # Use provided path or fall back to config
            if path is None:
                path = self.target_fs_config.get('target_path', '/')
            return self.target_fs.ls(path)
        except Exception as e:
            logging.error(f"Failed to list files in {path}: {e}")
            return []
    
    def get_file_info(self, filepath: str) -> Dict[str, Any]:
        """
        Get information about a file in the target filesystem.
        
        Args:
            filepath: Path to the file
            
        Returns:
            Dict[str, Any]: File information
        """
        if not self.target_fs:
            return {}
        
        try:
            info = self.target_fs.info(filepath)
            return {
                'size': info.get('size'),
                'type': info.get('type'),
                'modified': info.get('mtime'),
                'path': filepath
            }
        except Exception as e:
            logging.error(f"Failed to get file info for {filepath}: {e}")
            return {}
    
    def validate_config(self) -> bool:
        """
        Validate the fsspec configuration.
        
        Returns:
            bool: True if configuration is valid
        """
        try:
            # Test target filesystem connection
            if self.target_fs:
                # Try to list root directory
                self.target_fs.ls('/')
                logging.info("Target filesystem configuration validated successfully")
            
            # Test source filesystem connection if configured
            if self.source_fs:
                # Try to list root directory
                self.source_fs.ls('/')
                logging.info("Source filesystem configuration validated successfully")
            
            return True
            
        except Exception as e:
            logging.error(f"FSSpec configuration validation failed: {e}")
            return False
    
    def get_transfer_info(self) -> Dict[str, Any]:
        """
        Get information about the fsspec transfer configuration.
        
        Returns:
            dict: Transfer configuration information
        """
        # Determine protocols from filesystem instances or config
        source_protocol = 'unknown'
        target_protocol = 'unknown'
        
        if self.source_fs:
            protocol = getattr(self.source_fs, 'protocol', 'unknown')
            # Handle case where protocol is a tuple (e.g., ('file', 'local'))
            source_protocol = protocol[0] if isinstance(protocol, tuple) else protocol
        elif self.source_fs_config:
            source_protocol = self.source_fs_config.get('protocol', 'file')
            
        if self.target_fs:
            protocol = getattr(self.target_fs, 'protocol', 'unknown')
            # Handle case where protocol is a tuple (e.g., ('file', 'local'))
            target_protocol = protocol[0] if isinstance(protocol, tuple) else protocol
        elif self.target_fs_config:
            target_protocol = self.target_fs_config.get('protocol', 'file')
        
        return {
            'transfer_method': 'fsspec',
            'source_protocol': source_protocol,
            'target_protocol': target_protocol,
            'source_fs_configured': self.source_fs is not None,
            'target_fs_configured': self.target_fs is not None,
            'source_fs_type': type(self.source_fs).__name__ if self.source_fs else None,
            'target_fs_type': type(self.target_fs).__name__ if self.target_fs else None,
            'source_config': self.source_fs_config,
            'target_config': self.target_fs_config,
            'transfer_options': self.transfer_options
        }


class UnifiedFileTransferManager:
    """Unified file transfer manager that can use both traditional and fsspec methods."""
    
    def __init__(self, use_fsspec: bool = False, fsspec_config: Dict[str, Any] = None,
                 source_fs: AbstractFileSystem = None, target_fs: AbstractFileSystem = None,
                 **traditional_kwargs):
        """
        Initialize unified file transfer manager.
        
        Args:
            use_fsspec: Whether to use fsspec for transfers
            fsspec_config: Configuration for fsspec transfer manager
            source_fs: Pre-configured source filesystem instance
            target_fs: Pre-configured target filesystem instance
            **traditional_kwargs: Arguments for traditional FileTransferManager
        """
        self.use_fsspec = use_fsspec and FSSPEC_AVAILABLE
        
        if self.use_fsspec:
            fsspec_config = fsspec_config or {}
            # Pass filesystem instances to fsspec manager
            self.fsspec_manager = FSSpecFileTransferManager(
                source_fs=source_fs,
                target_fs=target_fs,
                **fsspec_config
            )
            self.traditional_manager = None
        else:
            self.traditional_manager = FileTransferManager(**traditional_kwargs)
            self.fsspec_manager = None
    
    def transfer_files(self, filepaths: List[str], target_table: str, target_path: str = None) -> bool:
        """
        Transfer files using the configured method.
        
        Args:
            filepaths: List of file paths to transfer
            target_table: Target table name (for logging purposes)
            target_path: Target directory path (for fsspec transfers)
            
        Returns:
            bool: True if transfer successful, False otherwise
        """
        if self.use_fsspec:
            return self.fsspec_manager.transfer_files(filepaths, target_table, target_path)
        else:
            return self.traditional_manager.transfer_files(filepaths, target_table)
    
    def validate_config(self) -> bool:
        """
        Validate the transfer configuration.
        
        Returns:
            bool: True if configuration is valid
        """
        if self.use_fsspec:
            return self.fsspec_manager.validate_config()
        else:
            return self.traditional_manager.validate_transfer_config()
    
    def get_transfer_info(self) -> Dict[str, Any]:
        """
        Get information about the transfer configuration.
        
        Returns:
            dict: Transfer configuration information
        """
        if self.use_fsspec:
            return self.fsspec_manager.get_transfer_info()
        else:
            return self.traditional_manager.get_transfer_info()
    
    @staticmethod
    def create_hdfs_config(host: str, port: int = 8020, user: str = None) -> Dict[str, Any]:
        """
        Create HDFS filesystem configuration.
        
        Args:
            host: HDFS namenode host
            port: HDFS namenode port
            user: HDFS user
            
        Returns:
            Dict[str, Any]: HDFS configuration
        """
        config = {
            'protocol': 'hdfs',
            'host': host,
            'port': port
        }
        if user:
            config['user'] = user
        return config
    
    @staticmethod
    def create_s3_config(bucket: str, access_key: str = None, secret_key: str = None,
                        endpoint_url: str = None) -> Dict[str, Any]:
        """
        Create S3 filesystem configuration.
        
        Args:
            bucket: S3 bucket name
            access_key: AWS access key
            secret_key: AWS secret key
            endpoint_url: S3 endpoint URL (for non-AWS S3)
            
        Returns:
            Dict[str, Any]: S3 configuration
        """
        config = {
            'protocol': 's3',
            'bucket': bucket
        }
        if access_key:
            config['key'] = access_key
        if secret_key:
            config['secret'] = secret_key
        if endpoint_url:
            config['endpoint_url'] = endpoint_url
        return config
    
    @staticmethod
    def create_gcs_config(bucket: str, project: str = None, 
                         credentials_file: str = None) -> Dict[str, Any]:
        """
        Create Google Cloud Storage filesystem configuration.
        
        Args:
            bucket: GCS bucket name
            project: GCP project ID
            credentials_file: Path to service account credentials file
            
        Returns:
            Dict[str, Any]: GCS configuration
        """
        config = {
            'protocol': 'gcs',
            'bucket': bucket
        }
        if project:
            config['project'] = project
        if credentials_file:
            config['token'] = credentials_file
        return config
    
    @staticmethod
    def create_azure_config(account_name: str, account_key: str = None,
                           container: str = None) -> Dict[str, Any]:
        """
        Create Azure Blob Storage filesystem configuration.
        
        Args:
            account_name: Azure storage account name
            account_key: Azure storage account key
            container: Azure blob container name
            
        Returns:
            Dict[str, Any]: Azure configuration
        """
        config = {
            'protocol': 'abfs',
            'account_name': account_name
        }
        if account_key:
            config['account_key'] = account_key
        if container:
            config['container'] = container
        return config 