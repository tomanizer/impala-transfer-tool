"""
Utility functions module.
Contains file system operations and other utility functions.
"""

import os
import logging
import shutil
from typing import List


class FileManager:
    """Handles file system operations and cleanup."""
    
    @staticmethod
    def cleanup_temp_files(filepaths: List[str]):
        """
        Clean up temporary files.
        
        Args:
            filepaths: List of file paths to remove
        """
        for filepath in filepaths:
            try:
                os.remove(filepath)
                logging.debug(f"Cleaned up: {filepath}")
            except Exception as e:
                logging.warning(f"Failed to cleanup {filepath}: {e}")
    
    @staticmethod
    def ensure_temp_directory(temp_dir: str):
        """
        Ensure temporary directory exists.
        
        Args:
            temp_dir: Path to the temporary directory
        """
        os.makedirs(temp_dir, exist_ok=True)
    
    @staticmethod
    def get_file_size(filepath: str) -> int:
        """
        Get file size in bytes.
        
        Args:
            filepath: Path to the file
            
        Returns:
            int: File size in bytes, -1 if file doesn't exist
        """
        try:
            return os.path.getsize(filepath)
        except OSError:
            return -1
    
    @staticmethod
    def get_directory_size(directory: str) -> int:
        """
        Get total size of directory in bytes.
        
        Args:
            directory: Path to the directory
            
        Returns:
            int: Total size in bytes
        """
        total_size = 0
        try:
            for dirpath, dirnames, filenames in os.walk(directory):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    total_size += FileManager.get_file_size(filepath)
        except OSError:
            logging.warning(f"Could not calculate size for directory: {directory}")
        
        return total_size
    
    @staticmethod
    def cleanup_directory(directory: str, pattern: str = None):
        """
        Clean up files in directory matching pattern.
        
        Args:
            directory: Directory to clean
            pattern: File pattern to match (e.g., "*.parquet")
        """
        try:
            if pattern:
                import glob
                files_to_remove = glob.glob(os.path.join(directory, pattern))
            else:
                files_to_remove = [os.path.join(directory, f) for f in os.listdir(directory)
                                 if os.path.isfile(os.path.join(directory, f))]
            
            for filepath in files_to_remove:
                try:
                    os.remove(filepath)
                    logging.debug(f"Cleaned up: {filepath}")
                except Exception as e:
                    logging.warning(f"Failed to cleanup {filepath}: {e}")
        except Exception as e:
            logging.error(f"Failed to cleanup directory {directory}: {e}")
    
    @staticmethod
    def create_backup(filepath: str, backup_suffix: str = ".backup") -> str:
        """
        Create a backup of a file.
        
        Args:
            filepath: Path to the file to backup
            backup_suffix: Suffix for the backup file
            
        Returns:
            str: Path to the backup file
        """
        backup_path = filepath + backup_suffix
        try:
            shutil.copy2(filepath, backup_path)
            logging.info(f"Created backup: {backup_path}")
            return backup_path
        except Exception as e:
            logging.error(f"Failed to create backup of {filepath}: {e}")
            return None
    
    @staticmethod
    def restore_backup(backup_path: str, original_path: str = None) -> bool:
        """
        Restore a file from backup.
        
        Args:
            backup_path: Path to the backup file
            original_path: Path to restore to (defaults to backup_path without .backup suffix)
            
        Returns:
            bool: True if restore successful
        """
        if original_path is None:
            original_path = backup_path.replace(".backup", "")
        
        try:
            shutil.copy2(backup_path, original_path)
            logging.info(f"Restored from backup: {original_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to restore from backup {backup_path}: {e}")
            return False
    
    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """
        Format file size in human-readable format.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            str: Formatted size string
        """
        if size_bytes < 0:
            return "Unknown"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        
        return f"{size_bytes:.1f} PB"
    
    @staticmethod
    def get_file_info(filepath: str) -> dict:
        """
        Get comprehensive file information.
        
        Args:
            filepath: Path to the file
            
        Returns:
            dict: File information including size, modification time, etc.
        """
        try:
            stat = os.stat(filepath)
            return {
                'path': filepath,
                'size': stat.st_size,
                'size_formatted': FileManager.format_file_size(stat.st_size),
                'modified_time': stat.st_mtime,
                'created_time': stat.st_ctime,
                'is_file': os.path.isfile(filepath),
                'is_directory': os.path.isdir(filepath),
                'exists': True
            }
        except OSError:
            return {
                'path': filepath,
                'exists': False
            } 