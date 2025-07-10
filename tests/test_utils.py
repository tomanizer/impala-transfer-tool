#!/usr/bin/env python3
"""
Test suite for the utils module.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import os
import shutil

from impala_transfer.utils import FileManager


class TestFileManager(unittest.TestCase):
    """Test the FileManager class."""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_cleanup_temp_files_success(self):
        """Test successful cleanup of temporary files."""
        # Create some test files
        test_files = ['test1.parquet', 'test2.csv', 'test3.parquet']
        filepaths = []
        for filename in test_files:
            filepath = os.path.join(self.temp_dir, filename)
            with open(filepath, 'w') as f:
                f.write('test content')
            filepaths.append(filepath)
        
        FileManager.cleanup_temp_files(filepaths)
        # Verify files are deleted
        for filepath in filepaths:
            self.assertFalse(os.path.exists(filepath))

    def test_cleanup_temp_files_partial_failure(self):
        """Test cleanup with partial failure."""
        test_file = os.path.join(self.temp_dir, 'test.parquet')
        with open(test_file, 'w') as f:
            f.write('test content')
        
        with patch('os.remove', side_effect=[OSError("Permission denied"), None]):
            FileManager.cleanup_temp_files([test_file, test_file])
        # Should not raise

    def test_ensure_temp_directory(self):
        """Test ensuring temporary directory exists."""
        import shutil
        shutil.rmtree(self.temp_dir)
        FileManager.ensure_temp_directory(self.temp_dir)
        self.assertTrue(os.path.exists(self.temp_dir))
        self.assertTrue(os.path.isdir(self.temp_dir))

    def test_get_directory_size_oserror(self):
        """Test get_directory_size handles OSError and returns 0."""
        with patch('os.walk', side_effect=OSError("walk error")):
            with self.assertLogs('root', level='WARNING') as cm:
                size = FileManager.get_directory_size(self.temp_dir)
        self.assertEqual(size, 0)
        self.assertTrue(any('Could not calculate size for directory' in msg for msg in cm.output))

    def test_get_file_size_oserror(self):
        """Test get_file_size handles OSError and returns -1."""
        non_existent_file = os.path.join(self.temp_dir, 'does_not_exist.txt')
        size = FileManager.get_file_size(non_existent_file)
        self.assertEqual(size, -1)

    def test_cleanup_directory_with_pattern(self):
        """Test cleanup_directory with pattern matching."""
        # Create test files
        test_files = ['test1.parquet', 'test2.csv', 'test3.parquet', 'test4.txt']
        for filename in test_files:
            filepath = os.path.join(self.temp_dir, filename)
            with open(filepath, 'w') as f:
                f.write('test content')
        
        # Clean up only parquet files
        FileManager.cleanup_directory(self.temp_dir, "*.parquet")
        
        # Check that parquet files are gone but others remain
        self.assertFalse(os.path.exists(os.path.join(self.temp_dir, 'test1.parquet')))
        self.assertFalse(os.path.exists(os.path.join(self.temp_dir, 'test3.parquet')))
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'test2.csv')))
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'test4.txt')))

    def test_cleanup_directory_without_pattern(self):
        """Test cleanup_directory without pattern (all files)."""
        # Create test files
        test_files = ['test1.parquet', 'test2.csv', 'test3.parquet']
        for filename in test_files:
            filepath = os.path.join(self.temp_dir, filename)
            with open(filepath, 'w') as f:
                f.write('test content')
        
        # Clean up all files
        FileManager.cleanup_directory(self.temp_dir)
        
        # Check that all files are gone
        for filename in test_files:
            self.assertFalse(os.path.exists(os.path.join(self.temp_dir, filename)))

    def test_cleanup_directory_oserror(self):
        """Test cleanup_directory handles OSError."""
        with patch('os.listdir', side_effect=OSError("Permission denied")):
            with self.assertLogs('root', level='ERROR') as cm:
                FileManager.cleanup_directory(self.temp_dir)
        self.assertTrue(any('Failed to cleanup directory' in msg for msg in cm.output))

    def test_cleanup_directory_remove_error(self):
        """Test cleanup_directory handles file removal errors."""
        # Create a test file
        test_file = os.path.join(self.temp_dir, 'test.parquet')
        with open(test_file, 'w') as f:
            f.write('test content')
        
        with patch('os.remove', side_effect=OSError("Permission denied")):
            with self.assertLogs('root', level='WARNING') as cm:
                FileManager.cleanup_directory(self.temp_dir, "*.parquet")
        self.assertTrue(any('Failed to cleanup' in msg for msg in cm.output))

    def test_create_backup_success(self):
        """Test successful backup creation."""
        # Create a test file
        test_file = os.path.join(self.temp_dir, 'test.txt')
        with open(test_file, 'w') as f:
            f.write('test content')
        
        backup_path = FileManager.create_backup(test_file)
        self.assertIsNotNone(backup_path)
        self.assertTrue(os.path.exists(backup_path))
        self.assertEqual(backup_path, test_file + '.backup')

    def test_create_backup_custom_suffix(self):
        """Test backup creation with custom suffix."""
        # Create a test file
        test_file = os.path.join(self.temp_dir, 'test.txt')
        with open(test_file, 'w') as f:
            f.write('test content')
        
        backup_path = FileManager.create_backup(test_file, "_backup")
        self.assertIsNotNone(backup_path)
        self.assertTrue(os.path.exists(backup_path))
        self.assertEqual(backup_path, test_file + '_backup')

    def test_create_backup_failure(self):
        """Test backup creation failure."""
        non_existent_file = os.path.join(self.temp_dir, 'does_not_exist.txt')
        with self.assertLogs('root', level='ERROR') as cm:
            backup_path = FileManager.create_backup(non_existent_file)
        self.assertIsNone(backup_path)
        self.assertTrue(any('Failed to create backup' in msg for msg in cm.output))

    def test_restore_backup_success(self):
        """Test successful backup restoration."""
        # Create original and backup files
        original_file = os.path.join(self.temp_dir, 'test.txt')
        backup_file = os.path.join(self.temp_dir, 'test.txt.backup')
        
        with open(original_file, 'w') as f:
            f.write('original content')
        with open(backup_file, 'w') as f:
            f.write('backup content')
        
        # Remove original file
        os.remove(original_file)
        
        # Restore from backup
        success = FileManager.restore_backup(backup_file)
        self.assertTrue(success)
        self.assertTrue(os.path.exists(original_file))
        
        with open(original_file, 'r') as f:
            content = f.read()
        self.assertEqual(content, 'backup content')

    def test_restore_backup_with_original_path(self):
        """Test backup restoration with explicit original path."""
        # Create backup file
        backup_file = os.path.join(self.temp_dir, 'test.txt.backup')
        original_file = os.path.join(self.temp_dir, 'restored.txt')
        
        with open(backup_file, 'w') as f:
            f.write('backup content')
        
        # Restore to different path
        success = FileManager.restore_backup(backup_file, original_file)
        self.assertTrue(success)
        self.assertTrue(os.path.exists(original_file))
        
        with open(original_file, 'r') as f:
            content = f.read()
        self.assertEqual(content, 'backup content')

    def test_restore_backup_failure(self):
        """Test backup restoration failure."""
        non_existent_backup = os.path.join(self.temp_dir, 'does_not_exist.backup')
        with self.assertLogs('root', level='ERROR') as cm:
            success = FileManager.restore_backup(non_existent_backup)
        self.assertFalse(success)
        self.assertTrue(any('Failed to restore from backup' in msg for msg in cm.output))

    def test_format_file_size_negative(self):
        """Test format_file_size with negative value."""
        result = FileManager.format_file_size(-1)
        self.assertEqual(result, "Unknown")

    def test_format_file_size_bytes(self):
        """Test format_file_size with bytes."""
        result = FileManager.format_file_size(512)
        self.assertEqual(result, "512.0 B")

    def test_format_file_size_kb(self):
        """Test format_file_size with kilobytes."""
        result = FileManager.format_file_size(2048)
        self.assertEqual(result, "2.0 KB")

    def test_format_file_size_mb(self):
        """Test format_file_size with megabytes."""
        result = FileManager.format_file_size(2 * 1024 * 1024)
        self.assertEqual(result, "2.0 MB")

    def test_format_file_size_gb(self):
        """Test format_file_size with gigabytes."""
        result = FileManager.format_file_size(2 * 1024 * 1024 * 1024)
        self.assertEqual(result, "2.0 GB")

    def test_format_file_size_tb(self):
        """Test format_file_size with terabytes."""
        result = FileManager.format_file_size(2 * 1024 * 1024 * 1024 * 1024)
        self.assertEqual(result, "2.0 TB")

    def test_format_file_size_pb(self):
        """Test format_file_size with petabytes."""
        result = FileManager.format_file_size(2 * 1024 * 1024 * 1024 * 1024 * 1024)
        self.assertEqual(result, "2.0 PB")

    def test_get_file_info_success(self):
        """Test get_file_info with existing file."""
        # Create a test file
        test_file = os.path.join(self.temp_dir, 'test.txt')
        with open(test_file, 'w') as f:
            f.write('test content')
        
        info = FileManager.get_file_info(test_file)
        
        self.assertEqual(info['path'], test_file)
        self.assertEqual(info['size'], 12)  # 'test content' is 12 bytes
        self.assertEqual(info['size_formatted'], "12.0 B")
        self.assertTrue(info['exists'])
        self.assertTrue(info['is_file'])
        self.assertFalse(info['is_directory'])
        self.assertIn('modified_time', info)
        self.assertIn('created_time', info)

    def test_get_file_info_directory(self):
        """Test get_file_info with directory."""
        info = FileManager.get_file_info(self.temp_dir)
        
        self.assertEqual(info['path'], self.temp_dir)
        self.assertTrue(info['exists'])
        self.assertFalse(info['is_file'])
        self.assertTrue(info['is_directory'])

    def test_get_file_info_nonexistent(self):
        """Test get_file_info with non-existent file."""
        non_existent_file = os.path.join(self.temp_dir, 'does_not_exist.txt')
        info = FileManager.get_file_info(non_existent_file)
        
        self.assertEqual(info['path'], non_existent_file)
        self.assertFalse(info['exists'])


if __name__ == '__main__':
    unittest.main() 