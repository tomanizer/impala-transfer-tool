#!/usr/bin/env python3
"""
Test suite for the utils module.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import os

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


if __name__ == '__main__':
    unittest.main() 