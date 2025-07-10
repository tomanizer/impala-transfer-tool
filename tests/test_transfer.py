#!/usr/bin/env python3
"""
Test suite for the transfer module.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import os

from impala_transfer.transfer import FileTransferManager


class TestFileTransferManager(unittest.TestCase):
    """Test the FileTransferManager class."""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.transfer_manager = FileTransferManager(target_hdfs_path='/test/hdfs/path')
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)
    
    @patch('impala_transfer.transfer.subprocess.run')
    def test_transfer_to_hdfs_success(self, mock_run):
        """Test successful HDFS transfer."""
        mock_run.return_value.returncode = 0
        
        result = self.transfer_manager.transfer_files(['test_file.parquet'], 'test_table')
        
        self.assertTrue(result)
        mock_run.assert_called()
    
    @patch('impala_transfer.transfer.subprocess.run')
    def test_transfer_to_hdfs_path_creation_failure(self, mock_run):
        """Test HDFS transfer with path creation failure."""
        mock_run.side_effect = [
            Mock(returncode=1),  # mkdir fails
            Mock(returncode=0)   # put succeeds
        ]
        
        result = self.transfer_manager.transfer_files(['test_file.parquet'], 'test_table')
        
        self.assertFalse(result)
    
    @patch('impala_transfer.transfer.subprocess.run')
    def test_transfer_via_scp_success(self, mock_run):
        """Test successful SCP transfer."""
        # Set target_hdfs_path to None to trigger SCP transfer
        self.transfer_manager.target_hdfs_path = None
        mock_run.return_value.returncode = 0
        
        result = self.transfer_manager.transfer_files(['test_file.parquet'], 'test_table')
        
        self.assertTrue(result)
        mock_run.assert_called()
    
    @patch('impala_transfer.transfer.subprocess.run')
    def test_transfer_via_scp_failure(self, mock_run):
        """Test failed SCP transfer."""
        # Set target_hdfs_path to None to trigger SCP transfer
        self.transfer_manager.target_hdfs_path = None
        mock_run.return_value.returncode = 1
        
        result = self.transfer_manager.transfer_files(['test_file.parquet'], 'test_table')
        
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main() 