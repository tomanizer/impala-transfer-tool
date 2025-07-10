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
        self.transfer_manager = FileTransferManager(
            target_hdfs_path='/test/hdfs/path',
            use_distcp=False  # Disable distcp for basic HDFS tests
        )
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)
    
    @patch('impala_transfer.transfer.subprocess.run')
    def test_transfer_via_hdfs_put_success(self, mock_run):
        """Test successful HDFS put transfer."""
        mock_run.return_value.returncode = 0
        
        result = self.transfer_manager.transfer_files(['test_file.parquet'], 'test_table')
        
        self.assertTrue(result)
        mock_run.assert_called()
    
    @patch('impala_transfer.transfer.subprocess.run')
    def test_transfer_via_hdfs_put_path_creation_failure(self, mock_run):
        """Test HDFS put transfer with path creation failure."""
        mock_run.side_effect = [
            Mock(returncode=1),  # mkdir fails
            Mock(returncode=0)   # put succeeds
        ]
        
        result = self.transfer_manager.transfer_files(['test_file.parquet'], 'test_table')
        
        self.assertFalse(result)
    
    @patch('impala_transfer.transfer.subprocess.run')
    def test_transfer_via_hdfs_cp_success(self, mock_run):
        """Test successful HDFS cp transfer."""
        # First call fails (hdfs put), second call succeeds (hdfs cp)
        mock_run.side_effect = [
            Mock(returncode=1),  # hdfs put fails
            Mock(returncode=0),  # mkdir succeeds
            Mock(returncode=0)   # hdfs cp succeeds
        ]
        
        result = self.transfer_manager.transfer_files(['test_file.parquet'], 'test_table')
        
        self.assertTrue(result)
        mock_run.assert_called()
    
    @patch('impala_transfer.transfer.subprocess.run')
    def test_transfer_via_scp_success(self, mock_run):
        """Test successful SCP transfer."""
        # Set target_hdfs_path to None to trigger SCP transfer
        self.transfer_manager.target_hdfs_path = None
        self.transfer_manager.use_distcp = False  # Ensure distcp is disabled
        self.transfer_manager.scp_target_host = 'test-host'
        self.transfer_manager.scp_target_path = '/tmp/test'
        mock_run.return_value.returncode = 0
        
        result = self.transfer_manager.transfer_files(['test_file.parquet'], 'test_table')
        
        self.assertTrue(result)
        mock_run.assert_called()
    
    @patch('impala_transfer.transfer.subprocess.run')
    def test_transfer_via_scp_failure(self, mock_run):
        """Test failed SCP transfer."""
        # Set target_hdfs_path to None to trigger SCP transfer
        self.transfer_manager.target_hdfs_path = None
        self.transfer_manager.use_distcp = False  # Ensure distcp is disabled
        self.transfer_manager.scp_target_host = 'test-host'
        self.transfer_manager.scp_target_path = '/tmp/test'
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = "SCP failed"
        
        result = self.transfer_manager.transfer_files(['test_file.parquet'], 'test_table')
        
        self.assertFalse(result)
    
    @patch('impala_transfer.transfer.subprocess.run')
    def test_transfer_via_distcp_success(self, mock_run):
        """Test successful distcp transfer."""
        # Configure distcp transfer
        self.transfer_manager.use_distcp = True
        self.transfer_manager.source_hdfs_path = '/source/path'
        self.transfer_manager.target_cluster = 'cluster2.example.com'
        mock_run.return_value.returncode = 0
        
        result = self.transfer_manager.transfer_files(['test_file.parquet'], 'test_table')
        
        self.assertTrue(result)
        mock_run.assert_called()
    
    @patch('impala_transfer.transfer.subprocess.run')
    def test_transfer_via_distcp_failure(self, mock_run):
        """Test distcp transfer failure."""
        # Configure distcp transfer
        self.transfer_manager.use_distcp = True
        self.transfer_manager.source_hdfs_path = '/source/path'
        self.transfer_manager.target_cluster = 'cluster2.example.com'
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = "Distcp failed"
        
        result = self.transfer_manager.transfer_files(['test_file.parquet'], 'test_table')
        
        self.assertFalse(result)
    
    def test_get_transfer_info_hdfs_put(self):
        """Test get_transfer_info for HDFS put transfer."""
        info = self.transfer_manager.get_transfer_info()
        
        self.assertEqual(info['primary_transfer_method'], 'hdfs_put')
        self.assertEqual(info['target_hdfs_path'], '/test/hdfs/path')
        self.assertFalse(info['use_distcp'])
        self.assertIn('hdfs_put', info['available_methods'])
        self.assertIn('hdfs_cp', info['available_methods'])
    
    def test_get_transfer_info_distcp(self):
        """Test get_transfer_info for distcp transfer."""
        self.transfer_manager.use_distcp = True
        self.transfer_manager.source_hdfs_path = '/source/path'
        self.transfer_manager.target_cluster = 'cluster2.example.com'
        
        info = self.transfer_manager.get_transfer_info()
        
        self.assertEqual(info['primary_transfer_method'], 'distcp')
        self.assertTrue(info['use_distcp'])
        self.assertEqual(info['source_hdfs_path'], '/source/path')
        self.assertEqual(info['target_cluster'], 'cluster2.example.com')
        self.assertIn('distcp', info['available_methods'])
    
    def test_get_transfer_info_scp(self):
        """Test get_transfer_info for SCP transfer."""
        self.transfer_manager.target_hdfs_path = None
        self.transfer_manager.use_distcp = False  # Ensure distcp is disabled
        
        info = self.transfer_manager.get_transfer_info()
        
        self.assertEqual(info['primary_transfer_method'], 'scp')
        self.assertFalse(info['use_distcp'])
        self.assertIn('scp', info['available_methods'])
    
    def test_validate_transfer_config_hdfs_put(self):
        """Test validate_transfer_config for HDFS put transfer."""
        result = self.transfer_manager.validate_transfer_config()
        self.assertTrue(result)
    
    def test_validate_transfer_config_hdfs_put_invalid_path(self):
        """Test validate_transfer_config with invalid HDFS path."""
        self.transfer_manager.target_hdfs_path = 'invalid/path'
        result = self.transfer_manager.validate_transfer_config()
        self.assertFalse(result)
    
    def test_validate_transfer_config_distcp_valid(self):
        """Test validate_transfer_config for valid distcp configuration."""
        self.transfer_manager.use_distcp = True
        self.transfer_manager.source_hdfs_path = '/source/path'
        self.transfer_manager.target_cluster = 'cluster2.example.com'
        
        result = self.transfer_manager.validate_transfer_config()
        self.assertTrue(result)
    
    def test_validate_transfer_config_distcp_missing_source(self):
        """Test validate_transfer_config for distcp with missing source path."""
        self.transfer_manager.use_distcp = True
        self.transfer_manager.target_cluster = 'cluster2.example.com'
        # Missing source_hdfs_path
        
        result = self.transfer_manager.validate_transfer_config()
        self.assertFalse(result)
    
    def test_validate_transfer_config_distcp_missing_target(self):
        """Test validate_transfer_config for distcp with missing target cluster."""
        self.transfer_manager.use_distcp = True
        self.transfer_manager.source_hdfs_path = '/source/path'
        # Missing target_cluster
        
        result = self.transfer_manager.validate_transfer_config()
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main() 