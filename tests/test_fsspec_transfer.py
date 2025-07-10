"""
Tests for fsspec-based file transfer functionality.
"""

import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from impala_transfer.transfer import (
    FSSpecFileTransferManager, 
    UnifiedFileTransferManager,
    FSSPEC_AVAILABLE
)


class TestFSSpecFileTransferManager:
    """Test FSSpecFileTransferManager functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_files = []
        
        # Create test files
        for i in range(3):
            filepath = os.path.join(self.temp_dir, f"test_file_{i}.txt")
            with open(filepath, 'w') as f:
                f.write(f"Test content {i}")
            self.test_files.append(filepath)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        for filepath in self.test_files:
            if os.path.exists(filepath):
                os.remove(filepath)
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_init_with_fsspec_available(self):
        """Test initialization when fsspec is available."""
        config = {'protocol': 'file'}
        manager = FSSpecFileTransferManager(target_fs_config=config)
        assert manager.target_fs is not None
        assert manager.source_fs is None
    
    @pytest.mark.skipif(FSSPEC_AVAILABLE, reason="fsspec is available")
    def test_init_without_fsspec(self):
        """Test initialization when fsspec is not available."""
        with pytest.raises(ImportError, match="fsspec is required"):
            FSSpecFileTransferManager()
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_transfer_files_success(self):
        """Test successful file transfer."""
        config = {'protocol': 'file', 'target_path': self.temp_dir}
        manager = FSSpecFileTransferManager(target_fs_config=config)
        
        result = manager.transfer_files(self.test_files, "test_table")
        assert result is True
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_transfer_files_no_target_fs(self):
        """Test transfer when target filesystem is not configured."""
        manager = FSSpecFileTransferManager()
        result = manager.transfer_files(self.test_files, "test_table")
        assert result is False
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_ensure_target_path_exists(self):
        """Test target path creation."""
        config = {'protocol': 'file'}
        manager = FSSpecFileTransferManager(target_fs_config=config)
        
        new_path = "/tmp/test_fsspec_path"
        result = manager._ensure_target_path_exists(new_path)
        assert result is True
        
        # Clean up
        if os.path.exists(new_path):
            os.rmdir(new_path)
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_transfer_single_file(self):
        """Test single file transfer."""
        config = {'protocol': 'file', 'target_path': self.temp_dir}
        manager = FSSpecFileTransferManager(target_fs_config=config)
        
        result = manager._transfer_single_file(self.test_files[0], self.temp_dir)
        assert result is True
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_list_files(self):
        """Test listing files in filesystem."""
        config = {'protocol': 'file', 'target_path': self.temp_dir}
        manager = FSSpecFileTransferManager(target_fs_config=config)
        
        files = manager.list_files(self.temp_dir)
        assert isinstance(files, list)
        assert len(files) >= 3  # Should include our test files
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_get_file_info(self):
        """Test getting file information."""
        config = {'protocol': 'file'}
        manager = FSSpecFileTransferManager(target_fs_config=config)
        
        info = manager.get_file_info(self.test_files[0])
        assert isinstance(info, dict)
        assert 'size' in info
        assert 'type' in info
        assert 'path' in info
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_validate_config(self):
        """Test configuration validation."""
        config = {'protocol': 'file'}
        manager = FSSpecFileTransferManager(target_fs_config=config)
        
        result = manager.validate_config()
        assert result is True
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_get_transfer_info(self):
        """Test getting transfer information."""
        config = {'protocol': 'file', 'target_path': '/test/path'}
        manager = FSSpecFileTransferManager(target_fs_config=config)
        
        info = manager.get_transfer_info()
        assert info['transfer_method'] == 'fsspec'
        assert info['target_protocol'] == 'file'
        assert info['target_fs_configured'] is True


class TestUnifiedFileTransferManager:
    """Test UnifiedFileTransferManager functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_files = []
        
        # Create test files
        for i in range(2):
            filepath = os.path.join(self.temp_dir, f"test_file_{i}.txt")
            with open(filepath, 'w') as f:
                f.write(f"Test content {i}")
            self.test_files.append(filepath)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        for filepath in self.test_files:
            if os.path.exists(filepath):
                os.remove(filepath)
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)
    
    def test_init_traditional_mode(self):
        """Test initialization in traditional mode."""
        manager = UnifiedFileTransferManager(
            use_fsspec=False,
            target_hdfs_path="/test/path"
        )
        assert manager.use_fsspec is False
        assert manager.traditional_manager is not None
        assert manager.fsspec_manager is None
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_init_fsspec_mode(self):
        """Test initialization in fsspec mode."""
        fsspec_config = {
            'target_fs_config': {'protocol': 'file', 'target_path': self.temp_dir}
        }
        manager = UnifiedFileTransferManager(
            use_fsspec=True,
            fsspec_config=fsspec_config
        )
        assert manager.use_fsspec is True
        assert manager.fsspec_manager is not None
        assert manager.traditional_manager is None
    
    def test_transfer_files_traditional_mode(self):
        """Test file transfer in traditional mode."""
        manager = UnifiedFileTransferManager(
            use_fsspec=False,
            target_hdfs_path="/test/path"
        )
        
        # Mock the traditional manager's transfer_files method
        manager.traditional_manager.transfer_files = Mock(return_value=True)
        
        result = manager.transfer_files(self.test_files, "test_table")
        assert result is True
        manager.traditional_manager.transfer_files.assert_called_once_with(
            self.test_files, "test_table"
        )
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_transfer_files_fsspec_mode(self):
        """Test file transfer in fsspec mode."""
        fsspec_config = {
            'target_fs_config': {'protocol': 'file', 'target_path': self.temp_dir}
        }
        manager = UnifiedFileTransferManager(
            use_fsspec=True,
            fsspec_config=fsspec_config
        )
        
        result = manager.transfer_files(self.test_files, "test_table")
        assert result is True
    
    def test_validate_config_traditional_mode(self):
        """Test configuration validation in traditional mode."""
        manager = UnifiedFileTransferManager(
            use_fsspec=False,
            target_hdfs_path="/test/path"
        )
        
        # Mock the traditional manager's validate_transfer_config method
        manager.traditional_manager.validate_transfer_config = Mock(return_value=True)
        
        result = manager.validate_config()
        assert result is True
        manager.traditional_manager.validate_transfer_config.assert_called_once()
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_validate_config_fsspec_mode(self):
        """Test configuration validation in fsspec mode."""
        fsspec_config = {
            'target_fs_config': {'protocol': 'file', 'target_path': self.temp_dir}
        }
        manager = UnifiedFileTransferManager(
            use_fsspec=True,
            fsspec_config=fsspec_config
        )
        
        result = manager.validate_config()
        assert result is True
    
    def test_get_transfer_info_traditional_mode(self):
        """Test getting transfer info in traditional mode."""
        manager = UnifiedFileTransferManager(
            use_fsspec=False,
            target_hdfs_path="/test/path"
        )
        
        # Mock the traditional manager's get_transfer_info method
        mock_info = {'method': 'traditional'}
        manager.traditional_manager.get_transfer_info = Mock(return_value=mock_info)
        
        info = manager.get_transfer_info()
        assert info == mock_info
        manager.traditional_manager.get_transfer_info.assert_called_once()
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_get_transfer_info_fsspec_mode(self):
        """Test getting transfer info in fsspec mode."""
        fsspec_config = {
            'target_fs_config': {'protocol': 'file', 'target_path': self.temp_dir}
        }
        manager = UnifiedFileTransferManager(
            use_fsspec=True,
            fsspec_config=fsspec_config
        )
        
        info = manager.get_transfer_info()
        assert info['transfer_method'] == 'fsspec'
    
    def test_create_hdfs_config(self):
        """Test HDFS configuration creation."""
        config = UnifiedFileTransferManager.create_hdfs_config(
            host="namenode.example.com",
            port=8020,
            user="hdfs"
        )
        
        assert config['protocol'] == 'hdfs'
        assert config['host'] == 'namenode.example.com'
        assert config['port'] == 8020
        assert config['user'] == 'hdfs'
    
    def test_create_s3_config(self):
        """Test S3 configuration creation."""
        config = UnifiedFileTransferManager.create_s3_config(
            bucket="my-bucket",
            access_key="access123",
            secret_key="secret456",
            endpoint_url="https://s3.example.com"
        )
        
        assert config['protocol'] == 's3'
        assert config['bucket'] == 'my-bucket'
        assert config['key'] == 'access123'
        assert config['secret'] == 'secret456'
        assert config['endpoint_url'] == 'https://s3.example.com'
    
    def test_create_gcs_config(self):
        """Test GCS configuration creation."""
        config = UnifiedFileTransferManager.create_gcs_config(
            bucket="my-gcs-bucket",
            project="my-project",
            credentials_file="/path/to/credentials.json"
        )
        
        assert config['protocol'] == 'gcs'
        assert config['bucket'] == 'my-gcs-bucket'
        assert config['project'] == 'my-project'
        assert config['token'] == '/path/to/credentials.json'
    
    def test_create_azure_config(self):
        """Test Azure configuration creation."""
        config = UnifiedFileTransferManager.create_azure_config(
            account_name="myaccount",
            account_key="account-key-123",
            container="my-container"
        )
        
        assert config['protocol'] == 'abfs'
        assert config['account_name'] == 'myaccount'
        assert config['account_key'] == 'account-key-123'
        assert config['container'] == 'my-container'


class TestFSSpecIntegration:
    """Integration tests for fsspec functionality."""
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_local_to_local_transfer(self):
        """Test local to local file transfer using fsspec."""
        with tempfile.TemporaryDirectory() as source_dir, \
             tempfile.TemporaryDirectory() as target_dir:
            
            # Create source file
            source_file = os.path.join(source_dir, "test.txt")
            with open(source_file, 'w') as f:
                f.write("Test content")
            
            # Configure fsspec manager
            source_config = {'protocol': 'file'}
            target_config = {'protocol': 'file', 'target_path': target_dir}
            
            manager = FSSpecFileTransferManager(
                source_fs_config=source_config,
                target_fs_config=target_config
            )
            
            # Transfer file
            result = manager.transfer_files([source_file], "test_table")
            assert result is True
            
            # Verify file was transferred
            target_file = os.path.join(target_dir, "test.txt")
            assert os.path.exists(target_file)
            
            with open(target_file, 'r') as f:
                content = f.read()
            assert content == "Test content"
    
    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
    def test_memory_filesystem_transfer(self):
        """Test transfer using memory filesystem (for testing)."""
        source_config = {'protocol': 'memory'}
        target_config = {'protocol': 'memory', 'target_path': '/test'}
        
        manager = FSSpecFileTransferManager(
            source_fs_config=source_config,
            target_fs_config=target_config
        )
        
        # Create test content in memory
        test_content = "Test content for memory filesystem"
        manager.source_fs.write_text('/test_file.txt', test_content)
        
        # Transfer file
        result = manager.transfer_files(['/test_file.txt'], "test_table")
        assert result is True
        
        # Verify content was transferred
        transferred_content = manager.target_fs.read_text('/test/test_file.txt')
        assert transferred_content == test_content 