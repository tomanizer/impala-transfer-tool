"""
Tests for FSSpec filesystem object functionality.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch

try:
    import fsspec
    FSSPEC_AVAILABLE = True
except ImportError:
    FSSPEC_AVAILABLE = False

from impala_transfer.transfer import FSSpecFileTransferManager, UnifiedFileTransferManager


@pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
class TestFSSpecFilesystemObjects:
    """Test FSSpec filesystem object functionality."""
    
    def test_init_with_filesystem_objects(self):
        """Test initialization with filesystem objects."""
        # Create mock filesystem objects
        source_fs = Mock()
        source_fs.protocol = 'file'
        source_fs.open = Mock()
        
        target_fs = Mock()
        target_fs.protocol = 's3'
        target_fs.open = Mock()
        target_fs.exists = Mock(return_value=True)
        target_fs.makedirs = Mock()
        
        # Initialize with filesystem objects
        manager = FSSpecFileTransferManager(
            source_fs=source_fs,
            target_fs=target_fs,
            transfer_options={'chunk_size': 1024}
        )
        
        assert manager.source_fs == source_fs
        assert manager.target_fs == target_fs
        assert manager.transfer_options['chunk_size'] == 1024
    
    def test_init_with_mixed_configuration(self):
        """Test initialization with filesystem object and config."""
        # Create source filesystem object
        source_fs = Mock()
        source_fs.protocol = 'hdfs'
        source_fs.open = Mock()
        
        # Use config for target
        target_config = {
            'protocol': 's3',
            'bucket': 'test-bucket',
            'key': 'test-key',
            'secret': 'test-secret'
        }
        
        with patch('fsspec.filesystem') as mock_fsspec:
            mock_target_fs = Mock()
            mock_target_fs.protocol = 's3'
            mock_target_fs.open = Mock()
            mock_target_fs.exists = Mock(return_value=True)
            mock_target_fs.makedirs = Mock()
            mock_fsspec.return_value = mock_target_fs
            
            manager = FSSpecFileTransferManager(
                source_fs=source_fs,
                target_fs_config=target_config
            )
            
            assert manager.source_fs == source_fs
            assert manager.target_fs == mock_target_fs
            mock_fsspec.assert_called_once_with('s3', bucket='test-bucket', key='test-key', secret='test-secret')
    
    def test_transfer_files_with_explicit_target_path(self):
        """Test transfer_files with explicit target path."""
        # Create mock filesystems
        source_fs = Mock()
        source_fs.protocol = 'file'
        source_fs.open = Mock()
        
        target_fs = Mock()
        target_fs.protocol = 's3'
        target_fs.exists = Mock(return_value=True)
        target_fs.makedirs = Mock()
        target_fs.open = Mock()
        
        manager = FSSpecFileTransferManager(
            source_fs=source_fs,
            target_fs=target_fs
        )
        
        # Mock file transfer
        with patch.object(manager, '_transfer_single_file', return_value=True):
            success = manager.transfer_files(
                filepaths=['/test/file.csv'],
                target_table='test_table',
                target_path='/custom/target/path/'
            )
            
            assert success is True
    
    def test_get_transfer_info_with_filesystem_objects(self):
        """Test get_transfer_info with filesystem objects."""
        source_fs = Mock()
        source_fs.protocol = 'file'
        
        target_fs = Mock()
        target_fs.protocol = 's3'
        
        manager = FSSpecFileTransferManager(
            source_fs=source_fs,
            target_fs=target_fs
        )
        
        info = manager.get_transfer_info()
        
        assert info['transfer_method'] == 'fsspec'
        assert info['source_protocol'] == 'file'
        assert info['target_protocol'] == 's3'
        assert info['source_fs_configured'] is True
        assert info['target_fs_configured'] is True
        assert info['source_fs_type'] == 'Mock'
        assert info['target_fs_type'] == 'Mock'
    
    def test_validate_config_with_filesystem_objects(self):
        """Test validate_config with filesystem objects."""
        source_fs = Mock()
        source_fs.ls = Mock(return_value=['/'])
        
        target_fs = Mock()
        target_fs.ls = Mock(return_value=['/'])
        
        manager = FSSpecFileTransferManager(
            source_fs=source_fs,
            target_fs=target_fs
        )
        
        assert manager.validate_config() is True
    
    def test_unified_manager_with_filesystem_objects(self):
        """Test UnifiedFileTransferManager with filesystem objects."""
        source_fs = Mock()
        source_fs.protocol = 'file'
        source_fs.open = Mock()
        
        target_fs = Mock()
        target_fs.protocol = 's3'
        target_fs.open = Mock()
        target_fs.exists = Mock(return_value=True)
        target_fs.makedirs = Mock()
        
        unified_manager = UnifiedFileTransferManager(
            use_fsspec=True,
            source_fs=source_fs,
            target_fs=target_fs,
            fsspec_config={'transfer_options': {'chunk_size': 512}}
        )
        
        assert unified_manager.use_fsspec is True
        assert unified_manager.fsspec_manager is not None
        assert unified_manager.fsspec_manager.source_fs == source_fs
        assert unified_manager.fsspec_manager.target_fs == target_fs
    
    def test_create_filesystem_with_filesystem_object(self):
        """Test _create_filesystem with a filesystem object."""
        # Create a filesystem object
        fs_obj = Mock()
        fs_obj.protocol = 'test'
        fs_obj.open = Mock()
        
        manager = FSSpecFileTransferManager()
        
        # Should return the filesystem object as-is
        result = manager._create_filesystem(fs_obj)
        assert result == fs_obj
    
    def test_create_filesystem_with_config(self):
        """Test _create_filesystem with configuration dictionary."""
        config = {
            'protocol': 'file',
            'target_path': '/test/path'
        }
        
        with patch('fsspec.filesystem') as mock_fsspec:
            mock_fs = Mock()
            mock_fsspec.return_value = mock_fs
            
            manager = FSSpecFileTransferManager()
            result = manager._create_filesystem(config)
            
            assert result == mock_fs
            mock_fsspec.assert_called_once_with('file', target_path='/test/path')
    
    def test_list_files_with_filesystem_objects(self):
        """Test list_files with filesystem objects."""
        target_fs = Mock()
        target_fs.ls = Mock(return_value=['/file1.txt', '/file2.txt'])
        
        manager = FSSpecFileTransferManager(target_fs=target_fs)
        
        files = manager.list_files('/test/path')
        
        assert files == ['/file1.txt', '/file2.txt']
        target_fs.ls.assert_called_once_with('/test/path')
    
    def test_get_file_info_with_filesystem_objects(self):
        """Test get_file_info with filesystem objects."""
        target_fs = Mock()
        target_fs.info = Mock(return_value={
            'size': 1024,
            'type': 'file',
            'mtime': 1234567890,
            'path': '/test/file.txt'
        })
        
        manager = FSSpecFileTransferManager(target_fs=target_fs)
        
        info = manager.get_file_info('/test/file.txt')
        
        assert info['size'] == 1024
        assert info['type'] == 'file'
        assert info['modified'] == 1234567890
        assert info['path'] == '/test/file.txt'
        target_fs.info.assert_called_once_with('/test/file.txt')


@pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not available")
class TestFSSpecFilesystemObjectsIntegration:
    """Integration tests for FSSpec filesystem objects."""
    
    def test_real_filesystem_transfer(self, tmp_path):
        """Test transfer with real local filesystem."""
        # Create test files
        source_file = tmp_path / "test_source.txt"
        source_file.write_text("test content")
        
        # Create filesystem objects
        source_fs = fsspec.filesystem('file')
        target_fs = fsspec.filesystem('file')
        
        manager = FSSpecFileTransferManager(
            source_fs=source_fs,
            target_fs=target_fs
        )
        
        # Test transfer
        with patch.object(manager, '_transfer_single_file', return_value=True):
            success = manager.transfer_files(
                filepaths=[str(source_file)],
                target_table='test_table',
                target_path=str(tmp_path / "target") + "/"
            )
            
            assert success is True
    
    def test_filesystem_validation(self):
        """Test filesystem validation with real filesystem."""
        fs = fsspec.filesystem('file')
        
        manager = FSSpecFileTransferManager(target_fs=fs)
        
        # Should pass validation
        assert manager.validate_config() is True 