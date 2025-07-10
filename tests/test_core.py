#!/usr/bin/env python3
"""
Test suite for the core module.
"""

import unittest
from unittest.mock import Mock, patch

from impala_transfer.core import ImpalaTransferTool


class TestImpalaTransferTool(unittest.TestCase):
    """Test the ImpalaTransferTool class."""
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    @patch('impala_transfer.connection.PYODBC_AVAILABLE', False)
    @patch('impala_transfer.connection.SQLALCHEMY_AVAILABLE', False)
    def test_init_auto_connection_type(self):
        """Test initialization with auto connection type selection."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            source_port=21050,
            source_database='test_db'
        )
        
        self.assertEqual(tool.connection_type, 'impyla')
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', False)
    @patch('impala_transfer.connection.PYODBC_AVAILABLE', True)
    @patch('impala_transfer.connection.SQLALCHEMY_AVAILABLE', False)
    def test_init_auto_connection_type_pyodbc(self):
        """Test initialization with pyodbc as fallback."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            source_port=21050,
            source_database='test_db'
        )
        
        self.assertEqual(tool.connection_type, 'pyodbc')
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', False)
    @patch('impala_transfer.connection.PYODBC_AVAILABLE', False)
    @patch('impala_transfer.connection.SQLALCHEMY_AVAILABLE', True)
    def test_init_auto_connection_type_sqlalchemy(self):
        """Test initialization with SQLAlchemy as fallback."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            source_port=21050,
            source_database='test_db'
        )
        
        self.assertEqual(tool.connection_type, 'sqlalchemy')
    
    def test_init_invalid_connection_type(self):
        """Test initialization with invalid connection type."""
        with self.assertRaises(ValueError):
            ImpalaTransferTool(
                source_host='test-host',
                connection_type='invalid_type'
            )

    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', False)
    @patch('impala_transfer.connection.PYODBC_AVAILABLE', False)
    @patch('impala_transfer.connection.SQLALCHEMY_AVAILABLE', False)
    def test_init_no_available_connection_types(self):
        """Test initialization when no connection types are available."""
        with self.assertRaises(ValueError):
            ImpalaTransferTool(
                source_host='test-host',
                connection_type='auto'
            )

    def test_init_sqlalchemy_without_url(self):
        """Test initialization with SQLAlchemy connection type but no URL."""
        with self.assertRaises(ValueError):
            ImpalaTransferTool(
                source_host='test-host',
                connection_type='sqlalchemy'
                # No sqlalchemy_url provided
            )

    def test_init_sqlalchemy_with_url(self):
        """Test initialization with SQLAlchemy connection type and URL."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='sqlalchemy',
            sqlalchemy_url='impala://test-host:21050/default'
        )
        
        self.assertEqual(tool.connection_type, 'sqlalchemy')

    @patch('impala_transfer.connection.PYODBC_AVAILABLE', True)
    def test_init_pyodbc_with_driver(self):
        """Test initialization with pyodbc connection type and driver."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='pyodbc',
            odbc_driver='Cloudera ODBC Driver for Impala'
        )
        
        self.assertEqual(tool.connection_type, 'pyodbc')

    @patch('impala_transfer.connection.PYODBC_AVAILABLE', True)
    def test_init_pyodbc_with_connection_string(self):
        """Test initialization with pyodbc connection type and connection string."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='pyodbc',
            odbc_connection_string='DRIVER={Cloudera ODBC Driver for Impala};HOST=test-host;PORT=21050;DATABASE=default'
        )
        
        self.assertEqual(tool.connection_type, 'pyodbc')
    
    def test_transfer_table(self):
        """Test table transfer method."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        tool.orchestrator = Mock()
        tool.orchestrator.transfer_query.return_value = True
        
        result = tool.transfer_table('test_table')
        
        self.assertTrue(result)
        tool.orchestrator.transfer_query.assert_called_once_with('SELECT * FROM test_table', None, 'parquet')

    def test_transfer_table_with_custom_target(self):
        """Test table transfer method with custom target table."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        tool.orchestrator = Mock()
        tool.orchestrator.transfer_query.return_value = True
        
        result = tool.transfer_table('source_table', 'target_table')
        
        self.assertTrue(result)
        tool.orchestrator.transfer_query.assert_called_once_with('SELECT * FROM source_table', 'target_table', 'parquet')

    def test_transfer_table_with_custom_format(self):
        """Test table transfer method with custom output format."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        tool.orchestrator = Mock()
        tool.orchestrator.transfer_query.return_value = True
        
        result = tool.transfer_table('test_table', output_format='csv')
        
        self.assertTrue(result)
        tool.orchestrator.transfer_query.assert_called_once_with('SELECT * FROM test_table', None, 'csv')

    def test_transfer_query(self):
        """Test query transfer method."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        tool.orchestrator = Mock()
        tool.orchestrator.transfer_query.return_value = True
        
        result = tool.transfer_query('SELECT * FROM test_table WHERE id > 100')
        
        self.assertTrue(result)
        tool.orchestrator.transfer_query.assert_called_once_with('SELECT * FROM test_table WHERE id > 100', None, 'parquet')

    def test_transfer_query_with_custom_target(self):
        """Test query transfer method with custom target table."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        tool.orchestrator = Mock()
        tool.orchestrator.transfer_query.return_value = True
        
        result = tool.transfer_query('SELECT * FROM test_table', 'custom_target')
        
        self.assertTrue(result)
        tool.orchestrator.transfer_query.assert_called_once_with('SELECT * FROM test_table', 'custom_target', 'parquet')

    def test_transfer_query_with_custom_format(self):
        """Test query transfer method with custom output format."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        tool.orchestrator = Mock()
        tool.orchestrator.transfer_query.return_value = True
        
        result = tool.transfer_query('SELECT * FROM test_table', output_format='csv')
        
        self.assertTrue(result)
        tool.orchestrator.transfer_query.assert_called_once_with('SELECT * FROM test_table', None, 'csv')

    def test_transfer_query_with_progress(self):
        """Test query transfer with progress method."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        tool.orchestrator = Mock()
        tool.orchestrator.transfer_query_with_progress.return_value = True
        
        progress_callback = Mock()
        result = tool.transfer_query_with_progress(
            'SELECT * FROM test_table', 
            'custom_target', 
            'csv', 
            progress_callback
        )
        
        self.assertTrue(result)
        tool.orchestrator.transfer_query_with_progress.assert_called_once_with(
            'SELECT * FROM test_table', 'custom_target', 'csv', progress_callback
        )

    def test_get_configuration(self):
        """Test get_configuration method."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        
        # Mock the components
        tool.connection_manager = Mock()
        tool.connection_manager.get_connection_info.return_value = {'host': 'test-host'}
        
        tool.file_transfer_manager = Mock()
        tool.file_transfer_manager.get_transfer_info.return_value = {'files_transferred': 5}
        
        with patch('impala_transfer.core.get_available_connection_types', return_value=['impyla', 'pyodbc']):
            config = tool.get_configuration()
        
        self.assertEqual(config['connection_type'], 'impyla')
        self.assertEqual(config['connection_info'], {'host': 'test-host'})
        self.assertEqual(config['chunk_size'], 1000000)
        self.assertEqual(config['max_workers'], 4)
        self.assertEqual(config['temp_dir'], '/tmp/impala_transfer')
        self.assertEqual(config['transfer_info'], {'files_transferred': 5})
        self.assertEqual(config['available_connection_types'], ['impyla', 'pyodbc'])

    def test_test_connection_success(self):
        """Test test_connection method with successful connection."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        
        # Mock the components
        tool.connection_manager = Mock()
        tool.connection_manager.connect.return_value = True
        tool.connection_manager.close = Mock()
        
        tool.orchestrator = Mock()
        tool.orchestrator.query_executor = Mock()
        tool.orchestrator.query_executor.test_connection.return_value = True
        
        result = tool.test_connection()
        
        self.assertTrue(result)
        tool.connection_manager.connect.assert_called_once()
        tool.connection_manager.close.assert_called_once()
        tool.orchestrator.query_executor.test_connection.assert_called_once()

    def test_test_connection_connect_failure(self):
        """Test test_connection method with connection failure."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        
        # Mock the components
        tool.connection_manager = Mock()
        tool.connection_manager.connect.return_value = False
        tool.connection_manager.close = Mock()
        
        result = tool.test_connection()
        
        self.assertFalse(result)
        tool.connection_manager.connect.assert_called_once()
        tool.connection_manager.close.assert_called_once()

    def test_test_connection_query_test_failure(self):
        """Test test_connection method with query test failure."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        
        # Mock the components
        tool.connection_manager = Mock()
        tool.connection_manager.connect.return_value = True
        tool.connection_manager.close = Mock()
        
        tool.orchestrator = Mock()
        tool.orchestrator.query_executor = Mock()
        tool.orchestrator.query_executor.test_connection.return_value = False
        
        result = tool.test_connection()
        
        self.assertFalse(result)
        tool.connection_manager.connect.assert_called_once()
        tool.connection_manager.close.assert_called_once()
        tool.orchestrator.query_executor.test_connection.assert_called_once()

    def test_validate_configuration_success(self):
        """Test validate_configuration method with valid configuration."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        
        # Mock the components
        tool.chunk_processor = Mock()
        tool.chunk_processor.chunk_size = 1000000
        
        tool.file_transfer_manager = Mock()
        tool.file_transfer_manager.validate_transfer_config.return_value = True
        
        with patch('impala_transfer.core.validate_connection_type', return_value=True):
            result = tool.validate_configuration()
        
        self.assertTrue(result)

    def test_validate_configuration_invalid_connection_type(self):
        """Test validate_configuration method with invalid connection type."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        
        with patch('impala_transfer.core.validate_connection_type', return_value=False):
            result = tool.validate_configuration()
        
        self.assertFalse(result)

    def test_validate_configuration_sqlalchemy_missing_url(self):
        """Test validate_configuration method with SQLAlchemy but missing URL."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='sqlalchemy',
            sqlalchemy_url='impala://test-host:21050/default'
        )
        
        # Mock connection manager to not have sqlalchemy_url in kwargs
        tool.connection_manager = Mock()
        tool.connection_manager.kwargs = {}
        
        tool.chunk_processor = Mock()
        tool.chunk_processor.chunk_size = 1000000
        
        tool.file_transfer_manager = Mock()
        tool.file_transfer_manager.validate_transfer_config.return_value = True
        
        with patch('impala_transfer.core.validate_connection_type', return_value=True):
            result = tool.validate_configuration()
        
        self.assertFalse(result)

    def test_validate_configuration_invalid_chunk_size(self):
        """Test validate_configuration method with invalid chunk size."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        
        # Mock the components
        tool.chunk_processor = Mock()
        tool.chunk_processor.chunk_size = 0  # Invalid chunk size
        
        tool.file_transfer_manager = Mock()
        tool.file_transfer_manager.validate_transfer_config.return_value = True
        
        with patch('impala_transfer.core.validate_connection_type', return_value=True):
            result = tool.validate_configuration()
        
        self.assertFalse(result)

    def test_validate_configuration_invalid_transfer_config(self):
        """Test validate_configuration method with invalid transfer config."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        
        # Mock the components
        tool.chunk_processor = Mock()
        tool.chunk_processor.chunk_size = 1000000
        
        tool.file_transfer_manager = Mock()
        tool.file_transfer_manager.validate_transfer_config.return_value = False
        
        with patch('impala_transfer.core.validate_connection_type', return_value=True):
            result = tool.validate_configuration()
        
        self.assertFalse(result)

    def test_validate_configuration_exception(self):
        """Test validate_configuration method with exception."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        
        # Mock the components to raise an exception
        tool.chunk_processor = Mock()
        tool.chunk_processor.chunk_size = 1000000
        
        # Mock validate_connection_type to raise an exception
        with patch('impala_transfer.core.validate_connection_type', side_effect=Exception("Test exception")):
            with self.assertLogs('root', level='ERROR') as cm:
                result = tool.validate_configuration()
        
        self.assertFalse(result)
        self.assertTrue(any('Configuration validation failed' in msg for msg in cm.output))


if __name__ == '__main__':
    unittest.main() 