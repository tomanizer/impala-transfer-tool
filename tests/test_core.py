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


if __name__ == '__main__':
    unittest.main() 