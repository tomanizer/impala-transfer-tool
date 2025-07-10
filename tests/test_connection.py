#!/usr/bin/env python3
"""
Test suite for the connection module.
"""

import unittest
from unittest.mock import Mock, patch
import pytest

try:
    import pyodbc
    PYODBC_INSTALLED = True
except ImportError:
    PYODBC_INSTALLED = False

from impala_transfer.connection import ConnectionManager, get_available_connection_types, validate_connection_type


class TestConnectionManager(unittest.TestCase):
    """Test the ConnectionManager class."""
    
    def setUp(self):
        self.connection_kwargs = {
            'source_host': 'test-host',
            'source_port': 21050,
            'source_database': 'test_db'
        }
    
    @patch('impala_transfer.connection.impala.dbapi.connect')
    def test_connect_impyla_success(self, mock_connect):
        """Test successful Impyla connection."""
        manager = ConnectionManager('impyla', **self.connection_kwargs)
        
        result = manager.connect()
        
        self.assertTrue(result)
        mock_connect.assert_called_once_with(
            host='test-host',
            port=21050,
            database='test_db',
            auth_mechanism='PLAIN'
        )
    
    @patch('impala_transfer.connection.impala.dbapi.connect')
    def test_connect_impyla_failure(self, mock_connect):
        """Test failed Impyla connection."""
        mock_connect.side_effect = Exception("Connection failed")
        manager = ConnectionManager('impyla', **self.connection_kwargs)
        
        result = manager.connect()
        
        self.assertFalse(result)
    
    @pytest.mark.skipif(not PYODBC_INSTALLED, reason="pyodbc or unixODBC not installed")
    @patch('pyodbc.connect')
    def test_connect_pyodbc_success(self, mock_connect):
        """Test successful pyodbc connection."""
        kwargs = self.connection_kwargs.copy()
        kwargs['odbc_driver'] = 'Test Driver'
        manager = ConnectionManager('pyodbc', **kwargs)
        
        result = manager.connect()
        
        self.assertTrue(result)
        mock_connect.assert_called_once()
    
    def test_connect_pyodbc_missing_driver(self):
        """Test pyodbc connection without driver."""
        manager = ConnectionManager('pyodbc', **self.connection_kwargs)
        
        # The code logs error and returns False, doesn't raise ValueError
        result = manager.connect()
        self.assertFalse(result)
    
    @patch('impala_transfer.connection.sqlalchemy.create_engine')
    def test_connect_sqlalchemy_success(self, mock_create_engine):
        """Test successful SQLAlchemy connection."""
        kwargs = self.connection_kwargs.copy()
        kwargs['sqlalchemy_url'] = 'postgresql://test'
        manager = ConnectionManager('sqlalchemy', **kwargs)
        
        result = manager.connect()
        
        self.assertTrue(result)
        mock_create_engine.assert_called_once()

    @patch('impala_transfer.connection.impala.dbapi.connect')
    def test_connect_impyla_with_auth_mechanism(self, mock_connect):
        """Test Impyla connection with custom auth mechanism."""
        kwargs = self.connection_kwargs.copy()
        kwargs['auth_mechanism'] = 'GSSAPI'
        manager = ConnectionManager('impyla', **kwargs)
        
        result = manager.connect()
        
        self.assertTrue(result)
        mock_connect.assert_called_once_with(
            host='test-host',
            port=21050,
            database='test_db',
            auth_mechanism='GSSAPI'
        )

    @pytest.mark.skipif(not PYODBC_INSTALLED, reason="pyodbc or unixODBC not installed")
    @patch('impala_transfer.connection.PYODBC_AVAILABLE', True)
    @patch('pyodbc.connect')
    def test_connect_pyodbc_with_connection_string(self, mock_connect):
        """Test pyodbc connection with full connection string."""
        kwargs = self.connection_kwargs.copy()
        kwargs['odbc_connection_string'] = 'DRIVER={Test Driver};SERVER=test-host;PORT=21050;DATABASE=test_db'
        manager = ConnectionManager('pyodbc', **kwargs)
        
        result = manager.connect()
        
        self.assertTrue(result)
        mock_connect.assert_called_once_with('DRIVER={Test Driver};SERVER=test-host;PORT=21050;DATABASE=test_db')

    @patch('impala_transfer.connection.sqlalchemy.create_engine')
    def test_connect_sqlalchemy_with_engine_kwargs(self, mock_create_engine):
        """Test SQLAlchemy connection with engine kwargs."""
        kwargs = self.connection_kwargs.copy()
        kwargs['sqlalchemy_url'] = 'postgresql://test'
        kwargs['sqlalchemy_engine_kwargs'] = {'pool_size': 10, 'max_overflow': 20}
        manager = ConnectionManager('sqlalchemy', **kwargs)
        
        result = manager.connect()
        
        self.assertTrue(result)
        mock_create_engine.assert_called_once_with('postgresql://test', pool_size=10, max_overflow=20)

    def test_connect_unsupported_type(self):
        """Test connection with unsupported connection type."""
        manager = ConnectionManager('unsupported_type', **self.connection_kwargs)
        
        result = manager.connect()
        self.assertFalse(result)

    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', False)
    def test_connect_impyla_not_available(self):
        """Test Impyla connection when library is not available."""
        manager = ConnectionManager('impyla', **self.connection_kwargs)
        
        result = manager.connect()
        self.assertFalse(result)

    @patch('impala_transfer.connection.PYODBC_AVAILABLE', False)
    def test_connect_pyodbc_not_available(self):
        """Test pyodbc connection when library is not available."""
        manager = ConnectionManager('pyodbc', **self.connection_kwargs)
        
        result = manager.connect()
        self.assertFalse(result)

    @patch('impala_transfer.connection.SQLALCHEMY_AVAILABLE', False)
    def test_connect_sqlalchemy_not_available(self):
        """Test SQLAlchemy connection when library is not available."""
        kwargs = self.connection_kwargs.copy()
        kwargs['sqlalchemy_url'] = 'postgresql://test'
        manager = ConnectionManager('sqlalchemy', **kwargs)
        
        result = manager.connect()
        self.assertFalse(result)

    def test_close_with_connection(self):
        """Test closing connection."""
        manager = ConnectionManager('impyla', **self.connection_kwargs)
        manager.connection = Mock()
        
        manager.close()
        
        manager.connection.close.assert_called_once()

    def test_close_with_engine(self):
        """Test closing connection with engine."""
        manager = ConnectionManager('sqlalchemy', **self.connection_kwargs)
        manager.connection = Mock()
        manager.engine = Mock()
        
        manager.close()
        
        manager.connection.close.assert_called_once()
        manager.engine.dispose.assert_called_once()

    def test_close_without_connection(self):
        """Test closing when no connection exists."""
        manager = ConnectionManager('impyla', **self.connection_kwargs)
        
        # Should not raise any exception
        manager.close()

    def test_get_connection_info_with_connection(self):
        """Test getting connection info with active connection."""
        manager = ConnectionManager('impyla', **self.connection_kwargs)
        manager.connection = Mock()
        
        info = manager.get_connection_info()
        
        self.assertEqual(info['type'], 'impyla')
        self.assertTrue(info['connected'])
        self.assertEqual(info['parameters'], self.connection_kwargs)

    def test_get_connection_info_without_connection(self):
        """Test getting connection info without active connection."""
        manager = ConnectionManager('impyla', **self.connection_kwargs)
        
        info = manager.get_connection_info()
        
        self.assertEqual(info['type'], 'impyla')
        self.assertFalse(info['connected'])
        self.assertEqual(info['parameters'], self.connection_kwargs)

    def test_get_connection_info_with_additional_params(self):
        """Test getting connection info with additional parameters."""
        kwargs = self.connection_kwargs.copy()
        kwargs['odbc_driver'] = 'Test Driver'
        kwargs['auth_mechanism'] = 'GSSAPI'
        
        manager = ConnectionManager('pyodbc', **kwargs)
        manager.connection = Mock()
        
        info = manager.get_connection_info()
        
        self.assertEqual(info['type'], 'pyodbc')
        self.assertTrue(info['connected'])
        self.assertEqual(info['parameters'], kwargs)


class TestConnectionUtilities(unittest.TestCase):
    """Test connection utility functions."""
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    @patch('impala_transfer.connection.PYODBC_AVAILABLE', True)
    @patch('impala_transfer.connection.SQLALCHEMY_AVAILABLE', True)
    def test_get_available_connection_types_all_available(self):
        """Test get_available_connection_types when all are available."""
        available = get_available_connection_types()
        
        self.assertIn('impyla', available)
        self.assertIn('pyodbc', available)
        self.assertIn('sqlalchemy', available)
        self.assertEqual(len(available), 3)

    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    @patch('impala_transfer.connection.PYODBC_AVAILABLE', False)
    @patch('impala_transfer.connection.SQLALCHEMY_AVAILABLE', False)
    def test_get_available_connection_types_only_impyla(self):
        """Test get_available_connection_types when only impyla is available."""
        available = get_available_connection_types()
        
        self.assertIn('impyla', available)
        self.assertNotIn('pyodbc', available)
        self.assertNotIn('sqlalchemy', available)
        self.assertEqual(len(available), 1)

    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', False)
    @patch('impala_transfer.connection.PYODBC_AVAILABLE', False)
    @patch('impala_transfer.connection.SQLALCHEMY_AVAILABLE', False)
    def test_get_available_connection_types_none_available(self):
        """Test get_available_connection_types when none are available."""
        available = get_available_connection_types()
        
        self.assertEqual(available, [])

    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    @patch('impala_transfer.connection.PYODBC_AVAILABLE', True)
    @patch('impala_transfer.connection.SQLALCHEMY_AVAILABLE', True)
    def test_validate_connection_type_valid(self):
        """Test validate_connection_type with valid connection type."""
        self.assertTrue(validate_connection_type('impyla'))
        self.assertTrue(validate_connection_type('pyodbc'))
        self.assertTrue(validate_connection_type('sqlalchemy'))

    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    @patch('impala_transfer.connection.PYODBC_AVAILABLE', False)
    @patch('impala_transfer.connection.SQLALCHEMY_AVAILABLE', False)
    def test_validate_connection_type_partial_availability(self):
        """Test validate_connection_type with partial availability."""
        self.assertTrue(validate_connection_type('impyla'))
        self.assertFalse(validate_connection_type('pyodbc'))
        self.assertFalse(validate_connection_type('sqlalchemy'))

    def test_validate_connection_type_invalid(self):
        """Test validate_connection_type with invalid connection type."""
        self.assertFalse(validate_connection_type('invalid_type'))
        self.assertFalse(validate_connection_type('mysql'))
        self.assertFalse(validate_connection_type(''))


if __name__ == '__main__':
    unittest.main() 