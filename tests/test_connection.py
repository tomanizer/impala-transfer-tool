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

from impala_transfer.connection import ConnectionManager


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


if __name__ == '__main__':
    unittest.main() 