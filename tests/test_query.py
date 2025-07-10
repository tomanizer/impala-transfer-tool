#!/usr/bin/env python3
"""
Test suite for the query module.
"""

import unittest
from unittest.mock import Mock, patch

from impala_transfer.query import QueryExecutor


class TestQueryExecutor(unittest.TestCase):
    """Test the QueryExecutor class."""
    
    def setUp(self):
        self.connection_manager = Mock()
        self.connection_manager.connection_type = 'impyla'
        self.connection_manager.connection = Mock()
        self.executor = QueryExecutor(self.connection_manager)
    
    def test_get_query_info_cursor(self):
        """Test getting query info with cursor-based execution."""
        # Mock cursor and its methods
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [(1000,), ('col1', 'col2', 'col3')]
        self.connection_manager.connection.cursor.return_value = mock_cursor
        
        result = self.executor.get_query_info("SELECT * FROM test_table")
        
        self.assertEqual(result['row_count'], 1000)
        self.assertEqual(result['query'], "SELECT * FROM test_table")
        mock_cursor.execute.assert_called()
        mock_cursor.close.assert_called_once()
    
    def test_get_query_info_sqlalchemy(self):
        """Test getting query info with SQLAlchemy execution."""
        self.connection_manager.connection_type = 'sqlalchemy'
        executor = QueryExecutor(self.connection_manager)
        
        # Mock SQLAlchemy result
        mock_result = Mock()
        mock_result.fetchone.side_effect = [(1000,), ('val1', 'val2', 'val3')]
        self.connection_manager.connection.execute.return_value = mock_result
        
        # Don't need to patch sqlalchemy.text since it's imported inside the method
        result = executor.get_query_info("SELECT * FROM test_table")
        
        self.assertEqual(result['row_count'], 1000)
        self.assertEqual(result['query'], "SELECT * FROM test_table")


if __name__ == '__main__':
    unittest.main() 