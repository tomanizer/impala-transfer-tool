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
        mock_cursor.fetchall.return_value = [('col1', 'int'), ('col2', 'string')]
        self.connection_manager.connection.cursor.return_value = mock_cursor
        
        result = self.executor.get_query_info("SELECT * FROM test_table")
        
        self.assertEqual(result['row_count'], 1000)
        self.assertEqual(result['query'], "SELECT * FROM test_table")
        self.assertEqual(result['sample_data'], ('col1', 'col2', 'col3'))
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
        self.assertEqual(result['sample_data'], ('val1', 'val2', 'val3'))
        self.assertEqual(len(result['columns']), 3)

    def test_get_query_info_sqlalchemy_empty_result(self):
        """Test getting query info with SQLAlchemy when no results."""
        self.connection_manager.connection_type = 'sqlalchemy'
        executor = QueryExecutor(self.connection_manager)
        
        # Mock SQLAlchemy result with no data
        mock_result = Mock()
        mock_result.fetchone.side_effect = [(0,), None]
        self.connection_manager.connection.execute.return_value = mock_result
        
        result = executor.get_query_info("SELECT * FROM empty_table")
        
        self.assertEqual(result['row_count'], 0)
        self.assertEqual(result['sample_data'], None)
        self.assertEqual(result['columns'], [])

    def test_get_query_info_cursor_empty_result(self):
        """Test getting query info with cursor when no results."""
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [(0,), None]
        mock_cursor.fetchall.return_value = []
        self.connection_manager.connection.cursor.return_value = mock_cursor
        
        result = self.executor.get_query_info("SELECT * FROM empty_table")
        
        self.assertEqual(result['row_count'], 0)
        self.assertEqual(result['sample_data'], None)
        self.assertEqual(result['columns'], [])

    def test_execute_query_cursor(self):
        """Test execute_query with cursor-based execution."""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [('row1', 'data1'), ('row2', 'data2')]
        self.connection_manager.connection.cursor.return_value = mock_cursor
        
        result = self.executor.execute_query("SELECT * FROM test_table")
        
        self.assertEqual(result, [('row1', 'data1'), ('row2', 'data2')])
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")
        mock_cursor.close.assert_called_once()

    def test_execute_query_sqlalchemy(self):
        """Test execute_query with SQLAlchemy execution."""
        self.connection_manager.connection_type = 'sqlalchemy'
        executor = QueryExecutor(self.connection_manager)
        
        mock_result = Mock()
        mock_result.fetchall.return_value = [('row1', 'data1'), ('row2', 'data2')]
        self.connection_manager.connection.execute.return_value = mock_result
        
        result = executor.execute_query("SELECT * FROM test_table")
        
        self.assertEqual(result, [('row1', 'data1'), ('row2', 'data2')])

    def test_execute_query_with_batching_cursor(self):
        """Test execute_query_with_batching with cursor-based execution."""
        mock_cursor = Mock()
        # Simulate multiple batches
        mock_cursor.fetchmany.side_effect = [
            [('row1', 'data1'), ('row2', 'data2')],
            [('row3', 'data3')],
            []
        ]
        self.connection_manager.connection.cursor.return_value = mock_cursor
        
        result = self.executor.execute_query_with_batching("SELECT * FROM test_table", batch_size=2)
        
        expected = [('row1', 'data1'), ('row2', 'data2'), ('row3', 'data3')]
        self.assertEqual(result, expected)
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")
        mock_cursor.close.assert_called_once()

    def test_execute_query_with_batching_sqlalchemy(self):
        """Test execute_query_with_batching with SQLAlchemy execution."""
        self.connection_manager.connection_type = 'sqlalchemy'
        executor = QueryExecutor(self.connection_manager)
        
        mock_result = Mock()
        # Simulate multiple batches
        mock_result.fetchmany.side_effect = [
            [('row1', 'data1'), ('row2', 'data2')],
            [('row3', 'data3')],
            []
        ]
        self.connection_manager.connection.execute.return_value = mock_result
        
        result = executor.execute_query_with_batching("SELECT * FROM test_table", batch_size=2)
        
        expected = [('row1', 'data1'), ('row2', 'data2'), ('row3', 'data3')]
        self.assertEqual(result, expected)

    def test_execute_query_with_batching_empty_result(self):
        """Test execute_query_with_batching with empty result."""
        mock_cursor = Mock()
        mock_cursor.fetchmany.return_value = []
        self.connection_manager.connection.cursor.return_value = mock_cursor
        
        result = self.executor.execute_query_with_batching("SELECT * FROM empty_table")
        
        self.assertEqual(result, [])
        mock_cursor.execute.assert_called_once_with("SELECT * FROM empty_table")
        mock_cursor.close.assert_called_once()

    def test_execute_query_with_batching_sqlalchemy_empty_result(self):
        """Test execute_query_with_batching with SQLAlchemy empty result."""
        self.connection_manager.connection_type = 'sqlalchemy'
        executor = QueryExecutor(self.connection_manager)
        
        mock_result = Mock()
        mock_result.fetchmany.return_value = []
        self.connection_manager.connection.execute.return_value = mock_result
        
        result = executor.execute_query_with_batching("SELECT * FROM empty_table")
        
        self.assertEqual(result, [])

    def test_test_connection_cursor_success(self):
        """Test test_connection with cursor-based execution success."""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        self.connection_manager.connection.cursor.return_value = mock_cursor
        
        result = self.executor.test_connection()
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_cursor.fetchone.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_test_connection_sqlalchemy_success(self):
        """Test test_connection with SQLAlchemy execution success."""
        self.connection_manager.connection_type = 'sqlalchemy'
        executor = QueryExecutor(self.connection_manager)
        
        mock_result = Mock()
        mock_result.fetchone.return_value = (1,)
        self.connection_manager.connection.execute.return_value = mock_result
        
        result = executor.test_connection()
        
        self.assertTrue(result)

    def test_test_connection_cursor_failure(self):
        """Test test_connection with cursor-based execution failure."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Connection failed")
        self.connection_manager.connection.cursor.return_value = mock_cursor
        
        with self.assertLogs('root', level='ERROR') as cm:
            result = self.executor.test_connection()
        
        self.assertFalse(result)
        self.assertTrue(any('Connection test failed' in msg for msg in cm.output))

    def test_test_connection_sqlalchemy_failure(self):
        """Test test_connection with SQLAlchemy execution failure."""
        self.connection_manager.connection_type = 'sqlalchemy'
        executor = QueryExecutor(self.connection_manager)
        
        self.connection_manager.connection.execute.side_effect = Exception("Connection failed")
        
        with self.assertLogs('root', level='ERROR') as cm:
            result = executor.test_connection()
        
        self.assertFalse(result)
        self.assertTrue(any('Connection test failed' in msg for msg in cm.output))

    def test_execute_query_cursor_exception(self):
        """Test execute_query with cursor exception handling."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Query failed")
        self.connection_manager.connection.cursor.return_value = mock_cursor
        
        with self.assertRaises(Exception):
            self.executor.execute_query("SELECT * FROM test_table")
        
        mock_cursor.close.assert_called_once()

    def test_execute_query_with_batching_cursor_exception(self):
        """Test execute_query_with_batching with cursor exception handling."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Query failed")
        self.connection_manager.connection.cursor.return_value = mock_cursor
        
        with self.assertRaises(Exception):
            self.executor.execute_query_with_batching("SELECT * FROM test_table")
        
        mock_cursor.close.assert_called_once()

    def test_get_query_info_cursor_exception(self):
        """Test get_query_info with cursor exception handling."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Query failed")
        self.connection_manager.connection.cursor.return_value = mock_cursor
        
        with self.assertRaises(Exception):
            self.executor.get_query_info("SELECT * FROM test_table")
        
        mock_cursor.close.assert_called_once()


if __name__ == '__main__':
    unittest.main() 