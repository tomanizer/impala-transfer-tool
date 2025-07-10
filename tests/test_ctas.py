#!/usr/bin/env python3
"""
Tests for CTAS (CREATE TABLE AS SELECT) functionality.
"""

import unittest
from unittest.mock import Mock, patch
from impala_transfer import ImpalaTransferTool
from impala_transfer.query import QueryExecutor
from impala_transfer.connection import ConnectionManager


class TestCTASFunctionality(unittest.TestCase):
    """Test CTAS functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.connection_manager = Mock(spec=ConnectionManager)
        self.connection_manager.connection_type = 'impyla'
        self.query_executor = QueryExecutor(self.connection_manager)
    
    def test_build_ctas_query_basic(self):
        """Test building basic CTAS query."""
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'PARQUET', 'SNAPPY', None, None, None, None, False
        )
        
        expected = "CREATE TABLE IF NOT EXISTS test_table STORED AS PARQUET COMPRESSION 'SNAPPY' AS SELECT * FROM source_table"
        self.assertEqual(ctas_query, expected)
    
    def test_build_ctas_query_with_overwrite(self):
        """Test building CTAS query with overwrite."""
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'PARQUET', 'SNAPPY', None, None, None, None, True
        )
        
        expected = "CREATE TABLE test_table STORED AS PARQUET COMPRESSION 'SNAPPY' AS SELECT * FROM source_table"
        self.assertEqual(ctas_query, expected)
    
    def test_build_ctas_query_with_location(self):
        """Test building CTAS query with custom location."""
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        location = "/data/custom/tables/test_table"
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'PARQUET', 'SNAPPY', location, None, None, None, False
        )
        
        expected = f"CREATE TABLE IF NOT EXISTS test_table STORED AS PARQUET COMPRESSION 'SNAPPY' LOCATION '{location}' AS SELECT * FROM source_table"
        self.assertEqual(ctas_query, expected)
    
    def test_build_ctas_query_with_partitioning(self):
        """Test building CTAS query with partitioning."""
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        partitioned_by = ["date", "region"]
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'PARQUET', 'SNAPPY', None, partitioned_by, None, None, False
        )
        
        expected = "CREATE TABLE IF NOT EXISTS test_table STORED AS PARQUET COMPRESSION 'SNAPPY' PARTITIONED BY (date, region) AS SELECT * FROM source_table"
        self.assertEqual(ctas_query, expected)
    
    def test_build_ctas_query_with_clustering(self):
        """Test building CTAS query with clustering."""
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        clustered_by = ["user_id"]
        buckets = 32
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'PARQUET', 'SNAPPY', None, None, clustered_by, buckets, False
        )
        
        expected = "CREATE TABLE IF NOT EXISTS test_table STORED AS PARQUET COMPRESSION 'SNAPPY' CLUSTERED BY (user_id) INTO 32 BUCKETS AS SELECT * FROM source_table"
        self.assertEqual(ctas_query, expected)
    
    def test_build_ctas_query_with_all_options(self):
        """Test building CTAS query with all options."""
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        location = "/data/custom/tables/test_table"
        partitioned_by = ["date"]
        clustered_by = ["user_id"]
        buckets = 16
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'ORC', 'GZIP', location, partitioned_by, clustered_by, buckets, True
        )
        
        expected = f"CREATE TABLE test_table STORED AS ORC COMPRESSION 'GZIP' LOCATION '{location}' PARTITIONED BY (date) CLUSTERED BY (user_id) INTO 16 BUCKETS AS SELECT * FROM source_table"
        self.assertEqual(ctas_query, expected)
    
    def test_build_ctas_query_no_compression(self):
        """Test building CTAS query with no compression."""
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'TEXTFILE', 'NONE', None, None, None, None, False
        )
        
        expected = "CREATE TABLE IF NOT EXISTS test_table STORED AS TEXTFILE AS SELECT * FROM source_table"
        self.assertEqual(ctas_query, expected)
    
    def test_execute_ctas_cursor_success(self):
        """Test successful CTAS execution with cursor."""
        self.connection_manager.connection = Mock()
        cursor = Mock()
        self.connection_manager.connection.cursor.return_value = cursor
        
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        
        success = self.query_executor._execute_ctas_cursor(
            query, target_table, 'PARQUET', 'SNAPPY', None, None, None, None, False
        )
        
        self.assertTrue(success)
        cursor.execute.assert_called_once()
        cursor.close.assert_called_once()
    
    def test_execute_ctas_cursor_failure(self):
        """Test CTAS execution failure with cursor."""
        self.connection_manager.connection = Mock()
        cursor = Mock()
        cursor.execute.side_effect = Exception("CTAS failed")
        self.connection_manager.connection.cursor.return_value = cursor
        
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        
        success = self.query_executor._execute_ctas_cursor(
            query, target_table, 'PARQUET', 'SNAPPY', None, None, None, None, False
        )
        
        self.assertFalse(success)
        cursor.close.assert_called_once()
    
    def test_execute_ctas_sqlalchemy_success(self):
        """Test successful CTAS execution with SQLAlchemy."""
        self.connection_manager.connection = Mock()
        
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        
        success = self.query_executor._execute_ctas_sqlalchemy(
            query, target_table, 'PARQUET', 'SNAPPY', None, None, None, None, False
        )
        
        self.assertTrue(success)
        self.connection_manager.connection.execute.assert_called_once()
    
    def test_execute_ctas_sqlalchemy_failure(self):
        """Test CTAS execution failure with SQLAlchemy."""
        self.connection_manager.connection = Mock()
        self.connection_manager.connection.execute.side_effect = Exception("CTAS failed")
        
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        
        success = self.query_executor._execute_ctas_sqlalchemy(
            query, target_table, 'PARQUET', 'SNAPPY', None, None, None, None, False
        )
        
        self.assertFalse(success)
    
    def test_drop_table_cursor_success(self):
        """Test successful table drop with cursor."""
        self.connection_manager.connection = Mock()
        cursor = Mock()
        self.connection_manager.connection.cursor.return_value = cursor
        
        success = self.query_executor._drop_table_cursor("test_table", True)
        
        self.assertTrue(success)
        cursor.execute.assert_called_once_with("DROP TABLE IF EXISTS test_table")
        cursor.close.assert_called_once()
    
    def test_drop_table_cursor_failure(self):
        """Test table drop failure with cursor."""
        self.connection_manager.connection = Mock()
        cursor = Mock()
        cursor.execute.side_effect = Exception("Drop failed")
        self.connection_manager.connection.cursor.return_value = cursor
        
        success = self.query_executor._drop_table_cursor("test_table", True)
        
        self.assertFalse(success)
        cursor.close.assert_called_once()
    
    def test_table_exists_cursor_true(self):
        """Test table exists check returning True with cursor."""
        self.connection_manager.connection = Mock()
        cursor = Mock()
        self.connection_manager.connection.cursor.return_value = cursor
        
        exists = self.query_executor._table_exists_cursor("test_table")
        
        self.assertTrue(exists)
        cursor.execute.assert_called_once_with("DESCRIBE test_table")
        cursor.close.assert_called_once()
    
    def test_table_exists_cursor_false(self):
        """Test table exists check returning False with cursor."""
        self.connection_manager.connection = Mock()
        cursor = Mock()
        cursor.execute.side_effect = Exception("Table does not exist")
        self.connection_manager.connection.cursor.return_value = cursor
        
        exists = self.query_executor._table_exists_cursor("test_table")
        
        self.assertFalse(exists)
        cursor.close.assert_called_once()


class TestCTASIntegration(unittest.TestCase):
    """Test CTAS integration with ImpalaTransferTool."""
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    def test_create_table_as_select_success(self):
        """Test successful CTAS operation through ImpalaTransferTool."""
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
        tool.orchestrator.query_executor.execute_ctas.return_value = True
        
        success = tool.create_table_as_select(
            query="SELECT * FROM source_table",
            target_table="test_table"
        )
        
        self.assertTrue(success)
        tool.connection_manager.connect.assert_called_once()
        tool.connection_manager.close.assert_called_once()
        tool.orchestrator.query_executor.execute_ctas.assert_called_once()
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    def test_create_table_as_select_connection_failure(self):
        """Test CTAS operation with connection failure."""
        tool = ImpalaTransferTool(
            source_host='test-host',
            connection_type='impyla'
        )
        
        # Mock the components
        tool.connection_manager = Mock()
        tool.connection_manager.connect.return_value = False
        tool.connection_manager.close = Mock()
        
        success = tool.create_table_as_select(
            query="SELECT * FROM source_table",
            target_table="test_table"
        )
        
        self.assertFalse(success)
        tool.connection_manager.connect.assert_called_once()
        tool.connection_manager.close.assert_called_once()
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    def test_create_table_as_select_with_progress(self):
        """Test CTAS operation with progress reporting."""
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
        tool.orchestrator.query_executor.get_query_info.return_value = {'row_count': 1000}
        tool.orchestrator.query_executor.execute_ctas.return_value = True
        
        progress_callback = Mock()
        
        success = tool.create_table_as_select_with_progress(
            query="SELECT * FROM source_table",
            target_table="test_table",
            progress_callback=progress_callback
        )
        
        self.assertTrue(success)
        tool.connection_manager.connect.assert_called_once()
        tool.connection_manager.close.assert_called_once()
        tool.orchestrator.query_executor.execute_ctas.assert_called_once()
        
        # Check progress callbacks
        progress_callback.assert_any_call("Connecting to database...", 0)
        progress_callback.assert_any_call("Analyzing query...", 20)
        progress_callback.assert_any_call("Executing CTAS for 1000 rows...", 50)
        progress_callback.assert_any_call("CTAS operation completed successfully!", 100)
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    def test_drop_table_success(self):
        """Test successful table drop through ImpalaTransferTool."""
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
        tool.orchestrator.query_executor.drop_table.return_value = True
        
        success = tool.drop_table("test_table")
        
        self.assertTrue(success)
        tool.connection_manager.connect.assert_called_once()
        tool.connection_manager.close.assert_called_once()
        tool.orchestrator.query_executor.drop_table.assert_called_once_with("test_table", True)
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    def test_table_exists_success(self):
        """Test table exists check through ImpalaTransferTool."""
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
        tool.orchestrator.query_executor.table_exists.return_value = True
        
        exists = tool.table_exists("test_table")
        
        self.assertTrue(exists)
        tool.connection_manager.connect.assert_called_once()
        tool.connection_manager.close.assert_called_once()
        tool.orchestrator.query_executor.table_exists.assert_called_once_with("test_table")


if __name__ == '__main__':
    unittest.main() 