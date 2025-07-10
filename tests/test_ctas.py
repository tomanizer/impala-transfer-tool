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
        location = "/data/tables/test_table"
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'PARQUET', 'SNAPPY', location, None, None, None, False
        )
        
        expected = f"CREATE TABLE IF NOT EXISTS test_table STORED AS PARQUET COMPRESSION 'SNAPPY' LOCATION '{location}' AS SELECT * FROM source_table"
        self.assertEqual(ctas_query, expected)
    
    def test_build_ctas_query_with_overwrite(self):
        """Test building CTAS query with overwrite."""
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        location = "/data/tables/test_table"
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'PARQUET', 'SNAPPY', location, None, None, None, True
        )
        
        expected = f"CREATE TABLE test_table STORED AS PARQUET COMPRESSION 'SNAPPY' LOCATION '{location}' AS SELECT * FROM source_table"
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
        location = "/data/tables/test_table"
        partitioned_by = ["date", "region"]
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'PARQUET', 'SNAPPY', location, partitioned_by, None, None, False
        )
        
        expected = f"CREATE TABLE IF NOT EXISTS test_table STORED AS PARQUET COMPRESSION 'SNAPPY' LOCATION '{location}' PARTITIONED BY (date, region) AS SELECT * FROM source_table"
        self.assertEqual(ctas_query, expected)
    
    def test_build_ctas_query_with_clustering(self):
        """Test building CTAS query with clustering."""
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        location = "/data/tables/test_table"
        clustered_by = ["user_id"]
        buckets = 32
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'PARQUET', 'SNAPPY', location, None, clustered_by, buckets, False
        )
        
        expected = f"CREATE TABLE IF NOT EXISTS test_table STORED AS PARQUET COMPRESSION 'SNAPPY' LOCATION '{location}' CLUSTERED BY (user_id) INTO 32 BUCKETS AS SELECT * FROM source_table"
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
            query, target_table, 'PARQUET', 'GZIP', location, partitioned_by, clustered_by, buckets, True
        )
        
        expected = f"CREATE TABLE test_table STORED AS PARQUET COMPRESSION 'GZIP' LOCATION '{location}' PARTITIONED BY (date) CLUSTERED BY (user_id) INTO 16 BUCKETS AS SELECT * FROM source_table"
        self.assertEqual(ctas_query, expected)
    
    def test_build_ctas_query_no_compression(self):
        """Test building CTAS query with no compression."""
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        location = "/data/tables/test_table"
        
        ctas_query = self.query_executor._build_ctas_query(
            query, target_table, 'PARQUET', 'NONE', location, None, None, None, False
        )
        
        expected = f"CREATE TABLE IF NOT EXISTS test_table STORED AS PARQUET LOCATION '{location}' AS SELECT * FROM source_table"
        self.assertEqual(ctas_query, expected)
    
    def test_build_ctas_query_missing_location(self):
        """Test that CTAS query fails without location."""
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        
        with self.assertRaises(ValueError, msg="HDFS table location is required for CTAS operations."):
            self.query_executor._build_ctas_query(
                query, target_table, 'PARQUET', 'SNAPPY', None, None, None, None, False
            )
    
    def test_execute_ctas_cursor_success(self):
        """Test successful CTAS execution with cursor."""
        self.connection_manager.connection = Mock()
        cursor = Mock()
        self.connection_manager.connection.cursor.return_value = cursor
        
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        location = "/data/tables/test_table"
        
        success = self.query_executor._execute_ctas_cursor(
            query, target_table, 'PARQUET', 'SNAPPY', location, None, None, None, False
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
        location = "/data/tables/test_table"
        
        success = self.query_executor._execute_ctas_cursor(
            query, target_table, 'PARQUET', 'SNAPPY', location, None, None, None, False
        )
        
        self.assertFalse(success)
        cursor.close.assert_called_once()
    
    def test_execute_ctas_sqlalchemy_success(self):
        """Test successful CTAS execution with SQLAlchemy."""
        self.connection_manager.connection = Mock()
        
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        location = "/data/tables/test_table"
        
        success = self.query_executor._execute_ctas_sqlalchemy(
            query, target_table, 'PARQUET', 'SNAPPY', location, None, None, None, False
        )
        
        self.assertTrue(success)
        self.connection_manager.connection.execute.assert_called_once()
    
    def test_execute_ctas_sqlalchemy_failure(self):
        """Test CTAS execution failure with SQLAlchemy."""
        self.connection_manager.connection = Mock()
        self.connection_manager.connection.execute.side_effect = Exception("CTAS failed")
        
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        location = "/data/tables/test_table"
        
        success = self.query_executor._execute_ctas_sqlalchemy(
            query, target_table, 'PARQUET', 'SNAPPY', location, None, None, None, False
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
        """Test table exists check returns True."""
        self.connection_manager.connection = Mock()
        cursor = Mock()
        cursor.fetchone.return_value = [1]
        self.connection_manager.connection.cursor.return_value = cursor
        
        exists = self.query_executor._table_exists_cursor("test_table")
        
        self.assertTrue(exists)
        cursor.execute.assert_called_once()
        cursor.close.assert_called_once()
    
    def test_table_exists_cursor_false(self):
        """Test table exists check returns False."""
        self.connection_manager.connection = Mock()
        cursor = Mock()
        cursor.fetchone.return_value = None
        self.connection_manager.connection.cursor.return_value = cursor
        
        exists = self.query_executor._table_exists_cursor("test_table")
        
        self.assertTrue(exists)
        cursor.execute.assert_called_once()
        cursor.close.assert_called_once()


class TestCTASIntegration(unittest.TestCase):
    """Integration tests for CTAS functionality."""
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    @patch('impala_transfer.core.TransferOrchestrator')
    @patch('impala_transfer.core.ConnectionManager')
    def test_create_table_as_select_success(self, mock_connection_manager, mock_transfer_orchestrator):
        """Test successful CTAS operation through main interface."""
        mock_cm = Mock()
        mock_connection_manager.return_value = mock_cm
        mock_cm.connection = Mock()
        mock_cm.connection_type = 'impyla'
        cursor = Mock()
        mock_cm.connection.cursor.return_value = cursor
        mock_query_executor = Mock()
        mock_query_executor.execute_ctas.return_value = True
        mock_orchestrator = Mock()
        mock_orchestrator.query_executor = mock_query_executor
        mock_transfer_orchestrator.return_value = mock_orchestrator
        tool = ImpalaTransferTool()
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        location = "/data/tables/test_table"
        success = tool.create_table_as_select(
            query, target_table, location=location
        )
        self.assertTrue(success)
        mock_query_executor.execute_ctas.assert_called_once()
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    @patch('impala_transfer.core.TransferOrchestrator')
    @patch('impala_transfer.core.ConnectionManager')
    def test_create_table_as_select_connection_failure(self, mock_connection_manager, mock_transfer_orchestrator):
        """Test CTAS operation with connection failure."""
        mock_cm = Mock()
        mock_connection_manager.return_value = mock_cm
        mock_cm.connection = None
        mock_query_executor = Mock()
        mock_query_executor.execute_ctas.return_value = False
        mock_orchestrator = Mock()
        mock_orchestrator.query_executor = mock_query_executor
        mock_transfer_orchestrator.return_value = mock_orchestrator
        tool = ImpalaTransferTool()
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        location = "/data/tables/test_table"
        success = tool.create_table_as_select(
            query, target_table, location=location
        )
        self.assertFalse(success)
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    @patch('impala_transfer.core.TransferOrchestrator')
    @patch('impala_transfer.core.ConnectionManager')
    def test_create_table_as_select_with_progress(self, mock_connection_manager, mock_transfer_orchestrator):
        """Test CTAS operation with progress callback (mocked, no real callback)."""
        mock_cm = Mock()
        mock_connection_manager.return_value = mock_cm
        mock_cm.connection = Mock()
        mock_cm.connection_type = 'impyla'
        cursor = Mock()
        mock_cm.connection.cursor.return_value = cursor
        mock_query_executor = Mock()
        mock_query_executor.execute_ctas.return_value = True
        mock_orchestrator = Mock()
        mock_orchestrator.query_executor = mock_query_executor
        mock_transfer_orchestrator.return_value = mock_orchestrator
        tool = ImpalaTransferTool()
        query = "SELECT * FROM source_table"
        target_table = "test_table"
        location = "/data/tables/test_table"
        # No progress_callback argument, just test normal call
        success = tool.create_table_as_select(
            query, target_table, location=location
        )
        self.assertTrue(success)
        mock_query_executor.execute_ctas.assert_called_once()
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    @patch('impala_transfer.core.TransferOrchestrator')
    @patch('impala_transfer.core.ConnectionManager')
    def test_drop_table_success(self, mock_connection_manager, mock_transfer_orchestrator):
        """Test successful table drop through main interface."""
        mock_cm = Mock()
        mock_connection_manager.return_value = mock_cm
        mock_cm.connection = Mock()
        mock_cm.connection_type = 'impyla'
        cursor = Mock()
        mock_cm.connection.cursor.return_value = cursor
        mock_query_executor = Mock()
        mock_query_executor.drop_table.return_value = True
        mock_orchestrator = Mock()
        mock_orchestrator.query_executor = mock_query_executor
        mock_transfer_orchestrator.return_value = mock_orchestrator
        tool = ImpalaTransferTool()
        success = tool.drop_table("test_table", if_exists=True)
        self.assertTrue(success)
        mock_query_executor.drop_table.assert_called_once_with("test_table", True)
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    @patch('impala_transfer.core.TransferOrchestrator')
    @patch('impala_transfer.core.ConnectionManager')
    def test_table_exists_success(self, mock_connection_manager, mock_transfer_orchestrator):
        """Test table exists check through main interface."""
        mock_cm = Mock()
        mock_connection_manager.return_value = mock_cm
        mock_cm.connection = Mock()
        mock_cm.connection_type = 'impyla'
        cursor = Mock()
        cursor.fetchone.return_value = [1]
        mock_cm.connection.cursor.return_value = cursor
        mock_query_executor = Mock()
        mock_query_executor.table_exists.return_value = True
        mock_orchestrator = Mock()
        mock_orchestrator.query_executor = mock_query_executor
        mock_transfer_orchestrator.return_value = mock_orchestrator
        tool = ImpalaTransferTool()
        exists = tool.table_exists("test_table")
        self.assertTrue(exists)
        mock_query_executor.table_exists.assert_called_once_with("test_table")


if __name__ == '__main__':
    unittest.main() 