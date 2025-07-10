#!/usr/bin/env python3
"""
Test suite for the orchestrator module.
"""

import unittest
from unittest.mock import Mock, patch

from impala_transfer.orchestrator import TransferOrchestrator


class TestTransferOrchestrator(unittest.TestCase):
    """Test the TransferOrchestrator class."""
    
    def setUp(self):
        self.connection_manager = Mock()
        self.chunk_processor = Mock()
        self.file_transfer_manager = Mock()
        self.orchestrator = TransferOrchestrator(
            self.connection_manager,
            self.chunk_processor,
            self.file_transfer_manager,
            max_workers=2
        )
    
    def test_transfer_query_success(self):
        """Test successful query transfer."""
        # Mock all the components
        self.connection_manager.connect.return_value = True
        # Mock the query_executor that gets created internally
        self.orchestrator.query_executor = Mock()
        self.orchestrator.query_executor.get_query_info.return_value = {
            'row_count': 100,
            'query': 'SELECT * FROM test_table'
        }
        self.chunk_processor.generate_chunk_queries.return_value = [
            'SELECT * FROM test_table LIMIT 100 OFFSET 0'
        ]
        # Mock the internal _process_chunks_parallel method
        self.orchestrator._process_chunks_parallel = Mock(return_value=['test_file.parquet'])
        self.file_transfer_manager.transfer_files.return_value = True
        
        result = self.orchestrator.transfer_query('SELECT * FROM test_table')
        
        self.assertTrue(result)
        self.connection_manager.connect.assert_called_once()
        self.orchestrator.query_executor.get_query_info.assert_called_once()
        self.chunk_processor.generate_chunk_queries.assert_called_once()
        self.orchestrator._process_chunks_parallel.assert_called_once()
        self.file_transfer_manager.transfer_files.assert_called_once()
    
    def test_transfer_query_connection_failure(self):
        """Test query transfer with connection failure."""
        self.connection_manager.connect.return_value = False
        
        result = self.orchestrator.transfer_query('SELECT * FROM test_table')
        
        self.assertFalse(result)
        self.connection_manager.connect.assert_called_once()
        # Other methods should not be called
    
    def test_transfer_query_transfer_failure(self):
        """Test query transfer with transfer failure."""
        self.connection_manager.connect.return_value = True
        # Mock the query_executor that gets created internally
        self.orchestrator.query_executor = Mock()
        self.orchestrator.query_executor.get_query_info.return_value = {
            'row_count': 100,
            'query': 'SELECT * FROM test_table'
        }
        self.chunk_processor.generate_chunk_queries.return_value = [
            'SELECT * FROM test_table LIMIT 100 OFFSET 0'
        ]
        # Mock the internal _process_chunks_parallel method
        self.orchestrator._process_chunks_parallel = Mock(return_value=['test_file.parquet'])
        self.file_transfer_manager.transfer_files.return_value = False
        
        result = self.orchestrator.transfer_query('SELECT * FROM test_table')
        
        self.assertFalse(result)
        # Cleanup should still be called even on transfer failure


if __name__ == '__main__':
    unittest.main() 