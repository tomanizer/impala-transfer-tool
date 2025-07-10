#!/usr/bin/env python3
"""
Test suite for the orchestrator module.
"""

import unittest
from unittest.mock import Mock, patch
from concurrent.futures import Future

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

    def test_transfer_query_with_custom_target_table(self):
        """Test transfer_query with custom target table."""
        self.connection_manager.connect.return_value = True
        self.orchestrator.query_executor = Mock()
        self.orchestrator.query_executor.get_query_info.return_value = {
            'row_count': 100,
            'query': 'SELECT * FROM test_table'
        }
        self.chunk_processor.generate_chunk_queries.return_value = [
            'SELECT * FROM test_table LIMIT 100 OFFSET 0'
        ]
        self.orchestrator._process_chunks_parallel = Mock(return_value=['test_file.parquet'])
        self.file_transfer_manager.transfer_files.return_value = True
        
        result = self.orchestrator.transfer_query('SELECT * FROM test_table', 'custom_table')
        
        self.assertTrue(result)
        self.file_transfer_manager.transfer_files.assert_called_with(['test_file.parquet'], 'custom_table')

    def test_transfer_query_with_custom_output_format(self):
        """Test transfer_query with custom output format."""
        self.connection_manager.connect.return_value = True
        self.orchestrator.query_executor = Mock()
        self.orchestrator.query_executor.get_query_info.return_value = {
            'row_count': 100,
            'query': 'SELECT * FROM test_table'
        }
        self.chunk_processor.generate_chunk_queries.return_value = [
            'SELECT * FROM test_table LIMIT 100 OFFSET 0'
        ]
        self.orchestrator._process_chunks_parallel = Mock(return_value=['test_file.csv'])
        self.file_transfer_manager.transfer_files.return_value = True
        
        result = self.orchestrator.transfer_query('SELECT * FROM test_table', output_format='csv')
        
        self.assertTrue(result)
        self.orchestrator._process_chunks_parallel.assert_called_with(
            ['SELECT * FROM test_table LIMIT 100 OFFSET 0'], 'csv'
        )

    def test_transfer_query_chunk_validation_failure(self):
        """Test transfer_query with chunk validation failure."""
        self.connection_manager.connect.return_value = True
        self.orchestrator.query_executor = Mock()
        self.orchestrator.query_executor.get_query_info.return_value = {
            'row_count': 100,
            'query': 'SELECT * FROM test_table'
        }
        self.chunk_processor.validate_chunk_size.return_value = False
        self.chunk_processor.generate_chunk_queries.return_value = [
            'SELECT * FROM test_table LIMIT 100 OFFSET 0'
        ]
        self.orchestrator._process_chunks_parallel = Mock(return_value=['test_file.parquet'])
        self.file_transfer_manager.transfer_files.return_value = True
        
        with self.assertLogs('root', level='WARNING') as cm:
            result = self.orchestrator.transfer_query('SELECT * FROM test_table')
        
        self.assertTrue(result)
        self.assertTrue(any('Chunk size validation failed' in msg for msg in cm.output))

    def test_transfer_query_chunk_processing_failure(self):
        """Test transfer_query with chunk processing failure."""
        self.connection_manager.connect.return_value = True
        self.orchestrator.query_executor = Mock()
        self.orchestrator.query_executor.get_query_info.return_value = {
            'row_count': 100,
            'query': 'SELECT * FROM test_table'
        }
        self.chunk_processor.generate_chunk_queries.return_value = [
            'SELECT * FROM test_table LIMIT 100 OFFSET 0'
        ]
        self.orchestrator._process_chunks_parallel = Mock(return_value=[])
        self.file_transfer_manager.transfer_files.return_value = True
        
        result = self.orchestrator.transfer_query('SELECT * FROM test_table')
        
        self.assertFalse(result)

    def test_transfer_query_exception_handling(self):
        """Test transfer_query with exception handling."""
        self.connection_manager.connect.side_effect = Exception("Connection error")
        
        with self.assertLogs('root', level='ERROR') as cm:
            result = self.orchestrator.transfer_query('SELECT * FROM test_table')
        
        self.assertFalse(result)
        self.assertTrue(any('Transfer failed' in msg for msg in cm.output))
        self.connection_manager.close.assert_called_once()

    def test_process_chunks_parallel_success(self):
        """Test _process_chunks_parallel with successful processing."""
        queries = ['query1', 'query2', 'query3']
        
        # Mock the chunk processor to return file paths
        self.chunk_processor.process_chunk.side_effect = [
            'test_file_0.parquet',
            'test_file_1.parquet', 
            'test_file_2.parquet'
        ]
        
        with patch('concurrent.futures.ThreadPoolExecutor') as mock_executor:
            # Mock the executor context manager
            mock_executor_instance = Mock()
            mock_executor.return_value.__enter__.return_value = mock_executor_instance
            
            # Mock submit to return futures
            futures = []
            for i in range(3):
                future = Mock()
                future.result.return_value = f'test_file_{i}.parquet'
                futures.append(future)
            
            # Mock submit to return different futures for each call
            mock_executor_instance.submit.side_effect = futures
            
            # Mock as_completed to return the same futures
            with patch('concurrent.futures.as_completed', return_value=futures):
                result = self.orchestrator._process_chunks_parallel(queries, 'parquet')
        
        self.assertEqual(len(result), 3)
        self.assertIn('test_file_0.parquet', result)
        self.assertIn('test_file_1.parquet', result)
        self.assertIn('test_file_2.parquet', result)

    def test_process_chunks_parallel_failure(self):
        """Test _process_chunks_parallel with chunk failure."""
        queries = ['query1', 'query2']
        
        # Mock the chunk processor to return file paths for first call, raise exception for second
        self.chunk_processor.process_chunk.side_effect = [
            'test_file_0.parquet',
            Exception("Chunk processing failed")
        ]
        
        with patch('concurrent.futures.ThreadPoolExecutor') as mock_executor:
            # Mock the executor context manager
            mock_executor_instance = Mock()
            mock_executor.return_value.__enter__.return_value = mock_executor_instance
            
            # Mock submit to return futures
            futures = []
            for i in range(2):
                future = Mock()
                if i == 0:  # First future succeeds
                    future.result.return_value = f'test_file_{i}.parquet'
                else:  # Second future fails
                    future.result.side_effect = Exception("Chunk processing failed")
                futures.append(future)
            
            # Mock submit to return different futures for each call
            mock_executor_instance.submit.side_effect = futures
            
            # Mock as_completed to return the same futures
            with patch('concurrent.futures.as_completed', return_value=futures):
                with patch('impala_transfer.orchestrator.logging') as mock_logging:
                    result = self.orchestrator._process_chunks_parallel(queries, 'parquet')
        
        self.assertEqual(result, [])
        mock_logging.error.assert_called()

    def test_transfer_query_with_progress_success(self):
        """Test transfer_query_with_progress with successful transfer."""
        self.connection_manager.connect.return_value = True
        self.orchestrator.query_executor = Mock()
        self.orchestrator.query_executor.get_query_info.return_value = {
            'row_count': 100,
            'query': 'SELECT * FROM test_table'
        }
        self.chunk_processor.generate_chunk_queries.return_value = [
            'SELECT * FROM test_table LIMIT 100 OFFSET 0'
        ]
        self.orchestrator._process_chunks_with_progress = Mock(return_value=['test_file.parquet'])
        self.file_transfer_manager.transfer_files.return_value = True
        
        progress_callback = Mock()
        
        result = self.orchestrator.transfer_query_with_progress(
            'SELECT * FROM test_table', progress_callback=progress_callback
        )
        
        self.assertTrue(result)
        progress_callback.assert_any_call("Connecting to database...", 0)
        progress_callback.assert_any_call("Analyzing query...", 10)
        progress_callback.assert_any_call("Processing 1 chunks...", 20)
        progress_callback.assert_any_call("Transferring files to target cluster...", 90)
        progress_callback.assert_any_call("Cleaning up temporary files...", 95)
        progress_callback.assert_any_call("Transfer completed successfully!", 100)

    def test_transfer_query_with_progress_connection_failure(self):
        """Test transfer_query_with_progress with connection failure."""
        self.connection_manager.connect.return_value = False
        progress_callback = Mock()
        
        result = self.orchestrator.transfer_query_with_progress(
            'SELECT * FROM test_table', progress_callback=progress_callback
        )
        
        self.assertFalse(result)
        progress_callback.assert_called_once_with("Connecting to database...", 0)

    def test_transfer_query_with_progress_chunk_failure(self):
        """Test transfer_query_with_progress with chunk processing failure."""
        self.connection_manager.connect.return_value = True
        self.orchestrator.query_executor = Mock()
        self.orchestrator.query_executor.get_query_info.return_value = {
            'row_count': 100,
            'query': 'SELECT * FROM test_table'
        }
        self.chunk_processor.generate_chunk_queries.return_value = [
            'SELECT * FROM test_table LIMIT 100 OFFSET 0'
        ]
        self.orchestrator._process_chunks_with_progress = Mock(return_value=[])
        progress_callback = Mock()
        
        result = self.orchestrator.transfer_query_with_progress(
            'SELECT * FROM test_table', progress_callback=progress_callback
        )
        
        self.assertFalse(result)

    def test_transfer_query_with_progress_transfer_failure(self):
        """Test transfer_query_with_progress with transfer failure."""
        self.connection_manager.connect.return_value = True
        self.orchestrator.query_executor = Mock()
        self.orchestrator.query_executor.get_query_info.return_value = {
            'row_count': 100,
            'query': 'SELECT * FROM test_table'
        }
        self.chunk_processor.generate_chunk_queries.return_value = [
            'SELECT * FROM test_table LIMIT 100 OFFSET 0'
        ]
        self.orchestrator._process_chunks_with_progress = Mock(return_value=['test_file.parquet'])
        self.file_transfer_manager.transfer_files.return_value = False
        progress_callback = Mock()
        
        result = self.orchestrator.transfer_query_with_progress(
            'SELECT * FROM test_table', progress_callback=progress_callback
        )
        
        self.assertFalse(result)

    def test_transfer_query_with_progress_exception(self):
        """Test transfer_query_with_progress with exception."""
        self.connection_manager.connect.side_effect = Exception("Connection error")
        progress_callback = Mock()
        
        with self.assertLogs('root', level='ERROR') as cm:
            result = self.orchestrator.transfer_query_with_progress(
                'SELECT * FROM test_table', progress_callback=progress_callback
            )
        
        self.assertFalse(result)
        self.assertTrue(any('Transfer failed' in msg for msg in cm.output))
        progress_callback.assert_any_call("Transfer failed: Connection error", -1)

    def test_process_chunks_with_progress_success(self):
        """Test _process_chunks_with_progress with successful processing."""
        queries = ['query1', 'query2']
        progress_callback = Mock()
        
        # Mock the chunk processor to return file paths
        self.chunk_processor.process_chunk.side_effect = [
            'test_file_0.parquet',
            'test_file_1.parquet'
        ]
        
        with patch('concurrent.futures.ThreadPoolExecutor') as mock_executor:
            # Mock the executor context manager
            mock_executor_instance = Mock()
            mock_executor.return_value.__enter__.return_value = mock_executor_instance
            
            # Mock submit to return futures
            futures = []
            for i in range(2):
                future = Mock()
                future.result.return_value = f'test_file_{i}.parquet'
                futures.append(future)
            
            # Mock submit to return different futures for each call
            mock_executor_instance.submit.side_effect = futures
            
            # Mock as_completed to return the same futures
            with patch('concurrent.futures.as_completed', return_value=futures):
                result = self.orchestrator._process_chunks_with_progress(queries, 'parquet', progress_callback)
        
        self.assertEqual(len(result), 2)
        progress_callback.assert_any_call("Processed chunk 1/2", 50.0)
        progress_callback.assert_any_call("Processed chunk 2/2", 80.0)

    def test_process_chunks_with_progress_failure(self):
        """Test _process_chunks_with_progress with chunk failure."""
        queries = ['query1', 'query2']
        progress_callback = Mock()
        
        # Mock the chunk processor to return file path for first call, raise exception for second
        self.chunk_processor.process_chunk.side_effect = [
            'test_file_0.parquet',
            Exception("Chunk processing failed")
        ]
        
        with patch('concurrent.futures.ThreadPoolExecutor') as mock_executor:
            # Mock the executor context manager
            mock_executor_instance = Mock()
            mock_executor.return_value.__enter__.return_value = mock_executor_instance
            
            # Mock submit to return futures
            futures = []
            for i in range(2):
                future = Mock()
                if i == 0:  # First future succeeds
                    future.result.return_value = f'test_file_{i}.parquet'
                else:  # Second future fails
                    future.result.side_effect = Exception("Chunk processing failed")
                futures.append(future)
            
            # Mock submit to return different futures for each call
            mock_executor_instance.submit.side_effect = futures
            
            # Mock as_completed to return the same futures
            with patch('concurrent.futures.as_completed', return_value=futures):
                with patch('impala_transfer.orchestrator.logging') as mock_logging:
                    result = self.orchestrator._process_chunks_with_progress(queries, 'parquet', progress_callback)
        
        self.assertEqual(result, [])
        mock_logging.error.assert_called()

    def test_get_transfer_status(self):
        """Test get_transfer_status method."""
        self.connection_manager.get_connection_info.return_value = {'status': 'connected'}
        self.file_transfer_manager.get_transfer_info.return_value = {'files_transferred': 5}
        
        status = self.orchestrator.get_transfer_status()
        
        expected = {
            'connection_info': {'status': 'connected'},
            'transfer_info': {'files_transferred': 5},
            'max_workers': 2
        }
        self.assertEqual(status, expected)
        self.connection_manager.get_connection_info.assert_called_once()
        self.file_transfer_manager.get_transfer_info.assert_called_once()


if __name__ == '__main__':
    unittest.main() 