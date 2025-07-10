#!/usr/bin/env python3
"""
Test suite for the chunking module.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import os
import pandas as pd

from impala_transfer.chunking import ChunkProcessor


class TestChunkProcessor(unittest.TestCase):
    """Test the ChunkProcessor class."""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.processor = ChunkProcessor(chunk_size=100, temp_dir=self.temp_dir)
        self.query_executor = Mock()
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_generate_chunk_queries(self):
        """Test chunk query generation."""
        base_query = "SELECT * FROM test_table"
        total_rows = 250
        
        queries = self.processor.generate_chunk_queries(base_query, total_rows)
        
        self.assertEqual(len(queries), 3)  # 250 rows / 100 chunk_size + 1
        self.assertIn("LIMIT 100 OFFSET 0", queries[0])
        self.assertIn("LIMIT 100 OFFSET 100", queries[1])
        self.assertIn("LIMIT 100 OFFSET 200", queries[2])
    
    def test_generate_chunk_queries_exact_multiple(self):
        """Test chunk query generation when total_rows is exact multiple of chunk_size."""
        base_query = "SELECT * FROM test_table"
        total_rows = 200
        
        queries = self.processor.generate_chunk_queries(base_query, total_rows)
        
        self.assertEqual(len(queries), 3)  # 200 rows / 100 chunk_size + 1 (always adds 1)
        self.assertIn("LIMIT 100 OFFSET 0", queries[0])
        self.assertIn("LIMIT 100 OFFSET 100", queries[1])
        self.assertIn("LIMIT 100 OFFSET 200", queries[2])
    
    def test_generate_chunk_queries_small_dataset(self):
        """Test chunk query generation for small datasets."""
        base_query = "SELECT * FROM test_table"
        total_rows = 50
        
        queries = self.processor.generate_chunk_queries(base_query, total_rows)
        
        self.assertEqual(len(queries), 1)  # 50 rows < 100 chunk_size
        self.assertIn("LIMIT 100 OFFSET 0", queries[0])
    
    def test_process_chunk_parquet(self):
        """Test processing a chunk to parquet format."""
        # Mock query execution result
        mock_data = [('row1', 'val1'), ('row2', 'val2')]
        self.query_executor.execute_query.return_value = mock_data
        
        filepath = self.processor.process_chunk(1, "SELECT * FROM test", self.query_executor, 'parquet')
        
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(filepath.endswith('.parquet'))
        
        # Verify the parquet file contains the data
        df = pd.read_parquet(filepath)
        self.assertEqual(len(df), 2)
    
    def test_process_chunk_csv(self):
        """Test processing a chunk to CSV format."""
        mock_data = [('row1', 'val1'), ('row2', 'val2')]
        self.query_executor.execute_query.return_value = mock_data
        
        filepath = self.processor.process_chunk(1, "SELECT * FROM test", self.query_executor, 'csv')
        
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(filepath.endswith('.csv'))
    
    def test_process_chunk_invalid_format(self):
        """Test processing with invalid output format."""
        mock_data = [('row1', 'val1')]
        self.query_executor.execute_query.return_value = mock_data
        
        with self.assertRaises(ValueError):
            self.processor.process_chunk(1, "SELECT * FROM test", self.query_executor, 'invalid_format')

    def test_process_chunk_exception_handling(self):
        """Test process_chunk with exception handling."""
        self.query_executor.execute_query.side_effect = Exception("Query failed")
        
        with self.assertLogs('root', level='ERROR') as cm:
            with self.assertRaises(Exception):
                self.processor.process_chunk(1, "SELECT * FROM test", self.query_executor, 'parquet')
        
        self.assertTrue(any('Error processing chunk 1' in msg for msg in cm.output))

    def test_process_chunk_with_batching_parquet(self):
        """Test processing a chunk with batching to parquet format."""
        mock_data = [('row1', 'val1'), ('row2', 'val2'), ('row3', 'val3')]
        self.query_executor.execute_query_with_batching.return_value = mock_data
        
        filepath = self.processor.process_chunk_with_batching(
            1, "SELECT * FROM test", self.query_executor, 'parquet', batch_size=2
        )
        
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(filepath.endswith('.parquet'))
        
        # Verify the parquet file contains the data
        df = pd.read_parquet(filepath)
        self.assertEqual(len(df), 3)
        
        self.query_executor.execute_query_with_batching.assert_called_once_with(
            "SELECT * FROM test", 2
        )

    def test_process_chunk_with_batching_csv(self):
        """Test processing a chunk with batching to CSV format."""
        mock_data = [('row1', 'val1'), ('row2', 'val2')]
        self.query_executor.execute_query_with_batching.return_value = mock_data
        
        filepath = self.processor.process_chunk_with_batching(
            1, "SELECT * FROM test", self.query_executor, 'csv', batch_size=1000
        )
        
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(filepath.endswith('.csv'))
        
        self.query_executor.execute_query_with_batching.assert_called_once_with(
            "SELECT * FROM test", 1000
        )

    def test_process_chunk_with_batching_invalid_format(self):
        """Test process_chunk_with_batching with invalid output format."""
        mock_data = [('row1', 'val1')]
        self.query_executor.execute_query_with_batching.return_value = mock_data
        
        with self.assertRaises(ValueError):
            self.processor.process_chunk_with_batching(
                1, "SELECT * FROM test", self.query_executor, 'invalid_format'
            )

    def test_process_chunk_with_batching_exception_handling(self):
        """Test process_chunk_with_batching with exception handling."""
        self.query_executor.execute_query_with_batching.side_effect = Exception("Query failed")
        
        with self.assertLogs('root', level='ERROR') as cm:
            with self.assertRaises(Exception):
                self.processor.process_chunk_with_batching(
                    1, "SELECT * FROM test", self.query_executor, 'parquet'
                )
        
        self.assertTrue(any('Error processing chunk 1' in msg for msg in cm.output))

    def test_get_chunk_info(self):
        """Test get_chunk_info method."""
        base_query = "SELECT * FROM test_table"
        total_rows = 250
        
        info = self.processor.get_chunk_info(base_query, total_rows)
        
        self.assertEqual(info['total_rows'], 250)
        self.assertEqual(info['chunk_size'], 100)
        self.assertEqual(info['num_chunks'], 3)
        self.assertEqual(info['estimated_chunk_sizes'], [100, 100, 50])

    def test_get_chunk_info_exact_multiple(self):
        """Test get_chunk_info when total_rows is exact multiple of chunk_size."""
        base_query = "SELECT * FROM test_table"
        total_rows = 200
        
        info = self.processor.get_chunk_info(base_query, total_rows)
        
        self.assertEqual(info['total_rows'], 200)
        self.assertEqual(info['chunk_size'], 100)
        self.assertEqual(info['num_chunks'], 3)  # Always adds 1
        self.assertEqual(info['estimated_chunk_sizes'], [100, 100, 0])  # Last chunk is empty

    def test_get_chunk_info_small_dataset(self):
        """Test get_chunk_info for small datasets."""
        base_query = "SELECT * FROM test_table"
        total_rows = 50
        
        info = self.processor.get_chunk_info(base_query, total_rows)
        
        self.assertEqual(info['total_rows'], 50)
        self.assertEqual(info['chunk_size'], 100)
        self.assertEqual(info['num_chunks'], 1)
        self.assertEqual(info['estimated_chunk_sizes'], [50])

    def test_validate_chunk_size_valid(self):
        """Test validate_chunk_size with valid chunk size."""
        result = self.processor.validate_chunk_size(1000)
        self.assertTrue(result)

    def test_validate_chunk_size_zero_chunk(self):
        """Test validate_chunk_size with zero chunk size."""
        processor = ChunkProcessor(chunk_size=0, temp_dir=self.temp_dir)
        result = processor.validate_chunk_size(1000)
        self.assertFalse(result)

    def test_validate_chunk_size_negative_chunk(self):
        """Test validate_chunk_size with negative chunk size."""
        processor = ChunkProcessor(chunk_size=-100, temp_dir=self.temp_dir)
        result = processor.validate_chunk_size(1000)
        self.assertFalse(result)

    def test_validate_chunk_size_zero_rows(self):
        """Test validate_chunk_size with zero total rows."""
        result = self.processor.validate_chunk_size(0)
        self.assertFalse(result)

    def test_validate_chunk_size_negative_rows(self):
        """Test validate_chunk_size with negative total rows."""
        result = self.processor.validate_chunk_size(-100)
        self.assertFalse(result)

    def test_validate_chunk_size_too_small(self):
        """Test validate_chunk_size with chunk size too small."""
        processor = ChunkProcessor(chunk_size=50, temp_dir=self.temp_dir)
        
        with self.assertLogs('root', level='WARNING') as cm:
            result = processor.validate_chunk_size(1000)
        
        self.assertFalse(result)
        self.assertTrue(any('Chunk size 50 is very small' in msg for msg in cm.output))

    def test_validate_chunk_size_too_large(self):
        """Test validate_chunk_size with chunk size too large."""
        processor = ChunkProcessor(chunk_size=600, temp_dir=self.temp_dir)
        
        with self.assertLogs('root', level='WARNING') as cm:
            result = processor.validate_chunk_size(1000)
        
        self.assertFalse(result)
        self.assertTrue(any('Chunk size 600 is very large relative to total rows 1000' in msg for msg in cm.output))

    def test_validate_chunk_size_boundary_case(self):
        """Test validate_chunk_size with boundary case (50% of total rows)."""
        processor = ChunkProcessor(chunk_size=500, temp_dir=self.temp_dir)
        
        # 500 is exactly 50% of 1000, so it should not trigger the warning
        result = processor.validate_chunk_size(1000)
        
        self.assertTrue(result)  # Should be valid since it's exactly 50%

    def test_validate_chunk_size_just_below_threshold(self):
        """Test validate_chunk_size with chunk size just below the large threshold."""
        processor = ChunkProcessor(chunk_size=499, temp_dir=self.temp_dir)
        result = processor.validate_chunk_size(1000)
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main() 