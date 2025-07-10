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


if __name__ == '__main__':
    unittest.main() 