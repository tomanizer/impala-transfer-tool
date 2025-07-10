"""
Data chunking and file processing module.
Handles chunking of large queries and processing chunks to files.
"""

import os
import logging
import pandas as pd
from datetime import datetime
from typing import List
from .query import QueryExecutor


class ChunkProcessor:
    """Handles chunking of large queries and parallel processing."""
    
    def __init__(self, chunk_size: int, temp_dir: str):
        """
        Initialize chunk processor.
        
        Args:
            chunk_size: Number of rows per chunk
            temp_dir: Temporary directory for storing chunk files
        """
        self.chunk_size = chunk_size
        self.temp_dir = temp_dir
    
    def generate_chunk_queries(self, base_query: str, total_rows: int) -> List[str]:
        """
        Generate queries for parallel chunk processing.
        
        Args:
            base_query: Base SQL query to chunk
            total_rows: Total number of rows in the query result
            
        Returns:
            List of chunked queries with LIMIT and OFFSET
        """
        queries = []
        num_chunks = (total_rows // self.chunk_size) + 1
        
        for i in range(num_chunks):
            offset = i * self.chunk_size
            query = f"{base_query} LIMIT {self.chunk_size} OFFSET {offset}"
            queries.append(query)
        
        return queries
    
    def process_chunk(self, chunk_id: int, query: str, query_executor: QueryExecutor, 
                     output_format: str = 'parquet') -> str:
        """
        Process a single chunk of data.
        
        Args:
            chunk_id: Unique identifier for the chunk
            query: SQL query for this chunk
            query_executor: Query executor instance
            output_format: Output format ('parquet' or 'csv')
            
        Returns:
            str: Path to the generated file
            
        Raises:
            ValueError: If output format is not supported
        """
        try:
            start_time = datetime.now()
            
            # Execute query and fetch data
            data = query_executor.execute_query(query)
            
            # Convert to pandas DataFrame
            df = pd.DataFrame(data)
            
            # Save to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"chunk_{chunk_id}_{timestamp}.{output_format}"
            filepath = os.path.join(self.temp_dir, filename)
            
            if output_format == 'parquet':
                df.to_parquet(filepath, index=False, compression='snappy')
            elif output_format == 'csv':
                df.to_csv(filepath, index=False, compression='gzip')
            else:
                raise ValueError(f"Unsupported output format: {output_format}")
            
            processing_time = (datetime.now() - start_time).total_seconds()
            logging.info(f"Chunk {chunk_id}: {len(data)} rows processed in {processing_time:.2f}s")
            
            return filepath
            
        except Exception as e:
            logging.error(f"Error processing chunk {chunk_id}: {e}")
            raise
    
    def process_chunk_with_batching(self, chunk_id: int, query: str, query_executor: QueryExecutor,
                                  output_format: str = 'parquet', batch_size: int = 10000) -> str:
        """
        Process a single chunk of data using batching for memory efficiency.
        
        Args:
            chunk_id: Unique identifier for the chunk
            query: SQL query for this chunk
            query_executor: Query executor instance
            output_format: Output format ('parquet' or 'csv')
            batch_size: Number of rows to process per batch
            
        Returns:
            str: Path to the generated file
        """
        try:
            start_time = datetime.now()
            
            # Execute query with batching
            data = query_executor.execute_query_with_batching(query, batch_size)
            
            # Convert to pandas DataFrame
            df = pd.DataFrame(data)
            
            # Save to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"chunk_{chunk_id}_{timestamp}.{output_format}"
            filepath = os.path.join(self.temp_dir, filename)
            
            if output_format == 'parquet':
                df.to_parquet(filepath, index=False, compression='snappy')
            elif output_format == 'csv':
                df.to_csv(filepath, index=False, compression='gzip')
            else:
                raise ValueError(f"Unsupported output format: {output_format}")
            
            processing_time = (datetime.now() - start_time).total_seconds()
            logging.info(f"Chunk {chunk_id}: {len(data)} rows processed in {processing_time:.2f}s")
            
            return filepath
            
        except Exception as e:
            logging.error(f"Error processing chunk {chunk_id}: {e}")
            raise
    
    def get_chunk_info(self, base_query: str, total_rows: int) -> dict:
        """
        Get information about chunking strategy.
        
        Args:
            base_query: Base SQL query
            total_rows: Total number of rows
            
        Returns:
            dict: Information about chunking strategy
        """
        queries = self.generate_chunk_queries(base_query, total_rows)
        
        return {
            'total_rows': total_rows,
            'chunk_size': self.chunk_size,
            'num_chunks': len(queries),
            'estimated_chunk_sizes': [
                min(self.chunk_size, total_rows - i * self.chunk_size)
                for i in range(len(queries))
            ]
        }
    
    def validate_chunk_size(self, total_rows: int) -> bool:
        """
        Validate if the chunk size is appropriate for the total rows.
        
        Args:
            total_rows: Total number of rows
            
        Returns:
            bool: True if chunk size is valid
        """
        if self.chunk_size <= 0:
            return False
        
        if total_rows <= 0:
            return False
        
        # Check if chunk size is reasonable (not too small, not too large)
        if self.chunk_size < 100:
            logging.warning(f"Chunk size {self.chunk_size} is very small")
            return False
        
        if self.chunk_size > total_rows * 0.5:
            logging.warning(f"Chunk size {self.chunk_size} is very large relative to total rows {total_rows}")
            return False
        
        return True 