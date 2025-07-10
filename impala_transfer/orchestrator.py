"""
Transfer orchestration module.
Coordinates the complete transfer process between components.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from .connection import ConnectionManager
from .query import QueryExecutor
from .chunking import ChunkProcessor
from .transfer import FileTransferManager
from .utils import FileManager


class TransferOrchestrator:
    """Orchestrates the entire transfer process."""
    
    def __init__(self, connection_manager: ConnectionManager, chunk_processor: ChunkProcessor,
                 file_transfer_manager: FileTransferManager, max_workers: int = 4):
        """
        Initialize transfer orchestrator.
        
        Args:
            connection_manager: Connection manager instance
            chunk_processor: Chunk processor instance
            file_transfer_manager: File transfer manager instance
            max_workers: Maximum number of parallel workers
        """
        self.connection_manager = connection_manager
        self.chunk_processor = chunk_processor
        self.file_transfer_manager = file_transfer_manager
        self.max_workers = max_workers
        self.query_executor = QueryExecutor(connection_manager)
    
    def transfer_query(self, query: str, target_table: str = None, 
                      output_format: str = 'parquet') -> bool:
        """
        Main method to transfer query results from cluster 1 to cluster 2.
        
        Args:
            query: SQL query to execute on source cluster
            target_table: Target table name (defaults to 'query_result')
            output_format: Output format (parquet or csv)
            
        Returns:
            bool: True if transfer successful, False otherwise
        """
        if target_table is None:
            target_table = 'query_result'
        
        logging.info(f"Starting transfer of query results")
        logging.info(f"Query: {query}")
        start_time = time.time()
        
        try:
            # Connect to database
            if not self.connection_manager.connect():
                return False
            
            # Get query information
            query_info = self.query_executor.get_query_info(query)
            logging.info(f"Query result: {query_info['row_count']} rows")
            
            # Validate chunk size
            if not self.chunk_processor.validate_chunk_size(query_info['row_count']):
                logging.warning("Chunk size validation failed, but continuing...")
            
            # Generate chunk queries
            queries = self.chunk_processor.generate_chunk_queries(query, query_info['row_count'])
            logging.info(f"Generated {len(queries)} chunks for parallel processing")
            
            # Process chunks in parallel
            filepaths = self._process_chunks_parallel(queries, output_format)
            if not filepaths:
                return False
            
            # Transfer to cluster 2
            if not self.file_transfer_manager.transfer_files(filepaths, target_table):
                return False
            
            # Cleanup
            FileManager.cleanup_temp_files(filepaths)
            
            total_time = time.time() - start_time
            logging.info(f"Transfer completed successfully in {total_time:.2f} seconds")
            
            return True
            
        except Exception as e:
            logging.error(f"Transfer failed: {e}")
            return False
        finally:
            self.connection_manager.close()
    
    def _process_chunks_parallel(self, queries: List[str], output_format: str) -> List[str]:
        """
        Process chunks in parallel using ThreadPoolExecutor.
        
        Args:
            queries: List of chunk queries to process
            output_format: Output format for files
            
        Returns:
            List[str]: List of generated file paths, empty list if any chunk fails
        """
        filepaths = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunk processing tasks
            future_to_chunk = {
                executor.submit(self.chunk_processor.process_chunk, i, chunk_query, 
                              self.query_executor, output_format): i 
                for i, chunk_query in enumerate(queries)
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_chunk):
                chunk_id = future_to_chunk[future]
                try:
                    filepath = future.result()
                    filepaths.append(filepath)
                    logging.info(f"Chunk {chunk_id} completed: {filepath}")
                except Exception as e:
                    logging.error(f"Chunk {chunk_id} failed: {e}")
                    return []  # Return empty list if any chunk fails
        
        return filepaths
    
    def transfer_query_with_progress(self, query: str, target_table: str = None,
                                   output_format: str = 'parquet', progress_callback=None) -> bool:
        """
        Transfer query with progress reporting.
        
        Args:
            query: SQL query to execute
            target_table: Target table name
            output_format: Output format
            progress_callback: Callback function for progress updates
            
        Returns:
            bool: True if transfer successful
        """
        if target_table is None:
            target_table = 'query_result'
        
        try:
            # Connect to database
            if progress_callback:
                progress_callback("Connecting to database...", 0)
            
            if not self.connection_manager.connect():
                return False
            
            # Get query information
            if progress_callback:
                progress_callback("Analyzing query...", 10)
            
            query_info = self.query_executor.get_query_info(query)
            logging.info(f"Query result: {query_info['row_count']} rows")
            
            # Generate chunk queries
            queries = self.chunk_processor.generate_chunk_queries(query, query_info['row_count'])
            logging.info(f"Generated {len(queries)} chunks for parallel processing")
            
            if progress_callback:
                progress_callback(f"Processing {len(queries)} chunks...", 20)
            
            # Process chunks with progress
            filepaths = self._process_chunks_with_progress(queries, output_format, progress_callback)
            if not filepaths:
                return False
            
            # Transfer to cluster 2
            if progress_callback:
                progress_callback("Transferring files to target cluster...", 90)
            
            if not self.file_transfer_manager.transfer_files(filepaths, target_table):
                return False
            
            # Cleanup
            if progress_callback:
                progress_callback("Cleaning up temporary files...", 95)
            
            FileManager.cleanup_temp_files(filepaths)
            
            if progress_callback:
                progress_callback("Transfer completed successfully!", 100)
            
            return True
            
        except Exception as e:
            logging.error(f"Transfer failed: {e}")
            if progress_callback:
                progress_callback(f"Transfer failed: {e}", -1)
            return False
        finally:
            self.connection_manager.close()
    
    def _process_chunks_with_progress(self, queries: List[str], output_format: str, 
                                    progress_callback) -> List[str]:
        """
        Process chunks with progress reporting.
        
        Args:
            queries: List of chunk queries
            output_format: Output format
            progress_callback: Progress callback function
            
        Returns:
            List[str]: List of generated file paths
        """
        filepaths = []
        completed_chunks = 0
        total_chunks = len(queries)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunk processing tasks
            future_to_chunk = {
                executor.submit(self.chunk_processor.process_chunk, i, chunk_query, 
                              self.query_executor, output_format): i 
                for i, chunk_query in enumerate(queries)
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_chunk):
                chunk_id = future_to_chunk[future]
                try:
                    filepath = future.result()
                    filepaths.append(filepath)
                    completed_chunks += 1
                    
                    # Calculate progress (20% to 80% for chunk processing)
                    progress = 20 + (completed_chunks / total_chunks) * 60
                    if progress_callback:
                        progress_callback(f"Processed chunk {chunk_id + 1}/{total_chunks}", progress)
                    
                    logging.info(f"Chunk {chunk_id} completed: {filepath}")
                except Exception as e:
                    logging.error(f"Chunk {chunk_id} failed: {e}")
                    return []
        
        return filepaths
    
    def get_transfer_status(self) -> dict:
        """
        Get current transfer status information.
        
        Returns:
            dict: Transfer status information
        """
        return {
            'connection_info': self.connection_manager.get_connection_info(),
            'transfer_info': self.file_transfer_manager.get_transfer_info(),
            'max_workers': self.max_workers
        } 