"""
Core module containing the main ImpalaTransferTool class.
Provides the main interface for the transfer tool.
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

from .connection import ConnectionManager, get_available_connection_types, validate_connection_type
from .chunking import ChunkProcessor
from .transfer import FileTransferManager
from .orchestrator import TransferOrchestrator
from .utils import FileManager


class ImpalaTransferTool:
    """Main transfer tool class - orchestrates all components.
    
    This class provides the main interface for transferring data between database
    clusters. It orchestrates connection management, query execution, chunking,
    and file transfer operations.
    """
    
    def __init__(self, 
                 source_host: Optional[str] = None,
                 source_port: int = 21050,
                 source_database: str = "default",
                 target_hdfs_path: Optional[str] = None,
                 chunk_size: int = 1000000,
                 max_workers: int = 4,
                 temp_dir: str = "/tmp/impala_transfer",
                 connection_type: str = "auto",
                 odbc_driver: Optional[str] = None,
                 odbc_connection_string: Optional[str] = None,
                 sqlalchemy_url: Optional[str] = None,
                 sqlalchemy_engine_kwargs: Optional[dict] = None,
                 use_distcp: bool = True,
                 source_hdfs_path: Optional[str] = None,
                 target_cluster: Optional[str] = None):
        """Initialize the transfer tool.
        
        :param source_host: Database host for cluster 1 (not needed for SQLAlchemy URL)
        :type source_host: Optional[str]
        :param source_port: Database port (default 21050 for Impala)
        :type source_port: int
        :param source_database: Source database name
        :type source_database: str
        :param target_hdfs_path: HDFS path on cluster 2 for data landing
        :type target_hdfs_path: Optional[str]
        :param chunk_size: Number of rows per chunk for parallel processing
        :type chunk_size: int
        :param max_workers: Number of parallel workers
        :type max_workers: int
        :param temp_dir: Temporary directory for intermediate files
        :type temp_dir: str
        :param connection_type: "auto", "impyla", "pyodbc", or "sqlalchemy" (default: "auto")
        :type connection_type: str
        :param odbc_driver: ODBC driver name (required for pyodbc)
        :type odbc_driver: Optional[str]
        :param odbc_connection_string: Full ODBC connection string (alternative to individual params)
        :type odbc_connection_string: Optional[str]
        :param sqlalchemy_url: SQLAlchemy connection URL (e.g., "postgresql://user:pass@host:port/db")
        :type sqlalchemy_url: Optional[str]
        :param sqlalchemy_engine_kwargs: Additional kwargs for SQLAlchemy engine creation
        :type sqlalchemy_engine_kwargs: Optional[dict]
        :raises ValueError: If connection type is invalid or configuration is missing
        """
        self._validate_and_set_connection_type(connection_type)
        self._validate_sqlalchemy_config(connection_type, sqlalchemy_url)
        
        # Setup components
        self._setup_logging(temp_dir)
        FileManager.ensure_temp_directory(temp_dir)
        
        # Create component instances
        connection_kwargs = self._build_connection_kwargs(
            source_host, source_port, source_database, odbc_driver, 
            odbc_connection_string, sqlalchemy_url, sqlalchemy_engine_kwargs
        )
        
        self.connection_manager = ConnectionManager(self.connection_type, **connection_kwargs)
        self.chunk_processor = ChunkProcessor(chunk_size, temp_dir)
        self.file_transfer_manager = FileTransferManager(
            target_hdfs_path, use_distcp, source_hdfs_path, target_cluster
        )
        self.orchestrator = TransferOrchestrator(
            self.connection_manager, self.chunk_processor, 
            self.file_transfer_manager, max_workers
        )
    
    def _validate_and_set_connection_type(self, connection_type: str) -> None:
        """Validate and set connection type.
        
        :param connection_type: Connection type to validate
        :type connection_type: str
        :raises ValueError: If connection type is invalid or not available
        """
        if connection_type == "auto":
            available_types = get_available_connection_types()
            if not available_types:
                raise ValueError("No database connection libraries available")
            self.connection_type = available_types[0]  # Use first available
        else:
            self.connection_type = connection_type
        
        # Validate connection type availability
        if not validate_connection_type(self.connection_type):
            available_types = get_available_connection_types()
            raise ValueError(f"{self.connection_type} requested but not available. "
                           f"Available types: {available_types}")
    
    def _validate_sqlalchemy_config(self, connection_type: str, sqlalchemy_url: Optional[str]) -> None:
        """Validate SQLAlchemy configuration.
        
        :param connection_type: Connection type being used
        :type connection_type: str
        :param sqlalchemy_url: SQLAlchemy URL to validate
        :type sqlalchemy_url: Optional[str]
        :raises ValueError: If SQLAlchemy URL is missing when required
        """
        if connection_type == "sqlalchemy" and not sqlalchemy_url:
            raise ValueError("SQLAlchemy URL must be provided when using sqlalchemy connection type")
    
    def _build_connection_kwargs(self, source_host: Optional[str], source_port: int, 
                                source_database: str, odbc_driver: Optional[str], 
                                odbc_connection_string: Optional[str], sqlalchemy_url: Optional[str], 
                                sqlalchemy_engine_kwargs: Optional[dict]) -> dict:
        """Build connection kwargs based on connection type.
        
        :param source_host: Database host
        :type source_host: Optional[str]
        :param source_port: Database port
        :type source_port: int
        :param source_database: Database name
        :type source_database: str
        :param odbc_driver: ODBC driver name
        :type odbc_driver: Optional[str]
        :param odbc_connection_string: Full ODBC connection string
        :type odbc_connection_string: Optional[str]
        :param sqlalchemy_url: SQLAlchemy connection URL
        :type sqlalchemy_url: Optional[str]
        :param sqlalchemy_engine_kwargs: Additional SQLAlchemy engine kwargs
        :type sqlalchemy_engine_kwargs: Optional[dict]
        :return: Connection kwargs dictionary
        :rtype: dict
        """
        kwargs = {
            'source_host': source_host,
            'source_port': source_port,
            'source_database': source_database,
        }
        
        if self.connection_type == "pyodbc":
            kwargs.update({
                'odbc_driver': odbc_driver,
                'odbc_connection_string': odbc_connection_string
            })
        elif self.connection_type == "sqlalchemy":
            kwargs.update({
                'sqlalchemy_url': sqlalchemy_url,
                'sqlalchemy_engine_kwargs': sqlalchemy_engine_kwargs or {}
            })
        
        return kwargs
    
    def _setup_logging(self, temp_dir: str) -> None:
        """Setup logging configuration.
        
        Configures logging with both file and console handlers, using
        a timestamp-based log file name to avoid conflicts.
        """
        os.makedirs(temp_dir, exist_ok=True)
        log_file = os.path.join(temp_dir, f'impala_transfer_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def transfer_query(self, query: str, target_table: Optional[str] = None, 
                      output_format: str = 'parquet') -> bool:
        """Transfer query results from cluster 1 to cluster 2.
        
        :param query: SQL query to execute
        :type query: str
        :param target_table: Target table name (defaults to 'query_result')
        :type target_table: Optional[str]
        :param output_format: Output format (parquet or csv)
        :type output_format: str
        :return: True if transfer successful, False otherwise
        :rtype: bool
        """
        return self.orchestrator.transfer_query(query, target_table, output_format)
    
    def transfer_table(self, table_name: str, target_table: Optional[str] = None, 
                      output_format: str = 'parquet') -> bool:
        """Convenience method to transfer an entire table.
        
        :param table_name: Source table name
        :type table_name: str
        :param target_table: Target table name (defaults to source table name)
        :type target_table: Optional[str]
        :param output_format: Output format (parquet or csv)
        :type output_format: str
        :return: True if transfer successful, False otherwise
        :rtype: bool
        """
        query = f"SELECT * FROM {table_name}"
        return self.transfer_query(query, target_table, output_format)
    
    def transfer_query_with_progress(self, query: str, target_table: Optional[str] = None,
                                   output_format: str = 'parquet', progress_callback: Optional[callable] = None) -> bool:
        """Transfer query with progress reporting.
        
        :param query: SQL query to execute
        :type query: str
        :param target_table: Target table name
        :type target_table: Optional[str]
        :param output_format: Output format
        :type output_format: str
        :param progress_callback: Callback function for progress updates
        :type progress_callback: Optional[callable]
        :return: True if transfer successful
        :rtype: bool
        """
        return self.orchestrator.transfer_query_with_progress(
            query, target_table, output_format, progress_callback
        )
    
    def get_configuration(self) -> Dict[str, Any]:
        """Get current configuration information.
        
        :return: Configuration information dictionary
        :rtype: Dict[str, Any]
        """
        return {
            'connection_type': self.connection_type,
            'connection_info': self.connection_manager.get_connection_info(),
            'chunk_size': self.chunk_processor.chunk_size,
            'max_workers': self.orchestrator.max_workers,
            'temp_dir': self.chunk_processor.temp_dir,
            'transfer_info': self.file_transfer_manager.get_transfer_info(),
            'available_connection_types': get_available_connection_types()
        }
    
    def test_connection(self) -> bool:
        """Test the database connection.
        
        :return: True if connection test successful
        :rtype: bool
        """
        try:
            if not self.connection_manager.connect():
                return False
            
            return self.orchestrator.query_executor.test_connection()
        finally:
            self.connection_manager.close()
    
    def validate_configuration(self) -> bool:
        """Validate the current configuration.
        
        :return: True if configuration is valid
        :rtype: bool
        """
        try:
            # Validate connection type
            if not validate_connection_type(self.connection_type):
                return False
            
            # Validate SQLAlchemy config if needed
            if self.connection_type == "sqlalchemy":
                if not hasattr(self.connection_manager, 'kwargs') or \
                   'sqlalchemy_url' not in self.connection_manager.kwargs:
                    return False
            
            # Validate chunk processor
            if self.chunk_processor.chunk_size <= 0:
                return False
            
            # Validate transfer manager
            if not self.file_transfer_manager.validate_transfer_config():
                return False
            
            return True
        except Exception as e:
            logging.error(f"Configuration validation failed: {e}")
            return False
    
    def create_table_as_select(self, query: str, target_table: str,
                              file_format: str = 'PARQUET',
                              compression: str = 'SNAPPY',
                              location: Optional[str] = None,
                              partitioned_by: Optional[List[str]] = None,
                              clustered_by: Optional[List[str]] = None,
                              buckets: Optional[int] = None,
                              overwrite: bool = False) -> bool:
        """Create a table using CREATE TABLE AS SELECT (CTAS).
        
        :param query: SELECT query to execute
        :type query: str
        :param target_table: Name of the table to create
        :type target_table: str
        :param file_format: File format for the table (PARQUET, TEXTFILE, etc.)
        :type file_format: str
        :param compression: Compression format (SNAPPY, GZIP, etc.)
        :type compression: str
        :param location: HDFS location for the table data
        :type location: Optional[str]
        :param partitioned_by: List of columns to partition by
        :type partitioned_by: Optional[List[str]]
        :param clustered_by: List of columns to cluster by
        :type clustered_by: Optional[List[str]]
        :param buckets: Number of buckets for clustering
        :type buckets: Optional[int]
        :param overwrite: Whether to overwrite existing table
        :type overwrite: bool
        :return: True if CTAS operation successful
        :rtype: bool
        """
        try:
            if not self.connection_manager.connect():
                return False
            
            success = self.orchestrator.query_executor.execute_ctas(
                query, target_table, file_format, compression, location,
                partitioned_by, clustered_by, buckets, overwrite
            )
            
            return success
        finally:
            self.connection_manager.close()
    
    def create_table_as_select_with_progress(self, query: str, target_table: str,
                                           file_format: str = 'PARQUET',
                                           compression: str = 'SNAPPY',
                                           location: Optional[str] = None,
                                           partitioned_by: Optional[List[str]] = None,
                                           clustered_by: Optional[List[str]] = None,
                                           buckets: Optional[int] = None,
                                           overwrite: bool = False,
                                           progress_callback: Optional[callable] = None) -> bool:
        """Create a table using CTAS with progress reporting.
        
        :param query: SELECT query to execute
        :type query: str
        :param target_table: Name of the table to create
        :type target_table: str
        :param file_format: File format for the table (PARQUET, TEXTFILE, etc.)
        :type file_format: str
        :param compression: Compression format (SNAPPY, GZIP, etc.)
        :type compression: str
        :param location: HDFS location for the table data
        :type location: Optional[str]
        :param partitioned_by: List of columns to partition by
        :type partitioned_by: Optional[List[str]]
        :param clustered_by: List of columns to cluster by
        :type clustered_by: Optional[List[str]]
        :param buckets: Number of buckets for clustering
        :type buckets: Optional[int]
        :param overwrite: Whether to overwrite existing table
        :type overwrite: bool
        :param progress_callback: Callback function for progress updates
        :type progress_callback: Optional[callable]
        :return: True if CTAS operation successful
        :rtype: bool
        """
        try:
            if progress_callback:
                progress_callback("Connecting to database...", 0)
            
            if not self.connection_manager.connect():
                return False
            
            if progress_callback:
                progress_callback("Analyzing query...", 20)
            
            # Get query info to estimate progress
            query_info = self.orchestrator.query_executor.get_query_info(query)
            
            if progress_callback:
                progress_callback(f"Executing CTAS for {query_info['row_count']} rows...", 50)
            
            success = self.orchestrator.query_executor.execute_ctas(
                query, target_table, file_format, compression, location,
                partitioned_by, clustered_by, buckets, overwrite
            )
            
            if progress_callback:
                if success:
                    progress_callback("CTAS operation completed successfully!", 100)
                else:
                    progress_callback("CTAS operation failed!", -1)
            
            return success
        finally:
            self.connection_manager.close()
    
    def drop_table(self, table_name: str, if_exists: bool = True) -> bool:
        """Drop a table.
        
        :param table_name: Name of the table to drop
        :type table_name: str
        :param if_exists: Whether to add IF EXISTS clause
        :type if_exists: bool
        :return: True if table dropped successfully
        :rtype: bool
        """
        try:
            if not self.connection_manager.connect():
                return False
            
            success = self.orchestrator.query_executor.drop_table(table_name, if_exists)
            return success
        finally:
            self.connection_manager.close()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.
        
        :param table_name: Name of the table to check
        :type table_name: str
        :return: True if table exists
        :rtype: bool
        """
        try:
            if not self.connection_manager.connect():
                return False
            
            exists = self.orchestrator.query_executor.table_exists(table_name)
            return exists
        finally:
            self.connection_manager.close() 