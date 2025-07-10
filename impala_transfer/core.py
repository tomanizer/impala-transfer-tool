"""
Core module containing the main ImpalaTransferTool class.
Provides the main interface for the transfer tool.
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

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
                 sqlalchemy_engine_kwargs: Optional[dict] = None):
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
        self._setup_logging()
        FileManager.ensure_temp_directory(temp_dir)
        
        # Create component instances
        connection_kwargs = self._build_connection_kwargs(
            source_host, source_port, source_database, odbc_driver, 
            odbc_connection_string, sqlalchemy_url, sqlalchemy_engine_kwargs
        )
        
        self.connection_manager = ConnectionManager(self.connection_type, **connection_kwargs)
        self.chunk_processor = ChunkProcessor(chunk_size, temp_dir)
        self.file_transfer_manager = FileTransferManager(target_hdfs_path)
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
    
    def _setup_logging(self) -> None:
        """Setup logging configuration.
        
        Configures logging with both file and console handlers, using
        a timestamp-based log file name to avoid conflicts.
        """
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'impala_transfer_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
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