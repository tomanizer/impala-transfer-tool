"""
Database Data Transfer Tool Package
Transfers large tables from Cluster 1 (Impala/ODBC/SQLAlchemy) to Cluster 2 (HDFS/Hive)
Optimized for performance and reliability with large datasets.
Supports Impyla, pyodbc, and SQLAlchemy connections.
"""

from .core import ImpalaTransferTool
from .connection import ConnectionManager
from .query import QueryExecutor
from .chunking import ChunkProcessor
from .transfer import FileTransferManager
from .utils import FileManager
from .orchestrator import TransferOrchestrator

__version__ = "2.0.0"
__author__ = "Data Transfer Team"

__all__ = [
    'ImpalaTransferTool',
    'ConnectionManager', 
    'QueryExecutor',
    'ChunkProcessor',
    'FileTransferManager',
    'FileManager',
    'TransferOrchestrator'
] 