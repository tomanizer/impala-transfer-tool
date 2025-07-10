# API Reference

This document provides detailed API documentation for the Impala Transfer Tool.

## Table of Contents

- [Core Classes](#core-classes)
- [Connection Management](#connection-management)
- [Query Execution](#query-execution)
- [Data Chunking](#data-chunking)
- [File Transfer](#file-transfer)
- [Utilities](#utilities)
- [Orchestration](#orchestration)
- [CLI Interface](#cli-interface)

## Core Classes

### ImpalaTransferTool

The main class for transferring data from database clusters to HDFS/Hive.

#### Constructor

```python
ImpalaTransferTool(
    source_host: str,
    source_port: int = 21050,
    source_database: str = "default",
    target_hdfs_path: str = None,
    chunk_size: int = 1000000,
    max_workers: int = 4,
    temp_dir: str = None,  # Used for temp files and log files
    connection_type: str = "auto",
    odbc_driver: str = None,
    odbc_connection_string: str = None,
    sqlalchemy_url: str = None,
    sqlalchemy_engine_kwargs: dict = None
)
```

**Parameters:**
- `source_host` (str): Hostname of the source database cluster
- `source_port` (int): Port number for the database connection (default: 21050)
- `source_database` (str): Database name to connect to (default: "default")
- `target_hdfs_path` (str): HDFS path for storing transferred files
- `chunk_size` (int): Number of rows per chunk (default: 1,000,000)
- `max_workers` (int): Maximum number of parallel workers (default: 4)
- `temp_dir` (str): Temporary directory for processing files **and log files** (default: `/tmp/impala_transfer`)
- `connection_type` (str): Database connection type ("impyla", "pyodbc", "sqlalchemy", "auto")
- `odbc_driver` (str): ODBC driver name for pyodbc connections
- `odbc_connection_string` (str): ODBC connection string
- `sqlalchemy_url` (str): SQLAlchemy connection URL
- `sqlalchemy_engine_kwargs` (dict): Additional SQLAlchemy engine parameters

#### Methods

##### transfer_table(table_name: str) -> bool

Transfer an entire table to the target cluster.

**Parameters:**
- `table_name` (str): Name of the table to transfer

**Returns:**
- `bool`: True if transfer successful, False otherwise

##### transfer_query(query: str, target_table: str = None, output_format: str = "parquet") -> bool

Transfer query results to the target cluster.

**Parameters:**
- `query` (str): SQL query to execute
- `target_table` (str): Target table name (defaults to "query_result")
- `output_format` (str): Output format ("parquet" or "csv")

**Returns:**
- `bool`: True if transfer successful, False otherwise

##### transfer_query_with_progress(query: str, target_table: str = None, output_format: str = "parquet", progress_callback=None) -> bool

Transfer query results with progress reporting.

**Parameters:**
- `query` (str): SQL query to execute
- `target_table` (str): Target table name (defaults to "query_result")
- `output_format` (str): Output format ("parquet" or "csv")
- `progress_callback` (callable): Function to call with progress updates

**Returns:**
- `bool`: True if transfer successful, False otherwise

## Connection Management

### ConnectionManager

Manages database connections for different connection types.

#### Constructor

```python
ConnectionManager(
    connection_type: str,
    source_host: str,
    source_port: int = 21050,
    source_database: str = "default",
    odbc_driver: str = None,
    odbc_connection_string: str = None,
    sqlalchemy_url: str = None,
    sqlalchemy_engine_kwargs: dict = None
)
```

#### Methods

##### connect() -> bool

Establish a connection to the database.

**Returns:**
- `bool`: True if connection successful, False otherwise

##### close()

Close the database connection.

##### get_connection_info() -> dict

Get information about the current connection.

**Returns:**
- `dict`: Connection information

## Query Execution

### QueryExecutor

Executes SQL queries and processes results.

#### Constructor

```python
QueryExecutor(connection_manager: ConnectionManager)
```

#### Methods

##### get_query_info(query: str) -> dict

Get information about a query before execution.

**Parameters:**
- `query` (str): SQL query to analyze

**Returns:**
- `dict`: Query information including row count and column names

##### execute_query(query: str) -> List[tuple]

Execute a SQL query and return results.

**Parameters:**
- `query` (str): SQL query to execute

**Returns:**
- `List[tuple]`: Query results as list of tuples

## Data Chunking

### ChunkProcessor

Handles data chunking and file processing.

#### Constructor

```python
ChunkProcessor(
    chunk_size: int = 1000000,
    temp_dir: str = None
)
```

#### Methods

##### generate_chunk_queries(base_query: str, total_rows: int) -> List[str]

Generate chunk queries for parallel processing.

**Parameters:**
- `base_query` (str): Base SQL query
- `total_rows` (int): Total number of rows to process

**Returns:**
- `List[str]`: List of chunk queries

##### process_chunk(chunk_id: int, query: str, query_executor: QueryExecutor, output_format: str = "parquet") -> str

Process a single chunk and save to file.

**Parameters:**
- `chunk_id` (int): Chunk identifier
- `query` (str): SQL query for this chunk
- `query_executor` (QueryExecutor): Query executor instance
- `output_format` (str): Output format ("parquet" or "csv")

**Returns:**
- `str`: Path to the generated file

##### validate_chunk_size(row_count: int) -> bool

Validate if the chunk size is appropriate for the data size.

**Parameters:**
- `row_count` (int): Total number of rows

**Returns:**
- `bool`: True if chunk size is valid

## File Transfer

### FileTransferManager

Handles file transfer operations to target clusters.

#### Constructor

```python
FileTransferManager(target_hdfs_path: str = None)
```

#### Methods

##### transfer_files(filepaths: List[str], target_table: str) -> bool

Transfer files to the target cluster.

**Parameters:**
- `filepaths` (List[str]): List of file paths to transfer
- `target_table` (str): Target table name

**Returns:**
- `bool`: True if transfer successful, False otherwise

##### get_transfer_info() -> dict

Get information about the transfer configuration.

**Returns:**
- `dict`: Transfer configuration information

## Utilities

### FileManager

Static utility class for file operations.

#### Static Methods

##### cleanup_temp_files(filepaths: List[str])

Clean up temporary files.

**Parameters:**
- `filepaths` (List[str]): List of file paths to remove

##### ensure_temp_directory(temp_dir: str)

Ensure temporary directory exists.

**Parameters:**
- `temp_dir` (str): Path to the temporary directory

##### get_file_size(filepath: str) -> int

Get file size in bytes.

**Parameters:**
- `filepath` (str): Path to the file

**Returns:**
- `int`: File size in bytes, -1 if file doesn't exist

##### format_file_size(size_bytes: int) -> str

Format file size in human-readable format.

**Parameters:**
- `size_bytes` (int): Size in bytes

**Returns:**
- `str`: Formatted size string

## Orchestration

### TransferOrchestrator

Orchestrates the complete transfer process.

#### Constructor

```python
TransferOrchestrator(
    connection_manager: ConnectionManager,
    chunk_processor: ChunkProcessor,
    file_transfer_manager: FileTransferManager,
    max_workers: int = 4
)
```

#### Methods

##### transfer_query(query: str, target_table: str = None, output_format: str = "parquet") -> bool

Main method to transfer query results from cluster 1 to cluster 2.

**Parameters:**
- `query` (str): SQL query to execute on source cluster
- `target_table` (str): Target table name (defaults to "query_result")
- `output_format` (str): Output format ("parquet" or "csv")

**Returns:**
- `bool`: True if transfer successful, False otherwise

##### transfer_query_with_progress(query: str, target_table: str = None, output_format: str = "parquet", progress_callback=None) -> bool

Transfer query with progress reporting.

**Parameters:**
- `query` (str): SQL query to execute
- `target_table` (str): Target table name (defaults to "query_result")
- `output_format` (str): Output format ("parquet" or "csv")
- `progress_callback` (callable): Function to call with progress updates

**Returns:**
- `bool`: True if transfer successful, False otherwise

## CLI Interface

### Command Line Arguments

#### Required Arguments

- `--source-host`: Hostname of the source database cluster
- `--table` or `--query`: Either table name or SQL query to transfer

#### Optional Arguments

- `--source-port`: Port number (default: 21050)
- `--source-database`: Database name (default: "default")
- `--connection-type`: Connection type ("impyla", "pyodbc", "sqlalchemy", "auto")
- `--target-hdfs-path`: HDFS path for storing files
- `--chunk-size`: Number of rows per chunk (default: 1,000,000)
- `--max-workers`: Maximum parallel workers (default: 4)
- `--output-format`: Output format ("parquet" or "csv")
- `--temp-dir`: Temporary directory for processing
- `--config`: Configuration file path
- `--verbose`: Enable verbose logging
- `--dry-run`: Show what would be done without executing

#### Connection-Specific Arguments

**For ODBC connections:**
- `--odbc-driver`: ODBC driver name
- `--odbc-connection-string`: ODBC connection string

**For SQLAlchemy connections:**
- `--sqlalchemy-url`: SQLAlchemy connection URL
- `--sqlalchemy-engine-kwargs`: JSON string of SQLAlchemy engine parameters

#### Utility Arguments

- `--test-connection`: Test database connection
- `--validate-config`: Validate configuration
- `--show-config`: Show current configuration
- `--query-file`: Read query from file

### Environment Variables

The CLI supports configuration via environment variables:

- `IMPALA_HOST`: Source host
- `IMPALA_PORT`: Source port
- `IMPALA_DATABASE`: Source database
- `CONNECTION_TYPE`: Connection type
- `TARGET_HDFS_PATH`: Target HDFS path
- `CHUNK_SIZE`: Chunk size
- `MAX_WORKERS`: Maximum workers
- `OUTPUT_FORMAT`: Output format
- `ODBC_DRIVER`: ODBC driver
- `SQLALCHEMY_URL`: SQLAlchemy URL

### Exit Codes

- `0`: Success
- `1`: General error
- `2`: Configuration error
- `3`: Connection error
- `4`: Query error
- `5`: Permission error
- `6`: Transfer error

## Error Handling

All classes include comprehensive error handling:

- **Connection errors**: Logged and return False
- **Query errors**: Logged with details and return False
- **File system errors**: Logged with cleanup attempts
- **Transfer errors**: Logged with retry information

## Logging

The tool uses Python's logging module with configurable levels:

- `DEBUG`: Detailed debugging information
- `INFO`: General information about operations
- `WARNING`: Warning messages for non-critical issues
- `ERROR`: Error messages for failed operations

Log format: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`

> **Log files**: All logs are written to the temp directory specified by `temp_dir` (default: `/tmp/impala_transfer`). 