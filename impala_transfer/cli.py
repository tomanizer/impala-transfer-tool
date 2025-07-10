#!/usr/bin/env python3
"""
Command-line interface for the Impala Transfer Tool.
Provides the main CLI functionality for the transfer tool.
"""

import sys
import json
import argparse
import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any

# Import core module conditionally to avoid pandas dependency issues during testing
try:
    from .core import ImpalaTransferTool
    CORE_AVAILABLE = True
except ImportError:
    CORE_AVAILABLE = False
    ImpalaTransferTool = None


def create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser.
    
    :return: Configured argument parser
    :rtype: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        description='Transfer query results from database cluster 1 to cluster 2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Transfer entire table using Impyla
  python -m impala_transfer.cli --source-host impala-cluster.example.com --table my_table

  # Transfer query results using pyodbc
  python -m impala_transfer.cli --source-host sql-server.example.com --connection-type pyodbc --odbc-driver "ODBC Driver 17 for SQL Server" --query "SELECT * FROM my_table WHERE date = '2024-01-01'"

  # Transfer using SQLAlchemy
  python -m impala_transfer.cli --connection-type sqlalchemy --sqlalchemy-url "postgresql://user:pass@host:5432/db" --query "SELECT * FROM my_table"

  # Transfer with custom settings
  python -m impala_transfer.cli --source-host impala-cluster.example.com --query "SELECT * FROM large_table" --chunk-size 500000 --max-workers 8 --output-format csv
        """
    )
    
    # Database connection arguments
    parser.add_argument('--source-host', help='Database host for cluster 1')
    parser.add_argument('--source-port', type=int, default=21050, help='Database port')
    parser.add_argument('--source-database', default='default', help='Source database')
    
    # Query arguments
    parser.add_argument('--table', help='Table name to transfer (entire table)')
    parser.add_argument('--query', help='SQL query to execute (alternative to --table)')
    parser.add_argument('--query-file', help='File containing SQL query')
    
    # Target arguments
    parser.add_argument('--target-table', help='Target table name (defaults to table name or query_result)')
    parser.add_argument('--target-hdfs-path', help='HDFS path on cluster 2')
    
    # CTAS arguments
    parser.add_argument('--ctas', action='store_true', help='Use CREATE TABLE AS SELECT (CTAS) instead of file transfer')
    parser.add_argument('--compression', choices=['SNAPPY', 'GZIP', 'BZIP2', 'LZO', 'NONE'], 
                       default='SNAPPY', help='Compression format for CTAS table (default: SNAPPY)')
    parser.add_argument('--table-location', required=False, help='HDFS location for CTAS table data (REQUIRED for CTAS)')
    parser.add_argument('--partitioned-by', nargs='+', help='Columns to partition the CTAS table by')
    parser.add_argument('--clustered-by', nargs='+', help='Columns to cluster the CTAS table by')
    parser.add_argument('--buckets', type=int, help='Number of buckets for clustering')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing table in CTAS operation')
    
    # SCP arguments
    parser.add_argument('--scp-target-host', help='Target host for SCP transfer (if using SCP)')
    parser.add_argument('--scp-target-path', help='Target directory path for SCP transfer (if using SCP)')
    
    # Distcp arguments
    parser.add_argument('--use-distcp', action='store_true', default=True, 
                       help='Use distcp for cross-cluster transfers (default: True)')
    parser.add_argument('--no-distcp', dest='use_distcp', action='store_false',
                       help='Disable distcp and use hdfs put instead')
    parser.add_argument('--source-hdfs-path', help='Source HDFS path (required for distcp)')
    parser.add_argument('--target-cluster', help='Target cluster name/address (required for distcp)')
    
    # Processing arguments
    parser.add_argument('--chunk-size', type=int, default=1000000, help='Rows per chunk')
    parser.add_argument('--max-workers', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--output-format', choices=['parquet', 'csv'], default='parquet', 
                       help='Output format')
    parser.add_argument('--temp-dir', default='/tmp/impala_transfer', help='Temporary directory')
    
    # Connection type arguments
    parser.add_argument('--connection-type', choices=['auto', 'impyla', 'pyodbc', 'sqlalchemy'], 
                       default='auto', help='Database connection type (default: auto)')
    parser.add_argument('--odbc-driver', help='ODBC driver name (required for pyodbc)')
    parser.add_argument('--odbc-connection-string', help='Full ODBC connection string (alternative to individual params)')
    parser.add_argument('--sqlalchemy-url', help='SQLAlchemy connection URL (e.g., "postgresql://user:pass@host:port/db")')
    parser.add_argument('--sqlalchemy-engine-kwargs', help='JSON string of additional kwargs for SQLAlchemy engine')
    
    # Configuration file
    parser.add_argument('--config', type=Path, help='Configuration file path (JSON format)')
    
    # Utility arguments
    parser.add_argument('--test-connection', action='store_true', help='Test database connection and exit')
    parser.add_argument('--validate-config', action='store_true', help='Validate configuration and exit')
    parser.add_argument('--show-config', action='store_true', help='Show current configuration and exit')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without executing')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    return parser


def validate_arguments(args: argparse.Namespace) -> None:
    """Validate command line arguments.
    
    :param args: Parsed command line arguments
    :type args: argparse.Namespace
    :raises ValueError: If arguments are invalid or conflicting
    """
    # Check for required query specification
    if not args.table and not args.query and not args.query_file:
        raise ValueError("Must specify either --table, --query, or --query-file")
    
    # Check for conflicting arguments
    if args.table and (args.query or args.query_file):
        raise ValueError("Cannot specify both --table and --query/--query-file")
    
    # Validate connection-specific arguments
    if args.connection_type == "pyodbc":
        if not args.odbc_driver and not args.odbc_connection_string:
            raise ValueError("ODBC driver or connection string must be specified for pyodbc")
    
    if args.connection_type == "sqlalchemy":
        if not args.sqlalchemy_url:
            raise ValueError("SQLAlchemy URL must be specified for sqlalchemy connection type")
    
    # Validate SQLAlchemy engine kwargs
    if args.sqlalchemy_engine_kwargs:
        try:
            json.loads(args.sqlalchemy_engine_kwargs)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in --sqlalchemy-engine-kwargs: {e}")

    # CTAS specific validations
    if args.ctas:
        if not args.table_location:
            raise ValueError("HDFS table location (--table-location) is required for CTAS operations.")
        if not args.target_table:
            raise ValueError("Target table name is required for CTAS operations. Use --target-table.")


def get_query_from_args(args: argparse.Namespace) -> str:
    """Extract query from command line arguments.
    
    :param args: Parsed command line arguments
    :type args: argparse.Namespace
    :return: SQL query string
    :rtype: str
    :raises FileNotFoundError: If query file doesn't exist
    """
    if args.query_file:
        with open(args.query_file, 'r') as f:
            return f.read().strip()
    elif args.query:
        return args.query
    else:
        return f"SELECT * FROM {args.table}"


def parse_sqlalchemy_kwargs(args: argparse.Namespace) -> dict:
    """Parse SQLAlchemy engine kwargs from arguments.
    
    :param args: Parsed command line arguments
    :type args: argparse.Namespace
    :return: Parsed SQLAlchemy engine kwargs
    :rtype: dict
    :raises ValueError: If JSON parsing fails
    """
    if args.sqlalchemy_engine_kwargs:
        try:
            return json.loads(args.sqlalchemy_engine_kwargs)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in --sqlalchemy-engine-kwargs: {e}")
    return {}


def mask_sensitive_config(config: dict) -> dict:
    """Mask sensitive information in configuration for display.
    
    :param config: Configuration dictionary
    :type config: dict
    :return: Configuration with sensitive data masked
    :rtype: dict
    """
    import copy
    safe_config = copy.deepcopy(config)
    
    # Define sensitive keys to mask
    sensitive_keys = ['password', 'secret', 'key', 'token', 'credential', 'pwd']
    
    def mask_dict(d: dict) -> None:
        """Recursively mask sensitive values in dictionary."""
        for key, value in d.items():
            if isinstance(value, dict):
                mask_dict(value)
            elif isinstance(value, str):
                key_lower = key.lower()
                if any(sensitive in key_lower for sensitive in sensitive_keys):
                    d[key] = '***MASKED***'
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        mask_dict(item)
    
    mask_dict(safe_config)
    return safe_config


def get_environment_config() -> Dict[str, Any]:
    """Get configuration from environment variables.
    
    :return: Configuration from environment variables
    :rtype: Dict[str, Any]
    """
    config = {}
    
    # Database connection
    if os.getenv('IMPALA_HOST'):
        config['source_host'] = os.getenv('IMPALA_HOST')
    if os.getenv('IMPALA_PORT'):
        config['source_port'] = int(os.getenv('IMPALA_PORT'))
    if os.getenv('IMPALA_DATABASE'):
        config['source_database'] = os.getenv('IMPALA_DATABASE')
    if os.getenv('CONNECTION_TYPE'):
        config['connection_type'] = os.getenv('CONNECTION_TYPE')
    
    # Processing options
    if os.getenv('CHUNK_SIZE'):
        config['chunk_size'] = int(os.getenv('CHUNK_SIZE'))
    if os.getenv('MAX_WORKERS'):
        config['max_workers'] = int(os.getenv('MAX_WORKERS'))
    if os.getenv('TEMP_DIR'):
        config['temp_dir'] = os.getenv('TEMP_DIR')
    
    # Output configuration
    if os.getenv('TARGET_HDFS_PATH'):
        config['target_hdfs_path'] = os.getenv('TARGET_HDFS_PATH')
    if os.getenv('OUTPUT_FORMAT'):
        config['output_format'] = os.getenv('OUTPUT_FORMAT')
    
    # Distcp configuration
    if os.getenv('USE_DISTCP'):
        config['use_distcp'] = os.getenv('USE_DISTCP').lower() == 'true'
    if os.getenv('SOURCE_HDFS_PATH'):
        config['source_hdfs_path'] = os.getenv('SOURCE_HDFS_PATH')
    if os.getenv('TARGET_CLUSTER'):
        config['target_cluster'] = os.getenv('TARGET_CLUSTER')
    
    # SCP configuration
    if os.getenv('SCP_TARGET_HOST'):
        config['scp_target_host'] = os.getenv('SCP_TARGET_HOST')
    if os.getenv('SCP_TARGET_PATH'):
        config['scp_target_path'] = os.getenv('SCP_TARGET_PATH')
    
    # Connection-specific environment variables
    if os.getenv('ODBC_DRIVER'):
        config['odbc_driver'] = os.getenv('ODBC_DRIVER')
    if os.getenv('ODBC_CONNECTION_STRING'):
        config['odbc_connection_string'] = os.getenv('ODBC_CONNECTION_STRING')
    if os.getenv('SQLALCHEMY_URL'):
        config['sqlalchemy_url'] = os.getenv('SQLALCHEMY_URL')
    
    # CTAS configuration
    if os.getenv('CTAS'):
        config['ctas'] = os.getenv('CTAS').lower() == 'true'
    if os.getenv('COMPRESSION'):
        config['compression'] = os.getenv('COMPRESSION')
    if os.getenv('TABLE_LOCATION'):
        config['table_location'] = os.getenv('TABLE_LOCATION')
    if os.getenv('PARTITIONED_BY'):
        config['partitioned_by'] = os.getenv('PARTITIONED_BY').split(',')
    if os.getenv('CLUSTERED_BY'):
        config['clustered_by'] = os.getenv('CLUSTERED_BY').split(',')
    if os.getenv('BUCKETS'):
        config['buckets'] = int(os.getenv('BUCKETS'))
    if os.getenv('OVERWRITE'):
        config['overwrite'] = os.getenv('OVERWRITE').lower() == 'true'
    
    return config


def load_config_from_file(config_path: Optional[Path]) -> Dict[str, Any]:
    """Load configuration from JSON file.
    
    :param config_path: Path to configuration file
    :type config_path: Optional[Path]
    :return: Configuration dictionary
    :rtype: Dict[str, Any]
    :raises ValueError: If file cannot be loaded or contains invalid JSON
    """
    if not config_path or not config_path.exists():
        return {}
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate that no secrets are hardcoded in the config file
        validate_config_security(config)
        
        return config
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file {config_path}: {e}")
    except Exception as e:
        raise ValueError(f"Failed to load configuration file {config_path}: {e}")


def validate_config_security(config: Dict[str, Any]) -> None:
    """Validate that configuration doesn't contain hardcoded secrets.
    
    :param config: Configuration dictionary
    :type config: Dict[str, Any]
    :raises ValueError: If secrets are found in configuration
    """
    sensitive_keys = ['password', 'secret', 'key', 'token', 'credential', 'pwd']
    
    def check_dict(d: dict, path: str = "") -> None:
        for key, value in d.items():
            current_path = f"{path}.{key}" if path else key
            
            if isinstance(value, dict):
                check_dict(value, current_path)
            elif isinstance(value, str):
                key_lower = key.lower()
                if any(sensitive in key_lower for sensitive in sensitive_keys):
                    if value and value != "${ENV_VAR}":
                        raise ValueError(f"Hardcoded secret found in config: {current_path}")
    
    check_dict(config)


def merge_config_with_args(args: argparse.Namespace, env_config: Dict[str, Any], file_config: Dict[str, Any]) -> None:
    """Merge configuration from multiple sources with command line arguments.
    
    Priority order: command line arguments > environment variables > config file > defaults
    
    :param args: Command line arguments
    :type args: argparse.Namespace
    :param env_config: Environment configuration
    :type env_config: Dict[str, Any]
    :param file_config: File configuration
    :type file_config: Dict[str, Any]
    """
    # First, apply file config (lowest priority)
    for key, value in file_config.items():
        if hasattr(args, key) and getattr(args, key) is None:
            setattr(args, key, value)
    
    # Then, apply environment config (medium priority)
    if not args.source_host and 'source_host' in env_config:
        args.source_host = env_config['source_host']
    if not args.source_port and 'source_port' in env_config:
        args.source_port = env_config['source_port']
    if not args.source_database and 'source_database' in env_config:
        args.source_database = env_config['source_database']
    if not args.connection_type and 'connection_type' in env_config:
        args.connection_type = env_config['connection_type']
    if not args.chunk_size and 'chunk_size' in env_config:
        args.chunk_size = env_config['chunk_size']
    if not args.max_workers and 'max_workers' in env_config:
        args.max_workers = env_config['max_workers']
    if not args.temp_dir and 'temp_dir' in env_config:
        args.temp_dir = env_config['temp_dir']
    if not args.target_hdfs_path and 'target_hdfs_path' in env_config:
        args.target_hdfs_path = env_config['target_hdfs_path']
    if not args.output_format and 'output_format' in env_config:
        args.output_format = env_config['output_format']
    if not args.odbc_driver and 'odbc_driver' in env_config:
        args.odbc_driver = env_config['odbc_driver']
    if not args.odbc_connection_string and 'odbc_connection_string' in env_config:
        args.odbc_connection_string = env_config['odbc_connection_string']
    if not args.sqlalchemy_url and 'sqlalchemy_url' in env_config:
        args.sqlalchemy_url = env_config['sqlalchemy_url']
    
    # Distcp configuration
    if not hasattr(args, 'use_distcp') or args.use_distcp is None:
        if 'use_distcp' in env_config:
            args.use_distcp = env_config['use_distcp']
    if not args.source_hdfs_path and 'source_hdfs_path' in env_config:
        args.source_hdfs_path = env_config['source_hdfs_path']
    if not args.target_cluster and 'target_cluster' in env_config:
        args.target_cluster = env_config['target_cluster']

    # SCP configuration
    if not args.scp_target_host and 'scp_target_host' in env_config:
        args.scp_target_host = env_config['scp_target_host']
    if not args.scp_target_path and 'scp_target_path' in env_config:
        args.scp_target_path = env_config['scp_target_path']
    
    # CTAS configuration
    if not hasattr(args, 'ctas') or args.ctas is None:
        if 'ctas' in env_config:
            args.ctas = env_config['ctas']
    if not args.compression and 'compression' in env_config:
        args.compression = env_config['compression']
    if not args.table_location and 'table_location' in env_config:
        args.table_location = env_config['table_location']
    if not args.partitioned_by and 'partitioned_by' in env_config:
        args.partitioned_by = env_config['partitioned_by']
    if not args.clustered_by and 'clustered_by' in env_config:
        args.clustered_by = env_config['clustered_by']
    if not args.buckets and 'buckets' in env_config:
        args.buckets = env_config['buckets']
    if not hasattr(args, 'overwrite') or args.overwrite is None:
        if 'overwrite' in env_config:
            args.overwrite = env_config['overwrite']


def setup_logging(verbose: bool) -> None:
    """Setup logging configuration.
    
    :param verbose: Enable verbose logging if True
    :type verbose: bool
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main() -> int:
    """Main CLI function with proper exit codes.
    
    :return: Exit code (0=success, 1=general error, 2=config error, 3=connection error, 5=permission error)
    :rtype: int
    """
    parser = create_parser()
    args = parser.parse_args()
    
    # Setup logging first so it's available in exception handlers
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration from multiple sources
        env_config = get_environment_config()
        file_config = load_config_from_file(args.config)
        
        # Merge configurations with command line arguments
        merge_config_with_args(args, env_config, file_config)
        
        # Validate arguments
        validate_arguments(args)
        
        # Parse SQLAlchemy engine kwargs
        sqlalchemy_engine_kwargs = parse_sqlalchemy_kwargs(args)
        
        # Check if core module is available
        if not CORE_AVAILABLE:
            logger.error("Core module not available. Please install required dependencies.")
            return 1
        
        # Create transfer tool
        tool = ImpalaTransferTool(
            source_host=args.source_host,
            source_port=args.source_port,
            source_database=args.source_database,
            target_hdfs_path=args.target_hdfs_path,
            chunk_size=args.chunk_size,
            max_workers=args.max_workers,
            temp_dir=args.temp_dir,
            connection_type=args.connection_type,
            odbc_driver=args.odbc_driver,
            odbc_connection_string=args.odbc_connection_string,
            sqlalchemy_url=args.sqlalchemy_url,
            sqlalchemy_engine_kwargs=sqlalchemy_engine_kwargs,
            use_distcp=args.use_distcp,
            source_hdfs_path=args.source_hdfs_path,
            target_cluster=args.target_cluster,
            scp_target_host=args.scp_target_host,
            scp_target_path=args.scp_target_path,
            ctas=args.ctas,
            compression=args.compression,
            table_location=args.table_location,
            partitioned_by=args.partitioned_by,
            clustered_by=args.clustered_by,
            buckets=args.buckets,
            overwrite=args.overwrite
        )
        
        # Handle utility commands
        if args.test_connection:
            logger.info("Testing database connection...")
            if tool.test_connection():
                logger.info("✓ Connection test successful")
                return 0
            else:
                logger.error("✗ Connection test failed")
                return 1
        
        if args.validate_config:
            logger.info("Validating configuration...")
            if tool.validate_configuration():
                logger.info("✓ Configuration is valid")
                return 0
            else:
                logger.error("✗ Configuration validation failed")
                return 1
        
        if args.show_config:
            logger.info("Current configuration:")
            config = tool.get_configuration()
            # Mask sensitive information before displaying
            safe_config = mask_sensitive_config(config)
            print(json.dumps(safe_config, indent=2, default=str))
            return 0
        
        if args.dry_run:
            logger.info("=== DRY RUN MODE ===")
            logger.info(f"Source host: {args.source_host}")
            logger.info(f"Connection type: {args.connection_type}")
            logger.info(f"Chunk size: {args.chunk_size}")
            logger.info(f"Max workers: {args.max_workers}")
            logger.info(f"Output format: {args.output_format}")
            
            query = get_query_from_args(args)
            logger.info(f"Query: {query}")
            
            logger.info("=== DRY RUN COMPLETE ===")
            return 0
        
        # Get the query to execute
        query = get_query_from_args(args)
        
        # Handle CTAS operation
        if args.ctas:
            if not args.target_table:
                logger.error("Target table name is required for CTAS operations. Use --target-table.")
                return 2
            
            logger.info(f"Executing CTAS operation to create table '{args.target_table}'")
            success = tool.create_table_as_select(
                query=query,
                target_table=args.target_table,
                compression=args.compression,
                location=args.table_location,
                partitioned_by=args.partitioned_by,
                clustered_by=args.clustered_by,
                buckets=args.buckets,
                overwrite=args.overwrite
            )
        else:
            # Execute regular transfer
            success = tool.transfer_query(
                query=query,
                target_table=args.target_table,
                output_format=args.output_format
            )
        
        return 0 if success else 1
        
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 2  # EXIT_CONFIG_ERROR
    except ConnectionError as e:
        logger.error(f"Connection error: {e}")
        return 3  # EXIT_CONNECTION_ERROR
    except PermissionError as e:
        logger.error(f"Permission error: {e}")
        return 5  # EXIT_PERMISSION_ERROR
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
        return 1  # EXIT_GENERAL_ERROR
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1  # EXIT_GENERAL_ERROR


if __name__ == "__main__":
    sys.exit(main()) 