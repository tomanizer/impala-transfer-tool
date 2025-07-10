#!/usr/bin/env python3
"""
Test suite for the CLI module.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import os
import json
import argparse

from impala_transfer.cli import (
    create_parser, get_environment_config, mask_sensitive_config,
    merge_config_with_args, load_config_from_file,
    validate_config_security, parse_sqlalchemy_kwargs,
    validate_arguments, get_query_from_args, setup_logging, main
)


class TestCLI(unittest.TestCase):
    """Test the CLI module."""
    
    def test_create_parser(self):
        """Test parser creation."""
        parser = create_parser()
        
        self.assertIsNotNone(parser)
        # Test that parser has expected arguments
        args = parser.parse_args(['--source-host', 'test-host'])
        self.assertEqual(args.source_host, 'test-host')
    
    def test_environment_config(self):
        """Test environment configuration loading."""
        with patch.dict(os.environ, {
            'IMPALA_HOST': 'test-host',
            'IMPALA_PORT': '21050',
            'IMPALA_DATABASE': 'test_db'
        }):
            config = get_environment_config()
            
            self.assertEqual(config['source_host'], 'test-host')
            self.assertEqual(config['source_port'], 21050)
            self.assertEqual(config['source_database'], 'test_db')
    
    def test_environment_config_all_variables(self):
        """Test environment configuration loading with all variables."""
        with patch.dict(os.environ, {
            'IMPALA_HOST': 'test-host',
            'IMPALA_PORT': '21050',
            'IMPALA_DATABASE': 'test_db',
            'CONNECTION_TYPE': 'impyla',
            'CHUNK_SIZE': '1000000',
            'MAX_WORKERS': '4',
            'TEMP_DIR': '/tmp/test',
            'TARGET_HDFS_PATH': '/user/test',
            'OUTPUT_FORMAT': 'parquet',
            'USE_DISTCP': 'true',
            'SOURCE_HDFS_PATH': '/user/source',
            'TARGET_CLUSTER': 'cluster2',
            'SCP_TARGET_HOST': 'target-host',
            'SCP_TARGET_PATH': '/path/to/target',
            'ODBC_DRIVER': 'Test Driver',
            'ODBC_CONNECTION_STRING': 'DRIVER={Test};HOST=test',
            'SQLALCHEMY_URL': 'impala://test-host:21050/default'
        }):
            config = get_environment_config()
            
            self.assertEqual(config['source_host'], 'test-host')
            self.assertEqual(config['source_port'], 21050)
            self.assertEqual(config['source_database'], 'test_db')
            self.assertEqual(config['connection_type'], 'impyla')
            self.assertEqual(config['chunk_size'], 1000000)
            self.assertEqual(config['max_workers'], 4)
            self.assertEqual(config['temp_dir'], '/tmp/test')
            self.assertEqual(config['target_hdfs_path'], '/user/test')
            self.assertEqual(config['output_format'], 'parquet')
            self.assertTrue(config['use_distcp'])
            self.assertEqual(config['source_hdfs_path'], '/user/source')
            self.assertEqual(config['target_cluster'], 'cluster2')
            self.assertEqual(config['scp_target_host'], 'target-host')
            self.assertEqual(config['scp_target_path'], '/path/to/target')
            self.assertEqual(config['odbc_driver'], 'Test Driver')
            self.assertEqual(config['odbc_connection_string'], 'DRIVER={Test};HOST=test')
            self.assertEqual(config['sqlalchemy_url'], 'impala://test-host:21050/default')
    
    def test_mask_sensitive_config(self):
        """Test sensitive configuration masking."""
        test_config = {
            'host': 'localhost',
            'password': 'secret123',
            'nested': {
                'api_key': 'sk-1234567890'
            }
        }
        
        masked_config = mask_sensitive_config(test_config)
        
        self.assertEqual(masked_config['host'], 'localhost')
        self.assertEqual(masked_config['password'], '***MASKED***')
        self.assertEqual(masked_config['nested']['api_key'], '***MASKED***')
    
    def test_mask_sensitive_config_with_lists(self):
        """Test sensitive configuration masking with lists."""
        test_config = {
            'host': 'localhost',
            'credentials': [
                {'username': 'user1', 'password': 'secret1'},
                {'username': 'user2', 'api_key': 'sk-1234567890'}
            ]
        }
        
        masked_config = mask_sensitive_config(test_config)
        
        self.assertEqual(masked_config['host'], 'localhost')
        self.assertEqual(masked_config['credentials'][0]['password'], '***MASKED***')
        self.assertEqual(masked_config['credentials'][1]['api_key'], '***MASKED***')
    
    def test_merge_config_with_args(self):
        """Test configuration merging with command line arguments."""
        env_config = {'source_host': 'env-host', 'source_port': '21050'}
        file_config = {'source_database': 'test_db'}
        args = Mock()
        args.source_host = 'arg-host'
        args.source_port = None
        args.source_database = None
        
        merge_config_with_args(args, env_config, file_config)
        
        self.assertEqual(args.source_host, 'arg-host')  # Args override config
        self.assertEqual(args.source_port, '21050')     # Env config value preserved
        self.assertEqual(args.source_database, 'test_db')  # File config value preserved
    
    def test_load_config_from_file(self):
        """Test configuration file loading."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"source_host": "file-host", "source_port": "21050"}')
            config_file = f.name
        
        try:
            from pathlib import Path
            config = load_config_from_file(Path(config_file))
            
            self.assertEqual(config['source_host'], 'file-host')
            self.assertEqual(config['source_port'], '21050')
        finally:
            os.unlink(config_file)
    
    def test_load_config_from_file_nonexistent(self):
        """Test configuration file loading with nonexistent file."""
        from pathlib import Path
        config = load_config_from_file(Path('/nonexistent/file.json'))
        
        self.assertEqual(config, {})
    
    def test_load_config_from_file_invalid_json(self):
        """Test configuration file loading with invalid JSON."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"source_host": "file-host", "source_port": invalid}')  # Invalid JSON
            config_file = f.name
        
        try:
            from pathlib import Path
            with self.assertRaises(ValueError):
                load_config_from_file(Path(config_file))
        finally:
            os.unlink(config_file)
    
    def test_load_config_from_file_with_secrets(self):
        """Test configuration file loading with hardcoded secrets."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"source_host": "file-host", "password": "hardcoded_secret"}')
            config_file = f.name
        
        try:
            from pathlib import Path
            with self.assertRaises(ValueError):
                load_config_from_file(Path(config_file))
        finally:
            os.unlink(config_file)
    
    def test_validate_config_security_raises(self):
        """Test configuration security validation raises error for invalid config."""
        invalid_config = {"password": "hardcoded_secret"}  # Contains hardcoded secret
        
        with self.assertRaises(ValueError):
            validate_config_security(invalid_config)
    
    def test_validate_config_security_valid(self):
        """Test configuration security validation with valid config."""
        valid_config = {"host": "localhost", "port": 21050}
        
        # Should not raise any exception
        validate_config_security(valid_config)
    
    def test_validate_config_security_nested(self):
        """Test configuration security validation with nested secrets."""
        invalid_config = {
            "connection": {
                "credentials": {
                    "api_key": "sk-1234567890"
                }
            }
        }
        
        with self.assertRaises(ValueError):
            validate_config_security(invalid_config)
    
    def test_parse_sqlalchemy_kwargs(self):
        """Test SQLAlchemy kwargs parsing."""
        args = Mock()
        args.sqlalchemy_engine_kwargs = '{"pool_size": 10, "pool_timeout": 30, "pool_recycle": 3600}'
        
        parsed = parse_sqlalchemy_kwargs(args)
        
        self.assertEqual(parsed['pool_size'], 10)
        self.assertEqual(parsed['pool_timeout'], 30)
        self.assertEqual(parsed['pool_recycle'], 3600)
    
    def test_parse_sqlalchemy_kwargs_invalid(self):
        """Test SQLAlchemy kwargs parsing with invalid input."""
        args = Mock()
        args.sqlalchemy_engine_kwargs = '{"pool_size": invalid, "pool_timeout": 30}'  # Invalid JSON
        
        with self.assertRaises(ValueError):
            parse_sqlalchemy_kwargs(args)
    
    def test_parse_sqlalchemy_kwargs_none(self):
        """Test SQLAlchemy kwargs parsing with None input."""
        args = Mock()
        args.sqlalchemy_engine_kwargs = None
        
        parsed = parse_sqlalchemy_kwargs(args)
        
        self.assertEqual(parsed, {})
    
    def test_validate_arguments_no_query_specified(self):
        """Test argument validation with no query specified."""
        args = Mock()
        args.table = None
        args.query = None
        args.query_file = None
        args.connection_type = 'impyla'
        args.odbc_driver = None
        args.odbc_connection_string = None
        args.sqlalchemy_url = None
        args.sqlalchemy_engine_kwargs = None
        
        with self.assertRaises(ValueError):
            validate_arguments(args)
    
    def test_validate_arguments_conflicting_table_and_query(self):
        """Test argument validation with conflicting table and query."""
        args = Mock()
        args.table = 'test_table'
        args.query = 'SELECT * FROM other_table'
        args.query_file = None
        args.connection_type = 'impyla'
        args.odbc_driver = None
        args.odbc_connection_string = None
        args.sqlalchemy_url = None
        args.sqlalchemy_engine_kwargs = None
        
        with self.assertRaises(ValueError):
            validate_arguments(args)
    
    def test_validate_arguments_pyodbc_missing_driver(self):
        """Test argument validation with pyodbc but missing driver."""
        args = Mock()
        args.table = 'test_table'
        args.query = None
        args.query_file = None
        args.connection_type = 'pyodbc'
        args.odbc_driver = None
        args.odbc_connection_string = None
        args.sqlalchemy_url = None
        args.sqlalchemy_engine_kwargs = None
        
        with self.assertRaises(ValueError):
            validate_arguments(args)
    
    def test_validate_arguments_sqlalchemy_missing_url(self):
        """Test argument validation with sqlalchemy but missing URL."""
        args = Mock()
        args.table = 'test_table'
        args.query = None
        args.query_file = None
        args.connection_type = 'sqlalchemy'
        args.odbc_driver = None
        args.odbc_connection_string = None
        args.sqlalchemy_url = None
        args.sqlalchemy_engine_kwargs = None
        
        with self.assertRaises(ValueError):
            validate_arguments(args)
    
    def test_validate_arguments_invalid_json(self):
        """Test argument validation with invalid JSON in engine kwargs."""
        args = Mock()
        args.table = 'test_table'
        args.query = None
        args.query_file = None
        args.connection_type = 'sqlalchemy'
        args.odbc_driver = None
        args.odbc_connection_string = None
        args.sqlalchemy_url = 'impala://test'
        args.sqlalchemy_engine_kwargs = '{"pool_size": invalid}'
        
        with self.assertRaises(ValueError):
            validate_arguments(args)
    
    def test_validate_arguments_valid(self):
        """Test argument validation with valid arguments."""
        args = Mock()
        args.table = 'test_table'
        args.query = None
        args.query_file = None
        args.connection_type = 'impyla'
        args.odbc_driver = None
        args.odbc_connection_string = None
        args.sqlalchemy_url = None
        args.sqlalchemy_engine_kwargs = None
        
        # Should not raise any exception
        validate_arguments(args)
    
    def test_get_query_from_args_query_file(self):
        """Test getting query from query file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write('SELECT * FROM test_table WHERE id > 100')
            query_file = f.name
        
        try:
            args = Mock()
            args.query_file = query_file
            args.query = None
            args.table = None
            
            query = get_query_from_args(args)
            
            self.assertEqual(query, 'SELECT * FROM test_table WHERE id > 100')
        finally:
            os.unlink(query_file)
    
    def test_get_query_from_args_query(self):
        """Test getting query from query argument."""
        args = Mock()
        args.query_file = None
        args.query = 'SELECT * FROM test_table WHERE id > 100'
        args.table = None
        
        query = get_query_from_args(args)
        
        self.assertEqual(query, 'SELECT * FROM test_table WHERE id > 100')
    
    def test_get_query_from_args_table(self):
        """Test getting query from table argument."""
        args = Mock()
        args.query_file = None
        args.query = None
        args.table = 'test_table'
        
        query = get_query_from_args(args)
        
        self.assertEqual(query, 'SELECT * FROM test_table')
    
    def test_get_query_from_args_query_file_nonexistent(self):
        """Test getting query from nonexistent query file."""
        args = Mock()
        args.query_file = '/nonexistent/file.sql'
        args.query = None
        args.table = None
        
        with self.assertRaises(FileNotFoundError):
            get_query_from_args(args)
    
    def test_setup_logging(self):
        """Test logging setup."""
        with patch('impala_transfer.cli.logging.basicConfig') as mock_basic_config:
            setup_logging(verbose=True)
            
            mock_basic_config.assert_called_once()
            call_args = mock_basic_config.call_args
            self.assertEqual(call_args[1]['level'], 10)  # DEBUG level for verbose
    
    def test_setup_logging_not_verbose(self):
        """Test logging setup without verbose flag."""
        with patch('impala_transfer.cli.logging.basicConfig') as mock_basic_config:
            setup_logging(verbose=False)
            
            mock_basic_config.assert_called_once()
            call_args = mock_basic_config.call_args
            self.assertEqual(call_args[1]['level'], 20)  # INFO level for non-verbose
    
    @patch('impala_transfer.cli.create_parser')
    @patch('impala_transfer.cli.validate_arguments')
    @patch('impala_transfer.cli.get_environment_config')
    @patch('impala_transfer.cli.load_config_from_file')
    @patch('impala_transfer.cli.merge_config_with_args')
    @patch('impala_transfer.cli.setup_logging')
    @patch('impala_transfer.cli.ImpalaTransferTool')
    @patch('impala_transfer.cli.CORE_AVAILABLE', True)
    def test_main_success(self, mock_tool_class, mock_setup_logging, mock_merge_config,
                         mock_load_config, mock_get_env_config, mock_validate_args, mock_create_parser):
        """Test main function with successful execution."""
        # Mock parser and arguments
        mock_parser = Mock()
        mock_args = Mock()
        mock_args.verbose = False
        mock_args.config_file = None
        mock_args.table = 'test_table'
        mock_args.query = None
        mock_args.query_file = None
        mock_args.connection_type = 'impyla'
        mock_args.odbc_driver = None
        mock_args.odbc_connection_string = None
        mock_args.sqlalchemy_url = None
        mock_args.sqlalchemy_engine_kwargs = None
        mock_args.test_connection = False
        mock_args.validate_config = False
        mock_args.show_config = False
        mock_args.dry_run = False
        mock_args.target_table = None
        mock_args.output_format = 'parquet'
        mock_args.ctas = False
        mock_parser.parse_args.return_value = mock_args
        mock_create_parser.return_value = mock_parser
        
        # Mock environment and file config
        mock_get_env_config.return_value = {}
        mock_load_config.return_value = {}
        
        # Mock tool
        mock_tool = Mock()
        mock_tool.transfer_query.return_value = True
        mock_tool_class.return_value = mock_tool
        
        # Mock sys.argv
        with patch('sys.argv', ['impala_transfer', '--table', 'test_table']):
            result = main()
        
        self.assertEqual(result, 0)
        mock_validate_args.assert_called_once_with(mock_args)
        mock_setup_logging.assert_called_once_with(False)
        mock_tool.transfer_query.assert_called_once_with(query='SELECT * FROM test_table', target_table=None, output_format='parquet')
    
    @patch('impala_transfer.cli.create_parser')
    @patch('impala_transfer.cli.validate_arguments')
    @patch('impala_transfer.cli.get_environment_config')
    @patch('impala_transfer.cli.load_config_from_file')
    @patch('impala_transfer.cli.merge_config_with_args')
    @patch('impala_transfer.cli.setup_logging')
    @patch('impala_transfer.cli.ImpalaTransferTool')
    def test_main_validation_error(self, mock_tool_class, mock_setup_logging, mock_merge_config,
                                 mock_load_config, mock_get_env_config, mock_validate_args, mock_create_parser):
        """Test main function with validation error."""
        # Mock parser and arguments
        mock_parser = Mock()
        mock_args = Mock()
        mock_parser.parse_args.return_value = mock_args
        mock_create_parser.return_value = mock_parser
        
        # Mock validation to raise error
        mock_validate_args.side_effect = ValueError("Invalid arguments")
        
        # Mock sys.argv
        with patch('sys.argv', ['impala_transfer', '--table', 'test_table']):
            result = main()
        
        self.assertEqual(result, 2)
    
    @patch('impala_transfer.cli.create_parser')
    @patch('impala_transfer.cli.validate_arguments')
    @patch('impala_transfer.cli.get_environment_config')
    @patch('impala_transfer.cli.load_config_from_file')
    @patch('impala_transfer.cli.merge_config_with_args')
    @patch('impala_transfer.cli.setup_logging')
    @patch('impala_transfer.cli.ImpalaTransferTool')
    @patch('impala_transfer.cli.CORE_AVAILABLE', True)
    def test_main_transfer_failure(self, mock_tool_class, mock_setup_logging, mock_merge_config,
                                 mock_load_config, mock_get_env_config, mock_validate_args, mock_create_parser):
        """Test main function with transfer failure."""
        # Mock parser and arguments
        mock_parser = Mock()
        mock_args = Mock()
        mock_args.verbose = False
        mock_args.config_file = None
        mock_args.table = 'test_table'
        mock_args.query = None
        mock_args.query_file = None
        mock_args.connection_type = 'impyla'
        mock_args.odbc_driver = None
        mock_args.odbc_connection_string = None
        mock_args.sqlalchemy_url = None
        mock_args.sqlalchemy_engine_kwargs = None
        mock_args.test_connection = False
        mock_args.validate_config = False
        mock_args.show_config = False
        mock_args.dry_run = False
        mock_args.target_table = None
        mock_args.output_format = 'parquet'
        mock_args.ctas = False
        mock_parser.parse_args.return_value = mock_args
        mock_create_parser.return_value = mock_parser
        
        # Mock environment and file config
        mock_get_env_config.return_value = {}
        mock_load_config.return_value = {}
        
        # Mock tool with transfer failure
        mock_tool = Mock()
        mock_tool.transfer_query.return_value = False
        mock_tool_class.return_value = mock_tool
        
        # Mock sys.argv
        with patch('sys.argv', ['impala_transfer', '--table', 'test_table']):
            result = main()
        
        self.assertEqual(result, 1)
    
    @patch('impala_transfer.cli.create_parser')
    @patch('impala_transfer.cli.validate_arguments')
    @patch('impala_transfer.cli.get_environment_config')
    @patch('impala_transfer.cli.load_config_from_file')
    @patch('impala_transfer.cli.merge_config_with_args')
    @patch('impala_transfer.cli.setup_logging')
    @patch('impala_transfer.cli.ImpalaTransferTool')
    @patch('impala_transfer.cli.CORE_AVAILABLE', True)
    def test_main_exception(self, mock_tool_class, mock_setup_logging, mock_merge_config,
                          mock_load_config, mock_get_env_config, mock_validate_args, mock_create_parser):
        """Test main function with exception."""
        # Mock parser and arguments
        mock_parser = Mock()
        mock_args = Mock()
        mock_args.verbose = False
        mock_args.sqlalchemy_engine_kwargs = None
        mock_parser.parse_args.return_value = mock_args
        mock_create_parser.return_value = mock_parser
        
        # Mock environment and file config
        mock_get_env_config.return_value = {}
        mock_load_config.return_value = {}
        
        # Mock tool to raise exception
        mock_tool_class.side_effect = Exception("Tool initialization failed")
        
        # Mock sys.argv
        with patch('sys.argv', ['impala_transfer', '--table', 'test_table']):
            result = main()
        
        self.assertEqual(result, 1)


if __name__ == '__main__':
    unittest.main() 