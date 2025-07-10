#!/usr/bin/env python3
"""
Test suite for the CLI module.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import os

from impala_transfer.cli import (
    create_parser, get_environment_config, mask_sensitive_config,
    merge_config_with_args, load_config_from_file,
    validate_config_security, parse_sqlalchemy_kwargs
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
    
    def test_validate_config_security_raises(self):
        """Test configuration security validation raises error for invalid config."""
        invalid_config = {"password": "hardcoded_secret"}  # Contains hardcoded secret
        
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


if __name__ == '__main__':
    unittest.main() 