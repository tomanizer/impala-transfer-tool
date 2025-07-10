#!/usr/bin/env python3
"""
Test suite for the __init__.py module.
"""

import unittest
from unittest.mock import patch

import impala_transfer


class TestInit(unittest.TestCase):
    """Test the __init__.py module."""
    
    def test_version(self):
        """Test that version is defined."""
        self.assertIsNotNone(impala_transfer.__version__)
        self.assertEqual(impala_transfer.__version__, "2.0.0")
    
    def test_author(self):
        """Test that author is defined."""
        self.assertIsNotNone(impala_transfer.__author__)
        self.assertEqual(impala_transfer.__author__, "Data Transfer Team")
    
    def test_all_imports(self):
        """Test that all expected classes are importable."""
        from impala_transfer import (
            ImpalaTransferTool,
            ConnectionManager,
            QueryExecutor,
            ChunkProcessor,
            FileTransferManager,
            FileManager,
            TransferOrchestrator
        )
        
        # Verify all classes are imported successfully
        self.assertIsNotNone(ImpalaTransferTool)
        self.assertIsNotNone(ConnectionManager)
        self.assertIsNotNone(QueryExecutor)
        self.assertIsNotNone(ChunkProcessor)
        self.assertIsNotNone(FileTransferManager)
        self.assertIsNotNone(FileManager)
        self.assertIsNotNone(TransferOrchestrator)
    
    def test_all_list(self):
        """Test that __all__ contains all expected exports."""
        expected_exports = [
            'ImpalaTransferTool',
            'ConnectionManager',
            'QueryExecutor',
            'ChunkProcessor',
            'FileTransferManager',
            'FileManager',
            'TransferOrchestrator'
        ]
        
        self.assertEqual(impala_transfer.__all__, expected_exports)


if __name__ == '__main__':
    unittest.main() 