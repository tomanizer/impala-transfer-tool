#!/usr/bin/env python3
"""
Integration test suite for the Impala Transfer Tool.
"""

import unittest
from unittest.mock import Mock, patch

from impala_transfer import (
    ConnectionManager, QueryExecutor, ChunkProcessor, 
    FileTransferManager, FileManager, TransferOrchestrator, ImpalaTransferTool
)


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete workflow."""
    
    @patch('impala_transfer.connection.IMPYLA_AVAILABLE', True)
    @patch('impala_transfer.connection.PYODBC_AVAILABLE', False)
    @patch('impala_transfer.connection.SQLALCHEMY_AVAILABLE', False)
    def test_complete_transfer_workflow(self):
        """Test the complete transfer workflow from start to finish."""
        # Create the main tool
        tool = ImpalaTransferTool(
            source_host='test-host',
            source_port=21050,
            source_database='test_db',
            connection_type='impyla'
        )
        
        # Mock the orchestrator's transfer_query method
        tool.orchestrator = Mock()
        tool.orchestrator.transfer_query.return_value = True
        
        # Execute the transfer
        result = tool.transfer_table('test_table')
        
        # Verify the complete workflow
        self.assertTrue(result)
        tool.orchestrator.transfer_query.assert_called_once()


if __name__ == '__main__':
    unittest.main() 