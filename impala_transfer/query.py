"""
Query execution and result processing module.
Handles query execution for different database connection types.
"""

import logging
from typing import List, Dict, Any
from .connection import ConnectionManager


class QueryExecutor:
    """Handles query execution and result processing."""
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize query executor.
        
        Args:
            connection_manager: Connection manager instance
        """
        self.connection_manager = connection_manager
        self.connection_type = connection_manager.connection_type
    
    def get_query_info(self, query: str) -> Dict[str, Any]:
        """
        Get query result metadata and row count.
        
        Args:
            query: SQL query to analyze
            
        Returns:
            Dict containing query info (row_count, columns, sample_data)
        """
        if self.connection_type == "sqlalchemy":
            return self._get_query_info_sqlalchemy(query)
        else:
            return self._get_query_info_cursor(query)
    
    def _get_query_info_sqlalchemy(self, query: str) -> Dict[str, Any]:
        """Get query info using SQLAlchemy."""
        from sqlalchemy import text
        
        # Get row count
        count_query = f"SELECT COUNT(*) FROM ({query}) as count_subquery"
        result = self.connection_manager.connection.execute(text(count_query))
        row_count = result.fetchone()[0]
        
        # Get sample data
        sample_query = f"{query} LIMIT 1"
        result = self.connection_manager.connection.execute(text(sample_query))
        sample_data = result.fetchone()
        
        # Get column information from sample
        if sample_data:
            columns = [(i, str(type(val).__name__)) for i, val in enumerate(sample_data)]
        else:
            columns = []
        
        return {
            'query': query,
            'columns': columns,
            'row_count': row_count,
            'sample_data': sample_data
        }
    
    def _get_query_info_cursor(self, query: str) -> Dict[str, Any]:
        """Get query info using cursor-based execution."""
        cursor = self.connection_manager.connection.cursor()
        
        try:
            # Get row count
            count_query = f"SELECT COUNT(*) FROM ({query}) as count_subquery"
            cursor.execute(count_query)
            row_count = cursor.fetchone()[0]
            
            # Get sample data
            sample_query = f"{query} LIMIT 1"
            cursor.execute(sample_query)
            sample_data = cursor.fetchone()
            
            # Get column information
            cursor.execute(f"DESCRIBE ({query}) as schema_subquery")
            columns = cursor.fetchall()
            
            return {
                'query': query,
                'columns': columns,
                'row_count': row_count,
                'sample_data': sample_data
            }
        finally:
            cursor.close()
    
    def execute_query(self, query: str) -> List[tuple]:
        """
        Execute a query and return all results.
        
        Args:
            query: SQL query to execute
            
        Returns:
            List of tuples containing query results
        """
        if self.connection_type == "sqlalchemy":
            return self._execute_query_sqlalchemy(query)
        else:
            return self._execute_query_cursor(query)
    
    def _execute_query_sqlalchemy(self, query: str) -> List[tuple]:
        """Execute query using SQLAlchemy."""
        from sqlalchemy import text
        
        result = self.connection_manager.connection.execute(text(query))
        return result.fetchall()
    
    def _execute_query_cursor(self, query: str) -> List[tuple]:
        """Execute query using cursor."""
        cursor = self.connection_manager.connection.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()
    
    def execute_query_with_batching(self, query: str, batch_size: int = 10000) -> List[tuple]:
        """
        Execute a query and return results in batches to manage memory.
        
        Args:
            query: SQL query to execute
            batch_size: Number of rows to fetch per batch
            
        Returns:
            List of tuples containing all query results
        """
        if self.connection_type == "sqlalchemy":
            return self._execute_query_sqlalchemy_batched(query, batch_size)
        else:
            return self._execute_query_cursor_batched(query, batch_size)
    
    def _execute_query_sqlalchemy_batched(self, query: str, batch_size: int) -> List[tuple]:
        """Execute query using SQLAlchemy with batching."""
        from sqlalchemy import text
        
        result = self.connection_manager.connection.execute(text(query))
        data = []
        
        while True:
            batch = result.fetchmany(batch_size)
            if not batch:
                break
            data.extend(batch)
        
        return data
    
    def _execute_query_cursor_batched(self, query: str, batch_size: int) -> List[tuple]:
        """Execute query using cursor with batching."""
        cursor = self.connection_manager.connection.cursor()
        data = []
        
        try:
            cursor.execute(query)
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                data.extend(batch)
        finally:
            cursor.close()
        
        return data
    
    def test_connection(self) -> bool:
        """
        Test if the database connection is working.
        
        Returns:
            bool: True if connection test successful
        """
        try:
            if self.connection_type == "sqlalchemy":
                from sqlalchemy import text
                result = self.connection_manager.connection.execute(text("SELECT 1"))
                result.fetchone()
            else:
                cursor = self.connection_manager.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
            return True
        except Exception as e:
            logging.error(f"Connection test failed: {e}")
            return False 