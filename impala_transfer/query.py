"""
Query execution and result processing module.
Handles query execution for different database connection types.
"""

import logging
from typing import List, Dict, Any, Optional
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
    
    def execute_ctas(self, query: str, target_table: str, 
                    file_format: str = 'PARQUET', 
                    compression: str = 'SNAPPY',
                    location: Optional[str] = None,
                    partitioned_by: Optional[List[str]] = None,
                    clustered_by: Optional[List[str]] = None,
                    buckets: Optional[int] = None,
                    overwrite: bool = False) -> bool:
        """
        Execute CREATE TABLE AS SELECT (CTAS) operation.
        
        Args:
            query: SELECT query to execute
            target_table: Name of the table to create
            file_format: File format for the table (PARQUET, TEXTFILE, etc.)
            compression: Compression format (SNAPPY, GZIP, etc.)
            location: HDFS location for the table data
            partitioned_by: List of columns to partition by
            clustered_by: List of columns to cluster by
            buckets: Number of buckets for clustering
            overwrite: Whether to overwrite existing table
            
        Returns:
            bool: True if CTAS operation successful
        """
        if self.connection_type == "sqlalchemy":
            return self._execute_ctas_sqlalchemy(
                query, target_table, file_format, compression, location,
                partitioned_by, clustered_by, buckets, overwrite
            )
        else:
            return self._execute_ctas_cursor(
                query, target_table, file_format, compression, location,
                partitioned_by, clustered_by, buckets, overwrite
            )
    
    def _execute_ctas_sqlalchemy(self, query: str, target_table: str,
                               file_format: str, compression: str,
                               location: Optional[str], partitioned_by: Optional[List[str]],
                               clustered_by: Optional[List[str]], buckets: Optional[int],
                               overwrite: bool) -> bool:
        """Execute CTAS using SQLAlchemy."""
        from sqlalchemy import text
        
        try:
            ctas_query = self._build_ctas_query(
                query, target_table, file_format, compression, location,
                partitioned_by, clustered_by, buckets, overwrite
            )
            
            logging.info(f"Executing CTAS: {ctas_query}")
            result = self.connection_manager.connection.execute(text(ctas_query))
            
            # For CTAS, we don't need to fetch results, just execute
            logging.info(f"CTAS operation completed successfully. Table '{target_table}' created.")
            return True
            
        except Exception as e:
            logging.error(f"CTAS operation failed: {e}")
            return False
    
    def _execute_ctas_cursor(self, query: str, target_table: str,
                           file_format: str, compression: str,
                           location: Optional[str], partitioned_by: Optional[List[str]],
                           clustered_by: Optional[List[str]], buckets: Optional[int],
                           overwrite: bool) -> bool:
        """Execute CTAS using cursor."""
        cursor = self.connection_manager.connection.cursor()
        
        try:
            ctas_query = self._build_ctas_query(
                query, target_table, file_format, compression, location,
                partitioned_by, clustered_by, buckets, overwrite
            )
            
            logging.info(f"Executing CTAS: {ctas_query}")
            cursor.execute(ctas_query)
            
            logging.info(f"CTAS operation completed successfully. Table '{target_table}' created.")
            return True
            
        except Exception as e:
            logging.error(f"CTAS operation failed: {e}")
            return False
        finally:
            cursor.close()
    
    def _build_ctas_query(self, query: str, target_table: str,
                         file_format: str, compression: str,
                         location: Optional[str], partitioned_by: Optional[List[str]],
                         clustered_by: Optional[List[str]], buckets: Optional[int],
                         overwrite: bool) -> str:
        """
        Build CREATE TABLE AS SELECT query with Impala-specific options.
        
        Args:
            query: SELECT query to execute
            target_table: Name of the table to create
            file_format: File format for the table
            compression: Compression format
            location: HDFS location for the table data
            partitioned_by: List of columns to partition by
            clustered_by: List of columns to cluster by
            buckets: Number of buckets for clustering
            overwrite: Whether to overwrite existing table
            
        Returns:
            str: Complete CTAS query
        """
        # Start building the CTAS query
        ctas_parts = []
        
        # Add CREATE TABLE statement
        if overwrite:
            ctas_parts.append(f"CREATE TABLE {target_table}")
        else:
            ctas_parts.append(f"CREATE TABLE IF NOT EXISTS {target_table}")
        
        # Add file format and compression
        ctas_parts.append(f"STORED AS {file_format}")
        if compression and compression.upper() != 'NONE':
            ctas_parts.append(f"COMPRESSION '{compression}'")
        
        # Add location if specified
        if location:
            ctas_parts.append(f"LOCATION '{location}'")
        
        # Add partitioning if specified
        if partitioned_by:
            partition_cols = ', '.join(partitioned_by)
            ctas_parts.append(f"PARTITIONED BY ({partition_cols})")
        
        # Add clustering if specified
        if clustered_by and buckets:
            cluster_cols = ', '.join(clustered_by)
            ctas_parts.append(f"CLUSTERED BY ({cluster_cols}) INTO {buckets} BUCKETS")
        
        # Add the SELECT query
        ctas_parts.append(f"AS {query}")
        
        return ' '.join(ctas_parts)
    
    def drop_table(self, table_name: str, if_exists: bool = True) -> bool:
        """
        Drop a table.
        
        Args:
            table_name: Name of the table to drop
            if_exists: Whether to add IF EXISTS clause
            
        Returns:
            bool: True if table dropped successfully
        """
        if self.connection_type == "sqlalchemy":
            return self._drop_table_sqlalchemy(table_name, if_exists)
        else:
            return self._drop_table_cursor(table_name, if_exists)
    
    def _drop_table_sqlalchemy(self, table_name: str, if_exists: bool) -> bool:
        """Drop table using SQLAlchemy."""
        from sqlalchemy import text
        
        try:
            if if_exists:
                drop_query = f"DROP TABLE IF EXISTS {table_name}"
            else:
                drop_query = f"DROP TABLE {table_name}"
            
            logging.info(f"Dropping table: {drop_query}")
            self.connection_manager.connection.execute(text(drop_query))
            logging.info(f"Table '{table_name}' dropped successfully.")
            return True
            
        except Exception as e:
            logging.error(f"Failed to drop table '{table_name}': {e}")
            return False
    
    def _drop_table_cursor(self, table_name: str, if_exists: bool) -> bool:
        """Drop table using cursor."""
        cursor = self.connection_manager.connection.cursor()
        
        try:
            if if_exists:
                drop_query = f"DROP TABLE IF EXISTS {table_name}"
            else:
                drop_query = f"DROP TABLE {table_name}"
            
            logging.info(f"Dropping table: {drop_query}")
            cursor.execute(drop_query)
            logging.info(f"Table '{table_name}' dropped successfully.")
            return True
            
        except Exception as e:
            logging.error(f"Failed to drop table '{table_name}': {e}")
            return False
        finally:
            cursor.close()
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            bool: True if table exists
        """
        if self.connection_type == "sqlalchemy":
            return self._table_exists_sqlalchemy(table_name)
        else:
            return self._table_exists_cursor(table_name)
    
    def _table_exists_sqlalchemy(self, table_name: str) -> bool:
        """Check if table exists using SQLAlchemy."""
        from sqlalchemy import text
        
        try:
            # Try to describe the table
            describe_query = f"DESCRIBE {table_name}"
            self.connection_manager.connection.execute(text(describe_query))
            return True
        except Exception:
            return False
    
    def _table_exists_cursor(self, table_name: str) -> bool:
        """Check if table exists using cursor."""
        cursor = self.connection_manager.connection.cursor()
        
        try:
            # Try to describe the table
            cursor.execute(f"DESCRIBE {table_name}")
            return True
        except Exception:
            return False
        finally:
            cursor.close()
    
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