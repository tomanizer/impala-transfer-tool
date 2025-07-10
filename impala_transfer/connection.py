"""
Database connection management module.
Handles connections to different database types (Impyla, pyodbc, SQLAlchemy).
"""

import logging
from typing import Dict, Any, Optional

# Try to import connection libraries
try:
    import impala.dbapi
    IMPYLA_AVAILABLE = True
except ImportError:
    IMPYLA_AVAILABLE = False

try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

try:
    import sqlalchemy
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False


class ConnectionManager:
    """Manages database connections for different connection types.
    
    This class provides a unified interface for connecting to different database
    systems using various connection libraries (Impyla, pyodbc, SQLAlchemy).
    """
    
    def __init__(self, connection_type: str, **kwargs):
        """Initialize connection manager.
        
        :param connection_type: Type of connection ('impyla', 'pyodbc', 'sqlalchemy')
        :type connection_type: str
        :param kwargs: Connection-specific parameters
        :type kwargs: dict
        """
        self.connection_type = connection_type
        self.connection = None
        self.engine = None
        self.kwargs = kwargs
        
    def connect(self) -> bool:
        """Establish database connection based on connection type.
        
        :return: True if connection successful, False otherwise
        :rtype: bool
        :raises ValueError: If connection type is unsupported
        """
        try:
            if self.connection_type == "impyla":
                return self._connect_impyla()
            elif self.connection_type == "pyodbc":
                return self._connect_pyodbc()
            elif self.connection_type == "sqlalchemy":
                return self._connect_sqlalchemy()
            else:
                raise ValueError(f"Unsupported connection type: {self.connection_type}")
        except Exception as e:
            logging.error(f"Failed to connect to database: {e}")
            return False
    
    def _connect_impyla(self) -> bool:
        """Connect using Impyla.
        
        :return: True if connection successful
        :rtype: bool
        :raises ImportError: If Impyla is not available
        """
        if not IMPYLA_AVAILABLE:
            raise ImportError("Impyla is not available. Install with: pip install impyla")
            
        self.connection = impala.dbapi.connect(
            host=self.kwargs['source_host'],
            port=self.kwargs['source_port'],
            database=self.kwargs['source_database'],
            auth_mechanism=self.kwargs.get('auth_mechanism', 'PLAIN')
        )
        logging.info(f"Connected to Impala at {self.kwargs['source_host']}:{self.kwargs['source_port']}")
        return True
    
    def _connect_pyodbc(self) -> bool:
        """Connect using pyodbc.
        
        :return: True if connection successful
        :rtype: bool
        :raises ImportError: If pyodbc is not available
        :raises ValueError: If ODBC driver is not specified
        """
        if not PYODBC_AVAILABLE:
            raise ImportError("pyodbc is not available. Install with: pip install pyodbc")
            
        if self.kwargs.get('odbc_connection_string'):
            # Use full connection string
            self.connection = pyodbc.connect(self.kwargs['odbc_connection_string'])
        else:
            # Build connection string from parameters
            if not self.kwargs.get('odbc_driver'):
                raise ValueError("ODBC driver must be specified when using pyodbc")
            
            conn_str = (f"DRIVER={{{self.kwargs['odbc_driver']}}};"
                       f"SERVER={self.kwargs['source_host']};"
                       f"PORT={self.kwargs['source_port']};"
                       f"DATABASE={self.kwargs['source_database']}")
            self.connection = pyodbc.connect(conn_str)
        
        logging.info(f"Connected via ODBC to {self.kwargs['source_host']}:{self.kwargs['source_port']}")
        return True
    
    def _connect_sqlalchemy(self) -> bool:
        """Connect using SQLAlchemy.
        
        :return: True if connection successful
        :rtype: bool
        :raises ImportError: If SQLAlchemy is not available
        """
        if not SQLALCHEMY_AVAILABLE:
            raise ImportError("SQLAlchemy is not available. Install with: pip install sqlalchemy")
            
        from sqlalchemy import create_engine
        self.engine = create_engine(
            self.kwargs['sqlalchemy_url'], 
            **self.kwargs.get('sqlalchemy_engine_kwargs', {})
        )
        self.connection = self.engine.connect()
        logging.info(f"Connected via SQLAlchemy to {self.kwargs['sqlalchemy_url']}")
        return True
    
    def close(self) -> None:
        """Close the database connection.
        
        Closes both the connection and disposes of the engine if using SQLAlchemy.
        """
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the current connection.
        
        :return: Connection information dictionary
        :rtype: Dict[str, Any]
        """
        return {
            'type': self.connection_type,
            'connected': self.connection is not None,
            'parameters': self.kwargs
        }


def get_available_connection_types() -> list:
    """Get list of available connection types.
    
    :return: List of available connection types
    :rtype: list
    """
    available = []
    if IMPYLA_AVAILABLE:
        available.append('impyla')
    if PYODBC_AVAILABLE:
        available.append('pyodbc')
    if SQLALCHEMY_AVAILABLE:
        available.append('sqlalchemy')
    return available


def validate_connection_type(connection_type: str) -> bool:
    """Validate if a connection type is available.
    
    :param connection_type: Connection type to validate
    :type connection_type: str
    :return: True if connection type is available
    :rtype: bool
    """
    available_types = get_available_connection_types()
    return connection_type in available_types 