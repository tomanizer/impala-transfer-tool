# Coding Standards and Configurations

## 🎯 Core Principles

### 1. **Logging Over Print Statements**
**CRITICAL**: Always use Python's `logging` module instead of `print()` statements.

#### Why Logging?
- **Configurable levels** (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- **Structured output** with timestamps and context
- **Production-ready** with proper error handling
- **Performance benefits** (can be disabled in production)
- **Integration** with monitoring and alerting systems

#### Implementation Guidelines
```python
import logging

# ✅ CORRECT - Use logging
logger = logging.getLogger(__name__)

def process_data(data):
    logger.info("Starting data processing")
    try:
        result = transform_data(data)
        logger.debug(f"Data transformed successfully: {len(result)} records")
        return result
    except Exception as e:
        logger.error(f"Data processing failed: {e}", exc_info=True)
        raise

# ❌ WRONG - Never use print
def process_data(data):
    print("Starting data processing")  # Don't do this!
    result = transform_data(data)
    print(f"Processed {len(result)} records")  # Don't do this!
    return result
```

#### Logging Configuration
```python
# Standard logging setup for the project
import logging

def setup_logging(verbose: bool = False):
    """Configure logging for the application."""
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
```

### 2. **Modular Design with Single Responsibility**

#### Function Design Principles
- **Single Responsibility**: Each function should do ONE thing well
- **Clear Input/Output**: Explicit parameters and return values
- **Pure Functions**: When possible, avoid side effects
- **Descriptive Names**: Function names should clearly indicate purpose

#### Examples
```python
# ✅ GOOD - Single responsibility, testable
def validate_connection_params(host: str, port: int, database: str) -> bool:
    """Validate database connection parameters."""
    if not host or not isinstance(host, str):
        return False
    if not isinstance(port, int) or port < 1 or port > 65535:
        return False
    if not database or not isinstance(database, str):
        return False
    return True

def create_connection_string(host: str, port: int, database: str) -> str:
    """Create database connection string from parameters."""
    return f"host={host};port={port};database={database}"

def establish_connection(connection_string: str) -> Connection:
    """Establish database connection using connection string."""
    # Connection logic here
    pass

# ❌ BAD - Multiple responsibilities, hard to test
def connect_to_database(host, port, database):
    """Validate params, create connection string, and connect."""
    # Too many responsibilities in one function
    if not host:
        print("Invalid host")  # Wrong: using print
        return None
    
    conn_str = f"host={host};port={port};database={database}"
    # Connection logic mixed with validation
    return connection
```

#### Class Design Principles
- **Cohesion**: Related functionality grouped together
- **Low Coupling**: Minimal dependencies between classes
- **Interface Segregation**: Small, focused interfaces
- **Dependency Injection**: Pass dependencies as parameters

```python
# ✅ GOOD - Modular, testable class design
class ConnectionManager:
    """Manages database connections with clear responsibilities."""
    
    def __init__(self, connection_type: str, **kwargs):
        self.connection_type = connection_type
        self.connection_params = kwargs
        self.logger = logging.getLogger(__name__)
    
    def validate_config(self) -> bool:
        """Validate connection configuration."""
        # Single responsibility: validation only
        pass
    
    def create_connection(self) -> Connection:
        """Create and return a database connection."""
        # Single responsibility: connection creation only
        pass
    
    def test_connection(self) -> bool:
        """Test if connection can be established."""
        # Single responsibility: connection testing only
        pass

# ❌ BAD - Monolithic class with mixed responsibilities
class DatabaseHandler:
    """Handles everything database related."""
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def do_everything(self, host, port, database, query, process_data=True):
        # Too many responsibilities in one method
        # Validates, connects, queries, processes, and transfers data
        pass
```

### 3. **Testability Requirements**

#### Function Testability
- **Pure Functions**: No side effects, same input = same output
- **Dependency Injection**: Pass dependencies as parameters
- **Small Scope**: Functions should be easy to mock and test
- **Clear Contracts**: Well-defined input/output expectations

```python
# ✅ GOOD - Highly testable function
def calculate_chunk_size(total_rows: int, max_memory_mb: int) -> int:
    """
    Calculate optimal chunk size based on total rows and memory constraints.
    
    Args:
        total_rows: Total number of rows to process
        max_memory_mb: Maximum memory available in MB
        
    Returns:
        Optimal chunk size in rows
    """
    if total_rows <= 0 or max_memory_mb <= 0:
        raise ValueError("Total rows and max memory must be positive")
    
    # Simple calculation, easy to test
    estimated_memory_per_row = 0.001  # MB per row
    max_rows_per_chunk = int(max_memory_mb / estimated_memory_per_row)
    
    # Ensure reasonable bounds
    return max(1000, min(max_rows_per_chunk, total_rows))

# ❌ BAD - Hard to test function
def process_data_with_side_effects():
    """Process data with global state changes and file I/O."""
    global some_global_variable
    some_global_variable += 1
    
    with open('/tmp/data.txt', 'w') as f:
        f.write('processed')
    
    # Hard to test: depends on global state and file system
    return some_global_variable
```

#### Class Testability
- **Constructor Injection**: Dependencies passed to constructor
- **Interface Abstractions**: Use interfaces for external dependencies
- **Method Isolation**: Methods should be independently testable
- **State Management**: Clear state transitions and validation

```python
# ✅ GOOD - Testable class with dependency injection
class QueryExecutor:
    """Executes database queries with dependency injection."""
    
    def __init__(self, connection_manager: ConnectionManager):
        self.connection_manager = connection_manager
        self.logger = logging.getLogger(__name__)
    
    def execute_query(self, query: str) -> List[Dict]:
        """Execute a SQL query and return results."""
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        
        connection = self.connection_manager.create_connection()
        try:
            # Query execution logic
            return results
        finally:
            connection.close()

# ❌ BAD - Hard to test class with tight coupling
class QueryExecutor:
    def __init__(self):
        # Creates its own dependencies - hard to mock
        self.connection = create_real_database_connection()
    
    def execute_query(self, query):
        # Directly uses real database - can't test without DB
        return self.connection.execute(query)
```

## 📋 Code Quality Standards

### 1. **Type Hints**
Always use type hints for function parameters and return values:

```python
from typing import List, Dict, Optional, Union

def process_chunk(
    data: List[Dict[str, Any]], 
    chunk_size: int,
    output_format: str = 'parquet'
) -> bool:
    """Process a chunk of data with type hints."""
    pass
```

### 2. **Documentation**
- **Docstrings**: Every function and class must have docstrings in **reStructuredText format**
- **Parameter Documentation**: Document all parameters and return values
- **Examples**: Include usage examples for complex functions

```python
def transfer_query_results(
    query: str, 
    target_path: str, 
    chunk_size: int = 1000000
) -> bool:
    """Transfer query results to target location in chunks.
    
    This function executes a SQL query, processes the results in chunks,
    and transfers the data to the specified HDFS path. It uses vectorized
    operations for optimal performance when processing large datasets.
    
    :param query: SQL query to execute on the source database
    :type query: str
    :param target_path: HDFS path for output files
    :type target_path: str
    :param chunk_size: Number of rows per chunk (default: 1M)
    :type chunk_size: int
    :return: True if transfer successful, False otherwise
    :rtype: bool
    :raises ValueError: If query is empty or invalid
    :raises ConnectionError: If database connection fails
    :raises OSError: If file system operations fail
    
    :Example:
        Transfer user data for a specific date:
        
        >>> success = transfer_query_results(
        ...     "SELECT * FROM users WHERE date = '2024-01-01'",
        ...     "/data/landing/users"
        ... )
        >>> print(f"Transfer {'successful' if success else 'failed'}")
        Transfer successful
    """
    pass
```

### 3. **Error Handling**
- **Specific Exceptions**: Use specific exception types
- **Context Information**: Include relevant context in error messages
- **Graceful Degradation**: Handle errors without crashing
- **Logging**: Log all errors with appropriate levels

```python
def safe_database_operation(func):
    """Decorator for safe database operations with logging."""
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(__name__)
        try:
            return func(*args, **kwargs)
        except ConnectionError as e:
            logger.error(f"Database connection failed: {e}")
            raise
        except ValueError as e:
            logger.warning(f"Invalid input: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper
```

### 4. **Configuration Management**
- **Environment Variables**: Use environment variables for configuration
- **Configuration Files**: Support JSON/YAML configuration files
- **Validation**: Validate all configuration parameters
- **Defaults**: Provide sensible defaults for all parameters

```python
import os
from typing import Dict, Any

class Config:
    """Configuration management with validation."""
    
    def __init__(self, config_file: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_file)
        self._validate_config()
    
    def _load_config(self, config_file: Optional[str]) -> Dict[str, Any]:
        """Load configuration from file and environment variables."""
        config = {
            'chunk_size': int(os.getenv('CHUNK_SIZE', '1000000')),
            'max_workers': int(os.getenv('MAX_WORKERS', '4')),
            'temp_dir': os.getenv('TEMP_DIR', '/tmp/impala_transfer'),
            'output_format': os.getenv('OUTPUT_FORMAT', 'parquet')
        }
        
        if config_file:
            # Load from file and merge with environment variables
            pass
        
        return config
    
    def _validate_config(self):
        """Validate configuration parameters."""
        if self.config['chunk_size'] <= 0:
            raise ValueError("Chunk size must be positive")
        if self.config['max_workers'] <= 0:
            raise ValueError("Max workers must be positive")
```

## 🧪 Testing Standards

### 1. **Test Structure**
- **Unit Tests**: Test individual functions and methods
- **Integration Tests**: Test component interactions
- **Mocking**: Use mocks for external dependencies
- **Test Data**: Use realistic but minimal test data

```python
import pytest
from unittest.mock import Mock, patch

class TestQueryExecutor:
    """Test cases for QueryExecutor class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_connection_manager = Mock()
        self.executor = QueryExecutor(self.mock_connection_manager)
    
    def test_execute_query_success(self):
        """Test successful query execution."""
        # Arrange
        query = "SELECT * FROM test_table"
        expected_results = [{'id': 1, 'name': 'test'}]
        mock_connection = Mock()
        mock_connection.execute.return_value = expected_results
        self.mock_connection_manager.create_connection.return_value = mock_connection
        
        # Act
        result = self.executor.execute_query(query)
        
        # Assert
        assert result == expected_results
        mock_connection.close.assert_called_once()
    
    def test_execute_query_empty_query(self):
        """Test that empty query raises ValueError."""
        with pytest.raises(ValueError, match="Query cannot be empty"):
            self.executor.execute_query("")
```

### 2. **Test Coverage**
- **Minimum Coverage**: Aim for 90%+ code coverage
- **Critical Paths**: Ensure all critical paths are tested
- **Edge Cases**: Test boundary conditions and error cases
- **Performance Tests**: Test performance characteristics

### 3. **Test Naming**
- **Descriptive Names**: Test names should describe the scenario
- **Given-When-Then**: Structure tests with clear setup, action, assertion
- **Consistent Format**: Use consistent naming conventions

## 🔧 Development Tools Configuration

### 1. **Pre-commit Hooks**
```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.3.0
    hooks:
      - id: black
        language_version: python3
  
  - repo: https://github.com/pycqa/flake8
    rev: 6.0.0
    hooks:
      - id: flake8
  
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.3.0
    hooks:
      - id: mypy
        additional_dependencies: [types-all]
```

### 2. **Editor Configuration**
```json
// .vscode/settings.json
{
    "python.linting.enabled": true,
    "python.linting.flake8Enabled": true,
    "python.formatting.provider": "black",
    "python.testing.pytestEnabled": true,
    "python.testing.unittestEnabled": false,
    "python.testing.pytestArgs": [
        "tests"
    ]
}
```

### 3. **Makefile Commands**
```makefile
# Makefile
.PHONY: test lint format check clean

test:
	pytest tests/ --cov=impala_transfer --cov-report=html

lint:
	flake8 impala_transfer/ tests/
	mypy impala_transfer/

format:
	black impala_transfer/ tests/
	isort impala_transfer/ tests/

check: lint test

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .coverage htmlcov/
```

## 📝 Code Review Checklist

### Before Submitting Code
- [ ] All functions use logging instead of print statements
- [ ] Each function has a single responsibility
- [ ] All functions have type hints and docstrings
- [ ] Error handling is implemented with appropriate logging
- [ ] Unit tests are written and passing
- [ ] Code follows the established patterns and conventions
- [ ] No hardcoded values (use configuration)
- [ ] Dependencies are injected rather than created internally
- [ ] Code is formatted with Black and passes flake8
- [ ] Type checking passes with mypy

### During Code Review
- [ ] Check for logging usage instead of print statements
- [ ] Verify single responsibility principle
- [ ] Ensure testability of all functions
- [ ] Validate error handling and logging
- [ ] Check documentation completeness
- [ ] Verify configuration management
- [ ] Review test coverage and quality

## 🚀 Continuous Integration

### GitHub Actions Workflow
```yaml
# .github/workflows/ci.yml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.8, 3.9, 3.10, 3.11]
    
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        pip install -e .[dev]
    
    - name: Run tests
      run: |
        pytest tests/ --cov=impala_transfer --cov-report=xml
    
    - name: Run linting
      run: |
        flake8 impala_transfer/ tests/
        mypy impala_transfer/
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
```

## 🚀 Performance and Programming Style Standards

### 1. **Performance Optimization with Vectorized Operations**

#### Core Principle
**Prefer vectorized operations over pure Python loops** for data processing tasks. Use NumPy, Pandas, and PyArrow extensively for optimal performance.

#### Vectorized Operations Guidelines
- **NumPy Arrays**: Use for numerical computations and array operations
- **Pandas DataFrames**: Use for tabular data processing and analysis
- **PyArrow**: Use for efficient data serialization and memory management
- **Avoid Python Loops**: Replace loops with vectorized operations where possible

#### Examples
```python
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ✅ GOOD - Vectorized operations for performance
def process_data_vectorized(data: pd.DataFrame) -> pd.DataFrame:
    """Process data using vectorized operations for optimal performance.
    
    This function demonstrates the use of vectorized operations for
    data processing, avoiding slow Python loops.
    
    :param data: Input DataFrame to process
    :type data: pd.DataFrame
    :return: Processed DataFrame with computed columns
    :rtype: pd.DataFrame
    
    :Example:
        Process user data with vectorized operations:
        
        >>> df = pd.DataFrame({'age': [25, 30, 35], 'income': [50000, 60000, 70000]})
        >>> result = process_data_vectorized(df)
        >>> print(result['age_group'].tolist())
        ['young', 'adult', 'adult']
    """
    # Vectorized conditional operations
    data['age_group'] = np.where(data['age'] < 30, 'young', 'adult')
    
    # Vectorized mathematical operations
    data['income_normalized'] = (data['income'] - data['income'].mean()) / data['income'].std()
    
    # Vectorized string operations
    data['status'] = data['income'].apply(lambda x: 'high' if x > 65000 else 'medium' if x > 55000 else 'low')
    
    return data

# ❌ BAD - Pure Python loops (slow)
def process_data_slow(data: pd.DataFrame) -> pd.DataFrame:
    """Process data using slow Python loops."""
    for index, row in data.iterrows():
        if row['age'] < 30:
            data.at[index, 'age_group'] = 'young'
        else:
            data.at[index, 'age_group'] = 'adult'
        
        # More slow operations...
    return data

# ✅ GOOD - PyArrow for efficient data handling
def save_data_efficient(data: pd.DataFrame, filepath: str) -> None:
    """Save data efficiently using PyArrow.
    
    :param data: DataFrame to save
    :type data: pd.DataFrame
    :param filepath: Output file path
    :type filepath: str
    """
    # Convert to PyArrow table for efficient serialization
    table = pa.Table.from_pandas(data)
    
    # Write with compression for space efficiency
    pq.write_table(table, filepath, compression='snappy')

# ✅ GOOD - NumPy for numerical operations
def calculate_statistics_vectorized(values: np.ndarray) -> dict:
    """Calculate statistics using vectorized NumPy operations.
    
    :param values: Array of numerical values
    :type values: np.ndarray
    :return: Dictionary of calculated statistics
    :rtype: dict
    """
    return {
        'mean': np.mean(values),
        'std': np.std(values),
        'min': np.min(values),
        'max': np.max(values),
        'percentiles': np.percentile(values, [25, 50, 75])
    }
```

### 2. **Functional Programming Style**

#### Core Principle
**Prefer functional programming over class-heavy approaches**, but use classes where they make sense. Favor composition over inheritance.

#### Functional Programming Guidelines
- **Pure Functions**: Functions with no side effects, same input = same output
- **Immutable Data**: Avoid modifying input data, return new data instead
- **Higher-Order Functions**: Use functions that take or return other functions
- **Composition**: Combine simple functions to build complex behavior

#### Examples
```python
from typing import Callable, List, Dict, Any
from functools import reduce, partial
import pandas as pd

# ✅ GOOD - Functional programming approach
def create_data_pipeline() -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Create a data processing pipeline using functional composition.
    
    :return: Composed function that processes data
    :rtype: Callable[[pd.DataFrame], pd.DataFrame]
    """
    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        """Remove null values and duplicates."""
        return df.dropna().drop_duplicates()
    
    def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Add computed columns."""
        df = df.copy()  # Immutable approach
        df['processed'] = True
        return df
    
    def validate_data(df: pd.DataFrame) -> pd.DataFrame:
        """Validate data integrity."""
        if df.empty:
            raise ValueError("DataFrame cannot be empty")
        return df
    
    # Compose functions using functional programming
    pipeline = lambda df: validate_data(add_derived_columns(clean_data(df)))
    return pipeline

# ✅ GOOD - Higher-order functions
def apply_transformations(data: pd.DataFrame, transformations: List[Callable]) -> pd.DataFrame:
    """Apply a list of transformations to data.
    
    :param data: Input DataFrame
    :type data: pd.DataFrame
    :param transformations: List of transformation functions
    :type transformations: List[Callable]
    :return: Transformed DataFrame
    :rtype: pd.DataFrame
    """
    return reduce(lambda df, transform: transform(df), transformations, data)

# ✅ GOOD - Partial functions for configuration
def create_filter_function(column: str, value: Any) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Create a filter function for a specific column and value.
    
    :param column: Column name to filter on
    :type column: str
    :param value: Value to filter for
    :type value: Any
    :return: Filter function
    :rtype: Callable[[pd.DataFrame], pd.DataFrame]
    """
    return partial(lambda df, col, val: df[df[col] == val], col=column, val=value)

# ❌ BAD - Class-heavy approach for simple operations
class DataProcessor:
    """Over-engineered class for simple data processing."""
    
    def __init__(self):
        self.data = None
    
    def set_data(self, data):
        self.data = data
    
    def process_data(self):
        # Complex class logic for simple operations
        pass
    
    def get_result(self):
        return self.data

# ✅ GOOD - Simple functional approach
def process_data_functional(data: pd.DataFrame) -> pd.DataFrame:
    """Process data using functional programming principles."""
    return (data
            .dropna()
            .drop_duplicates()
            .assign(processed=True))
```

### 3. **Composition Over Inheritance**

#### Core Principle
**Prefer composition over inheritance** for code reuse and flexibility.

#### Composition Guidelines
- **Has-a relationships**: Use composition to build complex objects
- **Interface segregation**: Small, focused interfaces
- **Dependency injection**: Pass dependencies as parameters
- **Strategy pattern**: Use functions/objects for different behaviors

#### Examples
```python
from abc import ABC, abstractmethod
from typing import Protocol, Callable

# ✅ GOOD - Composition with protocols
class DataProcessor(Protocol):
    """Protocol for data processing strategies."""
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process the data."""
        ...

class ValidationStrategy(Protocol):
    """Protocol for validation strategies."""
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate the data."""
        ...

# Concrete implementations
class CleanDataProcessor:
    """Data processor that cleans the data."""
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Remove null values and duplicates."""
        return data.dropna().drop_duplicates()

class AgeValidationStrategy:
    """Validation strategy for age data."""
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate age values are reasonable."""
        return (data['age'] >= 0).all() and (data['age'] <= 120).all()

# ✅ GOOD - Composition-based design
class DataPipeline:
    """Data pipeline using composition."""
    
    def __init__(self, 
                 processor: DataProcessor,
                 validator: ValidationStrategy,
                 logger: logging.Logger):
        self.processor = processor
        self.validator = validator
        self.logger = logger
    
    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Execute the data pipeline."""
        self.logger.info("Starting data pipeline")
        
        # Process data
        processed_data = self.processor.process(data)
        
        # Validate data
        if not self.validator.validate(processed_data):
            raise ValueError("Data validation failed")
        
        self.logger.info("Data pipeline completed successfully")
        return processed_data

# ❌ BAD - Inheritance-based design
class BaseDataProcessor(ABC):
    """Base class with inheritance."""
    
    @abstractmethod
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
    
    def validate(self, data: pd.DataFrame) -> bool:
        # Common validation logic
        pass

class SpecificDataProcessor(BaseDataProcessor):
    """Specific processor inheriting from base."""
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        # Implementation
        pass
    
    # Forced to inherit validation logic even if not needed
```

### 4. **Performance Best Practices**

#### Memory Management
- **Use PyArrow**: For efficient memory management and serialization
- **Avoid copying**: Use views and references when possible
- **Chunk processing**: Process large datasets in chunks
- **Lazy evaluation**: Use generators and iterators for large datasets

#### JIT Compilation with Numba
- **Use Numba JIT**: For computationally intensive functions that can't be vectorized
- **Performance profiling**: Profile code to identify bottlenecks before applying JIT
- **Numba compatibility**: Ensure functions use Numba-compatible operations
- **Fallback strategy**: Provide non-JIT fallback for unsupported operations

#### Vectorization Guidelines
```python
# ✅ GOOD - Efficient vectorized operations
def efficient_data_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Process data efficiently using vectorized operations.
    
    :param df: Input DataFrame
    :type df: pd.DataFrame
    :return: Processed DataFrame
    :rtype: pd.DataFrame
    """
    # Use NumPy for numerical operations
    df['normalized_value'] = (df['value'] - df['value'].mean()) / df['value'].std()
    
    # Use Pandas vectorized string operations
    df['category'] = df['text'].str.extract(r'(\w+)').fillna('unknown')
    
    # Use NumPy for conditional operations
    df['status'] = np.select(
        [df['value'] > 100, df['value'] > 50],
        ['high', 'medium'],
        default='low'
    )
    
    return df

# ✅ GOOD - PyArrow for efficient I/O
def efficient_file_operations(data: pd.DataFrame, filepath: str) -> None:
    """Efficient file operations using PyArrow.
    
    :param data: DataFrame to save
    :type data: pd.DataFrame
    :param filepath: Output file path
    :type filepath: str
    """
    # Convert to PyArrow table for efficient serialization
    table = pa.Table.from_pandas(data)
    
    # Use efficient compression
    pq.write_table(table, filepath, compression='snappy', row_group_size=100000)

# ✅ GOOD - Numba JIT for computational bottlenecks
from numba import jit
import numpy as np

@jit(nopython=True, cache=True)
def compute_complex_statistics(values: np.ndarray) -> tuple:
    """Compute complex statistics using Numba JIT compilation.
    
    This function demonstrates JIT compilation for computationally
    intensive operations that can't be easily vectorized.
    
    :param values: Array of numerical values
    :type values: np.ndarray
    :return: Tuple of computed statistics (mean, std, skewness, kurtosis)
    :rtype: tuple
    """
    n = len(values)
    if n == 0:
        return 0.0, 0.0, 0.0, 0.0
    
    # Compute mean
    mean = 0.0
    for i in range(n):
        mean += values[i]
    mean /= n
    
    # Compute variance and higher moments
    variance = 0.0
    skewness = 0.0
    kurtosis = 0.0
    
    for i in range(n):
        diff = values[i] - mean
        diff2 = diff * diff
        diff3 = diff2 * diff
        diff4 = diff3 * diff
        
        variance += diff2
        skewness += diff3
        kurtosis += diff4
    
    variance /= n
    std = np.sqrt(variance)
    
    if std > 0:
        skewness = (skewness / n) / (std ** 3)
        kurtosis = (kurtosis / n) / (std ** 4) - 3.0
    else:
        skewness = 0.0
        kurtosis = 0.0
    
    return mean, std, skewness, kurtosis

# ✅ GOOD - Conditional JIT compilation
def process_data_with_jit_fallback(data: np.ndarray, use_jit: bool = True) -> np.ndarray:
    """Process data with optional JIT compilation.
    
    :param data: Input array to process
    :type data: np.ndarray
    :param use_jit: Whether to use JIT compilation
    :type use_jit: bool
    :return: Processed array
    :rtype: np.ndarray
    """
    if use_jit:
        return _process_data_jit(data)
    else:
        return _process_data_python(data)

@jit(nopython=True)
def _process_data_jit(data: np.ndarray) -> np.ndarray:
    """JIT-compiled version of data processing."""
    result = np.empty_like(data)
    for i in range(len(data)):
        # Complex computation that benefits from JIT
        result[i] = np.sqrt(data[i] * data[i] + 1.0) * np.sin(data[i])
    return result

def _process_data_python(data: np.ndarray) -> np.ndarray:
    """Python fallback version of data processing."""
    result = np.empty_like(data)
    for i in range(len(data)):
        result[i] = np.sqrt(data[i] * data[i] + 1.0) * np.sin(data[i])
    return result

# ❌ BAD - Using JIT where vectorization is better
@jit(nopython=True)
def bad_jit_usage(data: np.ndarray) -> np.ndarray:
    """Bad example: using JIT for simple vectorized operations."""
    result = np.empty_like(data)
    for i in range(len(data)):
        result[i] = data[i] * 2  # This should use vectorization instead
    return result

# ✅ GOOD - Vectorized alternative
def good_vectorized_alternative(data: np.ndarray) -> np.ndarray:
    """Good example: using vectorization for simple operations."""
    return data * 2  # Much faster than JIT for simple operations
```

## 📊 Table Data Operations Standards

### 1. **Pandas with PyArrow Backend**

#### Core Principle
**Always use PyArrow as the backend for pandas operations** when working with table data. This provides significant performance improvements and better memory efficiency.

#### PyArrow Backend Guidelines
- **Set PyArrow as default**: Configure pandas to use PyArrow backend globally
- **Arrow-compatible operations**: Use operations that leverage Arrow's memory layout
- **Zero-copy operations**: Minimize data copying between Arrow and pandas
- **Memory efficiency**: Arrow's columnar format reduces memory usage

#### Implementation
```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

# ✅ GOOD - Configure pandas for PyArrow backend
def configure_pandas_arrow():
    """Configure pandas to use PyArrow backend for optimal performance."""
    pd.options.mode.dtype_backend = "pyarrow"
    pd.options.mode.string_storage = "pyarrow"
    pd.options.mode.copy_on_write = True

# ✅ GOOD - Efficient data loading with PyArrow
def load_data_efficient(filepath: str) -> pd.DataFrame:
    """Load data efficiently using PyArrow backend.
    
    :param filepath: Path to data file
    :type filepath: str
    :return: DataFrame with PyArrow backend
    :rtype: pd.DataFrame
    """
    if filepath.endswith('.parquet'):
        # Direct PyArrow loading for Parquet
        table = pq.read_table(filepath)
        return table.to_pandas()
    elif filepath.endswith('.csv'):
        # PyArrow CSV reading with chunking
        table = csv.read_csv(filepath, read_options=csv.ReadOptions(block_size=1024*1024))
        return table.to_pandas()
    else:
        # Fallback with PyArrow backend
        return pd.read_csv(filepath, engine='pyarrow')

# ✅ GOOD - Memory-efficient data processing
def process_large_dataset(filepath: str, chunk_size: int = 100000) -> pd.DataFrame:
    """Process large datasets in chunks to manage memory usage.
    
    :param filepath: Path to large data file
    :type filepath: str
    :param chunk_size: Number of rows per chunk
    :type chunk_size: int
    :return: Processed DataFrame
    :rtype: pd.DataFrame
    """
    # Use PyArrow for efficient chunked reading
    reader = pq.ParquetFile(filepath)
    
    processed_chunks = []
    for batch in reader.iter_batches(batch_size=chunk_size):
        # Convert batch to pandas with PyArrow backend
        chunk_df = batch.to_pandas()
        
        # Process chunk (vectorized operations)
        processed_chunk = process_chunk_vectorized(chunk_df)
        processed_chunks.append(processed_chunk)
    
    # Efficient concatenation using PyArrow
    return pd.concat(processed_chunks, ignore_index=True, copy=False)
```

### 2. **DuckDB for Fast Analytics**

#### Core Principle
**Use DuckDB for analytical queries and data transformations** when working with table data. DuckDB provides SQL-like operations with excellent performance.

#### DuckDB Guidelines
- **In-memory analytics**: Use for complex analytical queries
- **SQL operations**: Leverage SQL for data transformations
- **Arrow integration**: DuckDB works seamlessly with Arrow data
- **Parallel processing**: DuckDB automatically parallelizes operations

#### Examples
```python
import duckdb
import pandas as pd
import pyarrow as pa

# ✅ GOOD - DuckDB for analytical operations
def analyze_data_with_duckdb(data: pd.DataFrame) -> pd.DataFrame:
    """Perform complex analytical operations using DuckDB.
    
    :param data: Input DataFrame
    :type data: pd.DataFrame
    :return: Analyzed DataFrame
    :rtype: pd.DataFrame
    """
    # Create DuckDB connection
    con = duckdb.connect()
    
    # Register DataFrame as table
    con.register('data_table', data)
    
    # Complex analytical query
    query = """
    SELECT 
        category,
        COUNT(*) as count,
        AVG(value) as avg_value,
        STDDEV(value) as std_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median_value
    FROM data_table 
    WHERE value > 0
    GROUP BY category
    HAVING COUNT(*) > 10
    ORDER BY avg_value DESC
    """
    
    result = con.execute(query).df()
    con.close()
    
    return result

# ✅ GOOD - DuckDB with Arrow for large datasets
def process_large_dataset_duckdb(filepath: str) -> pd.DataFrame:
    """Process large datasets using DuckDB with Arrow integration.
    
    :param filepath: Path to large data file
    :type filepath: str
    :return: Processed DataFrame
    :rtype: pd.DataFrame
    """
    con = duckdb.connect()
    
    # Direct SQL query on file (no memory loading)
    query = f"""
    SELECT 
        *,
        CASE 
            WHEN value > 100 THEN 'high'
            WHEN value > 50 THEN 'medium'
            ELSE 'low'
        END as category
    FROM read_parquet('{filepath}')
    WHERE date >= '2024-01-01'
    """
    
    result = con.execute(query).df()
    con.close()
    
    return result
```

### 3. **Dask for Parallel Processing**

#### Core Principle
**Use Dask for parallel processing of large datasets** that don't fit in memory or require distributed computation.

#### Dask Guidelines
- **Chunked operations**: Process data in manageable chunks
- **Lazy evaluation**: Dask operations are lazy until compute()
- **Memory management**: Automatic memory management and spilling
- **Scalability**: Can scale from single machine to cluster

#### Examples
```python
import dask.dataframe as dd
import pandas as pd

# ✅ GOOD - Dask for large dataset processing
def process_large_dataset_dask(filepath: str, partition_size: str = "100MB") -> pd.DataFrame:
    """Process large datasets using Dask for parallel processing.
    
    :param filepath: Path to large data file
    :type filepath: str
    :param partition_size: Size of each partition
    :type partition_size: str
    :return: Processed DataFrame
    :rtype: pd.DataFrame
    """
    # Read data with Dask
    ddf = dd.read_parquet(filepath, engine='pyarrow')
    
    # Repartition for optimal processing
    ddf = ddf.repartition(partition_size=partition_size)
    
    # Perform operations (lazy evaluation)
    processed_ddf = (ddf
                    .dropna()
                    .assign(
                        normalized_value=lambda x: (x['value'] - x['value'].mean()) / x['value'].std(),
                        category=lambda x: pd.cut(x['value'], bins=5, labels=['very_low', 'low', 'medium', 'high', 'very_high'])
                    )
                    .groupby('category')
                    .agg({
                        'value': ['count', 'mean', 'std'],
                        'normalized_value': 'mean'
                    })
                    .reset_index())
    
    # Compute result (triggers actual computation)
    return processed_ddf.compute()

# ✅ GOOD - Dask with custom functions
def apply_custom_function_dask(ddf: dd.DataFrame, func: callable) -> dd.DataFrame:
    """Apply custom function to Dask DataFrame.
    
    :param ddf: Dask DataFrame
    :type ddf: dd.DataFrame
    :param func: Custom function to apply
    :type func: callable
    :return: Processed Dask DataFrame
    :rtype: dd.DataFrame
    """
    # Use map_partitions for custom functions
    return ddf.map_partitions(func)
```

### 4. **Efficient Database Operations**

#### Core Principle
**Use the most efficient database operations** for loading and transferring data. Avoid row-by-row operations in favor of bulk operations.

#### Database-Specific Optimizations

##### PostgreSQL
```python
import psycopg2
import pandas as pd
import io

# ✅ GOOD - PostgreSQL COPY command for bulk operations
def bulk_load_postgres(data: pd.DataFrame, table_name: str, connection_params: dict) -> None:
    """Bulk load data into PostgreSQL using COPY command.
    
    :param data: DataFrame to load
    :type data: pd.DataFrame
    :param table_name: Target table name
    :type table_name: str
    :param connection_params: Database connection parameters
    :type connection_params: dict
    """
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    # Convert DataFrame to CSV string
    output = io.StringIO()
    data.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    
    # Use COPY command for bulk insert
    cursor.copy_from(
        output, 
        table_name, 
        sep='\t', 
        null=''
    )
    
    conn.commit()
    cursor.close()
    conn.close()

# ✅ GOOD - PostgreSQL COPY TO for bulk export
def bulk_export_postgres(query: str, connection_params: dict) -> pd.DataFrame:
    """Bulk export data from PostgreSQL using COPY command.
    
    :param query: SQL query to execute
    :type query: str
    :param connection_params: Database connection parameters
    :type connection_params: dict
    :return: Exported DataFrame
    :rtype: pd.DataFrame
    """
    conn = psycopg2.connect(**connection_params)
    
    # Use COPY TO for efficient export
    copy_query = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
    
    # Read directly into DataFrame
    df = pd.read_csv(
        io.StringIO(conn.cursor().copy_expert(copy_query).read()),
        engine='pyarrow'
    )
    
    conn.close()
    return df
```

##### Impala
```python
from impala.dbapi import connect

# ✅ GOOD - Impala LOAD DATA for bulk operations
def bulk_load_impala(data: pd.DataFrame, table_name: str, connection_params: dict) -> None:
    """Bulk load data into Impala using LOAD DATA command.
    
    :param data: DataFrame to load
    :type data: pd.DataFrame
    :param table_name: Target table name
    :type table_name: str
    :param connection_params: Database connection parameters
    :type connection_params: dict
    """
    # Save DataFrame to temporary Parquet file
    temp_file = f"/tmp/temp_load_{table_name}.parquet"
    data.to_parquet(temp_file, engine='pyarrow', compression='snappy')
    
    conn = connect(**connection_params)
    cursor = conn.cursor()
    
    # Use LOAD DATA for bulk insert
    load_query = f"LOAD DATA INPATH '{temp_file}' INTO TABLE {table_name}"
    cursor.execute(load_query)
    
    cursor.close()
    conn.close()
    
    # Clean up temporary file
    import os
    os.remove(temp_file)

# ❌ BAD - Row-by-row insertion
def bad_row_insertion(data: pd.DataFrame, table_name: str, connection_params: dict) -> None:
    """Bad example: inserting data row by row."""
    conn = connect(**connection_params)
    cursor = conn.cursor()
    
    for _, row in data.iterrows():
        # Very slow: one INSERT per row
        cursor.execute(f"INSERT INTO {table_name} VALUES (%s, %s, %s)", 
                      (row['col1'], row['col2'], row['col3']))
    
    conn.commit()
    cursor.close()
    conn.close()
```

### 5. **Memory-Optimized Data Operations**

#### Core Principle
**Always optimize for both speed and memory utilization**. Use chunked operations and streaming to handle large datasets without memory issues.

#### Memory Optimization Guidelines
```python
# ✅ GOOD - Memory-efficient data processing pipeline
def memory_efficient_pipeline(filepath: str, output_path: str) -> None:
    """Process large datasets with memory-efficient pipeline.
    
    :param filepath: Input file path
    :type filepath: str
    :param output_path: Output file path
    :type output_path: str
    """
    # Use PyArrow for streaming read
    reader = pq.ParquetFile(filepath)
    
    # Process in chunks to manage memory
    for batch in reader.iter_batches(batch_size=50000):
        # Convert to pandas with PyArrow backend
        chunk_df = batch.to_pandas()
        
        # Process chunk
        processed_chunk = process_chunk_vectorized(chunk_df)
        
        # Write chunk to output (streaming write)
        write_chunk_to_output(processed_chunk, output_path, append=True)
        
        # Explicit cleanup
        del chunk_df, processed_chunk

# ✅ GOOD - Streaming data transformation
def stream_transform_data(input_path: str, output_path: str, transform_func: callable) -> None:
    """Stream transform data without loading entire dataset into memory.
    
    :param input_path: Input file path
    :type input_path: str
    :param output_path: Output file path
    :type output_path: str
    :param transform_func: Function to apply to each chunk
    :type transform_func: callable
    """
    # Use Dask for streaming operations
    ddf = dd.read_parquet(input_path, engine='pyarrow')
    
    # Apply transformation
    transformed_ddf = ddf.map_partitions(transform_func)
    
    # Write with streaming
    transformed_ddf.to_parquet(output_path, engine='pyarrow', compression='snappy')
```

## 🔐 Security and Secrets Management Standards

### 1. **Never Hardcode Secrets**

#### Core Principle
**NEVER include secrets, passwords, API keys, or sensitive configuration in source code**. All sensitive information must be externalized and managed securely.

#### What Constitutes a Secret
- **Database passwords** and connection credentials
- **API keys** and access tokens
- **Private keys** and certificates
- **Encryption keys** and salts
- **Service account credentials**
- **Database connection strings** with embedded credentials
- **Authentication tokens** and session keys

#### Examples of What NOT to Do
```python
# ❌ BAD - Hardcoded secrets in code
def connect_to_database():
    """Bad example: hardcoded database credentials."""
    connection_string = "postgresql://user:password123@localhost:5432/database"
    return psycopg2.connect(connection_string)

# ❌ BAD - Hardcoded API keys
class DataProcessor:
    def __init__(self):
        self.api_key = "sk-1234567890abcdef"  # Never do this!
        self.secret_token = "my_secret_token_123"  # Never do this!

# ❌ BAD - Secrets in configuration files
config = {
    "database": {
        "host": "localhost",
        "password": "mypassword123",  # Never in code!
        "username": "admin"
    }
}
```

### 2. **Secure Configuration Management**

#### Environment Variables
Use environment variables for all sensitive configuration:

```python
import os
from typing import Optional

# ✅ GOOD - Environment variables for secrets
def get_database_connection() -> psycopg2.extensions.connection:
    """Get database connection using environment variables.
    
    :return: Database connection
    :rtype: psycopg2.extensions.connection
    :raises ValueError: If required environment variables are missing
    """
    # Get credentials from environment variables
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT', '5432')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    
    # Validate required variables
    if not all([host, database, username, password]):
        raise ValueError("Missing required database environment variables")
    
    # Construct connection string without embedding credentials
    connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    return psycopg2.connect(connection_string)

# ✅ GOOD - Configuration class with environment variables
class DatabaseConfig:
    """Database configuration using environment variables."""
    
    def __init__(self):
        self.host = self._get_required_env('DB_HOST')
        self.port = int(self._get_env_with_default('DB_PORT', '5432'))
        self.database = self._get_required_env('DB_NAME')
        self.username = self._get_required_env('DB_USER')
        self.password = self._get_required_env('DB_PASSWORD')
    
    def _get_required_env(self, key: str) -> str:
        """Get required environment variable.
        
        :param key: Environment variable name
        :type key: str
        :return: Environment variable value
        :rtype: str
        :raises ValueError: If environment variable is not set
        """
        value = os.getenv(key)
        if not value:
            raise ValueError(f"Required environment variable {key} is not set")
        return value
    
    def _get_env_with_default(self, key: str, default: str) -> str:
        """Get environment variable with default value.
        
        :param key: Environment variable name
        :type key: str
        :param default: Default value if not set
        :type default: str
        :return: Environment variable value or default
        :rtype: str
        """
        return os.getenv(key, default)
```

#### Configuration Files
Use external configuration files that are not committed to version control:

```python
import json
import os
from pathlib import Path

# ✅ GOOD - External configuration file
def load_config_from_file(config_path: Optional[str] = None) -> dict:
    """Load configuration from external file.
    
    :param config_path: Path to configuration file
    :type config_path: Optional[str]
    :return: Configuration dictionary
    :rtype: dict
    """
    if config_path is None:
        config_path = os.getenv('CONFIG_PATH', 'config.json')
    
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # Validate that no secrets are in the config file
    self._validate_config_security(config)
    
    return config

def _validate_config_security(self, config: dict) -> None:
    """Validate that configuration doesn't contain hardcoded secrets.
    
    :param config: Configuration dictionary
    :type config: dict
    :raises ValueError: If secrets are found in configuration
    """
    sensitive_keys = ['password', 'secret', 'key', 'token', 'credential']
    
    def check_dict(d: dict, path: str = ""):
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
```

### 3. **Secret Management Tools**

#### Using Secret Management Services
Integrate with proper secret management services:

```python
import boto3
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import google.cloud.secretmanager as secretmanager

# ✅ GOOD - AWS Secrets Manager
def get_secret_from_aws(secret_name: str) -> str:
    """Get secret from AWS Secrets Manager.
    
    :param secret_name: Name of the secret
    :type secret_name: str
    :return: Secret value
    :rtype: str
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=os.getenv('AWS_REGION', 'us-east-1')
    )
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return response['SecretString']
    except Exception as e:
        raise ValueError(f"Failed to retrieve secret {secret_name}: {e}")

# ✅ GOOD - Azure Key Vault
def get_secret_from_azure(secret_name: str) -> str:
    """Get secret from Azure Key Vault.
    
    :param secret_name: Name of the secret
    :type secret_name: str
    :return: Secret value
    :rtype: str
    """
    credential = DefaultAzureCredential()
    client = SecretClient(
        vault_url=os.getenv('AZURE_KEY_VAULT_URL'),
        credential=credential
    )
    
    try:
        secret = client.get_secret(secret_name)
        return secret.value
    except Exception as e:
        raise ValueError(f"Failed to retrieve secret {secret_name}: {e}")

# ✅ GOOD - Google Secret Manager
def get_secret_from_gcp(secret_name: str) -> str:
    """Get secret from Google Cloud Secret Manager.
    
    :param secret_name: Name of the secret
    :type secret_name: str
    :return: Secret value
    :rtype: str
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{os.getenv('GCP_PROJECT_ID')}/secrets/{secret_name}/versions/latest"
    
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        raise ValueError(f"Failed to retrieve secret {secret_name}: {e}")
```

### 4. **Secure Connection Management**

#### Database Connections
Always use secure connection methods:

```python
# ✅ GOOD - Secure database connection with environment variables
class SecureDatabaseConnection:
    """Secure database connection manager."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.config = self._load_secure_config()
    
    def _load_secure_config(self) -> dict:
        """Load secure configuration from environment or secret manager.
        
        :return: Database configuration
        :rtype: dict
        """
        # Try environment variables first
        if all(os.getenv(var) for var in ['DB_HOST', 'DB_USER', 'DB_PASSWORD']):
            return {
                'host': os.getenv('DB_HOST'),
                'port': int(os.getenv('DB_PORT', '5432')),
                'database': os.getenv('DB_NAME'),
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD')
            }
        
        # Fallback to secret manager
        secret_name = os.getenv('DB_SECRET_NAME')
        if secret_name:
            secret_value = get_secret_from_aws(secret_name)  # or other secret manager
            return json.loads(secret_value)
        
        raise ValueError("No secure database configuration found")
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """Get secure database connection.
        
        :return: Database connection
        :rtype: psycopg2.extensions.connection
        """
        try:
            connection = psycopg2.connect(**self.config)
            self.logger.info("Database connection established successfully")
            return connection
        except Exception as e:
            self.logger.error(f"Failed to establish database connection: {e}")
            raise
```

### 5. **Security Best Practices**

#### Code Review Checklist for Security
- [ ] No hardcoded passwords, API keys, or secrets
- [ ] All sensitive configuration uses environment variables
- [ ] Configuration files are not committed to version control
- [ ] Secret management services are used where appropriate
- [ ] Connection strings don't contain embedded credentials
- [ ] Logging doesn't expose sensitive information
- [ ] Error messages don't leak sensitive data

#### Environment Setup
```bash
# ✅ GOOD - Environment variable setup
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=mydatabase
export DB_USER=myuser
export DB_PASSWORD=mysecurepassword
export API_KEY=my_api_key_here

# ✅ GOOD - .env file (not committed to git)
# .env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydatabase
DB_USER=myuser
DB_PASSWORD=mysecurepassword
API_KEY=my_api_key_here
```

#### Git Ignore Configuration
```gitignore
# ✅ GOOD - .gitignore entries for security
.env
*.key
*.pem
*.p12
*.pfx
config.json
secrets.json
credentials.json
*.secret
```

#### Logging Security
```python
# ✅ GOOD - Secure logging (no sensitive data)
def log_connection_attempt(host: str, database: str, success: bool) -> None:
    """Log connection attempt without exposing credentials.
    
    :param host: Database host
    :type host: str
    :param database: Database name
    :type database: str
    :param success: Whether connection was successful
    :type success: bool
    """
    if success:
        logger.info(f"Successfully connected to database {database} on {host}")
    else:
        logger.error(f"Failed to connect to database {database} on {host}")
    
    # Never log passwords, usernames, or connection strings
    # logger.info(f"Connected with user {username}")  # ❌ BAD
```

### 6. **Secret Rotation and Management**

#### Regular Secret Rotation
```python
# ✅ GOOD - Secret rotation support
class SecretManager:
    """Manages secret rotation and validation."""
    
    def __init__(self, secret_name: str):
        self.secret_name = secret_name
        self.logger = logging.getLogger(__name__)
    
    def get_current_secret(self) -> str:
        """Get current secret value.
        
        :return: Current secret value
        :rtype: str
        """
        return get_secret_from_aws(self.secret_name)
    
    def validate_secret(self, secret: str) -> bool:
        """Validate secret is still valid.
        
        :param secret: Secret to validate
        :type secret: str
        :return: True if secret is valid
        :rtype: bool
        """
        # Implement secret validation logic
        # e.g., test database connection, API call, etc.
        pass
    
    def rotate_secret(self) -> str:
        """Rotate secret and return new value.
        
        :return: New secret value
        :rtype: str
        """
        # Implement secret rotation logic
        # This would typically involve creating a new secret
        # and updating the secret manager
        pass
```

This comprehensive coding standards document ensures consistent, maintainable, testable, secure, and high-performance code throughout the Impala Transfer Tool project. 

## 🖥️ Command-Line Interface (CLI) Standards

### 1. **CLI Design Principles**

#### Core Principle
**Every program should have a well-structured, self-documenting CLI** that is suitable for both interactive use and automated execution by schedulers.

#### CLI Design Guidelines
- **Self-documenting**: Comprehensive help and usage information
- **Scheduler-friendly**: Consistent exit codes and non-interactive operation
- **Configuration-driven**: Support for configuration files and environment variables
- **Validation**: Robust input validation with clear error messages
- **Logging**: Structured logging suitable for automation
- **Progress reporting**: Real-time progress for long-running operations

#### CLI Structure
```python
#!/usr/bin/env python3
"""
Impala Transfer Tool - Command Line Interface

A tool for transferring large datasets from database clusters to HDFS/Hive
with support for multiple database connection types.

Usage:
    impala-transfer --source-host HOST --table TABLE_NAME
    impala-transfer --source-host HOST --query "SELECT * FROM table"
    impala-transfer --config config.json
    impala-transfer --help

Examples:
    # Transfer entire table
    impala-transfer --source-host impala-cluster.example.com --table users

    # Transfer query results
    impala-transfer --source-host impala-cluster.example.com \
        --query "SELECT * FROM users WHERE date = '2024-01-01'" \
        --target-table users_20240101

    # Use configuration file
    impala-transfer --config production_config.json

    # Test connection
    impala-transfer --source-host impala-cluster.example.com --test-connection

Exit Codes:
    0 - Success
    1 - General error
    2 - Configuration error
    3 - Connection error
    4 - Validation error
    5 - Permission error
"""

import sys
import argparse
import logging
import json
from pathlib import Path
from typing import Optional, Dict, Any

def create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser.
    
    :return: Configured argument parser
    :rtype: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        prog='impala-transfer',
        description='Transfer large datasets from database clusters to HDFS/Hive',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Transfer entire table
  impala-transfer --source-host impala-cluster.example.com --table users

  # Transfer query results with custom settings
  impala-transfer --source-host impala-cluster.example.com \\
      --query "SELECT * FROM users WHERE date = '2024-01-01'" \\
      --chunk-size 500000 --max-workers 8

  # Use configuration file
  impala-transfer --config production_config.json

  # Test connection
  impala-transfer --source-host impala-cluster.example.com --test-connection

  # Validate configuration
  impala-transfer --config config.json --validate-config

Exit Codes:
  0 - Success
  1 - General error
  2 - Configuration error
  3 - Connection error
  4 - Validation error
  5 - Permission error
        """
    )
    
    # Add argument groups for better organization
    connection_group = parser.add_argument_group('Database Connection')
    query_group = parser.add_argument_group('Query Configuration')
    processing_group = parser.add_argument_group('Processing Options')
    output_group = parser.add_argument_group('Output Configuration')
    utility_group = parser.add_argument_group('Utility Commands')
    
    # Database connection arguments
    connection_group.add_argument(
        '--source-host',
        required=True,
        help='Database host for source cluster (required)'
    )
    connection_group.add_argument(
        '--source-port',
        type=int,
        default=21050,
        help='Database port (default: 21050 for Impala)'
    )
    connection_group.add_argument(
        '--source-database',
        default='default',
        help='Source database name (default: default)'
    )
    connection_group.add_argument(
        '--connection-type',
        choices=['auto', 'impyla', 'pyodbc', 'sqlalchemy'],
        default='auto',
        help='Database connection type (default: auto)'
    )
    connection_group.add_argument(
        '--odbc-driver',
        help='ODBC driver name (required for pyodbc connection type)'
    )
    connection_group.add_argument(
        '--sqlalchemy-url',
        help='SQLAlchemy connection URL (e.g., "postgresql://user:pass@host:port/db")'
    )
    
    # Query configuration arguments
    query_group.add_argument(
        '--table',
        help='Table name to transfer (entire table)'
    )
    query_group.add_argument(
        '--query',
        help='SQL query to execute (alternative to --table)'
    )
    query_group.add_argument(
        '--query-file',
        type=Path,
        help='File containing SQL query'
    )
    query_group.add_argument(
        '--target-table',
        help='Target table name (defaults to table name or query_result)'
    )
    
    # Processing options
    processing_group.add_argument(
        '--chunk-size',
        type=int,
        default=1000000,
        help='Number of rows per chunk (default: 1000000)'
    )
    processing_group.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    processing_group.add_argument(
        '--temp-dir',
        default='/tmp/impala_transfer',
        help='Temporary directory for intermediate files'
    )
    
    # Output configuration
    output_group.add_argument(
        '--target-hdfs-path',
        help='HDFS path on target cluster'
    )
    output_group.add_argument(
        '--output-format',
        choices=['parquet', 'csv'],
        default='parquet',
        help='Output format (default: parquet)'
    )
    output_group.add_argument(
        '--compression',
        choices=['snappy', 'gzip', 'brotli', 'none'],
        default='snappy',
        help='Compression format (default: snappy)'
    )
    
    # Configuration file
    parser.add_argument(
        '--config',
        type=Path,
        help='Configuration file path (JSON format)'
    )
    
    # Utility commands
    utility_group.add_argument(
        '--test-connection',
        action='store_true',
        help='Test database connection and exit'
    )
    utility_group.add_argument(
        '--validate-config',
        action='store_true',
        help='Validate configuration and exit'
    )
    utility_group.add_argument(
        '--show-config',
        action='store_true',
        help='Show current configuration and exit'
    )
    utility_group.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without executing'
    )
    
    # Logging and output
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress non-error output'
    )
    parser.add_argument(
        '--log-file',
        type=Path,
        help='Log file path (default: stderr)'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    return parser
```

### 2. **Configuration Management**

#### Configuration File Support
```python
def load_configuration(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load configuration from file and environment variables.
    
    :param config_path: Path to configuration file
    :type config_path: Optional[Path]
    :return: Configuration dictionary
    :rtype: Dict[str, Any]
    """
    config = {}
    
    # Load from file if provided
    if config_path and config_path.exists():
        try:
            with open(config_path, 'r') as f:
                file_config = json.load(f)
            config.update(file_config)
        except Exception as e:
            raise ValueError(f"Failed to load configuration file {config_path}: {e}")
    
    # Override with environment variables
    config.update(get_environment_config())
    
    return config

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
    if os.getenv('COMPRESSION'):
        config['compression'] = os.getenv('COMPRESSION')
    
    return config

def validate_configuration(config: Dict[str, Any]) -> None:
    """Validate configuration parameters.
    
    :param config: Configuration dictionary
    :type config: Dict[str, Any]
    :raises ValueError: If configuration is invalid
    """
    # Required parameters
    if not config.get('source_host'):
        raise ValueError("source_host is required")
    
    # Validate numeric parameters
    if 'source_port' in config and not (1 <= config['source_port'] <= 65535):
        raise ValueError("source_port must be between 1 and 65535")
    
    if 'chunk_size' in config and config['chunk_size'] <= 0:
        raise ValueError("chunk_size must be positive")
    
    if 'max_workers' in config and config['max_workers'] <= 0:
        raise ValueError("max_workers must be positive")
    
    # Validate query parameters
    if not any([config.get('table'), config.get('query'), config.get('query_file')]):
        raise ValueError("Either --table, --query, or --query-file must be specified")
    
    # Validate connection type specific parameters
    if config.get('connection_type') == 'pyodbc' and not config.get('odbc_driver'):
        raise ValueError("odbc_driver is required for pyodbc connection type")
    
    if config.get('connection_type') == 'sqlalchemy' and not config.get('sqlalchemy_url'):
        raise ValueError("sqlalchemy_url is required for sqlalchemy connection type")
```

### 3. **Scheduler-Friendly Features**

#### Exit Codes
```python
# Exit code constants
EXIT_SUCCESS = 0
EXIT_GENERAL_ERROR = 1
EXIT_CONFIG_ERROR = 2
EXIT_CONNECTION_ERROR = 3
EXIT_VALIDATION_ERROR = 4
EXIT_PERMISSION_ERROR = 5

def main() -> int:
    """Main CLI function with proper exit codes.
    
    :return: Exit code
    :rtype: int
    """
    parser = create_parser()
    args = parser.parse_args()
    
    try:
        # Setup logging
        setup_logging(args)
        
        # Load and validate configuration
        config = load_configuration(args.config)
        merge_args_with_config(args, config)
        validate_configuration(config)
        
        # Handle utility commands
        if args.test_connection:
            return handle_test_connection(config)
        elif args.validate_config:
            return handle_validate_config(config)
        elif args.show_config:
            return handle_show_config(config)
        elif args.dry_run:
            return handle_dry_run(config)
        
        # Execute main operation
        return execute_transfer(config)
        
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return EXIT_CONFIG_ERROR
    except ConnectionError as e:
        print(f"Connection error: {e}", file=sys.stderr)
        return EXIT_CONNECTION_ERROR
    except PermissionError as e:
        print(f"Permission error: {e}", file=sys.stderr)
        return EXIT_PERMISSION_ERROR
    except KeyboardInterrupt:
        print("\nOperation interrupted by user", file=sys.stderr)
        return EXIT_GENERAL_ERROR
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return EXIT_GENERAL_ERROR
```

#### Non-Interactive Operation
```python
def setup_logging(args) -> None:
    """Setup logging for CLI operation.
    
    :param args: Parsed command line arguments
    :type args: argparse.Namespace
    """
    log_level = getattr(logging, args.log_level.upper())
    
    # Configure logging format
    if args.verbose:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    else:
        format_string = '%(levelname)s: %(message)s'
    
    # Setup logging
    if args.log_file:
        logging.basicConfig(
            level=log_level,
            format=format_string,
            handlers=[
                logging.FileHandler(args.log_file),
                logging.StreamHandler(sys.stderr)
            ]
        )
    else:
        logging.basicConfig(
            level=log_level,
            format=format_string
        )
    
    # Suppress non-error output if quiet mode
    if args.quiet:
        logging.getLogger().setLevel(logging.ERROR)

def execute_transfer(config: Dict[str, Any]) -> int:
    """Execute the main transfer operation.
    
    :param config: Configuration dictionary
    :type config: Dict[str, Any]
    :return: Exit code
    :rtype: int
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Create transfer tool
        tool = ImpalaTransferTool(**config)
        
        # Get query to execute
        query = get_query_from_config(config)
        
        # Execute transfer
        logger.info("Starting data transfer")
        success = tool.transfer_query(
            query=query,
            target_table=config.get('target_table'),
            output_format=config.get('output_format', 'parquet')
        )
        
        if success:
            logger.info("Data transfer completed successfully")
            return EXIT_SUCCESS
        else:
            logger.error("Data transfer failed")
            return EXIT_GENERAL_ERROR
            
    except Exception as e:
        logger.error(f"Transfer execution failed: {e}")
        raise
```

### 4. **Utility Commands**

#### Test Connection
```python
def handle_test_connection(config: Dict[str, Any]) -> int:
    """Handle test connection command.
    
    :param config: Configuration dictionary
    :type config: Dict[str, Any]
    :return: Exit code
    :rtype: int
    """
    logger = logging.getLogger(__name__)
    
    try:
        tool = ImpalaTransferTool(**config)
        
        if tool.test_connection():
            logger.info("✓ Database connection test successful")
            return EXIT_SUCCESS
        else:
            logger.error("✗ Database connection test failed")
            return EXIT_CONNECTION_ERROR
            
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return EXIT_CONNECTION_ERROR

def handle_validate_config(config: Dict[str, Any]) -> int:
    """Handle validate configuration command.
    
    :param config: Configuration dictionary
    :type config: Dict[str, Any]
    :return: Exit code
    :rtype: int
    """
    logger = logging.getLogger(__name__)
    
    try:
        tool = ImpalaTransferTool(**config)
        
        if tool.validate_configuration():
            logger.info("✓ Configuration validation successful")
            return EXIT_SUCCESS
        else:
            logger.error("✗ Configuration validation failed")
            return EXIT_CONFIG_ERROR
            
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        return EXIT_CONFIG_ERROR

def handle_show_config(config: Dict[str, Any]) -> int:
    """Handle show configuration command.
    
    :param config: Configuration dictionary
    :type config: Dict[str, Any]
    :return: Exit code
    :rtype: int
    """
    # Mask sensitive information
    safe_config = mask_sensitive_config(config)
    
    print("Current configuration:")
    print(json.dumps(safe_config, indent=2, default=str))
    return EXIT_SUCCESS

def handle_dry_run(config: Dict[str, Any]) -> int:
    """Handle dry run command.
    
    :param config: Configuration dictionary
    :type config: Dict[str, Any]
    :return: Exit code
    :rtype: int
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=== DRY RUN MODE ===")
    logger.info(f"Source host: {config.get('source_host')}")
    logger.info(f"Connection type: {config.get('connection_type')}")
    logger.info(f"Chunk size: {config.get('chunk_size')}")
    logger.info(f"Max workers: {config.get('max_workers')}")
    logger.info(f"Output format: {config.get('output_format')}")
    
    query = get_query_from_config(config)
    logger.info(f"Query: {query}")
    
    logger.info("=== DRY RUN COMPLETE ===")
    return EXIT_SUCCESS
```

### 5. **Scheduler Integration**

#### Cron Job Example
```bash
# Example cron job for daily data transfer
# Run at 2 AM daily
0 2 * * * /usr/bin/impala-transfer \
    --config /etc/impala-transfer/daily_config.json \
    --log-file /var/log/impala-transfer/daily_transfer.log \
    --log-level INFO

# Example cron job for hourly data transfer
# Run every hour
0 * * * * /usr/bin/impala-transfer \
    --source-host impala-cluster.example.com \
    --query "SELECT * FROM hourly_data WHERE hour = CURRENT_HOUR()" \
    --target-table hourly_data_$(date +\%Y\%m\%d\%H) \
    --log-file /var/log/impala-transfer/hourly_transfer.log
```

#### Systemd Service Example
```ini
# /etc/systemd/system/impala-transfer.service
[Unit]
Description=Impala Transfer Tool
After=network.target

[Service]
Type=oneshot
User=impala-transfer
Group=impala-transfer
Environment=IMPALA_HOST=impala-cluster.example.com
Environment=IMPALA_PORT=21050
Environment=IMPALA_DATABASE=default
Environment=CHUNK_SIZE=1000000
Environment=MAX_WORKERS=4
ExecStart=/usr/bin/impala-transfer --config /etc/impala-transfer/config.json
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

#### Docker Example
```dockerfile
# Dockerfile for Impala Transfer Tool
FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application
COPY impala_transfer/ ./impala_transfer/
COPY setup.py .

# Install application
RUN pip install -e .

# Create non-root user
RUN useradd -r -s /bin/false impala-transfer
USER impala-transfer

# Set entrypoint
ENTRYPOINT ["impala-transfer"]
```

### 6. **CLI Testing**

#### Unit Tests for CLI
```python
import pytest
from unittest.mock import patch, MagicMock
from impala_transfer.cli import main, create_parser

class TestCLI:
    """Test cases for CLI functionality."""
    
    def test_help_output(self):
        """Test that help output is comprehensive."""
        parser = create_parser()
        help_text = parser.format_help()
        
        # Check for required sections
        assert "Database Connection" in help_text
        assert "Query Configuration" in help_text
        assert "Processing Options" in help_text
        assert "Utility Commands" in help_text
        assert "Examples:" in help_text
        assert "Exit Codes:" in help_text
    
    def test_required_arguments(self):
        """Test that required arguments are enforced."""
        parser = create_parser()
        
        # Should fail without required arguments
        with pytest.raises(SystemExit):
            parser.parse_args([])
        
        # Should succeed with required arguments
        args = parser.parse_args(['--source-host', 'localhost', '--table', 'test'])
        assert args.source_host == 'localhost'
        assert args.table == 'test'
    
    def test_exit_codes(self):
        """Test that proper exit codes are returned."""
        with patch('sys.argv', ['impala-transfer', '--source-host', 'localhost', '--table', 'test']):
            with patch('impala_transfer.cli.ImpalaTransferTool') as mock_tool:
                mock_tool.return_value.transfer_query.return_value = True
                exit_code = main()
                assert exit_code == 0  # EXIT_SUCCESS
```

This comprehensive CLI standards ensure that the Impala Transfer Tool has a professional, self-documenting, and scheduler-friendly command-line interface. 