# Development Guide

This guide provides information for developers contributing to the Impala Transfer Tool project.

## Table of Contents

- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Development Environment](#development-environment)
- [Testing](#testing)
- [Code Style](#code-style)
- [Adding New Features](#adding-new-features)
- [Debugging](#debugging)
- [Performance Optimization](#performance-optimization)
- [Release Process](#release-process)

## Getting Started

### Prerequisites

- Python 3.12 or higher
- Git
- pip
- Virtual environment tool (venv, conda, etc.)

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/tomanizer/impala-transfer-tool.git
cd impala-transfer-tool

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e .[dev]

# Verify installation
python -c "import impala_transfer; print('Installation successful!')"
```

## Project Structure

```
impala-transfer-tool/
├── impala_transfer/           # Main package
│   ├── __init__.py           # Package initialization
│   ├── core.py               # Main ImpalaTransferTool class
│   ├── connection.py         # Database connection management
│   ├── query.py              # Query execution
│   ├── chunking.py           # Data chunking
│   ├── transfer.py           # File transfer operations
│   ├── utils.py              # Utility functions
│   ├── orchestrator.py       # Transfer orchestration
│   └── cli.py                # Command-line interface
├── tests/                    # Test suite
│   ├── test_connection.py    # Connection tests
│   ├── test_query.py         # Query tests
│   ├── test_chunking.py      # Chunking tests
│   ├── test_transfer.py      # Transfer tests
│   ├── test_utils.py         # Utility tests
│   ├── test_orchestrator.py  # Orchestrator tests
│   ├── test_core.py          # Core tests
│   ├── test_cli.py           # CLI tests
│   ├── test_init.py          # Init tests
│   └── test_integration.py   # Integration tests
├── example_queries/          # Example SQL queries
├── docs/                     # Documentation
├── pyproject.toml           # Project configuration
├── setup.py                 # Package setup
├── requirements.txt         # Dependencies
└── README.md               # Project README
```

## Development Environment

> **Note:** All log files are written to the temp directory specified by `temp_dir` (default: `/tmp/impala_transfer`).

### IDE Setup

**VS Code:**
- Install Python extension
- Configure Python interpreter to use the virtual environment
- Install recommended extensions for Python development

**PyCharm:**
- Open project directory
- Configure Python interpreter to use the virtual environment
- Enable auto-import and code completion

### Pre-commit Hooks

Install pre-commit hooks for code quality:

```bash
pip install pre-commit
pre-commit install
```

This will automatically run:
- Code formatting (black)
- Import sorting (isort)
- Linting (flake8)
- Type checking (mypy)

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=impala_transfer tests/

# Run specific test file
pytest tests/test_connection.py -v

# Run tests matching pattern
pytest -k "test_connect" -v

# Run tests with parallel execution
pytest -n auto tests/
```

### Test Structure

Follow the modular test structure:

1. **Unit Tests**: Test individual functions/methods with mocks
2. **Integration Tests**: Test component interactions
3. **End-to-End Tests**: Test complete workflows

### Writing Tests

#### Test File Naming
- Use `test_<module_name>.py` format
- Place in `tests/` directory

#### Test Class Naming
- Use `Test<ClassName>` format
- Inherit from `unittest.TestCase`

#### Test Method Naming
- Use `test_<description>` format
- Be descriptive about what is being tested

#### Example Test Structure

```python
#!/usr/bin/env python3
"""
Test suite for the connection module.
"""

import unittest
from unittest.mock import Mock, patch

from impala_transfer.connection import ConnectionManager


class TestConnectionManager(unittest.TestCase):
    """Test the ConnectionManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.connection_kwargs = {
            'source_host': 'test-host',
            'source_port': 21050,
            'source_database': 'test_db'
        }
    
    def test_connect_success(self):
        """Test successful connection."""
        # Test implementation
        pass
    
    def test_connect_failure(self):
        """Test connection failure."""
        # Test implementation
        pass
```

### Mocking Guidelines

1. **Mock External Dependencies**: Database connections, file system operations
2. **Use Descriptive Mock Names**: `mock_connection`, `mock_file_transfer`
3. **Set Return Values**: Configure mocks to return expected values
4. **Verify Calls**: Assert that mocks were called correctly

### Test Coverage

Maintain high test coverage:
- Aim for >90% line coverage
- Focus on critical paths and error conditions
- Test both success and failure scenarios

## Code Style

### Python Style Guide

Follow PEP 8 with these project-specific guidelines:

- **Line Length**: 88 characters (black default)
- **Docstrings**: Use reStructuredText format
- **Type Hints**: Use type hints for all function parameters and return values
- **Imports**: Group imports (standard library, third-party, local)

### Code Formatting

```bash
# Format code with black
black impala_transfer/ tests/

# Sort imports with isort
isort impala_transfer/ tests/

# Check code style with flake8
flake8 impala_transfer/ tests/
```

### Documentation

#### Docstring Format

```python
def transfer_data(source: str, target: str) -> bool:
    """Transfer data from source to target.
    
    :param source: Source location
    :type source: str
    :param target: Target location
    :type target: str
    :return: True if transfer successful
    :rtype: bool
    :raises ValueError: If source or target is invalid
    """
    pass
```

#### Module Documentation

```python
"""
Module description.

This module provides functionality for...
"""

# Imports...

# Module-level variables...

# Classes and functions...
```

## Adding New Features

### Feature Development Process

1. **Create Feature Branch**
   ```bash
   git checkout -b feature/new-feature-name
   ```

2. **Implement Feature**
   - Add code to appropriate module
   - Follow existing patterns and conventions
   - Add type hints and docstrings

3. **Add Tests**
   - Create/update test file for the module
   - Add unit tests for new functionality
   - Add integration tests if needed

4. **Update Documentation**
   - Update API reference if needed
   - Add examples to README
   - Update this guide if needed

5. **Test Thoroughly**
   ```bash
   pytest tests/ -v
   pytest --cov=impala_transfer tests/
   ```

6. **Submit Pull Request**
   - Create descriptive PR title and description
   - Reference any related issues
   - Request code review

### Adding New Connection Types

1. **Update ConnectionManager**
   - Add new connection type to `_connect_<type>()` method
   - Add validation in constructor
   - Update connection type detection

2. **Add Tests**
   - Test connection establishment
   - Test connection failure scenarios
   - Test connection type detection

3. **Update CLI**
   - Add command-line arguments if needed
   - Update help text and examples

### Adding New Output Formats

1. **Update ChunkProcessor**
   - Add format handling in `process_chunk()`
   - Add format validation

2. **Add Tests**
   - Test format processing
   - Test format validation
   - Test error handling

3. **Update Documentation**
   - Add format to examples
   - Update API reference

## Debugging

> **Log files**: All logs are written to the temp directory specified by `temp_dir` (default: `/tmp/impala_transfer`).

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Common Debugging Scenarios

#### Connection Issues
```python
# Test connection manually
from impala_transfer.connection import ConnectionManager

manager = ConnectionManager('impyla', source_host='your-host')
success = manager.connect()
print(f"Connection successful: {success}")
```

#### Query Issues
```python
# Test query execution
from impala_transfer.query import QueryExecutor

executor = QueryExecutor(connection_manager)
info = executor.get_query_info("SELECT COUNT(*) FROM my_table")
print(f"Query info: {info}")
```

#### Transfer Issues
```python
# Test file transfer
from impala_transfer.transfer import FileTransferManager

transfer_manager = FileTransferManager(target_hdfs_path='/test/path')
success = transfer_manager.transfer_files(['test.parquet'], 'test_table')
print(f"Transfer successful: {success}")
```

### Performance Profiling

```python
import cProfile
import pstats

# Profile a function
profiler = cProfile.Profile()
profiler.enable()

# Run your code
tool.transfer_table('large_table')

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(10)
```

## Performance Optimization

### Profiling Tools

- **cProfile**: Built-in Python profiler
- **line_profiler**: Line-by-line profiling
- **memory_profiler**: Memory usage profiling

### Optimization Guidelines

1. **Database Connections**
   - Use connection pooling
   - Reuse connections when possible
   - Close connections properly

2. **Memory Management**
   - Use generators for large datasets
   - Process data in chunks
   - Clean up temporary files

3. **Parallel Processing**
   - Use ThreadPoolExecutor for I/O-bound operations
   - Use ProcessPoolExecutor for CPU-bound operations
   - Monitor resource usage

4. **File Operations**
   - Use buffered I/O
   - Minimize file system calls
   - Use appropriate file formats

## Release Process

### Version Management

1. **Update Version**
   - Update version in `impala_transfer/__init__.py`
   - Update version in `pyproject.toml`

2. **Create Release Branch**
   ```bash
   git checkout -b release/v1.2.0
   ```

3. **Update Documentation**
   - Update CHANGELOG.md
   - Update version in README.md
   - Update API reference if needed

4. **Test Release**
   ```bash
   # Clean install
   pip uninstall impala-transfer -y
   pip install -e .
   
   # Run all tests
   pytest tests/ -v
   
   # Test CLI
   impala-transfer --help
   ```

5. **Create Release**
   - Merge to main branch
   - Create GitHub release
   - Tag release with version

### Distribution

```bash
# Build distribution
python -m build

# Upload to PyPI (if applicable)
twine upload dist/*
```

## Contributing Guidelines

### Pull Request Process

1. **Fork the repository**
2. **Create feature branch**
3. **Make changes**
4. **Add tests**
5. **Update documentation**
6. **Run test suite**
7. **Submit pull request**

### Code Review Checklist

- [ ] Code follows style guidelines
- [ ] Tests are included and passing
- [ ] Documentation is updated
- [ ] No breaking changes (or documented)
- [ ] Performance impact considered
- [ ] Security implications reviewed

### Issue Reporting

When reporting issues:

1. **Use descriptive title**
2. **Include environment details**
3. **Provide minimal reproduction case**
4. **Include error messages and logs**
5. **Describe expected vs actual behavior**

## Resources

- [Python Documentation](https://docs.python.org/)
- [pytest Documentation](https://docs.pytest.org/)
- [unittest Documentation](https://docs.python.org/3/library/unittest.html)
- [PEP 8 Style Guide](https://www.python.org/dev/peps/pep-0008/)
- [Type Hints Documentation](https://docs.python.org/3/library/typing.html) 