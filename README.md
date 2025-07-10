# Impala Transfer Tool

A modular, extensible tool for transferring large datasets from database clusters to HDFS/Hive with support for multiple database connection types.

## 🚀 Features

- **Multiple Database Support**: Impyla, pyodbc, and SQLAlchemy connections
- **Parallel Processing**: Chunked data processing with configurable parallelism
- **Flexible Output**: Parquet and CSV output formats with compression
- **Progress Tracking**: Real-time progress reporting
- **Error Handling**: Robust error handling and recovery
- **Modular Architecture**: Clean separation of concerns for easy testing and extension
- **Comprehensive Testing**: Well-structured test suite with individual module tests

## 📦 Package Structure

```
impala_transfer/
├── __init__.py          # Package initialization and exports
├── core.py              # Main ImpalaTransferTool class
├── connection.py        # Database connection management
├── query.py             # Query execution and result processing
├── chunking.py          # Data chunking and file processing
├── transfer.py          # File transfer operations
├── utils.py             # Utility functions and file operations
├── orchestrator.py      # Transfer orchestration
└── cli.py               # Command-line interface

tests/
├── test_connection.py   # Connection manager tests
├── test_query.py        # Query executor tests
├── test_chunking.py     # Chunk processor tests
├── test_transfer.py     # File transfer tests
├── test_utils.py        # Utility function tests
├── test_orchestrator.py # Transfer orchestrator tests
├── test_core.py         # Main tool tests
├── test_cli.py          # CLI tests
├── test_init.py         # Package initialization tests
└── test_integration.py  # Integration tests
```

## 🛠 Installation

### Basic Installation

```bash
pip install impala-transfer
```

### With Specific Database Support

```bash
# For Impyla connections
pip install impala-transfer[impyla]

# For ODBC connections
pip install impala-transfer[pyodbc]

# For SQLAlchemy connections
pip install impala-transfer[sqlalchemy]

# For all database types
pip install impala-transfer[all]

# For development
pip install impala-transfer[dev]
```

### From Source

```bash
git clone https://github.com/tomanizer/impala-transfer-tool.git
cd impala-transfer-tool
pip install -e .
```

## 🔧 Usage

### Command Line Interface

#### Basic Usage

```bash
# Transfer entire table using Impyla
impala-transfer --source-host impala-cluster.example.com --table my_table

# Transfer query results using pyodbc
impala-transfer --source-host sql-server.example.com \
    --connection-type pyodbc \
    --odbc-driver "ODBC Driver 17 for SQL Server" \
    --query "SELECT * FROM my_table WHERE date = '2024-01-01'"

# Transfer using SQLAlchemy
impala-transfer --connection-type sqlalchemy \
    --sqlalchemy-url "postgresql://user:pass@host:5432/db" \
    --query "SELECT * FROM my_table"
```

#### Advanced Options

```bash
# Custom chunking, parallelism, and temp directory
impala-transfer --source-host impala-cluster.example.com \
    --query "SELECT * FROM large_table" \
    --chunk-size 500000 \
    --max-workers 8 \
    --output-format csv \
    --temp-dir /tmp/impala_transfer

# Transfer to specific HDFS path
impala-transfer --source-host impala-cluster.example.com \
    --table my_table \
    --target-hdfs-path "/data/landing/my_table"

# Use query from file
impala-transfer --source-host impala-cluster.example.com \
    --query-file example_queries/date_filter_query.sql \
    --target-table custom_table_name
```

#### Utility Commands

```bash
# Test database connection
impala-transfer --source-host impala-cluster.example.com --test-connection

# Validate configuration
impala-transfer --source-host impala-cluster.example.com --validate-config

# Show current configuration
impala-transfer --source-host impala-cluster.example.com --show-config

# Dry run (show what would be done without executing)
impala-transfer --source-host impala-cluster.example.com --table users --dry-run

# Use configuration file
impala-transfer --config config.json --table users
```

### Python API

#### Basic Usage

```python
from impala_transfer import ImpalaTransferTool

# Create transfer tool
tool = ImpalaTransferTool(
    source_host="impala-cluster.example.com",
    target_hdfs_path="/data/landing"
)

# Transfer entire table
success = tool.transfer_table("my_table")

# Transfer custom query
success = tool.transfer_query("SELECT * FROM my_table WHERE date = '2024-01-01'")
```

#### Advanced Configuration

```python
# With custom settings
tool = ImpalaTransferTool(
    source_host="impala-cluster.example.com",
    target_hdfs_path="/data/landing",
    chunk_size=500000,
    max_workers=8,
    output_format="csv"
)

# With progress callback
def progress_callback(message, percentage):
    print(f"{percentage}%: {message}")

success = tool.transfer_query_with_progress(
    "SELECT * FROM large_table",
    progress_callback=progress_callback
)
```

#### Different Connection Types

```python
# Impyla connection
tool = ImpalaTransferTool(
    source_host="impala-cluster.example.com",
    connection_type="impyla"
)

# ODBC connection
tool = ImpalaTransferTool(
    source_host="sql-server.example.com",
    connection_type="pyodbc",
    odbc_driver="ODBC Driver 17 for SQL Server"
)

# SQLAlchemy connection
tool = ImpalaTransferTool(
    connection_type="sqlalchemy",
    sqlalchemy_url="postgresql://user:pass@host:5432/db"
)
```

## 📋 Example Queries

The `example_queries/` directory contains sample SQL queries:

- `date_filter_query.sql`: Simple date-based filtering
- `date_range_query.sql`: Date range queries
- `aggregated_query.sql`: Aggregation examples

## ⚙️ Configuration

### Environment Variables

The tool supports configuration via environment variables for secure credential management:

```bash
# Set environment variables
export IMPALA_HOST=impala-cluster.example.com
export IMPALA_PORT=21050
export IMPALA_DATABASE=default
export CONNECTION_TYPE=impyla
export TARGET_HDFS_PATH=/user/data/landing
export CHUNK_SIZE=1000000
export MAX_WORKERS=4
export OUTPUT_FORMAT=parquet

# Run the tool
impala-transfer --table users
```

See [docs/ENVIRONMENT_VARIABLES.md](docs/ENVIRONMENT_VARIABLES.md) for complete documentation.

### Configuration File

Create a `config.json` file based on `config_example.json`:

```json
{
  "source_host": "impala-cluster.example.com",
  "source_port": 21050,
  "source_database": "default",
  "target_hdfs_path": "/data/landing",
  "chunk_size": 1000000,
  "max_workers": 4,
  "connection_type": "auto",
  "output_format": "parquet",
  "temp_dir": "/tmp/impala_transfer"
}
```

> **Note:** All temporary files and log files are written to the directory specified by `temp_dir` (default: `/tmp/impala_transfer`).

## 🧪 Testing

### Running Tests

```bash
# Install development dependencies
pip install -e .[dev]

# Run all tests
pytest tests/

# Run with coverage
pytest --cov=impala_transfer tests/

# Run specific module tests
pytest tests/test_connection.py -v
pytest tests/test_cli.py -v

# Run tests for a specific module
pytest tests/test_core.py -v
```

### Test Structure

The test suite is organized into individual test files for each module:

- `test_connection.py` - Tests for database connection management
- `test_query.py` - Tests for query execution and processing
- `test_chunking.py` - Tests for data chunking functionality
- `test_transfer.py` - Tests for file transfer operations
- `test_utils.py` - Tests for utility functions
- `test_orchestrator.py` - Tests for transfer orchestration
- `test_core.py` - Tests for the main ImpalaTransferTool class
- `test_cli.py` - Tests for command-line interface
- `test_init.py` - Tests for package initialization
- `test_integration.py` - Integration tests for complete workflows

### Test Coverage

The test suite provides comprehensive coverage with:
- **38 passing tests**
- **1 skipped test** (due to missing pyodbc dependency)
- **Modular test structure** for easy maintenance
- **Mock-based testing** for isolated unit tests
- **Integration tests** for end-to-end workflows

## 🚀 Performance Optimization

### Chunk Size Tuning

- **Small datasets** (< 1M rows): 100,000 - 500,000 rows per chunk
- **Medium datasets** (1M - 100M rows): 500,000 - 1,000,000 rows per chunk
- **Large datasets** (> 100M rows): 1,000,000 - 5,000,000 rows per chunk

### Parallelism Tuning

- **CPU-bound**: Set `max_workers` to number of CPU cores
- **I/O-bound**: Set `max_workers` to 2-4x number of CPU cores
- **Network-bound**: Monitor network utilization and adjust accordingly

## 🛠 Troubleshooting

### Common Issues

1. **Connection Failures**
   - Verify network connectivity
   - Check firewall settings
   - Validate connection parameters

2. **Memory Issues**
   - Reduce chunk size
   - Use batching
   - Monitor system resources

3. **Performance Issues**
   - Increase parallelism
   - Optimize chunk size
   - Check network bandwidth

4. **File Transfer Failures**
   - Verify HDFS permissions
   - Check disk space
   - Validate target paths

### Debug Mode

```bash
# Enable verbose logging (log files are written to temp_dir)
impala-transfer --verbose --source-host impala-cluster.example.com --table my_table --temp-dir /tmp/impala_transfer

# Test connection with debug info
impala-transfer --verbose --source-host impala-cluster.example.com --test-connection --temp-dir /tmp/impala_transfer
```

> **Log files**: All logs are written to the temp directory specified by `--temp-dir` (default: `/tmp/impala_transfer`).

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

### Development Setup

```bash
git clone https://github.com/tomanizer/impala-transfer-tool.git
cd impala-transfer-tool
pip install -e .[dev]
```

### Adding New Tests

When adding new functionality, follow the modular test structure:

1. **Create/update the appropriate test file** in the `tests/` directory
2. **Follow the naming convention**: `test_<module_name>.py`
3. **Use descriptive test names** that explain what is being tested
4. **Include both unit tests** (with mocks) and **integration tests** where appropriate
5. **Ensure good test coverage** for new functionality

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📚 Documentation

- **[API Reference](docs/API_REFERENCE.md)** - Complete API documentation
- **[Development Guide](docs/DEVELOPMENT_GUIDE.md)** - Guide for developers
- **[Troubleshooting](docs/TROUBLESHOOTING.md)** - Common issues and solutions
- **[Contributing](docs/CONTRIBUTING.md)** - Guidelines for contributors
- **[Changelog](docs/CHANGELOG.md)** - Version history and changes

## 🆘 Support

- **Documentation**: [README.md](README.md)
- **Issues**: [GitHub Issues](https://github.com/tomanizer/impala-transfer-tool/issues)
- **Discussions**: [GitHub Discussions](https://github.com/tomanizer/impala-transfer-tool/discussions)

## 📈 Roadmap

- [ ] Support for additional database types
- [ ] Cloud storage integration (S3, Azure Blob)
- [ ] Real-time monitoring dashboard
- [ ] Incremental transfer support
- [ ] Data validation and quality checks
- [ ] Integration with workflow schedulers 