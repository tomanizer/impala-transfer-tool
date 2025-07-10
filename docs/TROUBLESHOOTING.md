# Troubleshooting Guide

This guide helps you diagnose and resolve common issues with the Impala Transfer Tool.

## Table of Contents

- [Connection Issues](#connection-issues)
- [Query Execution Problems](#query-execution-problems)
- [File Transfer Failures](#file-transfer-failures)
- [Performance Issues](#performance-issues)
- [Memory Problems](#memory-problems)
- [Configuration Issues](#configuration-issues)
- [Error Messages](#error-messages)
- [Debugging Techniques](#debugging-techniques)

## Connection Issues

### Impyla Connection Failures

**Symptoms:**
- `ConnectionError: Failed to connect to Impala`
- `AuthenticationError: Invalid credentials`
- `TimeoutError: Connection timed out`

**Diagnosis:**
```bash
# Test basic connectivity
telnet your-impala-host 21050

# Test with impyla directly
python -c "
from impala.dbapi import connect
conn = connect(host='your-host', port=21050)
cursor = conn.cursor()
cursor.execute('SELECT 1')
print('Connection successful')
"
```

**Solutions:**
1. **Network Issues**
   - Verify network connectivity
   - Check firewall settings
   - Ensure port 21050 is open

2. **Authentication Issues**
   - Verify username/password
   - Check Kerberos configuration
   - Ensure proper authentication mechanism

3. **Host Resolution**
   - Verify hostname resolution
   - Try using IP address instead of hostname
   - Check DNS configuration

### ODBC Connection Failures

**Symptoms:**
- `pyodbc.Error: ('01000', "[unixODBC][Driver Manager]Can't open lib 'driver' : file not found")`
- `pyodbc.Error: ('08001', '[unixODBC][Driver Manager]Unable to connect to data source')`

**Diagnosis:**
```bash
# Check available ODBC drivers
odbcinst -q -d

# Test ODBC connection
python -c "
import pyodbc
print('Available drivers:', pyodbc.drivers())
"
```

**Solutions:**
1. **Driver Installation**
   ```bash
   # Install ODBC driver (example for SQL Server)
   brew install microsoft/mssql-release/mssql-tools
   ```

2. **Driver Configuration**
   - Verify driver path in odbcinst.ini
   - Check driver permissions
   - Ensure driver is compatible with your system

3. **Connection String Issues**
   - Verify connection string format
   - Check for special characters in credentials
   - Test connection string with other tools

### SQLAlchemy Connection Failures

**Symptoms:**
- `sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) connection to server at "host" failed`
- `sqlalchemy.exc.ArgumentError: Could not parse rfc1738 URL from string`

**Diagnosis:**
```python
# Test SQLAlchemy connection
from sqlalchemy import create_engine
engine = create_engine('your-connection-url')
with engine.connect() as conn:
    result = conn.execute('SELECT 1')
    print('Connection successful')
```

**Solutions:**
1. **URL Format Issues**
   - Verify connection URL format
   - Check for special characters in credentials
   - Ensure proper URL encoding

2. **Database Driver Issues**
   - Install required database driver (psycopg2, pymysql, etc.)
   - Verify driver compatibility
   - Check driver version

## Query Execution Problems

### Query Timeout Issues

**Symptoms:**
- `TimeoutError: Query execution timed out`
- Long-running queries that never complete

**Solutions:**
1. **Optimize Query**
   ```sql
   -- Add LIMIT for testing
   SELECT * FROM large_table LIMIT 1000
   
   -- Use more specific WHERE clauses
   SELECT * FROM large_table WHERE date = '2024-01-01'
   ```

2. **Adjust Timeout Settings**
   ```python
   # Increase timeout for long-running queries
   tool = ImpalaTransferTool(
       source_host='your-host',
       query_timeout=3600  # 1 hour timeout
   )
   ```

3. **Use Chunking**
   - Break large queries into smaller chunks
   - Use date ranges or ID ranges
   - Process data incrementally

### Memory Issues During Query Execution

**Symptoms:**
- `MemoryError: Unable to allocate memory`
- High memory usage during query execution

**Solutions:**
1. **Reduce Chunk Size**
   ```python
   tool = ImpalaTransferTool(
       source_host='your-host',
       chunk_size=100000  # Reduce from default 1M
   )
   ```

2. **Use Streaming Queries**
   - Process results in batches
   - Avoid loading entire result set into memory
   - Use generators for large datasets

3. **Optimize Query**
   - Select only required columns
   - Use WHERE clauses to filter data
   - Consider using views or materialized tables

### Query Syntax Errors

**Symptoms:**
- `sqlalchemy.exc.ProgrammingError: syntax error`
- `pyodbc.Error: ('42000', '[Microsoft][ODBC Driver]Incorrect syntax')`

**Solutions:**
1. **Test Query Separately**
   ```bash
   # Test query in database client
   impala-shell -i your-host -q "YOUR_QUERY_HERE"
   ```

2. **Check SQL Dialect**
   - Verify query syntax for your database
   - Check for database-specific functions
   - Ensure proper quoting and escaping

3. **Use Query Validation**
   ```bash
   # Validate query before execution
   impala-transfer --dry-run --query "YOUR_QUERY_HERE"
   ```

## File Transfer Failures

### HDFS Transfer Issues

**Symptoms:**
- `Permission denied` errors
- `No such file or directory` errors
- Transfer failures with exit code 1

**Diagnosis:**
```bash
# Check HDFS permissions
hdfs dfs -ls /target/path

# Test HDFS connectivity
hdfs dfs -test -d /target/path

# Check HDFS space
hdfs dfs -df /target/path
```

**Solutions:**
1. **Permission Issues**
   ```bash
   # Set proper permissions
   hdfs dfs -chmod 755 /target/path
   hdfs dfs -chown your-user:your-group /target/path
   ```

2. **Path Issues**
   - Verify target path exists
   - Check for typos in path
   - Ensure path is accessible

3. **Space Issues**
   - Check available HDFS space
   - Clean up old files
   - Use different target location

### SCP Transfer Issues

**Symptoms:**
- `ssh: connect to host failed`
- `Permission denied (publickey,password)`
- `No such file or directory`

**Diagnosis:**
```bash
# Test SSH connectivity
ssh your-target-host "echo 'SSH connection successful'"

# Test SCP manually
scp test.txt your-target-host:/target/path/
```

**Solutions:**
1. **SSH Configuration**
   - Set up SSH key authentication
   - Configure SSH config file
   - Test SSH connection manually

2. **Target Directory**
   - Ensure target directory exists
   - Check directory permissions
   - Verify user has write access

3. **Network Issues**
   - Check network connectivity
   - Verify firewall settings
   - Test with different target host

## Performance Issues

### Slow Transfer Speeds

**Symptoms:**
- Transfer taking much longer than expected
- Low throughput during file transfer

**Diagnosis:**
```bash
# Check network speed
iperf3 -c your-target-host

# Monitor system resources
htop
iostat -x 1
```

**Solutions:**
1. **Optimize Chunk Size**
   ```python
   # Increase chunk size for better performance
   tool = ImpalaTransferTool(
       source_host='your-host',
       chunk_size=5000000  # 5M rows per chunk
   )
   ```

2. **Increase Parallelism**
   ```python
   # Use more parallel workers
   tool = ImpalaTransferTool(
       source_host='your-host',
       max_workers=8  # Increase from default 4
   )
   ```

3. **Network Optimization**
   - Use faster network connection
   - Optimize network settings
   - Consider using compression

### High CPU Usage

**Symptoms:**
- System becomes unresponsive
- High CPU usage during transfer

**Solutions:**
1. **Reduce Parallelism**
   ```python
   # Reduce number of workers
   tool = ImpalaTransferTool(
       source_host='your-host',
       max_workers=2  # Reduce from default 4
   )
   ```

2. **Optimize Processing**
   - Use more efficient data formats
   - Reduce data transformation overhead
   - Use streaming processing

3. **System Resources**
   - Monitor system resources
   - Close unnecessary applications
   - Consider using dedicated resources

## Memory Problems

### Out of Memory Errors

**Symptoms:**
- `MemoryError: Unable to allocate memory`
- System becomes unresponsive
- High memory usage

**Solutions:**
1. **Reduce Memory Usage**
   ```python
   # Use smaller chunk size
   tool = ImpalaTransferTool(
       source_host='your-host',
       chunk_size=100000  # Reduce chunk size
   )
   ```

2. **Optimize Data Processing**
   - Use streaming instead of loading all data
   - Process data in smaller batches
   - Use memory-efficient data structures

3. **System Configuration**
   - Increase system memory
   - Adjust swap space
   - Use 64-bit Python

### Memory Leaks

**Symptoms:**
- Memory usage increasing over time
- System performance degradation

**Solutions:**
1. **Proper Resource Management**
   ```python
   # Ensure proper cleanup
   try:
       tool = ImpalaTransferTool(...)
       result = tool.transfer_table('my_table')
   finally:
       # Clean up resources
       tool.connection_manager.close()
   ```

2. **Monitor Memory Usage**
   ```python
   import psutil
   import gc
   
   # Monitor memory usage
   process = psutil.Process()
   print(f"Memory usage: {process.memory_info().rss / 1024 / 1024} MB")
   
   # Force garbage collection
   gc.collect()
   ```

## Configuration Issues

### Environment Variable Problems

**Symptoms:**
- Configuration not being read
- Wrong values being used

**Diagnosis:**
```bash
# Check environment variables
env | grep IMPALA

# Test configuration loading
python -c "
from impala_transfer.cli import get_environment_config
config = get_environment_config()
print('Config:', config)
"
```

**Solutions:**
1. **Variable Names**
   - Use correct environment variable names
   - Check for typos
   - Ensure proper case sensitivity

2. **Variable Scope**
   - Export variables in current shell
   - Use .bashrc or .profile for persistence
   - Check shell environment

### Configuration File Issues

**Symptoms:**
- Configuration file not found
- Invalid JSON format
- Configuration not being applied

**Diagnosis:**
```bash
# Validate JSON format
python -m json.tool config.json

# Check file permissions
ls -la config.json
```

**Solutions:**
1. **File Format**
   - Ensure valid JSON format
   - Check for syntax errors
   - Use proper quoting

2. **File Path**
   - Use absolute path
   - Check file permissions
   - Verify file exists

## Error Messages

### Common Error Messages and Solutions

| Error Message | Cause | Solution |
|---------------|-------|----------|
| `ConnectionError: Failed to connect` | Network/authentication issue | Check connectivity and credentials |
| `TimeoutError: Query execution timed out` | Long-running query | Optimize query or increase timeout |
| `MemoryError: Unable to allocate memory` | Insufficient memory | Reduce chunk size or increase memory |
| `Permission denied` | File system permissions | Check and fix permissions |
| `No such file or directory` | Missing file or path | Verify file/path exists |
| `Invalid JSON` | Configuration file format | Fix JSON syntax |
| `Driver not found` | Missing database driver | Install required driver |

## Debugging Techniques

> **Log files**: All logs are written to the temp directory specified by `temp_dir` (default: `/tmp/impala_transfer`).

### Enable Debug Logging

```python
import logging

# Enable debug logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### Use Verbose Mode

```bash
# Enable verbose output
impala-transfer --verbose --source-host your-host --table my_table
```

### Test Individual Components

```python
# Test connection
from impala_transfer.connection import ConnectionManager
manager = ConnectionManager('impyla', source_host='your-host')
print(f"Connection successful: {manager.connect()}")

# Test query
from impala_transfer.query import QueryExecutor
executor = QueryExecutor(manager)
info = executor.get_query_info("SELECT COUNT(*) FROM my_table")
print(f"Query info: {info}")
```

### Monitor System Resources

```bash
# Monitor CPU and memory
htop

# Monitor disk I/O
iostat -x 1

# Monitor network
iftop

# Monitor HDFS
hdfs dfsadmin -report
```

### Use Dry Run Mode

```bash
# Test without actual execution
impala-transfer --dry-run --source-host your-host --table my_table
```

### Check Logs

```bash
# Check application logs
tail -f /var/log/impala-transfer.log

# Check system logs
journalctl -f

# Check HDFS logs
tail -f /var/log/hadoop/hdfs/hadoop-hdfs-namenode.log
```

## Getting Help

If you're still experiencing issues:

1. **Check Documentation**
   - Review this troubleshooting guide
   - Check the API reference
   - Look at example configurations

2. **Search Issues**
   - Check existing GitHub issues
   - Search for similar problems
   - Look for workarounds

3. **Create Issue Report**
   - Include error messages and logs
   - Provide environment details
   - Include minimal reproduction case
   - Describe expected vs actual behavior

4. **Community Support**
   - Ask in GitHub discussions
   - Check community forums
   - Contact maintainers

## Prevention

### Best Practices

1. **Test in Development**
   - Test configurations before production
   - Use small datasets for testing
   - Validate queries separately

2. **Monitor Resources**
   - Monitor system resources during transfers
   - Set up alerts for resource usage
   - Plan for peak usage

3. **Backup and Recovery**
   - Keep backups of configurations
   - Test recovery procedures
   - Document successful configurations

4. **Regular Maintenance**
   - Update dependencies regularly
   - Clean up temporary files
   - Monitor log files 