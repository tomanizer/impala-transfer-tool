# Environment Variables

The Impala Transfer Tool supports configuration via environment variables. This provides a secure way to configure the tool without hardcoding sensitive information in configuration files.

## Database Connection Variables

### General Connection
- `IMPALA_HOST` - Database host for source cluster
- `IMPALA_PORT` - Database port (default: 21050 for Impala)
- `IMPALA_DATABASE` - Source database name (default: "default")
- `CONNECTION_TYPE` - Database connection type ("auto", "impyla", "pyodbc", "sqlalchemy")

### ODBC Connection
- `ODBC_DRIVER` - ODBC driver name (required for pyodbc connection type)
- `ODBC_CONNECTION_STRING` - Full ODBC connection string (alternative to individual parameters)

### SQLAlchemy Connection
- `SQLALCHEMY_URL` - SQLAlchemy connection URL (e.g., "postgresql://user:pass@host:port/db")

## Processing Configuration

- `CHUNK_SIZE` - Number of rows per chunk for parallel processing (default: 1000000)
- `MAX_WORKERS` - Number of parallel workers (default: 4)
- `TEMP_DIR` - Temporary directory for intermediate files (default: "/tmp/impala_transfer")

## Output Configuration

- `TARGET_HDFS_PATH` - HDFS path on target cluster for data landing
- `OUTPUT_FORMAT` - Output format ("parquet" or "csv", default: "parquet")

## Distcp Configuration

- `USE_DISTCP` - Whether to use distcp for cross-cluster transfers (default: "true")
- `SOURCE_HDFS_PATH` - Source HDFS path (required for distcp transfers)
- `TARGET_CLUSTER` - Target cluster name/address (required for distcp transfers)

## Usage Examples

### Basic Impala Connection
```bash
export IMPALA_HOST=impala-cluster.example.com
export IMPALA_PORT=21050
export IMPALA_DATABASE=default
export CONNECTION_TYPE=impyla
export TARGET_HDFS_PATH=/user/data/landing
export CHUNK_SIZE=500000
export MAX_WORKERS=8

impala-transfer --table users
```

### SQL Server with ODBC
```bash
export CONNECTION_TYPE=pyodbc
export ODBC_DRIVER="ODBC Driver 17 for SQL Server"
export IMPALA_HOST=sql-server.example.com
export IMPALA_PORT=1433
export IMPALA_DATABASE=mydatabase
export TARGET_HDFS_PATH=/user/data/landing

impala-transfer --query "SELECT * FROM users WHERE date = '2024-01-01'"
```

### PostgreSQL with SQLAlchemy
```bash
export CONNECTION_TYPE=sqlalchemy
export SQLALCHEMY_URL="postgresql://username:password@postgres-server:5432/mydatabase"
export TARGET_HDFS_PATH=/user/data/landing
export OUTPUT_FORMAT=csv

impala-transfer --query "SELECT * FROM users WHERE created_at >= '2024-01-01'"
```

### Cross-Cluster Transfer with Distcp
```bash
export USE_DISTCP=true
export SOURCE_HDFS_PATH=/data/source
export TARGET_CLUSTER=cluster2.example.com
export TARGET_HDFS_PATH=/data/target
export CHUNK_SIZE=2000000
export MAX_WORKERS=8

impala-transfer \
    --source-host cluster1.example.com \
    --query "SELECT * FROM large_table WHERE date = '2024-01-01'"
```

### Single-Cluster Transfer (HDFS Put)
```bash
export USE_DISTCP=false
export TARGET_HDFS_PATH=/tmp/test_data
export CHUNK_SIZE=100000
export MAX_WORKERS=2

impala-transfer \
    --source-host localhost \
    --query "SELECT * FROM test_table LIMIT 1000"
```

### Using ODBC Connection String
```bash
export CONNECTION_TYPE=pyodbc
export ODBC_CONNECTION_STRING="DRIVER={ODBC Driver 17 for SQL Server};SERVER=sql-server.example.com;DATABASE=mydatabase;UID=username;PWD=password"
export TARGET_HDFS_PATH=/user/data/landing

impala-transfer --table users
```

## Security Best Practices

1. **Never hardcode passwords** in configuration files
2. **Use environment variables** for all sensitive information
3. **Set appropriate file permissions** for environment files
4. **Use secret management services** in production environments
5. **Rotate credentials regularly**

### Example .env file (not committed to git)
```bash
# .env
IMPALA_HOST=impala-cluster.example.com
IMPALA_PORT=21050
IMPALA_DATABASE=default
CONNECTION_TYPE=impyla
TARGET_HDFS_PATH=/user/data/landing
CHUNK_SIZE=1000000
MAX_WORKERS=4
OUTPUT_FORMAT=parquet
```

### Loading environment variables
```bash
# Load from .env file
source .env

# Or set individually
export IMPALA_HOST=impala-cluster.example.com
export IMPALA_PORT=21050

# Run the tool
impala-transfer --table users
```

## Configuration Priority

The tool uses the following priority order for configuration:

1. **Command line arguments** (highest priority)
2. **Environment variables** (medium priority)
3. **Configuration file** (low priority)
4. **Default values** (lowest priority)

This means that command line arguments will override environment variables, which will override configuration file values, which will override defaults. 

| Variable            | Description                                 | Example Value                  |
|---------------------|---------------------------------------------|-------------------------------|
| IMPALA_HOST         | Source database host                        | impala-cluster.example.com    |
| IMPALA_PORT         | Source database port                        | 21050                        |
| IMPALA_DATABASE     | Source database name                        | default                       |
| CONNECTION_TYPE     | Connection type (impyla, pyodbc, sqlalchemy)| impyla                        |
| TARGET_HDFS_PATH    | HDFS path for data landing                  | /user/data/landing            |
| CHUNK_SIZE          | Number of rows per chunk                    | 1000000                       |
| MAX_WORKERS         | Number of parallel workers                  | 4                             |
| OUTPUT_FORMAT       | Output format (parquet, csv)                | parquet                       |
| USE_DISTCP          | Use distcp for cross-cluster transfers      | true                          |
| SOURCE_HDFS_PATH    | Source HDFS path (for distcp)               | /data/source                  |
| TARGET_CLUSTER      | Target cluster name/address (for distcp)    | cluster2.example.com          |
| ODBC_DRIVER         | ODBC driver name                            | ODBC Driver 17 for SQL Server |
| ODBC_CONNECTION_STRING | Full ODBC connection string               | ...                           |
| SQLALCHEMY_URL      | SQLAlchemy connection URL                   | postgresql://...              |
| TEMP_DIR            | Directory for temp files and log files      | /tmp/impala_transfer          | 