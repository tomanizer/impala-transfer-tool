# Transfer Methods

The Impala Transfer Tool supports multiple methods for transferring data between clusters. This document explains the differences between these methods and provides guidance on when to use each one.

## Overview

The tool provides two main approaches for file transfers:

1. **Traditional Methods**: Direct command-line tools (distcp, hdfs, scp)
2. **FSSpec Methods**: Filesystem abstraction using fsspec library

FSSpec provides a unified interface for various filesystem types including HDFS, S3, Google Cloud Storage, Azure Blob Storage, and more.

## Available Transfer Methods

### 1. Distcp (Recommended for Cross-Cluster Transfers)

**Command**: `hadoop distcp`

**Description**: Distributed copy tool that uses MapReduce for parallel copying across multiple nodes.

**When to Use**:
- Cross-cluster transfers (primary use case)
- Large files (>100MB)
- When you need fault tolerance and resume capability
- When you want optimal network bandwidth utilization
- Production environments with high reliability requirements

**Benefits**:
- **Parallel Processing**: Uses MapReduce for distributed copying
- **Fault Tolerance**: Built-in error recovery and resume capability
- **Bandwidth Optimization**: Efficient network utilization
- **Cross-Cluster Optimized**: Designed specifically for cluster-to-cluster transfers
- **Progress Tracking**: Built-in progress reporting
- **Update Support**: Can update existing files efficiently

**Configuration**:
```bash
# Enable distcp (default)
impala-transfer --use-distcp --source-hdfs-path /source/path --target-cluster cluster2.example.com --target-hdfs-path /target/path

# Environment variables
export USE_DISTCP=true
export SOURCE_HDFS_PATH=/source/path
export TARGET_CLUSTER=cluster2.example.com
export TARGET_HDFS_PATH=/target/path
```

**Example Command**:
```bash
hadoop distcp -update -delete -strategy dynamic /source/path/file.parquet cluster2.example.com/target/path/file.parquet
```

### 2. HDFS Put (Local to HDFS)

**Command**: `hdfs dfs -put`

**Description**: Uploads files from local filesystem to HDFS on the same cluster.

**When to Use**:
- Single-cluster transfers
- Small files (<100MB)
- When distcp is not available
- Development/testing environments
- When you need simple, direct file uploads
- Fallback when distcp fails

**Benefits**:
- **Simple**: Straightforward command syntax
- **Fast for Small Files**: Efficient for smaller datasets
- **No Additional Dependencies**: Uses standard HDFS commands
- **Immediate**: Direct file transfer without MapReduce overhead
- **Reliable Fallback**: Works when distcp is unavailable

**Configuration**:
```bash
# Disable distcp to use hdfs put
impala-transfer --no-distcp --target-hdfs-path /target/path
```

**Example Command**:
```bash
hdfs dfs -put /local/path/file.parquet /target/hdfs/path/file.parquet
```

### 3. HDFS Copy (Within HDFS)

**Command**: `hdfs dfs -cp`

**Description**: Copies files within the same HDFS cluster.

**When to Use**:
- Moving files between directories on the same cluster
- Creating backups within the same cluster
- When you need to preserve file metadata
- Quick file operations within HDFS
- Fallback when hdfs put fails

**Benefits**:
- **Fast**: No network transfer required
- **Metadata Preservation**: Maintains file attributes
- **Atomic**: Single operation for file movement
- **Efficient**: Uses HDFS internal mechanisms
- **Reliable Fallback**: Works when hdfs put fails

**Configuration**:
```bash
# Automatically used as fallback when hdfs put fails
# No additional configuration required
```

**Example Command**:
```bash
hdfs dfs -cp /source/hdfs/path/file.parquet /target/hdfs/path/file.parquet
```

### 4. SCP (Secure Copy)

**Command**: `scp`

**Description**: Secure copy protocol for transferring files between hosts.

**When to Use**:
- When HDFS is not available on target
- Cross-platform transfers
- When you need encrypted transfers
- Emergency fallback method

**Benefits**:
- **Universal**: Works on any system with SSH
- **Secure**: Encrypted transfer
- **Simple**: Standard Unix tool
- **Reliable**: Well-tested protocol

**Configuration**:
```bash
# Currently hardcoded in the tool
# Should be made configurable in future versions
```

### 5. FSSpec (Filesystem Abstraction)

**Library**: `fsspec`

**Description**: Unified filesystem interface supporting multiple storage backends.

**When to Use**:
- Cloud storage transfers (S3, GCS, Azure)
- Cross-platform filesystem operations
- When you need a unified API for different storage types
- Modern Python applications
- When traditional methods are not suitable

**Benefits**:
- **Unified Interface**: Same API for different filesystems
- **Cloud Native**: Native support for cloud storage
- **Extensible**: Easy to add new filesystem types
- **Python Native**: Pure Python implementation
- **Cross-Platform**: Works on any platform with Python
- **Direct Filesystem Objects**: Can pass pre-configured filesystem instances
- **Flexible Configuration**: Mix filesystem objects and configuration dictionaries
- **Reusable Filesystems**: Create filesystem instances once, use multiple times

**Supported Filesystems**:
- **HDFS**: Hadoop Distributed File System
- **S3**: Amazon S3 and compatible services
- **GCS**: Google Cloud Storage
- **Azure**: Azure Blob Storage
- **Local**: Local filesystem
- **Memory**: In-memory filesystem (for testing)
- **HTTP/HTTPS**: Web-based storage
- **FTP/SFTP**: File transfer protocols

**Configuration**:
```python
# HDFS configuration
hdfs_config = {
    'protocol': 'hdfs',
    'host': 'namenode.example.com',
    'port': 8020,
    'user': 'hdfs',
    'target_path': '/user/data/transfers'
}

# S3 configuration
s3_config = {
    'protocol': 's3',
    'bucket': 'my-bucket',
    'key': 'your-access-key',
    'secret': 'your-secret-key',
    'target_path': 'transfers/'
}

# GCS configuration
gcs_config = {
    'protocol': 'gcs',
    'bucket': 'my-gcs-bucket',
    'project': 'my-gcp-project',
    'token': '/path/to/service-account.json',
    'target_path': 'transfers/'
}
```

## Performance Comparison

| Method | Speed | Scalability | Fault Tolerance | Cross-Cluster | Use Case |
|--------|-------|-------------|-----------------|---------------|----------|
| **Distcp** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Production cross-cluster |
| **HDFS Put** | ⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐ | ❌ | Single cluster, small files |
| **HDFS Copy** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ❌ | Same cluster operations |
| **SCP** | ⭐⭐ | ⭐ | ⭐⭐ | ⭐⭐⭐ | Emergency, non-HDFS targets |
| **FSSpec** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Cloud storage, unified API |

## Configuration Examples

### Production Cross-Cluster Transfer
```bash
# Using distcp (recommended)
export USE_DISTCP=true
export SOURCE_HDFS_PATH=/data/source
export TARGET_CLUSTER=cluster2.example.com
export TARGET_HDFS_PATH=/data/target

impala-transfer \
    --source-host cluster1.example.com \
    --query "SELECT * FROM large_table WHERE date = '2024-01-01'" \
    --chunk-size 2000000 \
    --max-workers 8
```

### Development Single-Cluster Transfer
```bash
# Using hdfs put (with automatic fallback to hdfs cp if needed)
export USE_DISTCP=false
export TARGET_HDFS_PATH=/tmp/test_data

impala-transfer \
    --source-host localhost \
    --query "SELECT * FROM test_table LIMIT 1000" \
    --chunk-size 100000 \
    --max-workers 2
```

### Emergency Transfer
```bash
# Using SCP (when HDFS not available)
# This would require configuration in the tool
impala-transfer \
    --source-host cluster1.example.com \
    --query "SELECT * FROM critical_table" \
    --output-format csv
```

### Cloud Storage Transfer (FSSpec)
```python
# Using FSSpec for S3 transfer
from impala_transfer.transfer import FSSpecFileTransferManager

s3_config = {
    'protocol': 's3',
    'bucket': 'my-data-bucket',
    'key': 'your-access-key',
    'secret': 'your-secret-key',
    'target_path': 'transfers/'
}

manager = FSSpecFileTransferManager(target_fs_config=s3_config)
success = manager.transfer_files(['/local/path/file.csv'], "target_table")
```

### Cross-Platform Transfer (FSSpec)
```python
# Transfer from HDFS to S3
from impala_transfer.transfer import FSSpecFileTransferManager

source_config = {
    'protocol': 'hdfs',
    'host': 'namenode.example.com',
    'port': 8020,
    'user': 'hdfs'
}

target_config = {
    'protocol': 's3',
    'bucket': 'my-bucket',
    'key': 'your-access-key',
    'secret': 'your-secret-key',
    'target_path': 'transfers/'
}

manager = FSSpecFileTransferManager(
    source_fs_config=source_config,
    target_fs_config=target_config
)

success = manager.transfer_files(['/user/data/file.csv'], "target_table")
```

### Using Filesystem Objects Directly (FSSpec)
```python
# Create filesystem instances directly
import fsspec
from impala_transfer.transfer import FSSpecFileTransferManager

# Create filesystem instances
s3_fs = fsspec.filesystem('s3', 
                         key='your-access-key',
                         secret='your-secret-key',
                         bucket='my-bucket')

hdfs_fs = fsspec.filesystem('hdfs', 
                           host='namenode.example.com',
                           port=8020,
                           user='hdfs')

# Use filesystem instances directly
manager = FSSpecFileTransferManager(
    source_fs=hdfs_fs,
    target_fs=s3_fs,
    transfer_options={'chunk_size': 1024*1024}  # 1MB chunks
)

# Transfer with explicit target path
success = manager.transfer_files(
    filepaths=['/user/data/file.csv'],
    target_table='my_table',
    target_path='s3://my-bucket/transfers/'
)
```

### Mixed Configuration (Filesystem Objects + Config)
```python
# Use filesystem object for source, config for target
source_fs = fsspec.filesystem('sftp',
                             host='sftp.example.com',
                             username='user',
                             password='pass')

target_config = {
    'protocol': 'abfs',  # Azure Blob Storage
    'account_name': 'myaccount',
    'account_key': 'my-key',
    'container': 'my-container',
    'target_path': '/data/'
}

manager = FSSpecFileTransferManager(
    source_fs=source_fs,
    target_fs_config=target_config
)

success = manager.transfer_files(['/remote/path/file.txt'], "logs_table")
```

### Using with Unified Transfer Manager
```python
# Use filesystem objects with the unified manager
from impala_transfer.transfer import UnifiedFileTransferManager

local_fs = fsspec.filesystem('file')
gcs_fs = fsspec.filesystem('gcs', 
                          project='my-project',
                          token='path/to/credentials.json')

unified_manager = UnifiedFileTransferManager(
    use_fsspec=True,
    source_fs=local_fs,
    target_fs=gcs_fs,
    fsspec_config={'transfer_options': {'chunk_size': 512*1024}}
)

success = unified_manager.transfer_files(
    filepaths=['/local/path/file.parquet'],
    target_table='analytics_table',
    target_path='gs://my-bucket/analytics/'
)
```

## Automatic Fallback Behavior

The tool implements an intelligent fallback system that automatically tries different transfer methods if the primary method fails:

### Traditional Methods Fallback
1. **Distcp** (if configured) → **HDFS Put** → **HDFS Copy** → **SCP**
2. **HDFS Put** → **HDFS Copy** → **SCP**
3. **SCP** (final fallback)

### FSSpec Methods
FSSpec provides its own error handling and retry mechanisms. The tool will:
1. Attempt the configured transfer method
2. Log detailed error information
3. Provide filesystem-specific error messages

This ensures maximum reliability and success rates for data transfers.

## Best Practices

### 1. Choose the Right Method
- **Cross-cluster production**: Always use distcp (with automatic fallbacks)
- **Single cluster**: Use hdfs put for simplicity (with automatic fallbacks)
- **Development/testing**: Use hdfs put for speed (with automatic fallbacks)
- **Emergency scenarios**: SCP is the final fallback
- **Cloud storage**: Use FSSpec for native cloud storage support
- **Cross-platform**: Use FSSpec for unified filesystem operations

### 2. Optimize Distcp Performance
```bash
# Use these flags for optimal performance
hadoop distcp -update -delete -strategy dynamic -m 20 source target
```

### 3. Monitor Transfer Progress
- Distcp provides built-in progress reporting
- Monitor network bandwidth during transfers
- Check HDFS space on both clusters

### 4. Handle Failures
- Distcp can resume interrupted transfers
- Monitor logs for error patterns
- Implement retry logic for critical transfers

## Troubleshooting

### Distcp Issues
```bash
# Check distcp availability
which hadoop
hadoop version

# Test distcp connectivity
hadoop distcp -dryrun source target

# Check HDFS permissions
hdfs dfs -ls /target/path
```

### HDFS Put Issues
```bash
# Check local file existence
ls -la /local/path/file.parquet

# Check HDFS space
hdfs dfs -df /target/path

# Check HDFS permissions
hdfs dfs -ls /target/path
```

### SCP Issues
```bash
# Test SSH connectivity
ssh target-host "echo 'connection test'"

# Check disk space on target
ssh target-host "df -h /target/path"
```

## Future Enhancements

1. **Configurable SCP**: Make SCP target host and path configurable
2. **HDFS Copy Support**: Add support for within-cluster HDFS copy operations
3. **Cloud Storage**: Add support for S3, Azure Blob, and Google Cloud Storage
4. **Transfer Validation**: Add checksum verification for transferred files
5. **Bandwidth Throttling**: Add options to limit transfer bandwidth
6. **Compression**: Add support for on-the-fly compression during transfer 