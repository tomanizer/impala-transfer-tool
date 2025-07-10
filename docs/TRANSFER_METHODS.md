# Transfer Methods

The Impala Transfer Tool supports multiple methods for transferring data between clusters. This document explains the differences between these methods and provides guidance on when to use each one.

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

## Performance Comparison

| Method | Speed | Scalability | Fault Tolerance | Cross-Cluster | Use Case |
|--------|-------|-------------|-----------------|---------------|----------|
| **Distcp** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Production cross-cluster |
| **HDFS Put** | ⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐ | ❌ | Single cluster, small files |
| **HDFS Copy** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ❌ | Same cluster operations |
| **SCP** | ⭐⭐ | ⭐ | ⭐⭐ | ⭐⭐⭐ | Emergency, non-HDFS targets |

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

## Automatic Fallback Behavior

The tool implements an intelligent fallback system that automatically tries different transfer methods if the primary method fails:

1. **Distcp** (if configured) → **HDFS Put** → **HDFS Copy** → **SCP**
2. **HDFS Put** → **HDFS Copy** → **SCP**
3. **SCP** (final fallback)

This ensures maximum reliability and success rates for data transfers.

## Best Practices

### 1. Choose the Right Method
- **Cross-cluster production**: Always use distcp (with automatic fallbacks)
- **Single cluster**: Use hdfs put for simplicity (with automatic fallbacks)
- **Development/testing**: Use hdfs put for speed (with automatic fallbacks)
- **Emergency scenarios**: SCP is the final fallback

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