#!/usr/bin/env python3
"""
Comprehensive FSSpec Examples and Demonstrations.

This file combines both educational demonstrations and production-ready examples
for using fsspec filesystem objects with the Impala Transfer Tool.

Sections:
1. Basic Demonstrations (runnable demos)
2. Production Examples (copy-paste ready)
3. Configuration Helpers
4. Advanced Patterns
"""

import fsspec
import tempfile
import os
from pathlib import Path

# Import the transfer managers
from impala_transfer.transfer import FSSpecFileTransferManager, UnifiedFileTransferManager


# ============================================================================
# SECTION 1: BASIC DEMONSTRATIONS (Runnable)
# ============================================================================

def demo_filesystem_objects():
    """Demonstrate using filesystem objects directly."""
    print("=== FSSpec Filesystem Objects Demo ===")
    
    # Create filesystem instances directly
    local_fs = fsspec.filesystem('file')
    memory_fs = fsspec.filesystem('memory')
    
    print(f"Local filesystem protocol: {local_fs.protocol}")
    print(f"Memory filesystem protocol: {memory_fs.protocol}")
    
    # Create transfer manager with filesystem objects
    manager = FSSpecFileTransferManager(
        source_fs=local_fs,
        target_fs=memory_fs,
        transfer_options={'chunk_size': 1024}
    )
    
    print(f"Transfer manager created with source: {type(manager.source_fs).__name__}")
    print(f"Transfer manager created with target: {type(manager.target_fs).__name__}")
    
    # Get transfer info
    info = manager.get_transfer_info()
    print(f"Source protocol: {info['source_protocol']}")
    print(f"Target protocol: {info['target_protocol']}")
    print(f"Source filesystem type: {info['source_fs_type']}")
    print(f"Target filesystem type: {info['target_fs_type']}")
    
    return manager


def demo_file_transfer():
    """Demonstrate actual file transfer with filesystem objects."""
    print("\n=== File Transfer Demo ===")
    
    # Create temporary files for demo
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a test file
        test_file = Path(temp_dir) / "test_data.txt"
        test_file.write_text("This is test data for the fsspec demo.\n" * 10)
        
        print(f"Created test file: {test_file}")
        print(f"File size: {test_file.stat().st_size} bytes")
        
        # Create filesystem instances
        local_fs = fsspec.filesystem('file')
        memory_fs = fsspec.filesystem('memory')
        
        # Create transfer manager
        manager = FSSpecFileTransferManager(
            source_fs=local_fs,
            target_fs=memory_fs
        )
        
        # Transfer file with explicit target path
        success = manager.transfer_files(
            filepaths=[str(test_file)],
            target_table='demo_table',
            target_path='/demo/transfers/'
        )
        
        print(f"Transfer successful: {success}")
        
        if success:
            # List files in target filesystem
            files = manager.list_files('/demo/transfers/')
            print(f"Files in target: {files}")
            
            # Get file info
            if files:
                # Memory filesystem returns dict objects, extract the name
                file_path = files[0]['name'] if isinstance(files[0], dict) else files[0]
                file_info = manager.get_file_info(file_path)
                print(f"File info: {file_info}")


# ============================================================================
# SECTION 2: PRODUCTION EXAMPLES (Copy-paste ready)
# ============================================================================

def example_s3_to_hdfs_transfer():
    """
    Example: Transfer files from S3 to HDFS using filesystem objects.
    
    Copy-paste ready example for production use.
    """
    # Create filesystem instances directly
    s3_fs = fsspec.filesystem('s3', 
                             key='your-access-key',
                             secret='your-secret-key',
                             endpoint_url='https://s3.amazonaws.com')
    
    hdfs_fs = fsspec.filesystem('hdfs', 
                               host='namenode.example.com',
                               port=8020,
                               user='hdfs')
    
    # Use filesystem instances directly in transfer manager
    transfer_manager = FSSpecFileTransferManager(
        source_fs=s3_fs,
        target_fs=hdfs_fs,
        transfer_options={'chunk_size': 1024*1024}  # 1MB chunks
    )
    
    # Transfer files with explicit target path
    filepaths = ['s3://bucket/path/file1.csv', 's3://bucket/path/file2.csv']
    success = transfer_manager.transfer_files(
        filepaths=filepaths,
        target_table='my_table',
        target_path='/user/data/my_table/'
    )
    
    return success


def example_local_to_gcs_transfer():
    """
    Example: Transfer files from local filesystem to Google Cloud Storage.
    
    Copy-paste ready example for production use.
    """
    # Create filesystem instances
    local_fs = fsspec.filesystem('file')
    gcs_fs = fsspec.filesystem('gcs', 
                              project='my-project',
                              token='path/to/credentials.json')
    
    # Use with unified manager
    unified_manager = UnifiedFileTransferManager(
        use_fsspec=True,
        source_fs=local_fs,
        target_fs=gcs_fs,
        fsspec_config={'transfer_options': {'chunk_size': 512*1024}}  # 512KB chunks
    )
    
    # Transfer files
    filepaths = ['/local/path/file1.parquet', '/local/path/file2.parquet']
    success = unified_manager.transfer_files(
        filepaths=filepaths,
        target_table='analytics_table',
        target_path='gs://my-bucket/analytics/'
    )
    
    return success


def example_sftp_to_azure_transfer():
    """
    Example: Transfer files from SFTP to Azure Blob Storage.
    
    Copy-paste ready example for production use.
    """
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
    
    transfer_manager = FSSpecFileTransferManager(
        source_fs=source_fs,
        target_fs_config=target_config
    )
    
    # Transfer files
    filepaths = ['/remote/path/file1.txt', '/remote/path/file2.txt']
    success = transfer_manager.transfer_files(
        filepaths=filepaths,
        target_table='logs_table'
    )
    
    return success


# ============================================================================
# SECTION 3: CONFIGURATION HELPERS
# ============================================================================

def demo_configuration_helpers():
    """Demonstrate using configuration helper methods."""
    print("\n=== Configuration Helpers Demo ===")
    
    # Use helper methods to create configurations
    hdfs_config = UnifiedFileTransferManager.create_hdfs_config(
        host='namenode.example.com',
        port=8020,
        user='hdfs'
    )
    
    s3_config = UnifiedFileTransferManager.create_s3_config(
        bucket='my-bucket',
        access_key='your-key',
        secret_key='your-secret'
    )
    
    gcs_config = UnifiedFileTransferManager.create_gcs_config(
        bucket='my-gcs-bucket',
        project='my-project',
        credentials_file='/path/to/credentials.json'
    )
    
    azure_config = UnifiedFileTransferManager.create_azure_config(
        account_name='myaccount',
        account_key='account-key-123',
        container='my-container'
    )
    
    print("HDFS config:", hdfs_config)
    print("S3 config:", s3_config)
    print("GCS config:", gcs_config)
    print("Azure config:", azure_config)
    
    # Create filesystem instances from configs (skip HDFS if Java not available)
    try:
        hdfs_fs = fsspec.filesystem(**hdfs_config)
        print(f"HDFS filesystem created: {type(hdfs_fs).__name__}")
    except Exception as e:
        print(f"HDFS filesystem creation failed (Java not available): {e}")
    
    # Try S3 config (this will fail without credentials, but won't crash)
    try:
        s3_fs = fsspec.filesystem(**s3_config)
        print(f"S3 filesystem created: {type(s3_fs).__name__}")
    except Exception as e:
        print(f"S3 filesystem creation failed (no credentials): {e}")


# ============================================================================
# SECTION 4: ADVANCED PATTERNS
# ============================================================================

def example_filesystem_validation():
    """
    Example: Validate filesystem configuration before transfer.
    
    Advanced pattern for production use.
    """
    # Create filesystem instances
    source_fs = fsspec.filesystem('file')
    target_fs = fsspec.filesystem('s3', 
                                 key='your-key',
                                 secret='your-secret',
                                 bucket='my-bucket')
    
    transfer_manager = FSSpecFileTransferManager(
        source_fs=source_fs,
        target_fs=target_fs
    )
    
    # Validate configuration
    if transfer_manager.validate_config():
        print("Filesystem configuration is valid")
        
        # Get transfer information
        info = transfer_manager.get_transfer_info()
        print(f"Source protocol: {info['source_protocol']}")
        print(f"Target protocol: {info['target_protocol']}")
        print(f"Source filesystem type: {info['source_fs_type']}")
        print(f"Target filesystem type: {info['target_fs_type']}")
        return True
    else:
        print("Filesystem configuration validation failed")
        return False


def example_mixed_configuration():
    """
    Example: Mix filesystem objects and configuration dictionaries.
    
    Advanced pattern showing flexibility.
    """
    # Use filesystem object for source, config for target
    source_fs = fsspec.filesystem('file')
    
    target_config = {
        'protocol': 'memory',
        'target_path': '/demo/'
    }
    
    manager = FSSpecFileTransferManager(
        source_fs=source_fs,
        target_fs_config=target_config
    )
    
    print(f"Mixed configuration - source: {type(manager.source_fs).__name__}")
    print(f"Mixed configuration - target: {type(manager.target_fs).__name__}")
    
    return manager


def example_unified_manager_with_filesystems():
    """
    Example: Use UnifiedFileTransferManager with filesystem objects.
    
    Advanced pattern showing unified approach.
    """
    # Create filesystem instances
    local_fs = fsspec.filesystem('file')
    memory_fs = fsspec.filesystem('memory')
    
    # Use with unified manager
    unified_manager = UnifiedFileTransferManager(
        use_fsspec=True,
        source_fs=local_fs,
        target_fs=memory_fs,
        fsspec_config={'transfer_options': {'chunk_size': 512}}
    )
    
    print(f"Unified manager using fsspec: {unified_manager.use_fsspec}")
    print(f"FSSpec manager source: {type(unified_manager.fsspec_manager.source_fs).__name__}")
    print(f"FSSpec manager target: {type(unified_manager.fsspec_manager.target_fs).__name__}")
    
    return unified_manager


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def run_demos():
    """Run all demonstration functions."""
    print("FSSpec Comprehensive Examples and Demonstrations")
    print("=" * 60)
    
    try:
        # Basic demonstrations
        demo_filesystem_objects()
        demo_file_transfer()
        demo_configuration_helpers()
        
        # Advanced patterns
        example_mixed_configuration()
        example_unified_manager_with_filesystems()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("\nFor production examples, see the individual functions:")
        print("- example_s3_to_hdfs_transfer()")
        print("- example_local_to_gcs_transfer()")
        print("- example_sftp_to_azure_transfer()")
        print("- example_filesystem_validation()")
        
    except Exception as e:
        print(f"Demo failed with error: {e}")
        import traceback
        traceback.print_exc()


def show_production_examples():
    """Show production-ready examples without executing them."""
    print("\n" + "=" * 60)
    print("PRODUCTION EXAMPLES (Copy-paste ready)")
    print("=" * 60)
    print("\n1. S3 to HDFS Transfer:")
    print("   example_s3_to_hdfs_transfer()")
    print("\n2. Local to GCS Transfer:")
    print("   example_local_to_gcs_transfer()")
    print("\n3. SFTP to Azure Transfer:")
    print("   example_sftp_to_azure_transfer()")
    print("\n4. Filesystem Validation:")
    print("   example_filesystem_validation()")
    print("\nNote: Configure credentials and paths before running.")


if __name__ == "__main__":
    # Run demonstrations
    run_demos()
    
    # Show production examples
    show_production_examples() 