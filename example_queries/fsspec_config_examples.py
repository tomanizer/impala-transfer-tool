"""
Example configurations for using fsspec with different filesystem types.
This file demonstrates how to configure the FSSpecFileTransferManager for various
cloud storage and filesystem providers.
"""

from impala_transfer.transfer import (
    FSSpecFileTransferManager,
    UnifiedFileTransferManager
)


def example_hdfs_transfer():
    """Example: Transfer files to HDFS cluster."""
    
    # HDFS configuration
    hdfs_config = {
        'protocol': 'hdfs',
        'host': 'namenode.example.com',
        'port': 8020,
        'user': 'hdfs',
        'target_path': '/user/data/transfers'
    }
    
    # Create transfer manager
    manager = FSSpecFileTransferManager(target_fs_config=hdfs_config)
    
    # Transfer files
    filepaths = ['/local/path/file1.csv', '/local/path/file2.csv']
    success = manager.transfer_files(filepaths, "target_table")
    
    print(f"HDFS transfer {'successful' if success else 'failed'}")


def example_s3_transfer():
    """Example: Transfer files to S3 bucket."""
    
    # S3 configuration
    s3_config = {
        'protocol': 's3',
        'bucket': 'my-data-bucket',
        'key': 'your-access-key',
        'secret': 'your-secret-key',
        'target_path': 'transfers/'
    }
    
    # Create transfer manager
    manager = FSSpecFileTransferManager(target_fs_config=s3_config)
    
    # Transfer files
    filepaths = ['/local/path/file1.csv', '/local/path/file2.csv']
    success = manager.transfer_files(filepaths, "target_table")
    
    print(f"S3 transfer {'successful' if success else 'failed'}")


def example_gcs_transfer():
    """Example: Transfer files to Google Cloud Storage."""
    
    # GCS configuration
    gcs_config = {
        'protocol': 'gcs',
        'bucket': 'my-gcs-bucket',
        'project': 'my-gcp-project',
        'token': '/path/to/service-account.json',
        'target_path': 'transfers/'
    }
    
    # Create transfer manager
    manager = FSSpecFileTransferManager(target_fs_config=gcs_config)
    
    # Transfer files
    filepaths = ['/local/path/file1.csv', '/local/path/file2.csv']
    success = manager.transfer_files(filepaths, "target_table")
    
    print(f"GCS transfer {'successful' if success else 'failed'}")


def example_azure_transfer():
    """Example: Transfer files to Azure Blob Storage."""
    
    # Azure configuration
    azure_config = {
        'protocol': 'abfs',
        'account_name': 'myaccount',
        'account_key': 'your-account-key',
        'container': 'my-container',
        'target_path': 'transfers/'
    }
    
    # Create transfer manager
    manager = FSSpecFileTransferManager(target_fs_config=azure_config)
    
    # Transfer files
    filepaths = ['/local/path/file1.csv', '/local/path/file2.csv']
    success = manager.transfer_files(filepaths, "target_table")
    
    print(f"Azure transfer {'successful' if success else 'failed'}")


def example_cross_cluster_transfer():
    """Example: Transfer files between different filesystem types."""
    
    # Source filesystem (HDFS)
    source_config = {
        'protocol': 'hdfs',
        'host': 'source-namenode.example.com',
        'port': 8020,
        'user': 'hdfs'
    }
    
    # Target filesystem (S3)
    target_config = {
        'protocol': 's3',
        'bucket': 'my-data-bucket',
        'key': 'your-access-key',
        'secret': 'your-secret-key',
        'target_path': 'transfers/'
    }
    
    # Create transfer manager
    manager = FSSpecFileTransferManager(
        source_fs_config=source_config,
        target_fs_config=target_config
    )
    
    # Transfer files (paths are relative to source filesystem)
    filepaths = ['/user/data/file1.csv', '/user/data/file2.csv']
    success = manager.transfer_files(filepaths, "target_table")
    
    print(f"Cross-cluster transfer {'successful' if success else 'failed'}")


def example_hdfs_to_hdfs_transfer():
    """Example: Transfer files between different HDFS clusters."""
    
    # Source HDFS cluster configuration
    source_config = {
        'protocol': 'hdfs',
        'host': 'source-cluster-namenode.example.com',
        'port': 8020,
        'user': 'hdfs',
        'namenode_rpc_address': 'source-cluster-namenode.example.com:8020',
        'namenode_http_address': 'source-cluster-namenode.example.com:9870'
    }
    
    # Target HDFS cluster configuration
    target_config = {
        'protocol': 'hdfs',
        'host': 'target-cluster-namenode.example.com',
        'port': 8020,
        'user': 'hdfs',
        'target_path': '/user/data/transfers',
        'namenode_rpc_address': 'target-cluster-namenode.example.com:8020',
        'namenode_http_address': 'target-cluster-namenode.example.com:9870'
    }
    
    # Create transfer manager with both source and target configurations
    manager = FSSpecFileTransferManager(
        source_fs_config=source_config,
        target_fs_config=target_config
    )
    
    # Transfer files (paths are relative to source HDFS filesystem)
    filepaths = [
        '/user/source/data/file1.csv',
        '/user/source/data/file2.csv',
        '/user/source/data/directory/file3.csv'
    ]
    
    success = manager.transfer_files(filepaths, "target_table")
    
    print(f"HDFS to HDFS transfer {'successful' if success else 'failed'}")


def example_hdfs_cluster_with_kerberos():
    """Example: HDFS to HDFS transfer with Kerberos authentication."""
    
    # Source HDFS cluster with Kerberos
    source_config = {
        'protocol': 'hdfs',
        'host': 'source-cluster-namenode.example.com',
        'port': 8020,
        'user': 'hdfs',
        'kerberos': True,
        'kerberos_principal': 'hdfs@EXAMPLE.COM',
        'keytab_file': '/path/to/source-keytab.keytab',
        'namenode_rpc_address': 'source-cluster-namenode.example.com:8020'
    }
    
    # Target HDFS cluster with Kerberos
    target_config = {
        'protocol': 'hdfs',
        'host': 'target-cluster-namenode.example.com',
        'port': 8020,
        'user': 'hdfs',
        'target_path': '/user/data/transfers',
        'kerberos': True,
        'kerberos_principal': 'hdfs@EXAMPLE.COM',
        'keytab_file': '/path/to/target-keytab.keytab',
        'namenode_rpc_address': 'target-cluster-namenode.example.com:8020'
    }
    
    # Create transfer manager
    manager = FSSpecFileTransferManager(
        source_fs_config=source_config,
        target_fs_config=target_config
    )
    
    # Transfer files
    filepaths = ['/user/source/data/file1.csv', '/user/source/data/file2.csv']
    success = manager.transfer_files(filepaths, "target_table")
    
    print(f"Kerberos HDFS to HDFS transfer {'successful' if success else 'failed'}")


def example_unified_manager():
    """Example: Using UnifiedFileTransferManager for flexible transfer methods."""
    
    # Option 1: Use fsspec for S3 transfer
    s3_config = {
        'target_fs_config': {
            'protocol': 's3',
            'bucket': 'my-bucket',
            'key': 'your-access-key',
            'secret': 'your-secret-key',
            'target_path': 'transfers/'
        }
    }
    
    fsspec_manager = UnifiedFileTransferManager(
        use_fsspec=True,
        fsspec_config=s3_config
    )
    
    # Option 2: Use traditional methods for HDFS transfer
    traditional_manager = UnifiedFileTransferManager(
        use_fsspec=False,
        target_hdfs_path='/user/data/transfers',
        use_distcp=True,
        source_hdfs_path='/user/source/data',
        target_cluster='target-cluster.example.com'
    )
    
    # Both managers have the same interface
    filepaths = ['/local/path/file1.csv', '/local/path/file2.csv']
    
    # Use fsspec manager
    fsspec_success = fsspec_manager.transfer_files(filepaths, "s3_table")
    
    # Use traditional manager
    traditional_success = traditional_manager.transfer_files(filepaths, "hdfs_table")
    
    print(f"FSSpec transfer: {'successful' if fsspec_success else 'failed'}")
    print(f"Traditional transfer: {'successful' if traditional_success else 'failed'}")


def example_using_helper_methods():
    """Example: Using helper methods to create configurations."""
    
    # Create HDFS config using helper method
    hdfs_config = UnifiedFileTransferManager.create_hdfs_config(
        host='namenode.example.com',
        port=8020,
        user='hdfs'
    )
    hdfs_config['target_path'] = '/user/data/transfers'
    
    # Create S3 config using helper method
    s3_config = UnifiedFileTransferManager.create_s3_config(
        bucket='my-bucket',
        access_key='your-access-key',
        secret_key='your-secret-key',
        endpoint_url='https://s3.example.com'  # Optional, for non-AWS S3
    )
    s3_config['target_path'] = 'transfers/'
    
    # Create GCS config using helper method
    gcs_config = UnifiedFileTransferManager.create_gcs_config(
        bucket='my-gcs-bucket',
        project='my-gcp-project',
        credentials_file='/path/to/service-account.json'
    )
    gcs_config['target_path'] = 'transfers/'
    
    # Create Azure config using helper method
    azure_config = UnifiedFileTransferManager.create_azure_config(
        account_name='myaccount',
        account_key='your-account-key',
        container='my-container'
    )
    azure_config['target_path'] = 'transfers/'
    
    # Use configurations
    managers = {
        'hdfs': FSSpecFileTransferManager(target_fs_config=hdfs_config),
        's3': FSSpecFileTransferManager(target_fs_config=s3_config),
        'gcs': FSSpecFileTransferManager(target_fs_config=gcs_config),
        'azure': FSSpecFileTransferManager(target_fs_config=azure_config)
    }
    
    filepaths = ['/local/path/file1.csv', '/local/path/file2.csv']
    
    for name, manager in managers.items():
        success = manager.transfer_files(filepaths, f"{name}_table")
        print(f"{name.upper()} transfer: {'successful' if success else 'failed'}")


def example_environment_based_config():
    """Example: Configuration based on environment variables."""
    import os
    
    # Get configuration from environment variables
    target_type = os.getenv('TARGET_FS_TYPE', 'file')
    
    if target_type == 's3':
        config = {
            'protocol': 's3',
            'bucket': os.getenv('S3_BUCKET'),
            'key': os.getenv('AWS_ACCESS_KEY_ID'),
            'secret': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'target_path': os.getenv('S3_TARGET_PATH', 'transfers/')
        }
    elif target_type == 'hdfs':
        config = {
            'protocol': 'hdfs',
            'host': os.getenv('HDFS_HOST'),
            'port': int(os.getenv('HDFS_PORT', '8020')),
            'user': os.getenv('HDFS_USER', 'hdfs'),
            'target_path': os.getenv('HDFS_TARGET_PATH', '/user/data/transfers')
        }
    elif target_type == 'gcs':
        config = {
            'protocol': 'gcs',
            'bucket': os.getenv('GCS_BUCKET'),
            'project': os.getenv('GCP_PROJECT'),
            'token': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
            'target_path': os.getenv('GCS_TARGET_PATH', 'transfers/')
        }
    else:
        # Default to local filesystem
        config = {
            'protocol': 'file',
            'target_path': os.getenv('LOCAL_TARGET_PATH', '/tmp/transfers')
        }
    
    # Create transfer manager
    manager = FSSpecFileTransferManager(target_fs_config=config)
    
    # Transfer files
    filepaths = ['/local/path/file1.csv', '/local/path/file2.csv']
    success = manager.transfer_files(filepaths, "target_table")
    
    print(f"Transfer to {target_type}: {'successful' if success else 'failed'}")


if __name__ == "__main__":
    """Run examples if this file is executed directly."""
    print("FSSpec Transfer Configuration Examples")
    print("=" * 50)
    
    # Note: These examples require proper configuration and credentials
    # Uncomment the examples you want to run:
    
    # example_hdfs_transfer()
    # example_s3_transfer()
    # example_gcs_transfer()
    # example_azure_transfer()
    # example_cross_cluster_transfer()
    # example_hdfs_to_hdfs_transfer()
    # example_hdfs_cluster_with_kerberos()
    # example_unified_manager()
    # example_using_helper_methods()
    # example_environment_based_config()
    
    print("\nNote: Configure proper credentials and paths before running examples.") 