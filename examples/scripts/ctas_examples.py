#!/usr/bin/env python3
"""
CTAS (CREATE TABLE AS SELECT) Examples for Impala Transfer Tool

This script demonstrates how to use the CTAS functionality programmatically
with various configurations and options.
"""

from impala_transfer import ImpalaTransferTool
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def basic_ctas_example():
    """Basic CTAS example - create a simple table from query results."""
    logger.info("=== Basic CTAS Example ===")
    
    tool = ImpalaTransferTool(
        source_host="impala-cluster.example.com",
        connection_type="impyla"
    )
    
    query = "SELECT * FROM source_table WHERE date = '2024-01-01'"
    target_table = "daily_summary"
    
    success = tool.create_table_as_select(
        query=query,
        target_table=target_table
    )
    
    if success:
        logger.info(f"✓ Successfully created table '{target_table}'")
    else:
        logger.error(f"✗ Failed to create table '{target_table}'")


def ctas_with_partitioning():
    """CTAS example with partitioning."""
    logger.info("=== CTAS with Partitioning Example ===")
    
    tool = ImpalaTransferTool(
        source_host="impala-cluster.example.com",
        connection_type="impyla"
    )
    
    query = """
    SELECT 
        user_id, 
        event_type, 
        event_date, 
        COUNT(*) as event_count 
    FROM events 
    GROUP BY user_id, event_type, event_date
    """
    target_table = "events_summary"
    
    success = tool.create_table_as_select(
        query=query,
        target_table=target_table,
        partitioned_by=["event_date", "event_type"]
    )
    
    if success:
        logger.info(f"✓ Successfully created partitioned table '{target_table}'")
    else:
        logger.error(f"✗ Failed to create partitioned table '{target_table}'")


def ctas_with_clustering():
    """CTAS example with clustering and bucketing."""
    logger.info("=== CTAS with Clustering Example ===")
    
    tool = ImpalaTransferTool(
        source_host="impala-cluster.example.com",
        connection_type="impyla"
    )
    
    query = """
    SELECT 
        user_id, 
        product_id, 
        purchase_date, 
        amount 
    FROM purchases 
    WHERE purchase_date >= '2024-01-01'
    """
    target_table = "user_purchases"
    
    success = tool.create_table_as_select(
        query=query,
        target_table=target_table,
        clustered_by=["user_id"],
        buckets=32
    )
    
    if success:
        logger.info(f"✓ Successfully created clustered table '{target_table}'")
    else:
        logger.error(f"✗ Failed to create clustered table '{target_table}'")


def ctas_with_custom_format():
    """CTAS example with custom file format and compression."""
    logger.info("=== CTAS with Custom Format Example ===")
    
    tool = ImpalaTransferTool(
        source_host="impala-cluster.example.com",
        connection_type="impyla"
    )
    
    query = "SELECT * FROM large_table WHERE region = 'US'"
    target_table = "us_data"
    
    success = tool.create_table_as_select(
        query=query,
        target_table=target_table,
        file_format="ORC",
        compression="GZIP"
    )
    
    if success:
        logger.info(f"✓ Successfully created ORC table '{target_table}'")
    else:
        logger.error(f"✗ Failed to create ORC table '{target_table}'")


def ctas_with_custom_location():
    """CTAS example with custom HDFS location."""
    logger.info("=== CTAS with Custom Location Example ===")
    
    tool = ImpalaTransferTool(
        source_host="impala-cluster.example.com",
        connection_type="impyla"
    )
    
    query = "SELECT * FROM source_table"
    target_table = "custom_location_table"
    location = "/data/custom/tables/custom_location_table"
    
    success = tool.create_table_as_select(
        query=query,
        target_table=target_table,
        location=location
    )
    
    if success:
        logger.info(f"✓ Successfully created table '{target_table}' at '{location}'")
    else:
        logger.error(f"✗ Failed to create table '{target_table}'")


def ctas_with_overwrite():
    """CTAS example with overwrite option."""
    logger.info("=== CTAS with Overwrite Example ===")
    
    tool = ImpalaTransferTool(
        source_host="impala-cluster.example.com",
        connection_type="impyla"
    )
    
    query = "SELECT * FROM source_table WHERE updated_date = CURRENT_DATE()"
    target_table = "daily_refresh"
    
    success = tool.create_table_as_select(
        query=query,
        target_table=target_table,
        overwrite=True
    )
    
    if success:
        logger.info(f"✓ Successfully created/overwritten table '{target_table}'")
    else:
        logger.error(f"✗ Failed to create/overwrite table '{target_table}'")


def ctas_with_progress():
    """CTAS example with progress reporting."""
    logger.info("=== CTAS with Progress Reporting Example ===")
    
    tool = ImpalaTransferTool(
        source_host="impala-cluster.example.com",
        connection_type="impyla"
    )
    
    def progress_callback(message, percentage):
        logger.info(f"Progress {percentage}%: {message}")
    
    query = """
    SELECT 
        user_id,
        DATE_TRUNC('month', event_date) as month,
        COUNT(*) as total_events,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
        AVG(amount) as avg_amount
    FROM user_events 
    WHERE event_date >= '2024-01-01'
    GROUP BY user_id, DATE_TRUNC('month', event_date)
    """
    target_table = "user_monthly_summary"
    
    success = tool.create_table_as_select_with_progress(
        query=query,
        target_table=target_table,
        partitioned_by=["month"],
        progress_callback=progress_callback
    )
    
    if success:
        logger.info(f"✓ Successfully created table '{target_table}' with progress reporting")
    else:
        logger.error(f"✗ Failed to create table '{target_table}'")


def table_management_examples():
    """Examples of table management operations."""
    logger.info("=== Table Management Examples ===")
    
    tool = ImpalaTransferTool(
        source_host="impala-cluster.example.com",
        connection_type="impyla"
    )
    
    table_name = "test_table"
    
    # Check if table exists
    if tool.table_exists(table_name):
        logger.info(f"Table '{table_name}' exists")
        
        # Drop the table
        if tool.drop_table(table_name):
            logger.info(f"✓ Successfully dropped table '{table_name}'")
        else:
            logger.error(f"✗ Failed to drop table '{table_name}'")
    else:
        logger.info(f"Table '{table_name}' does not exist")


def complex_ctas_example():
    """Complex CTAS example with multiple features."""
    logger.info("=== Complex CTAS Example ===")
    
    tool = ImpalaTransferTool(
        source_host="impala-cluster.example.com",
        connection_type="impyla"
    )
    
    query = """
    SELECT 
        region,
        product_category,
        total_sales,
        ROUND(total_sales / SUM(total_sales) OVER (PARTITION BY region) * 100, 2) as region_percentage
    FROM (
        SELECT 
            region,
            product_category,
            SUM(amount) as total_sales
        FROM sales
        WHERE sale_date >= '2024-01-01'
        GROUP BY region, product_category
    ) sales_summary
    """
    target_table = "regional_sales_analysis"
    
    success = tool.create_table_as_select(
        query=query,
        target_table=target_table,
        file_format="PARQUET",
        compression="SNAPPY",
        partitioned_by=["region"],
        clustered_by=["product_category"],
        buckets=16
    )
    
    if success:
        logger.info(f"✓ Successfully created complex table '{target_table}'")
    else:
        logger.error(f"✗ Failed to create complex table '{target_table}'")


def main():
    """Run all CTAS examples."""
    logger.info("Starting CTAS Examples")
    
    try:
        # Basic examples
        basic_ctas_example()
        ctas_with_partitioning()
        ctas_with_clustering()
        ctas_with_custom_format()
        ctas_with_custom_location()
        ctas_with_overwrite()
        
        # Advanced examples
        ctas_with_progress()
        complex_ctas_example()
        
        # Table management
        table_management_examples()
        
        logger.info("All CTAS examples completed")
        
    except Exception as e:
        logger.error(f"Error running CTAS examples: {e}")


if __name__ == "__main__":
    main() 