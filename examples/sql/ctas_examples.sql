-- CTAS (CREATE TABLE AS SELECT) Examples for Impala Transfer Tool
-- These examples demonstrate how to use the --ctas flag with required --table-location (HDFS path) and only PARQUET format

-- Basic CTAS: Create a simple table from query results
-- Command: impala-transfer --ctas --query "SELECT * FROM source_table WHERE date = '2024-01-01'" --target-table daily_summary --table-location "/user/sandbox/daily_summary"

-- CTAS with partitioning
-- Command: impala-transfer --ctas --query "SELECT user_id, event_type, event_date, COUNT(*) as event_count FROM events GROUP BY user_id, event_type, event_date" --target-table events_summary --partitioned-by event_date event_type --table-location "/user/sandbox/events_summary"

-- CTAS with clustering and bucketing
-- Command: impala-transfer --ctas --query "SELECT user_id, product_id, purchase_date, amount FROM purchases WHERE purchase_date >= '2024-01-01'" --target-table user_purchases --clustered-by user_id --buckets 32 --table-location "/user/sandbox/user_purchases"

-- CTAS with custom compression
-- Command: impala-transfer --ctas --query "SELECT * FROM large_table WHERE region = 'US'" --target-table us_data --compression GZIP --table-location "/user/sandbox/us_data"

-- CTAS with custom HDFS location
-- Command: impala-transfer --ctas --query "SELECT * FROM source_table" --target-table custom_location_table --table-location "/user/sandbox/custom_location_table"

-- CTAS with overwrite (drops existing table)
-- Command: impala-transfer --ctas --overwrite --query "SELECT * FROM source_table WHERE updated_date = CURRENT_DATE()" --target-table daily_refresh --table-location "/user/sandbox/daily_refresh"

-- CTAS with complex aggregation
-- Command: impala-transfer --ctas --query "
--   SELECT 
--     user_id,
--     DATE_TRUNC('month', event_date) as month,
--     COUNT(*) as total_events,
--     SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
--     SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
--     AVG(amount) as avg_amount
--   FROM user_events 
--   WHERE event_date >= '2024-01-01'
--   GROUP BY user_id, DATE_TRUNC('month', event_date)
-- " --target-table user_monthly_summary --partitioned-by month --table-location "/user/sandbox/user_monthly_summary"

-- CTAS with window functions
-- Command: impala-transfer --ctas --query "
--   SELECT 
--     user_id,
--     event_date,
--     event_type,
--     amount,
--     ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date DESC) as event_rank,
--     LAG(amount, 1) OVER (PARTITION BY user_id ORDER BY event_date) as prev_amount
--   FROM user_events
--   WHERE event_date >= '2024-01-01'
-- " --target-table user_events_with_rank --partitioned-by event_date --table-location "/user/sandbox/user_events_with_rank"

-- CTAS with multiple joins
-- Command: impala-transfer --ctas --query "
--   SELECT 
--     u.user_id,
--     u.user_name,
--     u.registration_date,
--     COUNT(e.event_id) as total_events,
--     SUM(e.amount) as total_spent,
--     MAX(e.event_date) as last_activity
--   FROM users u
--   LEFT JOIN events e ON u.user_id = e.user_id
--   WHERE u.registration_date >= '2024-01-01'
--   GROUP BY u.user_id, u.user_name, u.registration_date
-- " --target-table user_activity_summary --partitioned-by registration_date --table-location "/user/sandbox/user_activity_summary"

-- CTAS with subqueries
-- Command: impala-transfer --ctas --query "
--   SELECT 
--     region,
--     product_category,
--     total_sales,
--     ROUND(total_sales / SUM(total_sales) OVER (PARTITION BY region) * 100, 2) as region_percentage
--   FROM (
--     SELECT 
--       region,
--       product_category,
--       SUM(amount) as total_sales
--     FROM sales
--     WHERE sale_date >= '2024-01-01'
--     GROUP BY region, product_category
--   ) sales_summary
-- " --target-table regional_sales_analysis --table-location "/user/sandbox/regional_sales_analysis"

-- CTAS with UNION
-- Command: impala-transfer --ctas --query "
--   SELECT user_id, 'purchase' as activity_type, purchase_date as activity_date, amount as value
--   FROM purchases
--   WHERE purchase_date >= '2024-01-01'
--   UNION ALL
--   SELECT user_id, 'refund' as activity_type, refund_date as activity_date, -refund_amount as value
--   FROM refunds
--   WHERE refund_date >= '2024-01-01'
-- " --target-table user_activities --partitioned-by activity_date --table-location "/user/sandbox/user_activities"

-- CTAS with CASE statements
-- Command: impala-transfer --ctas --query "
--   SELECT 
--     user_id,
--     event_date,
--     CASE 
--       WHEN amount < 10 THEN 'low'
--       WHEN amount < 50 THEN 'medium'
--       WHEN amount < 100 THEN 'high'
--       ELSE 'premium'
--     END as spending_tier,
--     COUNT(*) as event_count,
--     SUM(amount) as total_amount
--   FROM user_events
--   WHERE event_date >= '2024-01-01'
--   GROUP BY user_id, event_date, 
--     CASE 
--       WHEN amount < 10 THEN 'low'
--       WHEN amount < 50 THEN 'medium'
--       WHEN amount < 100 THEN 'high'
--       ELSE 'premium'
--     END
-- " --target-table user_spending_tiers --partitioned-by event_date spending_tier --table-location "/user/sandbox/user_spending_tiers" 