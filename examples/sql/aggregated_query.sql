SELECT 
    date_column,
    region,
    SUM(amount) as total_amount,
    COUNT(*) as record_count
FROM large_table 
WHERE date_column >= '2024-01-01'
GROUP BY date_column, region
ORDER BY date_column, region 