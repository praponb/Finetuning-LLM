SELECT
    region,
    CASE WHEN sqft >= 16507 THEN 'high' WHEN sqft >= 8000 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    COUNT(sqft) AS sqft_count,
    COUNT(*) AS store_count
FROM stores
GROUP BY region, CASE WHEN sqft >= 16507 THEN 'high' WHEN sqft >= 8000 THEN 'mid' ELSE 'low' END
HAVING SUM(sqft) >= 58000
