SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    CASE WHEN sqft >= 13340 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    city,
    MAX(sqft) FILTER (WHERE region = 'South') AS south_max_sqft,
    SUM(CASE WHEN region = 'South' THEN sqft ELSE 0 END) AS south_sqft,
    COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count,
    APPROX_PERCENTILE(sqft, 0.95) AS sqft_p95,
    MAX(sqft) AS max_sqft,
    SUM(CASE WHEN region = 'South' THEN sqft ELSE 0 END) AS south_sqft_2
FROM stores
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', opened_date), CASE WHEN sqft >= 13340 THEN 'HIGH' ELSE 'LOW' END, city), (DATE_TRUNC('WEEK', opened_date), CASE WHEN sqft >= 13340 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('WEEK', opened_date)), ())
ORDER BY south_max_sqft DESC;
