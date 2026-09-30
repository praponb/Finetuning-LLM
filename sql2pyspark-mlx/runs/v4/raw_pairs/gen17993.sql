SELECT
    region,
    city,
    COUNT(*) AS num_rows,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN region = 'West' THEN 1 END), 0), 2) AS sqft_per_west,
    COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS sqft_per_singapore,
    SUM(CASE WHEN region = 'East' THEN sqft ELSE 0 END) AS east_sqft,
    SIZE(COLLECT_SET(region)) AS num_regions
FROM stores
GROUP BY region, city
HAVING ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN region = 'West' THEN 1 END), 0), 2) >= 0
ORDER BY region DESC;
