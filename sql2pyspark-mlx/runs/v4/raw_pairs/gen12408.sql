SELECT
    city,
    ROUND((100 * SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_south,
    ROUND((100 * SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_west,
    SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END) AS south_count
FROM stores
WHERE city <> 'Berlin' AND city NOT IN ('New York', 'Berlin', 'Bangkok') AND region = 'East'
GROUP BY city
ORDER BY city
LIMIT 50;
