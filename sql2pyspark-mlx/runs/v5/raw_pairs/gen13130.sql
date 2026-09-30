SELECT city, region, pct_east
FROM (
    SELECT city, region, ROUND((100 * SUM(CASE WHEN region = 'East' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_east
    FROM stores
    WHERE sqft BETWEEN 8375 AND 13304
    GROUP BY city, region
) AS s
WHERE pct_east >= 50;
