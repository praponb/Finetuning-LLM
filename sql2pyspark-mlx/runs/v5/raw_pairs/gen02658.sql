SELECT * FROM (SELECT city, region, ROUND((100 * SUM(CASE WHEN region = 'East' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_east FROM stores GROUP BY city, region) t WHERE pct_east >= 20;
