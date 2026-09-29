SELECT
    region,
    city,
    COUNT(DISTINCT sqft) AS unique_citys,
    COUNT(*) AS row_count,
    MIN(opened_date) AS min_opened_date
FROM stores
WHERE region IN ('Central', 'East')
GROUP BY region, city
ORDER BY region, min_opened_date, city
LIMIT 50;
