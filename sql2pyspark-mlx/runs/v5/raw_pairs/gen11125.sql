SELECT
    city,
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    COUNT(DISTINCT sqft) AS unique_citys,
    COUNT(sqft) AS sqft_count,
    ROUND((100 * SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_central
FROM stores
WHERE store_name LIKE '%Cent%'
GROUP BY ROLLUP(city, DATE_TRUNC('MONTH', opened_date));
