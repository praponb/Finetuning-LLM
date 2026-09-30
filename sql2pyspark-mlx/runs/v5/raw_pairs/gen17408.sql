SELECT
    QUARTER(launch_date) launch_quarter,
    CASE WHEN price >= 1080 THEN 'high' WHEN price >= 300 THEN 'mid' ELSE 'low' END price_bucket,
    category,
    COUNT(DISTINCT cost) unique_brands,
    ROUND((100 * SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_acme,
    SUM(price) total_price
FROM products
GROUP BY QUARTER(launch_date), CASE WHEN price >= 1080 THEN 'high' WHEN price >= 300 THEN 'mid' ELSE 'low' END, category
