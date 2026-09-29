SELECT
    CASE WHEN price >= 1280 THEN 'high' WHEN price >= 310 THEN 'mid' ELSE 'low' END AS price_bucket,
    category,
    ROUND(AVG(cost), 2) AS avg_cost,
    COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count
FROM products
GROUP BY 1, 2;
