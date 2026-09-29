SELECT
    CASE WHEN cost >= 650 THEN 'high' WHEN cost >= 300 THEN 'mid' ELSE 'low' END AS cost_bucket,
    COUNT(price) AS price_count,
    SUM(CASE WHEN category = 'Electronics' THEN price ELSE 0 END) AS electronics_price,
    MAX(launch_date) AS last_launch_date
FROM products
GROUP BY CASE WHEN cost >= 650 THEN 'high' WHEN cost >= 300 THEN 'mid' ELSE 'low' END
