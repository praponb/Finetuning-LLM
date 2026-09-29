SELECT brand, CASE WHEN price >= 1330 THEN 'high' WHEN price >= 440 THEN 'mid' ELSE 'low' END AS price_bucket, MAX(cost) - MIN(cost) AS cost_range
FROM products
WHERE category IN ('Grocery', 'Books', 'Home')
GROUP BY 1, 2
HAVING cost_range > 46;
