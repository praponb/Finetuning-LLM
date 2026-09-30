SELECT
    CASE WHEN price >= 1350 THEN 'high' WHEN price >= 580 THEN 'mid' ELSE 'low' END AS price_bucket,
    brand,
    category,
    AVG(stock_qty) AS avg_stock_qty
FROM products
WHERE brand IN ('Globex', 'Stark', 'Acme') AND brand = 'Globex'
GROUP BY price_bucket, brand, category
ORDER BY price_bucket;
