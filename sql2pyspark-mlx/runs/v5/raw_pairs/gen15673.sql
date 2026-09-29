SELECT brand, MAX(cost) - MIN(cost) AS cost_range, COUNT(price) AS price_count
FROM products
GROUP BY brand
ORDER BY brand, cost_range DESC
