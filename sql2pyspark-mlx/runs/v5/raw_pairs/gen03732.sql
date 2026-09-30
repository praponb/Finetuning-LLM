SELECT ROUND(AVG(price), 2) AS avg_price, SUM(price) AS total_price
FROM products
WHERE supplier_id IN (8, 21, 26, 36) AND cost > 640
ORDER BY avg_price;
