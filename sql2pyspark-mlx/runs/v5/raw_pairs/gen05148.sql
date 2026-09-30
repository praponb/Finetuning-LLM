SELECT category, COUNT(price) AS price_count
FROM products
WHERE launch_date > '2024-10-01' AND product_name LIKE '%Watc%'
GROUP BY category;
