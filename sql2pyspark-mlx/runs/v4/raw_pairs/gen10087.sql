SELECT launch_date, 'product' AS source
FROM products
WHERE launch_date BETWEEN '2023-04-01' AND '2024-06-01'
UNION ALL
SELECT order_date, 'order' AS source
FROM orders
WHERE discount <= 0.11
ORDER BY source
