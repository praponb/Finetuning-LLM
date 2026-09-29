SELECT shipping_fee AS value
FROM orders
UNION
SELECT cost AS value
FROM products
WHERE launch_date <= '2024-10-01';
