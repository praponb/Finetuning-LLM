SELECT id, customer_id
FROM orders
WHERE shipping_fee >= 26
UNION ALL
SELECT id, customer_id
FROM orders
WHERE order_date >= '2024-06-01'
