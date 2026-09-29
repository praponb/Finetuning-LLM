SELECT price AS value
FROM products
UNION ALL
SELECT cost AS value
FROM shipments
WHERE order_id IS NULL;
