SELECT launch_date
FROM products
INTERSECT
SELECT delivered_date
FROM shipments
WHERE order_id IS NULL
ORDER BY launch_date
