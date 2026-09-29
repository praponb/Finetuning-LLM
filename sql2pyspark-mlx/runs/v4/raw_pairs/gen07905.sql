SELECT order_date
FROM orders
INTERSECT
SELECT launch_date
FROM products
WHERE ARRAY_CONTAINS(tags, 'eco')
ORDER BY order_date;
