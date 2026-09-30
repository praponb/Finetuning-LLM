SELECT launch_date
FROM products
INTERSECT
SELECT order_date
FROM orders
ORDER BY launch_date;
