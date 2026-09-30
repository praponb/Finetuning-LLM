SELECT id, status
FROM orders
INTERSECT ALL
SELECT id, status
FROM orders
WHERE amount > 480;
