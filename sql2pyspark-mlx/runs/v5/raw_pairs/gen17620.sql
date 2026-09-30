SELECT status
FROM orders
WHERE store_id = 24
INTERSECT
SELECT status
FROM accounts
ORDER BY status DESC
