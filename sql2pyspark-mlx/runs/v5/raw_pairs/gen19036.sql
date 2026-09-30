SELECT status
FROM orders
INTERSECT
SELECT status
FROM subscriptions
ORDER BY status DESC;
