SELECT status
FROM orders
WHERE customer_id IN (8, 15)
INTERSECT
SELECT status
FROM subscriptions
WHERE start_date < '2023-11-01';
