SELECT id, customer_id
FROM subscriptions
INTERSECT ALL
SELECT id, customer_id
FROM subscriptions
WHERE start_date BETWEEN '2024-01-11' AND '2025-06-04';
