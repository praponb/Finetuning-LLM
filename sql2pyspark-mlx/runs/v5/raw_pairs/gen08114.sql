SELECT status, 'order' AS kind
FROM orders
UNION ALL
SELECT status, 'subscription' AS kind
FROM subscriptions
WHERE start_date BETWEEN '2024-07-01' AND '2024-11-01'
ORDER BY status DESC;
