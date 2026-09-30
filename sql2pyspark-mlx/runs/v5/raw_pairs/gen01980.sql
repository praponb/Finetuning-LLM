SELECT status
FROM orders
WHERE channel = 'partner'
UNION
SELECT status
FROM subscriptions
WHERE start_date BETWEEN '2024-05-01' AND '2025-09-01'
ORDER BY status;
