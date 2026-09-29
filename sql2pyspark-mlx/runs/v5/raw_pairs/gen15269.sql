SELECT status
FROM subscriptions
WHERE start_date BETWEEN '2025-04-23' AND '2025-06-01'
UNION
SELECT status
FROM subscriptions
WHERE status IN ('CANCELLED', 'ACTIVE', 'EXPIRED');
