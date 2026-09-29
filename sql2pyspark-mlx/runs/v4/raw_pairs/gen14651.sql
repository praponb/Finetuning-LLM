SELECT start_date, plan
FROM subscriptions
WHERE start_date BETWEEN '2024-05-19' AND '2025-06-01'
UNION ALL
SELECT start_date, plan
FROM subscriptions
WHERE status IN ('ACTIVE', 'PAUSED', 'EXPIRED')
ORDER BY plan
