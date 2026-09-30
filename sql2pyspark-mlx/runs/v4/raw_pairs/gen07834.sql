SELECT status
FROM accounts
WHERE customer_id IN (5, 12, 20)
UNION ALL
SELECT status
FROM subscriptions
WHERE start_date > '2025-08-26';
