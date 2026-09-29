SELECT 'store' AS record_type, opened_date
FROM stores
UNION
SELECT 'subscription' AS record_type, end_date
FROM subscriptions
WHERE start_date BETWEEN '2025-04-15' AND '2025-06-22'
ORDER BY record_type;
