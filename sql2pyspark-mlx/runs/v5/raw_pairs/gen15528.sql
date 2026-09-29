SELECT status, COUNT(DISTINCT monthly_fee) AS unique_statuss, COUNT(*) AS total_count
FROM subscriptions
WHERE plan NOT IN ('enterprise', 'basic', 'premium') AND monthly_fee >= 120
GROUP BY status
