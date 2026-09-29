SELECT
    status,
    MIN(monthly_fee) AS min_monthly_fee,
    COUNT(DISTINCT monthly_fee) AS unique_customers,
    ROUND((100 * SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_standard
FROM subscriptions
WHERE plan = 'standard'
GROUP BY status
