SELECT
    plan,
    status,
    MAX(monthly_fee) AS max_monthly_fee,
    COUNT(*) AS subscription_count,
    MIN(end_date) AS min_end_date
FROM subscriptions
GROUP BY plan, status
ORDER BY status;
