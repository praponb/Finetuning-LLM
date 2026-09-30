SELECT
    status,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(*) AS cnt,
    SUM(SUM(monthly_fee)) OVER (ORDER BY status) AS cumulative_monthly_fee
FROM subscriptions
WHERE plan = 'premium'
GROUP BY status
ORDER BY total_monthly_fee DESC
