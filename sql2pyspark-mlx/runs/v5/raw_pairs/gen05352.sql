SELECT
    status,
    MAX(monthly_fee) AS max_monthly_fee,
    SUM(CASE WHEN plan = 'premium' THEN monthly_fee ELSE 0 END) AS premium_monthly_fee,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
GROUP BY status
ORDER BY avg_monthly_fee DESC, status ASC
LIMIT 5;
