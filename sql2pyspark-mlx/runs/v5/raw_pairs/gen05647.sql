SELECT
    CASE WHEN monthly_fee >= 231 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    COUNT(*) FILTER (WHERE status = 'CANCELLED') AS cancelled_rows,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS monthly_fee_per_enterprise,
    COUNT(*) AS event_count,
    SUM(CASE WHEN status = 'ACTIVE' THEN monthly_fee ELSE 0 END) AS active_monthly_fee,
    SUM(CASE WHEN plan = 'standard' THEN monthly_fee ELSE 0 END) AS standard_monthly_fee,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
GROUP BY CUBE (CASE WHEN monthly_fee >= 231 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY active_monthly_fee DESC NULLS LAST
