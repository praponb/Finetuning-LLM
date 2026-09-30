SELECT
    CASE WHEN monthly_fee >= 227 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_tier,
    SUM(monthly_fee) FILTER (WHERE status = 'EXPIRED') AS expired_total_monthly_fee,
    COUNT_IF(plan = 'enterprise') AS enterprise_events,
    APPROX_PERCENTILE(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles
FROM subscriptions
GROUP BY monthly_fee_tier
HAVING expired_total_monthly_fee > 0
