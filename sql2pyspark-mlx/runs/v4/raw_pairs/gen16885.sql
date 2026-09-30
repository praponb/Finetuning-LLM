SELECT
    customer_id,
    CASE WHEN monthly_fee >= 224 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_tier,
    DATE_TRUNC('WEEK', end_date) AS week_start,
    SUM(monthly_fee) FILTER (WHERE plan = 'enterprise') AS enterprise_total_monthly_fee,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) AS monthly_fee_per_premium
FROM subscriptions
GROUP BY customer_id, CASE WHEN monthly_fee >= 224 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', end_date)
ORDER BY monthly_fee_tier
