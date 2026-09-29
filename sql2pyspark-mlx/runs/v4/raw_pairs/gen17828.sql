SELECT
    CASE WHEN monthly_fee >= 208 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_tier,
    DATE_TRUNC('MONTH', end_date) AS activity_month,
    COUNT(*) FILTER (WHERE status = 'PAUSED') AS paused_rows,
    COUNT(*) AS num_rows,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    GROUPING_ID() AS gid
FROM subscriptions
GROUP BY CUBE (CASE WHEN monthly_fee >= 208 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', end_date))
ORDER BY activity_month NULLS LAST;
