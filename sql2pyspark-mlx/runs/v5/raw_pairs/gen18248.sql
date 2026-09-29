SELECT
    DATE_TRUNC('MONTH', end_date) AS activity_month,
    DATE_TRUNC('YEAR', start_date) AS year_start,
    AVG(monthly_fee) FILTER (WHERE status = 'PAUSED') AS paused_avg_monthly_fee,
    COUNT(CASE WHEN status = 'PAUSED' THEN 1 END) AS paused_count,
    COUNT(*) FILTER (WHERE plan = 'premium') AS premium_rows
FROM subscriptions
GROUP BY ROLLUP (DATE_TRUNC('MONTH', end_date), DATE_TRUNC('YEAR', start_date))
ORDER BY premium_rows DESC;
