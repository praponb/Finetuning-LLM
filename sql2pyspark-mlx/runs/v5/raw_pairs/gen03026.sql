SELECT
    DATE_TRUNC('MONTH', start_date) AS activity_month,
    SIZE(COLLECT_SET(status)) AS num_status,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT(*) AS num_rows,
    COUNT_IF(status = 'CANCELLED') AS cancelled_events,
    COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count,
    COUNT(DISTINCT status) AS distinct_status
FROM subscriptions
GROUP BY DATE_TRUNC('MONTH', start_date)
