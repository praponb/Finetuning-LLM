SELECT
    DATE_TRUNC('WEEK', end_date) AS week_start,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT(*) AS event_count,
    COUNT_IF(plan = 'enterprise') AS enterprise_events
FROM subscriptions
GROUP BY DATE_TRUNC('WEEK', end_date);
