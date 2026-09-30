SELECT
    customer_id,
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    COUNT(CASE WHEN browser = 'Chrome' THEN 1 END) AS chrome_count,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(browser = 'Edge') AS edge_events
FROM sessions
GROUP BY customer_id, activity_month
ORDER BY edge_events ASC NULLS FIRST;
