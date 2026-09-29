SELECT
    page,
    MONTH(event_time) event_month,
    MAX(event_time) latest_event_time,
    COUNT(*) num_web_events,
    SUM(duration_sec) total_duration_sec
FROM web_events
WHERE TO_DATE(event_time) >= '2023-06-01'
GROUP BY 1, 2
ORDER BY event_month DESC
