SELECT
    CASE WHEN duration_sec >= 441 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    event_type,
    COUNT(*) AS row_count,
    SUM(duration_sec) AS total_duration_sec,
    SIZE(COLLECT_SET(event_type)) AS num_event_types
FROM web_events
GROUP BY CASE WHEN duration_sec >= 441 THEN 'PREMIUM' ELSE 'STANDARD' END, event_type;
