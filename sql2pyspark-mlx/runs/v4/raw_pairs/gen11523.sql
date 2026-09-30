SELECT
    *,
    COUNT(duration_sec) OVER (PARTITION BY event_type, page) event_type_count_duration_sec,
    SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_duration_sec
FROM web_events
