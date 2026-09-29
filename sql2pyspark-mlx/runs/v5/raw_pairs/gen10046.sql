SELECT
    QUARTER(event_time) AS event_quarter,
    page,
    COUNT(*) AS cnt,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    COUNT(DISTINCT duration_sec) AS unique_event_types
FROM web_events
WHERE session_id IN (4, 36)
GROUP BY QUARTER(event_time), page;
