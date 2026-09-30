SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    session_id,
    COUNT_IF(event_type = 'search') AS search_events,
    MAX(event_time) AS last_event_time,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    MAX(event_time) AS last_event_time_2
FROM web_events
GROUP BY 1, 2;
