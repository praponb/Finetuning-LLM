SELECT
    id,
    page,
    duration_sec,
    event_type,
    LAST_VALUE(event_type) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_event_type
FROM web_events
WHERE session_id IN (14, 17, 24);
