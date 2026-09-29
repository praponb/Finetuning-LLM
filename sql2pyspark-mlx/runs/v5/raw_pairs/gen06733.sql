SELECT
    id,
    duration_sec,
    event_time,
    LAST_VALUE(event_time) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_event_time
FROM web_events;
