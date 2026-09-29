SELECT
    id,
    session_id,
    event_time,
    event_type,
    FIRST_VALUE(event_type) OVER (PARTITION BY session_id ORDER BY event_time DESC, id) AS first_event_type,
    PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_time) AS event_time_pct_rank
FROM web_events
ORDER BY session_id DESC, first_event_type
