SELECT
    id,
    session_id,
    event_type,
    page,
    LEAD(duration_sec, 1) OVER (PARTITION BY session_id ORDER BY event_time, id) AS next_duration_sec,
    SUM(duration_sec) OVER w_ordered AS running_duration_sec
FROM web_events
WINDOW w_ordered AS (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
