SELECT
    id,
    session_id,
    page,
    event_time,
    duration_sec,
    SUM(duration_sec) OVER (PARTITION BY session_id, page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
    MAX(duration_sec) OVER (PARTITION BY session_id) AS session_id_max_duration_sec
FROM web_events
WHERE event_type IN ('purchase', 'page_view');
