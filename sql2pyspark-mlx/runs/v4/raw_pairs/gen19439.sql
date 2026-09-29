SELECT
    id,
    session_id,
    duration_sec,
    SUM(duration_sec) OVER (PARTITION BY session_id) AS session_id_sum_duration_sec
FROM web_events;
