WITH gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM web_events
),
sessions_web_events AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
)
SELECT
    id,
    session_id,
    event_type,
    duration_sec,
    new_group_flag,
    session_id_2
FROM sessions_web_events
ORDER BY duration_sec DESC NULLS FIRST, session_id;
