WITH gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.event_time,
    s.duration_sec,
    s.is_new_burst,
    s.session_id_2,
    (SELECT MAX(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS max_web_event_duration_sec
FROM sessionized s
ORDER BY page NULLS FIRST;
