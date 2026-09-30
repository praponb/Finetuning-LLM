WITH prev_events AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.event_time,
    s.prev_event_time,
    s.session_start_flag,
    CASE WHEN event_type RLIKE '^(purchase|search)$' THEN 'EXTENDED' WHEN event_type RLIKE '^(page_view|click)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS event_type_zone,
    (SELECT AVG(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM islands s
ORDER BY session_id, event_time;
