WITH with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
flagged AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
)
SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
FROM flagged;
