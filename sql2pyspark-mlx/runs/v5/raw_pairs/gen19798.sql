WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
)
-- Running sum of the flag = session number
SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM gaps;
