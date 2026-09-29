WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM web_events
)
-- Running sum of the flag = session number
SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    duration_sec,
    SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
FROM flagged
