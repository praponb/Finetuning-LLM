WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN session_id_2 >= 9 THEN 'LARGE' ELSE 'SMALL' END AS session_id_2_tier,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    COUNT(DISTINCT id) AS distinct_ids,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min
FROM sessionized
GROUP BY 1
ORDER BY distinct_ids DESC;
