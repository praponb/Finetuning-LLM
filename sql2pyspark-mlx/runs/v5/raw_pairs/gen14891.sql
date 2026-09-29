WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(duration_sec * 100, 2) AS duration_sec_scaled
    FROM web_events
    WHERE event_time >= DATE '2023-10-20'
      AND page IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    MAX(event_time) AS last_event_time,
    BIT_AND(duration_sec) AS duration_sec_bit_and
FROM islands
GROUP BY CUBE (DATE_TRUNC('WEEK', event_time))
ORDER BY duration_sec_bit_and NULLS FIRST
