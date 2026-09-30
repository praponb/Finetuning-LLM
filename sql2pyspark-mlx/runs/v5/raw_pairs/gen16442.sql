WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(event_type, 1, 3) AS event_type_prefix
    FROM web_events
    WHERE event_time > TO_TIMESTAMP('2023-05-06 16:00:00')
      AND page IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    session_id,
    event_type,
    AVG(duration_sec) FILTER (WHERE event_type = 'purchase') AS purchase_avg_duration_sec,
    COUNT(*) FILTER (WHERE page = '/home') AS home_rows,
    PERCENTILE_APPROX(session_id_2, ARRAY(0.25, 0.5, 0.75)) AS session_id_2_quartiles,
    GROUPING_ID() AS grouping_level
FROM sessionized
GROUP BY GROUPING SETS ((session_id, event_type), (session_id))
ORDER BY session_id DESC NULLS FIRST;
