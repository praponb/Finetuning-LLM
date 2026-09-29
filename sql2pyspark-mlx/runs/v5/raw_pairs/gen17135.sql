WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS session_id_tier,
    AVG(new_group_flag) FILTER (WHERE country = 'SG') AS sg_avg_new_group_flag,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes
FROM sessions_sessions
GROUP BY CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END
HAVING (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 >= 0
ORDER BY session_id_tier;
